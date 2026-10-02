#include <Storages/MergeTree/WhatIfProjectionEstimator.h>

#include <Access/Common/AccessFlags.h>
#include <Access/ContextAccess.h>
#include <Columns/ColumnSparse.h>
#include <Common/Stopwatch.h>
#include <Common/quoteString.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/InterpreterHypotheticalObjectQuery.h>
#include <Interpreters/sortBlock.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/IProcessor.h>
#include <Processors/QueryPlan/Optimizations/projectionsCommon.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/SizeLimits.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/HypotheticalProjections.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPartBuilder.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityAdaptive.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/PartDirIntent.h>
#include <Storages/MergeTree/WhatIfSettings.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ProjectionsDescription.h>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 max_rows_to_read;
    extern const SettingsUInt64 max_bytes_to_read;
    extern const SettingsOverflowMode read_overflow_mode;
    extern const SettingsBool optimize_use_projections;
    extern const SettingsBool force_optimize_projection;
    extern const SettingsBool prefer_optimize_projection;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 index_granularity;
    extern const MergeTreeSettingsUInt64 index_granularity_bytes;
    extern const MergeTreeSettingsBool use_const_adaptive_granularity;
    extern const MergeTreeSettingsNonZeroUInt64 merge_max_block_size;
    extern const MergeTreeSettingsUInt64 merge_max_block_size_bytes;
}

namespace
{

struct ProjectionPartData
{
    /// the projection key, one row per projection row, in read order
    Block key_block;
    /// sorted order over key_block
    IColumn::Permutation order;
    size_t rows = 0;
    /// uncompressed size of the projection part, drives the granularity
    size_t bytes = 0;
    /// per-row size in read order, empty unless the part needs the adaptive granule walk
    PaddedPODArray<UInt32> row_bytes;
};

/// per-row counterpart of `getBlockSizeForGranularity`, a fixed-width column costs the same in every row
void appendRowSizes(PaddedPODArray<UInt32> & row_bytes, const Block & block)
{
    const size_t rows = block.rows();
    const size_t offset = row_bytes.size();
    row_bytes.resize_fill(offset + rows, 0);

    UInt32 fixed = 0;
    for (const auto & elem : block)
    {
        if (!elem.column)
            continue;
        if (elem.column->valuesHaveFixedSize())
            fixed += static_cast<UInt32>(elem.column->sizeOfValueIfFixed());
        else
            for (size_t i = 0; i < rows; ++i)
                row_bytes[offset + i] += static_cast<UInt32>(elem.column->byteSizeAt(i));
    }
    for (size_t i = 0; i < rows; ++i)
        row_bytes[offset + i] += fixed;
}

/// why the ORDER BY tie-break is or is not available
/// reads the whole part, wired like the empirical index scan
Pipe makeWholePartPipe(const DataPartPtr & part, const Names & columns_to_read, ReadFromMergeTree * read_step, const ContextPtr & context)
{
    const auto & data = read_step->getMergeTreeData();
    const auto & mutations_snapshot = read_step->getMutationsSnapshot();

    /// apply patch parts / on-the-fly mutations so the projection sees the up-to-date values
    auto alter_conversions = mutations_snapshot
        ? MergeTreeData::getAlterConversionsForPart(part, mutations_snapshot, context
#if CLICKHOUSE_CLOUD
            , context->getAccess()->getEnabledMaskingPolicies()
#endif
            )
        : std::make_shared<AlterConversions>();

    Pipe pipe = createMergeTreeSequentialSource(
        MergeTreeSequentialSourceType::Merge,
        data,
        read_step->getStorageSnapshot(),
        RangesInDataPart(part),
        alter_conversions,
        nullptr,
        columns_to_read,
        MarkRanges{{0, part->index_granularity->getMarksCountWithoutFinal()}},
        std::make_shared<std::atomic<size_t>>(0),
        false,
        false,
        false);

    /// speed limits apply here too, size is checked by the caller
    if (auto query_limits = read_step->getQueryInfo().storage_limits)
    {
        auto speed_limits = std::make_shared<StorageLimitsList>(*query_limits);
        for (auto & entry : *speed_limits)
        {
            entry.local_limits.size_limits = {};
            entry.leaf_limits = {};
            entry.local_limits.speed_limits.max_execution_time = {};
        }
        for (const auto & processor : pipe.getProcessors())
            processor->setStorageLimits(speed_limits);
    }

    return pipe;
}

/// keep only the key columns, so peak memory is one key copy plus the permutation, false when a read limit was hit
bool buildProjectionPart(
    ProjectionPartData & out,
    const ProjectionDescription & projection,
    const DataPartPtr & part,
    ReadFromMergeTree * read_step,
    const SizeLimits & read_limits,
    bool need_row_bytes,
    UInt64 & total_rows_read,
    UInt64 & total_bytes_read,
    const ContextPtr & context)
{
    const auto & proj_key = projection.metadata->getSortingKey();

    Pipe pipe = makeWholePartPipe(part, projection.required_columns, read_step, context);
    QueryPipeline pipeline(std::move(pipe));
    pipeline.setProcessListElement(context->getProcessListElement());
    pipeline.setProgressCallback(context->getProgressCallback());
    pipeline.setQuota(context->getQuota());
    /// account the scan to the query's own quota bucket
    pipeline.setNormalizedQueryHash(context->getNormalizedQueryHash());
    PullingPipelineExecutor executor(pipeline);

    MutableColumns key_columns;
    Block block;
    while (executor.pull(block))
    {
        if (!block.rows())
            continue;

        total_rows_read += block.rows();
        total_bytes_read += block.bytes();
        /// softCheck, so `read_overflow_mode = 'throw'` degrades to `unsupported` like `break` does:
        /// this scan reads whole parts, and a query the user can still run must stay explainable
        if (!read_limits.softCheck(total_rows_read, total_bytes_read))
            return false;

        /// the key expression and the sort need full columns
        for (auto & column : block)
            column.column = recursiveRemoveSparse(column.column);

        /// the same measure the writer takes of the block it is about to store, before the key
        /// expression adds columns a normal projection recomputes on read instead of storing
        out.bytes += getBlockSizeForGranularity(block);
        if (need_row_bytes)
            appendRowSizes(out.row_bytes, block);
        /// `required_columns` drops a subcolumn whose physical column is there, and the writer puts it
        /// back before executing the key expression (`addSubcolumnsFromSortingKeyAndSkipIndicesExpression`)
        for (const auto & required : proj_key.expression->getRequiredColumns())
            if (!block.has(required))
                block.insert(block.getSubcolumnByName(required));
        proj_key.expression->execute(block);

        if (key_columns.empty())
            for (const auto & name : proj_key.column_names)
                key_columns.push_back(block.getByName(name).column->convertToFullColumnIfConst()->cloneEmpty());

        for (size_t i = 0; i < key_columns.size(); ++i)
        {
            auto source = block.getByName(proj_key.column_names[i]).column->convertToFullColumnIfConst();
            key_columns[i]->insertRangeFrom(*source, 0, source->size());
        }
    }

    if (key_columns.empty() || key_columns[0]->empty())
        return true;

    out.rows = key_columns[0]->size();
    /// a projection index also stores the parent offset, which the writer sizes but `required_columns` omits
    if (projection.with_parent_part_offset)
    {
        out.bytes += out.rows * sizeof(UInt64);
        for (auto & row_size : out.row_bytes)
            row_size += sizeof(UInt64);
    }
    for (size_t i = 0; i < key_columns.size(); ++i)
        out.key_block.insert({std::move(key_columns[i]), proj_key.data_types[i], proj_key.column_names[i]});

    return true;
}

/// replays the writer over a sequence of blocks, with one granule size for each block and the mark rules of the writer
std::vector<size_t> simulateWriterMarks(
    const ProjectionPartData & data,
    MergeTreeDataPartType part_type,
    const MergeTreeSettings & mt_settings,
    bool adaptive_marks,
    size_t block_rows_limit,
    size_t block_bytes_limit)
{
    const size_t granularity_bytes = mt_settings[MergeTreeSetting::index_granularity_bytes];
    const size_t fixed_granularity_rows = mt_settings[MergeTreeSetting::index_granularity];
    const bool per_row_bytes = data.row_bytes.size() == data.rows;
    const size_t average_row_bytes = data.rows != 0 ? std::max<size_t>(data.bytes / data.rows, 1) : 1;
    auto row_bytes = [&](size_t row) -> size_t { return per_row_bytes ? data.row_bytes[data.order[row]] : average_row_bytes; };

    MergeTreeIndexGranularityAdaptive granularity;
    size_t written = 0;
    for (size_t row = 0; row < data.rows;)
    {
        size_t block_rows = 0;
        size_t block_bytes = 0;
        while (row + block_rows < data.rows && block_rows < block_rows_limit)
        {
            const size_t bytes = row_bytes(row + block_rows);
            if (block_bytes_limit != 0 && block_rows != 0 && block_bytes + bytes > block_bytes_limit)
                break;
            block_bytes += bytes;
            ++block_rows;
        }
        row += block_rows;

        const size_t granule_rows = computeIndexGranularity(
            block_rows, block_bytes, granularity_bytes, fixed_granularity_rows, /* blocks_are_granules */ false, adaptive_marks);

        /// the rows that still go into the mark that the previous block left open
        size_t open_rows_missing = granularity.getTotalRows() - written;
        /// first, the wide writer shrinks an open mark that is wider than the granule of this block
        if (part_type == MergeTreeDataPartType::Wide && open_rows_missing > granule_rows)
        {
            granularity.adjustLastMark(std::max(granularity.getLastMarkRows() - open_rows_missing, granule_rows));
            open_rows_missing = granularity.getTotalRows() - written;
        }

        if (part_type == MergeTreeDataPartType::Compact)
            fillIndexGranularityForCompactPart(granularity, open_rows_missing, granule_rows, block_rows);
        else
            fillIndexGranularityForWidePart(granularity, open_rows_missing, granule_rows, block_rows);
        written += block_rows;
    }

    /// when the writer closes the part, it trims the last mark to the rows that it got
    if (granularity.getTotalRows() > written)
        granularity.adjustLastMark(granularity.getLastMarkRows() - (granularity.getTotalRows() - written));

    std::vector<size_t> mark_rows(granularity.getMarksCount());
    for (size_t mark = 0; mark < mark_rows.size(); ++mark)
        mark_rows[mark] = granularity.getMarkRows(mark);
    return mark_rows;
}

/// builds the projection part in memory for each layout that the writer can leave, with its primary index
/// with rows of one width, the merge block size sets the granule size
/// with rows of different widths, the merge cuts its blocks at each source, so `uneven_rows` selects the likeliest layout
/// returns one part for each layout, with the index of the likeliest layout
std::pair<std::vector<MergeTreeDataPartPtr>, size_t> buildSyntheticProjectionParts(
    ProjectionPartData & data,
    const ProjectionDescription & projection,
    const MergeTreeData & merge_tree,
    const DataPartPtr & parent_part,
    const MergeTreeSettings & mt_settings,
    bool uneven_rows)
{
    const auto & proj_key = projection.metadata->getSortingKey();

    SortDescription sort_description;
    sort_description.reserve(proj_key.column_names.size());
    for (const auto & name : proj_key.column_names)
        sort_description.emplace_back(name, 1, 1);

    /// sorted order via one permutation
    stableGetPermutation(data.key_block, sort_description, data.order);
    const auto part_type = merge_tree.choosePartFormat(data.bytes, data.rows, parent_part->info.level, &projection).part_type;
    const bool adaptive_marks = parent_part->index_granularity_info.mark_type.adaptive;
    /// a constant granularity object pins one granule size for the whole part, an adaptive one lets
    /// every block the writer stores size its own granules, so only then do the blocks matter
    const bool granularity_per_block = part_type == MergeTreeDataPartType::Compact
        || (adaptive_marks && !mt_settings[MergeTreeSetting::use_const_adaptive_granularity]);

    /// an insert or a materialization writes one squashed block, and a merge writes blocks of `merge_max_block_size`
    /// a merge also cuts a block at each source, and a part does not record which writer it had, so build all layouts
    const size_t merge_rows = mt_settings[MergeTreeSetting::merge_max_block_size];
    const size_t merge_bytes = mt_settings[MergeTreeSetting::merge_max_block_size_bytes];
    /// one granule worth of bytes is the shortest run whose width can still move the granule size
    const size_t granule_bytes = mt_settings[MergeTreeSetting::index_granularity_bytes];
    std::vector<std::pair<size_t, size_t>> chunkings;
    size_t primary = 0;
    chunkings.emplace_back(data.rows, 0);
    if (granularity_per_block)
    {
        chunkings.emplace_back(merge_rows, merge_bytes);
        chunkings.emplace_back(merge_rows, granule_bytes);
        /// a level-zero part was written in one go, a merged one block by block
        if (parent_part->info.level > 0)
            primary = uneven_rows ? chunkings.size() - 1 : chunkings.size() - 2;
    }

    std::vector<std::vector<size_t>> layouts;
    for (const auto & [rows_limit, bytes_limit] : chunkings)
        layouts.push_back(simulateWriterMarks(data, part_type, mt_settings, adaptive_marks, rows_limit, bytes_limit));

    auto build = [&](const std::vector<size_t> & rows_per_mark) -> MergeTreeDataPartPtr
    {
        const size_t num_marks = rows_per_mark.size();
        std::vector<size_t> partial_sums(num_marks);
        for (size_t mark = 0, row = 0; mark < num_marks; ++mark)
        {
            row += rows_per_mark[mark];
            partial_sums[mark] = row;
        }

        /// primary index = the key at the first row of every granule
        Columns index_columns;
        index_columns.reserve(data.key_block.columns());
        for (const auto & key_column : data.key_block)
        {
            auto index_column = key_column.column->cloneEmpty();
            for (size_t mark = 0; mark < num_marks; ++mark)
                index_column->insertFrom(*key_column.column, data.order[mark != 0 ? partial_sums[mark - 1] : 0]);
            index_columns.push_back(std::move(index_column));
        }

        /// `Synthetic` does not change the part directory, but `CreateFresh` can delete a leftover `.tmp_proj`
        auto part = const_cast<IMergeTreeDataPart &>(*parent_part)
                        .getProjectionPartBuilder(projection.name, &projection, PartDirIntent::Synthetic, /* is_temp_projection */ true)
                        .withPartType(MergeTreeDataPartType::Compact)
                        .withBytesAndRows(0, data.rows, 0)
                        .build();
        part->setColumns(
            projection.metadata->getColumns().getAllPhysical(),
            SerializationInfoByName(SerializationInfoSettings{}),
            projection.metadata->getMetadataVersion());
        part->index_granularity = std::make_shared<MergeTreeIndexGranularityAdaptive>(partial_sums);
        part->setIndex(index_columns);
        return part;
    };

    /// equal layouts use the same part
    std::vector<MergeTreeDataPartPtr> built(layouts.size());
    for (size_t i = 0; i < layouts.size(); ++i)
    {
        for (size_t j = 0; j < i && !built[i]; ++j)
            if (layouts[j] == layouts[i])
                built[i] = built[j];
        if (!built[i])
            built[i] = build(layouts[i]);
    }
    return {std::move(built), primary};
}

/// the reasons of the optimizer start in lower case, but the reasons of `EXPLAIN WHATIF` are sentences
String capitalized(String text)
{
    if (!text.empty())
        text[0] = static_cast<char>(std::toupper(static_cast<unsigned char>(text[0])));
    return text;
}

bool tryEstimateProjection(
    WhatIfCandidateResult & result,
    const ProjectionDescription & projection,
    std::string_view relaxing_setting,
    ReadFromMergeTree * read_step,
    const RangesInDataParts & baseline_parts,
    UInt64 baseline_marks,
    const WeighHypotheticalProjections & weigh,
    const ContextPtr & context)
{
    const auto & data = read_step->getMergeTreeData();
    const auto mt_settings_ptr = data.getSettings(&projection.settings_changes);
    const auto & mt_settings = *mt_settings_ptr;
    const auto & query_settings = context->getSettingsRef();

    /// enforce the read limits by hand
    const SizeLimits read_limits(
        query_settings[Setting::max_rows_to_read], query_settings[Setting::max_bytes_to_read], query_settings[Setting::read_overflow_mode]);
    UInt64 total_rows_read = 0;
    UInt64 total_bytes_read = 0;

    Stopwatch watch;

    /// the optimizer weighs the projection as a materialized projection, first with each part in its likeliest layout
    /// then it weighs each layout that the writer can leave, to find if the choice changes with the layout
    std::array<HypotheticalProjectionsPtr, 4> scenarios;
    for (auto & scenario : scenarios)
        scenario = std::make_shared<HypotheticalProjections>(projection.clone());
    bool layouts_differ = false;
    UInt64 scanned_parts = 0;
    UInt64 scanned_marks = 0;
    UInt64 uneven_width_parts = 0;

    for (const auto & part_with_ranges : baseline_parts)
    {
        const auto & part = part_with_ranges.data_part;
        const size_t part_marks = part->index_granularity->getMarksCountWithoutFinal();
        if (part_marks == 0)
            continue;

        /// the byte walk only differs from a fixed granule count when the writer would size by bytes
        const bool adaptive = part->index_granularity_info.mark_type.adaptive
            && mt_settings[MergeTreeSetting::index_granularity_bytes] != 0;

        ProjectionPartData part_data;
        if (!buildProjectionPart(
                part_data, projection, part, read_step, read_limits, adaptive, total_rows_read, total_bytes_read, context))
        {
            result.empirical_unsupported_reason
                = "The projection scan hit the read limit of the query (max_rows_to_read / max_bytes_to_read)";
            return false;
        }

        ++scanned_parts;
        scanned_marks += part_marks;
        /// the writer sizes a granule from the average width of the block it stores, so once the rows
        /// differ in width the layout follows the blocks it was fed, and that is not recorded anywhere
        bool uneven_rows = false;
        if (part_data.row_bytes.size() == part_data.rows && !part_data.row_bytes.empty())
        {
            const auto [narrowest, widest] = std::minmax_element(part_data.row_bytes.begin(), part_data.row_bytes.end());
            uneven_rows = *narrowest != *widest;
        }
        if (uneven_rows)
            ++uneven_width_parts;
        /// no key rows out of a part that has rows means the key needs something the scan cannot
        /// provide, `_part_offset` for one, so do not pass a zero-mark estimate off as measured
        if (part_data.rows == 0 && part->rows_count > 0)
        {
            result.empirical_unsupported_reason = "The projection key could not be built from the columns the projection stores";
            return false;
        }
        if (part_data.rows == 0)
            continue;

        const auto [parts, primary] = buildSyntheticProjectionParts(part_data, projection, data, part, mt_settings, uneven_rows);
        scenarios[0]->parts[part->name] = parts[primary];
        /// a part with one layout uses it in every scenario
        for (size_t layout = 0; layout + 1 < scenarios.size(); ++layout)
        {
            const auto & layout_part = parts[std::min(layout, parts.size() - 1)];
            scenarios[1 + layout]->parts[part->name] = layout_part;
            layouts_differ |= layout_part != parts[primary];
        }
    }

    const size_t weighed = layouts_differ ? scenarios.size() : 1;
    for (size_t i = 0; i < weighed; ++i)
        weigh(scenarios[i]);

    const auto & outcome = scenarios[0]->outcome;
    result.sampled_parts = scanned_parts;
    result.sampled_marks = scanned_marks;
    result.elapsed_us = watch.elapsedMicroseconds();
    if (!outcome.marks)
    {
        result.status = WhatIfCandidateResult::NotApplicable;
        result.not_applicable_reason
            = outcome.reason.empty() ? "The optimizer did not weigh the projection for this read" : capitalized(outcome.reason);
        return true;
    }

    const UInt64 projection_marks = *outcome.marks;
    UInt64 marks_low = projection_marks;
    UInt64 marks_high = projection_marks;
    size_t chosen_in = 0;
    for (size_t i = 0; i < weighed; ++i)
    {
        const auto & scenario_outcome = scenarios[i]->outcome;
        chosen_in += scenario_outcome.chosen;
        if (scenario_outcome.marks)
        {
            marks_low = std::min(marks_low, *scenario_outcome.marks);
            marks_high = std::max(marks_high, *scenario_outcome.marks);
        }
    }

    result.estimated_marks = projection_marks;
    result.estimated_rows = outcome.rows;
    result.estimated_marks_low = marks_low;
    result.estimated_marks_high = marks_high;
    auto marks_text = [](UInt64 marks) { return fmt::format("{} mark{}", marks, marks == 1 ? "" : "s"); };
    if (chosen_in == weighed && outcome.forced)
    {
        String cost;
        if (outcome.nothing_to_serve)
            cost = "the query has no filter or ORDER BY for the projection to help with";
        else if (projection_marks > baseline_marks)
            cost = fmt::format("the projection reads {} instead of {} from the base table", marks_text(projection_marks), baseline_marks);
        else if (projection_marks == baseline_marks && !outcome.serves_order)
            cost = fmt::format("the projection reads the same {} as the base table and serves no ORDER BY", marks_text(projection_marks));
        else
            cost = fmt::format("the projection reads {} against {} from the base table", marks_text(projection_marks), baseline_marks);
        result.verdict = "chosen (forced)";
        result.verdict_reason = fmt::format("`{} = 1` overrides the cost; {}", relaxing_setting, cost);
    }
    else if (uneven_width_parts != 0)
    {
        result.verdict = "too close to call";
        result.verdict_reason = fmt::format(
            "{} against {} from the base table, but the rows differ in width on {} of the {} parts read, so the granule "
            "layout depends on the blocks the writer was fed and the mark count is a model, not a measurement",
            marks_text(projection_marks),
            baseline_marks,
            uneven_width_parts,
            scanned_parts);
    }
    else if (chosen_in != 0 && chosen_in != weighed)
    {
        result.verdict = "too close to call";
        result.verdict_reason = fmt::format(
            "{} against {} from the base table, and the layout a merge would leave is not recorded in a part, so the "
            "comparison comes out both ways over `marks_span`",
            marks_text(projection_marks),
            baseline_marks);
    }
    else
    {
        result.verdict = chosen_in != 0 ? "chosen" : "not chosen";
        if (projection_marks != baseline_marks)
            result.verdict_reason
                = fmt::format("{} would be read instead of {} from the base table", marks_text(projection_marks), baseline_marks);
        else
            result.verdict_reason = fmt::format(
                "the same {} would be read, and the projection order {}",
                marks_text(projection_marks),
                outcome.serves_order ? "serves the ORDER BY" : "serves no ORDER BY");
        /// the projection is better than the base table, but another projection is better than it
        if (chosen_in == 0 && projection_marks < baseline_marks)
            result.verdict_reason = outcome.reason;
    }
    result.estimate_source = WhatIfCandidateResult::Empirical;
    result.empirical_status = WhatIfCandidateResult::Ok;
    return true;
}

}

std::optional<ProjectionDescription> refreshHypotheticalProjection(
    const ProjectionDescription & stored,
    const MergeTreeData & data,
    const StorageMetadataPtr & metadata,
    const ContextPtr & context,
    String & reason)
{
    if (!stored.required_columns.empty())
        context->checkAccess(AccessType::SELECT, data.getStorageID(), stored.required_columns);
    context->checkAccess(AccessType::ALTER_ADD_PROJECTION, data.getStorageID());

    std::optional<ProjectionDescription> fresh;
    try
    {
        checkHypotheticalProjectionIsAddable(data, metadata, stored.definition_ast, /* if_not_exists */ false, context);
        fresh = ProjectionDescription::getProjectionFromAST(
            stored.definition_ast, metadata->getColumns(), &metadata->partition_key, context, LoadingStrictnessLevel::CREATE);
    }
    catch (const Exception &)
    {
        reason = "Hypothetical projection can no longer be added to this table: " + getCurrentExceptionMessage(false);
        return std::nullopt;
    }

    /// an ALTER can re-point an ALIAS the definition selects, so the columns the scan will really read
    /// are not the ones stored at CREATE time, and a denial here must not read as drift
    if (!fresh->required_columns.empty())
        context->checkAccess(AccessType::SELECT, data.getStorageID(), fresh->required_columns);
    return fresh;
}

WhatIfCandidateResult evaluateProjection(
    const ProjectionDescription & stored_projection,
    ReadFromMergeTree * read_step,
    const ReadFromMergeTree::AnalysisResult & analysis,
    const RangesInDataParts & baseline_parts,
    const WhatIfSettings & settings,
    const WeighHypotheticalProjections & weigh,
    ContextPtr context)
{
    const auto & data = read_step->getMergeTreeData();

    WhatIfCandidateResult result;
    result.kind = WhatIfCandidateResult::Projection;
    result.name = stored_projection.name;
    result.type = stored_projection.type == ProjectionDescription::Type::Aggregate ? "aggregate projection" : "normal projection";
    result.status = WhatIfCandidateResult::NotApplicable;
    result.total_parts = data.getActivePartsCount();
    result.total_marks = data.getTotalMarksCount();

    /// answer this before the refresh below: the read step of a baseline served by a projection
    /// carries that projection's metadata, which a base-table definition would not validate against
    if (analysis.readFromProjection() && !baseline_parts.empty())
    {
        result.not_applicable_reason = "The query is already served from projection '" + baseline_parts.front().data_part->name
            + "', EXPLAIN WHATIF estimates candidates against the base table read only";
        return result;
    }

    auto metadata = read_step->getStorageMetadata();
    auto projection = refreshHypotheticalProjection(stored_projection, data, metadata, context, result.not_applicable_reason);
    if (!projection)
        return result;

    if (!context->getSettingsRef()[Setting::optimize_use_projections])
    {
        result.not_applicable_reason = "Projections are disabled by `optimize_use_projections = 0`";
        return result;
    }

    if (projection->type == ProjectionDescription::Type::Aggregate)
    {
        result.not_applicable_reason
            = "EXPLAIN WHATIF estimates normal (sorted) hypothetical projections only, aggregate projections are not estimated yet";
        return result;
    }

    if (projection->where_clause_ast)
    {
        result.not_applicable_reason = "EXPLAIN WHATIF does not estimate projections with a WHERE clause yet";
        return result;
    }

    if (!projection->metadata->getSecondaryIndices().empty())
    {
        result.not_applicable_reason = "EXPLAIN WHATIF does not estimate projections with their own skip indexes yet";
        return result;
    }

    /// with no parts the optimizer finds no projection parts to read, whatever the settings
    if (baseline_parts.empty())
    {
        result.not_applicable_reason = "The query reads no parts, so the optimizer would not consider a projection";
        return result;
    }

    if (!QueryPlanOptimizations::canUseProjectionForReadingStep(read_step))
    {
        result.not_applicable_reason = "The optimizer does not consider projections for this read (for example FINAL, SAMPLE, "
                                       "reading in order, pending mutations, or a parallel-replicas mode without projection support)";
        return result;
    }

    for (const auto & column_name : read_step->getAllColumnNames())
    {
        if (!projection->sample_block.findColumnOrSubcolumnByName(column_name) && !projection->metadata->virtuals.has(column_name))
        {
            result.not_applicable_reason = fmt::format(
                "Projection does not contain column {} required by the query, so it could only filter the base table's "
                "parts, which EXPLAIN WHATIF does not estimate yet",
                backQuoteIfNeed(column_name));
            return result;
        }
    }

    const auto & proj_key = projection->metadata->getSortingKey();
    if (proj_key.column_names.empty())
    {
        result.not_applicable_reason = "Projection has no sort key to prune on";
        return result;
    }

    /// both `TYPE commit_order` and its query form order by these, so name the surface instead of the type,
    /// and the scan reads what the projection stores, so a key over a virtual column has no source there
    for (const auto & required : proj_key.expression->getRequiredColumns())
    {
        if (required == BlockNumberColumn::name || required == BlockOffsetColumn::name)
        {
            result.not_applicable_reason = fmt::format(
                "Projection orders by the commit order ({}, {}), which EXPLAIN WHATIF does not estimate yet",
                backQuote(BlockNumberColumn::name),
                backQuote(BlockOffsetColumn::name));
            return result;
        }
        if (!metadata->getColumns().hasColumnOrSubcolumn(GetColumnsOptions::AllPhysical, required))
        {
            result.not_applicable_reason = fmt::format(
                "Projection orders by {}, which it does not store, so EXPLAIN WHATIF cannot rebuild its key",
                backQuoteIfNeed(required));
            return result;
        }
    }

    /// the writer skips these on insert and only builds them on the first merge, so a fresh part has
    /// no projection part to read and the optimizer charges the parent's marks instead
    if (projection->with_block_number)
    {
        result.not_applicable_reason = fmt::format(
            "Projection stores {}, so it is built only when a part is merged, which EXPLAIN WHATIF does not estimate yet",
            backQuote(BlockNumberColumn::name));
        return result;
    }

    /// the reason names the setting when it overrides the cost, and the optimizer reads it from the context of the read
    const auto & read_settings = read_step->getContext()->getSettingsRef();
    const std::string_view relaxing_setting = read_settings[Setting::force_optimize_projection] ? "force_optimize_projection"
        : read_settings[Setting::prefer_optimize_projection] ? "prefer_optimize_projection" : "";

    result.status = WhatIfCandidateResult::Applicable;

    if (settings.empirical)
    {
        if (tryEstimateProjection(result, *projection, relaxing_setting, read_step, baseline_parts, analysis.selected_marks, weigh, context))
            return result;
        result.empirical_status = WhatIfCandidateResult::Unsupported;
    }
    else
    {
        /// no data is read, but the optimizer still decides if the query gives the projection something to serve
        auto scenario = std::make_shared<HypotheticalProjections>(projection->clone());
        weigh(scenario);
        if (scenario->outcome.nothing_to_serve && relaxing_setting.empty())
        {
            result.status = WhatIfCandidateResult::NotApplicable;
            result.not_applicable_reason = capitalized(scenario->outcome.reason);
            return result;
        }
        result.empirical_status = WhatIfCandidateResult::Disabled;
    }

    result.estimate_source = WhatIfCandidateResult::ApplicabilityOnly;
    return result;
}

}
