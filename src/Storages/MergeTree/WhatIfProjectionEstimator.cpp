#include <Storages/MergeTree/WhatIfProjectionEstimator.h>

#include <Access/Common/AccessFlags.h>
#include <Access/ContextAccess.h>
#include <Columns/ColumnSparse.h>
#include <Common/HashTable/Hash.h>
#include <Common/SipHash.h>
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
#include <Storages/MergeTree/HypotheticalProjection.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPartBuilder.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityAdaptive.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/PartDirIntent.h>
#include <Storages/MergeTree/WhatIfSettings.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ProjectionsDescription.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <ranges>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 max_rows_to_read;
    extern const SettingsUInt64 max_bytes_to_read;
    extern const SettingsOverflowMode read_overflow_mode;
    extern const SettingsBool optimize_use_projections;
    extern const SettingsBool prefer_optimize_projection;
    extern const SettingsBool use_constant_folding_in_index_analysis;
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
    /// projection key columns, in read order
    Block key_block;
    /// sort permutation of key_block
    IColumn::Permutation order;
    size_t rows = 0;
    /// uncompressed bytes, used for granularity
    size_t bytes = 0;
    /// per-row bytes, only filled for adaptive granularity
    PaddedPODArray<UInt32> row_bytes;
    /// some stored column has values of different sizes
    bool variable_width = false;
};

/// per-row version of `getBlockSizeForGranularity`
void appendRowSizes(PaddedPODArray<UInt32> & row_bytes, bool & variable_width, const Block & block)
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
        {
            variable_width = true;
            for (size_t i = 0; i < rows; ++i)
                row_bytes[offset + i] += static_cast<UInt32>(elem.column->byteSizeAt(i));
        }
    }
    for (size_t i = 0; i < rows; ++i)
        row_bytes[offset + i] += fixed;
}


/// the whole part, or one granule from every run of `sample_step` at a hashed position,
/// so data that repeats with the step can't line up with the sample
MarkRanges marksToScan(const DataPartPtr & part, size_t sample_step)
{
    const size_t marks = part->index_granularity->getMarksCountWithoutFinal();
    if (sample_step <= 1)
        return {{0, marks}};
    const UInt64 seed = sipHash64(part->name);
    MarkRanges ranges;
    for (size_t first = 0; first < marks; first += sample_step)
    {
        const size_t mark = first + intHash64(seed ^ first) % std::min(sample_step, marks - first);
        ranges.emplace_back(mark, mark + 1);
    }
    return ranges;
}

/// grow a [low, high] mark range by `spread` on both sides
void widen(UInt64 & low, UInt64 & high, double spread)
{
    const auto by = static_cast<UInt64>(std::ceil(spread));
    low = low > by ? low - by : 0;
    high += by;
}

/// read the given granules of a part
Pipe makePartPipe(
    const DataPartPtr & part,
    const MarkRanges & ranges,
    const Names & columns_to_read,
    ReadFromMergeTree * read_step,
    const ContextPtr & context)
{
    const auto & data = read_step->getMergeTreeData();
    const auto & mutations_snapshot = read_step->getMutationsSnapshot();

    /// apply patch parts and on-the-fly mutations
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
        ranges,
        std::make_shared<std::atomic<size_t>>(0),
        false,
        false,
        false);

    /// keep speed limits, the caller checks sizes
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

/// read only the key columns, false if a read limit was hit
bool buildProjectionPart(
    ProjectionPartData & out,
    const ProjectionDescription & projection,
    const DataPartPtr & part,
    const MarkRanges & ranges,
    ReadFromMergeTree * read_step,
    const SizeLimits & read_limits,
    bool need_row_bytes,
    UInt64 & total_rows_read,
    UInt64 & total_bytes_read,
    const ContextPtr & context)
{
    const auto & proj_key = projection.metadata->getSortingKey();

    Pipe pipe = makePartPipe(part, ranges, projection.required_columns, read_step, context);
    QueryPipeline pipeline(std::move(pipe));
    pipeline.setProcessListElement(context->getProcessListElement());
    pipeline.setProgressCallback(context->getProgressCallback());
    pipeline.setQuota(context->getQuota());
    /// count against the query's quota
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
        /// past a limit the estimate is unsupported instead of failing the query, reaching it is fine as for the query
        if ((read_limits.max_rows && total_rows_read > read_limits.max_rows)
            || (read_limits.max_bytes && total_bytes_read > read_limits.max_bytes))
            return false;

        /// key expression and sort need full columns
        for (auto & column : block)
            column.column = recursiveRemoveSparse(column.column);

        /// measured before the key expression, as the writer does
        out.bytes += getBlockSizeForGranularity(block);
        if (need_row_bytes)
            appendRowSizes(out.row_bytes, out.variable_width, block);
        /// `required_columns` can skip a subcolumn the key needs, add it back like the writer does
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
    /// projection indexes also store the parent offset
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

/// the whole part seen through its sample, in projection order: part row `r` falls on sample row `r * sample rows / part rows`
struct PartFromSample
{
    const ProjectionPartData & sample;
    size_t rows;
    /// bytes of the part rows before the first one each sample row stands for
    std::vector<UInt64> bytes_before_sample_row;

    PartFromSample(const ProjectionPartData & sample_, size_t rows_)
        : sample(sample_), rows(rows_), bytes_before_sample_row(sample.rows + 1)
    {
        for (size_t sample_row = 0; sample_row < sample.rows; ++sample_row)
        {
            const size_t part_rows_covered = firstPartRow(sample_row + 1) - firstPartRow(sample_row);
            bytes_before_sample_row[sample_row + 1] = bytes_before_sample_row[sample_row] + sampleRowBytes(sample_row) * part_rows_covered;
        }
    }

    size_t sampleRow(size_t part_row) const { return static_cast<size_t>(UInt128(part_row) * sample.rows / rows); }
    size_t firstPartRow(size_t sample_row) const
    {
        return static_cast<size_t>((UInt128(sample_row) * rows + sample.rows - 1) / sample.rows);
    }
    /// the first sample row whose part rows start at or after `part_row`
    size_t firstSampleRowFrom(size_t part_row) const { return part_row == 0 ? 0 : sampleRow(part_row - 1) + 1; }

    size_t sampleRowBytes(size_t sample_row) const
    {
        const bool per_row = sample.row_bytes.size() == sample.rows;
        return per_row ? sample.row_bytes[sample.order[sample_row]] : std::max<size_t>(sample.bytes / sample.rows, 1);
    }

    UInt64 bytesBefore(size_t part_row) const
    {
        const size_t sample_row = sampleRow(part_row);
        if (sample_row == sample.rows)
            return bytes();
        return bytes_before_sample_row[sample_row] + sampleRowBytes(sample_row) * (part_row - firstPartRow(sample_row));
    }

    UInt64 bytes() const { return bytes_before_sample_row.back(); }
};

/// end of the block a writer gets at `row`: at most `rows_limit` rows and `bytes_limit` bytes, but at least one row
size_t blockEnd(const PartFromSample & part, size_t row, size_t rows_limit, size_t bytes_limit)
{
    const size_t end = std::min(part.rows, row + rows_limit);
    if (bytes_limit == 0)
        return end;
    const UInt64 start_bytes = part.bytesBefore(row);
    const auto longer_ends = std::views::iota(row + 2, end + 1);
    const auto fits = [&](size_t block_end) { return part.bytesBefore(block_end) - start_bytes <= bytes_limit; };
    return row + 1 + static_cast<size_t>(std::ranges::partition_point(longer_ends, fits) - longer_ends.begin());
}

/// replays the writer over the blocks, one granule size per block
std::vector<size_t> simulateWriterMarks(
    const PartFromSample & part,
    MergeTreeDataPartType part_type,
    const MergeTreeSettings & mt_settings,
    bool adaptive_marks,
    size_t block_rows_limit,
    size_t block_bytes_limit)
{
    const size_t granularity_bytes = mt_settings[MergeTreeSetting::index_granularity_bytes];
    const size_t fixed_granularity_rows = mt_settings[MergeTreeSetting::index_granularity];

    MergeTreeIndexGranularityAdaptive granularity;
    size_t written = 0;
    for (size_t row = 0; row < part.rows;)
    {
        const size_t end = blockEnd(part, row, block_rows_limit, block_bytes_limit);
        const size_t block_rows = end - row;
        const size_t block_bytes = part.bytesBefore(end) - part.bytesBefore(row);
        row = end;

        const size_t granule_rows = computeIndexGranularity(
            block_rows, block_bytes, granularity_bytes, fixed_granularity_rows, /* blocks_are_granules */ false, adaptive_marks);

        /// rows that go into the mark the previous block left open
        size_t open_rows_missing = granularity.getTotalRows() - written;
        /// the wide writer first shrinks an open mark wider than this granule
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

    /// the writer trims the last mark when it closes the part
    if (granularity.getTotalRows() > written)
        granularity.adjustLastMark(granularity.getLastMarkRows() - (granularity.getTotalRows() - written));

    std::vector<size_t> mark_rows(granularity.getMarksCount());
    for (size_t mark = 0; mark < mark_rows.size(); ++mark)
        mark_rows[mark] = granularity.getMarkRows(mark);
    return mark_rows;
}


/// builds an in-memory projection part of `part_rows` rows for each layout the writer can leave
/// returns the parts and the index of the likeliest layout
std::pair<std::vector<MergeTreeDataPartPtr>, size_t> buildSyntheticProjectionParts(
    ProjectionPartData & data,
    const ProjectionDescription & projection,
    const MergeTreeData & merge_tree,
    const DataPartPtr & parent_part,
    const MergeTreeSettings & mt_settings,
    bool uneven_rows,
    size_t part_rows)
{
    const auto & proj_key = projection.metadata->getSortingKey();

    SortDescription sort_description;
    sort_description.reserve(proj_key.column_names.size());
    for (const auto & name : proj_key.column_names)
        sort_description.emplace_back(name, 1, 1);

    /// sorted order via one permutation
    stableGetPermutation(data.key_block, sort_description, data.order);
    const PartFromSample whole_part(data, part_rows);
    const auto part_type = merge_tree.choosePartFormat(whole_part.bytes(), whole_part.rows, parent_part->info.level, &projection).part_type;
    const bool adaptive_marks = parent_part->index_granularity_info.mark_type.adaptive;
    /// only adaptive granularity lets block sizes change the layout
    const bool granularity_per_block = part_type == MergeTreeDataPartType::Compact
        || (adaptive_marks && !mt_settings[MergeTreeSetting::use_const_adaptive_granularity]);

    /// an insert writes one block, a merge writes blocks of `merge_max_block_size` cut at each source
    /// a part does not record which, so build each layout
    const size_t merge_rows = mt_settings[MergeTreeSetting::merge_max_block_size];
    const size_t merge_bytes = mt_settings[MergeTreeSetting::merge_max_block_size_bytes];
    /// one granule of bytes is the finest block size that matters
    const size_t granule_bytes = mt_settings[MergeTreeSetting::index_granularity_bytes];
    std::vector<std::pair<size_t, size_t>> chunkings;
    size_t primary = 0;
    chunkings.emplace_back(whole_part.rows, 0);
    if (granularity_per_block)
    {
        chunkings.emplace_back(merge_rows, merge_bytes);
        chunkings.emplace_back(merge_rows, granule_bytes);
        /// the writer writes a level-zero part in one block and a merged part in merge blocks
        /// when the row widths differ, the merge also cuts a block at each source
        if (parent_part->info.level > 0)
            primary = uneven_rows ? chunkings.size() - 1 : chunkings.size() - 2;
    }

    std::vector<std::vector<size_t>> layouts;
    for (const auto & [rows_limit, bytes_limit] : chunkings)
        layouts.push_back(simulateWriterMarks(whole_part, part_type, mt_settings, adaptive_marks, rows_limit, bytes_limit));

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
                index_column->insertFrom(*key_column.column, data.order[whole_part.sampleRow(mark != 0 ? partial_sums[mark - 1] : 0)]);
            index_columns.push_back(std::move(index_column));
        }

        /// `Synthetic` leaves the part directory alone, `CreateFresh` can delete a leftover `.tmp_proj`
        auto part = const_cast<IMergeTreeDataPart &>(*parent_part)
                        .getProjectionPartBuilder(projection.name, &projection, PartDirIntent::Synthetic, /* is_temp_projection */ true)
                        .withPartType(MergeTreeDataPartType::Compact)
                        .withBytesAndRows(0, whole_part.rows, 0)
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

/// fewer sampled granules can't give an error estimate
constexpr size_t min_sampled_granules = 30;

/// the granules to read from each part: all, or a sample past the budget
struct ScanPlan
{
    size_t sample_step = 1;
    std::vector<MarkRanges> ranges;
    /// the marks of a full scan
    UInt64 marks_to_scan = 0;
};

/// plans the scan, or returns false with the reason if a query limit forbids it
bool planScan(
    WhatIfCandidateResult & result,
    ScanPlan & plan,
    const RangesInDataParts & baseline_parts,
    bool filters_on_offsets,
    UInt64 projection_scan_budget_rows,
    const Settings & query_settings)
{
    UInt64 rows_to_scan = 0;
    for (const auto & part_with_ranges : baseline_parts)
    {
        rows_to_scan += part_with_ranges.data_part->rows_count;
        plan.marks_to_scan += part_with_ranges.data_part->index_granularity->getMarksCountWithoutFinal();
    }
    UInt64 budget = projection_scan_budget_rows;
    if (const UInt64 read_limit = query_settings[Setting::max_rows_to_read]; read_limit != 0)
        budget = budget == 0 ? read_limit : std::min(budget, read_limit);
    plan.sample_step = budget != 0 && rows_to_scan > budget ? (rows_to_scan + budget - 1) / budget : 1;
    /// the largest step that still samples `min_sampled_granules`
    if (plan.sample_step > 1)
        plan.sample_step
            = std::min<size_t>(plan.sample_step, std::max<size_t>(1, (plan.marks_to_scan - 1) / (min_sampled_granules - 1)));

    /// a sample's row offsets are not the part's, so an offset filter can't be applied to it
    if (plan.sample_step > 1 && filters_on_offsets)
    {
        result.empirical_unsupported_reason
            = "The query filters on part offsets, which an estimate from a sample of granules cannot follow "
              "(see projection_scan_budget_rows)";
        return false;
    }

    /// the granule floor and one granule per part can outgrow the budget, so check the read limit before reading
    plan.ranges.reserve(baseline_parts.size());
    UInt64 rows_planned = 0;
    for (const auto & part_with_ranges : baseline_parts)
    {
        plan.ranges.push_back(marksToScan(part_with_ranges.data_part, plan.sample_step));
        rows_planned += part_with_ranges.data_part->index_granularity->getRowsCountInRanges(plan.ranges.back());
    }
    if (const UInt64 read_limit = query_settings[Setting::max_rows_to_read]; read_limit != 0 && rows_planned > read_limit)
    {
        result.empirical_unsupported_reason = fmt::format(
            "The estimate would read {} rows, over max_rows_to_read = {} (a sample keeps at least ~{} granules and one per part)",
            rows_planned,
            read_limit,
            min_sampled_granules);
        return false;
    }
    return true;
}

/// a sampled part, kept for the error estimate
struct SampledPart
{
    String name;
    ProjectionPartData sample;
    size_t part_rows = 0;
    MarkRanges ranges_read;
    DataPartPtr parent;
    /// the projection part in its likeliest layout
    MergeTreeDataPartPtr synthetic;
};

/// scenario 0 has every part in its likeliest layout, scenarios 1 to 3 have every part in one layout each
struct Scenarios
{
    std::array<HypotheticalProjectionPtr, 4> scenarios;
    bool layouts_differ = false;
    UInt64 scanned_parts = 0;
    UInt64 scanned_marks = 0;
    UInt64 uneven_width_parts = 0;
    std::vector<SampledPart> samples;
};

/// reads the planned granules and builds the projection parts, or returns false with the reason
bool buildScenarios(
    WhatIfCandidateResult & result,
    Scenarios & out,
    const ProjectionDescription & projection,
    const ScanPlan & plan,
    ReadFromMergeTree * read_step,
    const RangesInDataParts & baseline_parts,
    const ContextPtr & context)
{
    const auto & data = read_step->getMergeTreeData();
    const auto mt_settings_ptr = data.getSettings(&projection.settings_changes);
    const auto & mt_settings = *mt_settings_ptr;
    const auto & query_settings = context->getSettingsRef();
    auto log = getLogger("WhatIfProjectionEstimator");

    /// check read limits by hand
    const SizeLimits read_limits(
        query_settings[Setting::max_rows_to_read], query_settings[Setting::max_bytes_to_read], query_settings[Setting::read_overflow_mode]);
    UInt64 total_rows_read = 0;
    UInt64 total_bytes_read = 0;

    for (auto & scenario : out.scenarios)
        scenario = std::make_shared<HypotheticalProjection>(projection.clone());

    for (size_t part_idx = 0; part_idx < baseline_parts.size(); ++part_idx)
    {
        const auto & part = baseline_parts[part_idx].data_part;
        if (part->index_granularity->getMarksCountWithoutFinal() == 0)
            continue;

        /// row bytes only matter when granules are sized by bytes
        const bool adaptive = part->index_granularity_info.mark_type.adaptive
            && mt_settings[MergeTreeSetting::index_granularity_bytes] != 0;

        const MarkRanges & ranges = plan.ranges[part_idx];
        ProjectionPartData part_data;
        if (!buildProjectionPart(
                part_data, projection, part, ranges, read_step, read_limits, adaptive, total_rows_read, total_bytes_read, context))
        {
            result.empirical_unsupported_reason
                = "The projection scan hit the read limit of the query (max_rows_to_read / max_bytes_to_read)";
            return false;
        }
        /// a `break` time limit or a cancelled query stops the read silently
        if (part_data.rows != part->index_granularity->getRowsCountInRanges(ranges))
        {
            result.empirical_unsupported_reason = "The projection scan was cut short by a time limit in `break` mode or a cancelled query";
            /// the limit also drops the output, so log the reason
            LOG_DEBUG(log, "{}", result.empirical_unsupported_reason);
            return false;
        }

        ++out.scanned_parts;
        out.scanned_marks += ranges.getNumberOfMarks();
        /// with uneven row widths the layout depends on block boundaries we can't know
        /// a sample can't show that rows it didn't read have the same width
        bool uneven_rows = plan.sample_step > 1 && part_data.variable_width;
        if (!uneven_rows && part_data.row_bytes.size() == part_data.rows && !part_data.row_bytes.empty())
        {
            const auto [narrowest, widest] = std::minmax_element(part_data.row_bytes.begin(), part_data.row_bytes.end());
            uneven_rows = *narrowest != *widest;
        }
        if (uneven_rows)
            ++out.uneven_width_parts;
        /// no key rows from a non-empty part: the key needs a column we don't read, e.g. `_part_offset`
        if (part_data.rows == 0 && part->rows_count > 0)
        {
            result.empirical_unsupported_reason = "The projection key could not be built from the columns the projection stores";
            return false;
        }
        if (part_data.rows == 0)
            continue;

        const size_t part_rows = plan.sample_step > 1 ? part->rows_count : part_data.rows;
        const auto [parts, primary]
            = buildSyntheticProjectionParts(part_data, projection, data, part, mt_settings, uneven_rows, part_rows);
        out.scenarios[0]->parts[part->name] = parts[primary];
        /// a part with one layout uses it in every scenario
        for (size_t layout = 0; layout + 1 < out.scenarios.size(); ++layout)
        {
            const auto & layout_part = parts[std::min(layout, parts.size() - 1)];
            out.scenarios[1 + layout]->parts[part->name] = layout_part;
            out.layouts_differ |= layout_part != parts[primary];
        }
        if (part_rows > part_data.rows)
            out.samples.push_back({part->name, std::move(part_data), part_rows, ranges, part, parts[primary]});
    }

    return true;
}

/// widens the mark range by the sampling error: range ends and the selected share
void widenForSamples(
    const std::vector<SampledPart> & samples, const HypotheticalProjection::Outcome & outcome, UInt64 & marks_low, UInt64 & marks_high)
{
    /// the estimate knows a range end only up to the gap between neighbouring sample rows
    /// that gap is a sampling step if the key follows the parent order
    std::vector<double> granule_shares;
    UInt64 layout_marks = 0;
    double range_end_spread = 0;
    for (const auto & sampled : samples)
    {
        const auto & sample = sampled.sample;
        const PartFromSample whole_part(sample, sampled.part_rows);
        const auto & granularity = *sampled.synthetic->index_granularity;
        const auto pruned_it = outcome.ranges.find(sampled.name);
        const MarkRanges pruned = pruned_it == outcome.ranges.end() ? MarkRanges{} : pruned_it->second;
        layout_marks += granularity.getMarksCount();

        /// the sampled granule each sample row came from, and the rows of each
        const auto & parent_granularity = *sampled.parent->index_granularity;
        std::vector<size_t> sampled_granule_rows;
        std::vector<UInt32> granule_of_row;
        granule_of_row.reserve(sample.rows);
        for (const auto & range : sampled.ranges_read)
            for (size_t mark = range.begin; mark < range.end; ++mark)
            {
                sampled_granule_rows.push_back(parent_granularity.getMarkRows(mark));
                granule_of_row.resize(
                    granule_of_row.size() + sampled_granule_rows.back(), static_cast<UInt32>(sampled_granule_rows.size() - 1));
            }
        chassert(granule_of_row.size() == sample.rows);

        size_t same_granule_neighbours = 0;
        for (size_t pos = 1; pos < sample.rows; ++pos)
            same_granule_neighbours += granule_of_row[sample.order[pos]] == granule_of_row[sample.order[pos - 1]];
        const double follows_parent
            = static_cast<double>(same_granule_neighbours) / static_cast<double>(std::max<size_t>(1, sample.rows - 1));
        const double sampling_step
            = static_cast<double>(parent_granularity.getMarksCountWithoutFinal()) / static_cast<double>(sampled_granule_rows.size());
        const double marks_per_sample_row = static_cast<double>(granularity.getMarksCount()) / static_cast<double>(sample.rows);
        const double granules_per_end = 1.0 + follows_parent * (sampling_step - 1.0) + (1.0 - follows_parent) * marks_per_sample_row;
        range_end_spread += 2.0 * static_cast<double>(std::max<size_t>(1, pruned.size())) * granules_per_end;

        std::vector<size_t> selected_rows(sampled_granule_rows.size());
        for (const auto & range : pruned)
        {
            const size_t sample_begin = whole_part.firstSampleRowFrom(granularity.getMarkStartingRow(range.begin));
            const size_t sample_end = whole_part.firstSampleRowFrom(granularity.getMarkStartingRow(range.end));
            for (size_t sample_row = sample_begin; sample_row < sample_end; ++sample_row)
                ++selected_rows[granule_of_row[sample.order[sample_row]]];
        }
        for (size_t i = 0; i < sampled_granule_rows.size(); ++i)
            granule_shares.push_back(static_cast<double>(selected_rows[i]) / static_cast<double>(sampled_granule_rows[i]));
    }
    widen(marks_low, marks_high, range_end_spread);

    /// widen by two standard errors of the selected share (successive differences, fits a systematic sample)
    if (granule_shares.size() > 1)
    {
        double sum_of_squares = 0;
        for (size_t i = 1; i < granule_shares.size(); ++i)
            sum_of_squares += (granule_shares[i] - granule_shares[i - 1]) * (granule_shares[i] - granule_shares[i - 1]);
        const double count = static_cast<double>(granule_shares.size());
        const double standard_error = std::sqrt(sum_of_squares / (2.0 * count * (count - 1.0)));
        widen(marks_low, marks_high, 2.0 * standard_error * static_cast<double>(layout_marks));
    }

}

/// the verdict and its reason from the scenario outcomes
void setVerdict(
    WhatIfCandidateResult & result,
    const HypotheticalProjection::Outcome & outcome,
    size_t chosen_in,
    size_t weighed,
    std::string_view relaxing_setting,
    UInt64 baseline_marks,
    const Scenarios & built)
{
    const UInt64 projection_marks = *outcome.marks;
    const UInt64 marks_low = result.estimated_marks_low;
    const UInt64 marks_high = result.estimated_marks_high;
    auto marks_text = [](UInt64 marks) { return fmt::format("{} mark{}", marks, marks == 1 ? "" : "s"); };
    /// the rule against the base table: fewer marks, or the same marks and a served ORDER BY
    auto beats_base = [&](UInt64 marks) { return marks < baseline_marks || (marks == baseline_marks && outcome.serves_order); };
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
    else if (built.uneven_width_parts != 0)
    {
        result.verdict = "too close to call";
        result.verdict_reason = fmt::format(
            "{} against {} from the base table, but the rows are not known to have the same width on {} of the {} parts read, "
            "so the granule layout depends on the blocks the writer was fed and the mark count is a model, not a measurement",
            marks_text(projection_marks),
            baseline_marks,
            built.uneven_width_parts,
            built.scanned_parts);
    }
    else if ((chosen_in != 0 && chosen_in != weighed) || beats_base(marks_low) != beats_base(marks_high))
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
        /// another projection beats this one
        if (chosen_in == 0 && projection_marks < baseline_marks)
            result.verdict_reason = outcome.reason;
    }
}

bool tryEstimateProjection(
    WhatIfCandidateResult & result,
    const ProjectionDescription & projection,
    bool filters_on_offsets,
    std::string_view relaxing_setting,
    ReadFromMergeTree * read_step,
    const RangesInDataParts & baseline_parts,
    UInt64 baseline_marks,
    UInt64 projection_scan_budget_rows,
    const WeighHypotheticalProjection & weigh,
    const ContextPtr & context)
{
    ScanPlan plan;
    if (!planScan(result, plan, baseline_parts, filters_on_offsets, projection_scan_budget_rows, context->getSettingsRef()))
        return false;

    Stopwatch watch;
    Scenarios built;
    if (!buildScenarios(result, built, projection, plan, read_step, baseline_parts, context))
        return false;

    const size_t weighed = built.layouts_differ ? built.scenarios.size() : 1;
    for (size_t i = 0; i < weighed; ++i)
        weigh(built.scenarios[i]);

    const auto & outcome = built.scenarios[0]->outcome;
    result.sampled_parts = built.scanned_parts;
    result.sampled_marks = built.scanned_marks;
    /// out of what a full scan would read
    result.total_marks = plan.marks_to_scan;
    result.elapsed_us = watch.elapsedMicroseconds();
    if (!outcome.marks)
    {
        result.status = WhatIfCandidateResult::NotApplicable;
        result.not_applicable_reason = outcome.reason.empty() ? "The optimizer did not weigh the projection for this read" : outcome.reason;
        return true;
    }

    UInt64 marks_low = *outcome.marks;
    UInt64 marks_high = *outcome.marks;
    size_t chosen_in = 0;
    for (size_t i = 0; i < weighed; ++i)
    {
        const auto & scenario_outcome = built.scenarios[i]->outcome;
        chosen_in += scenario_outcome.chosen;
        if (scenario_outcome.marks)
        {
            marks_low = std::min(marks_low, *scenario_outcome.marks);
            marks_high = std::max(marks_high, *scenario_outcome.marks);
        }
    }
    widenForSamples(built.samples, outcome, marks_low, marks_high);

    result.estimated_marks = *outcome.marks;
    result.estimated_rows = outcome.rows;
    result.estimated_marks_low = marks_low;
    result.estimated_marks_high = marks_high;
    setVerdict(result, outcome, chosen_in, weighed, relaxing_setting, baseline_marks, built);
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

    /// an ALTER may retarget an ALIAS, so check the columns actually read
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
    bool force_requested,
    const WeighHypotheticalProjection & weigh,
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

    /// before the refresh: a projection-served read carries that projection's metadata
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

    /// commit-order keys and keys over columns the projection doesn't store can't be rebuilt
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

    /// the writer builds these only on merge, so fresh parts have none
    if (projection->with_block_number)
    {
        result.not_applicable_reason = fmt::format(
            "Projection stores {}, so it is built only when a part is merged, which EXPLAIN WHATIF does not estimate yet",
            backQuote(BlockNumberColumn::name));
        return result;
    }

    /// offset conditions as `ReadFromMergeTree` builds them, except for projections storing parent offsets
    bool filters_on_offsets = false;
    if (const auto & filter_dag = read_step->getFilterActionsDAG())
    {
        const auto predicate_dag = std::make_shared<ActionsDAGWithInversionPushDown>(
            filter_dag->getOutputs().front(), context, /* boolean_context */ true);
        const bool skip_folding = !context->getSettingsRef()[Setting::use_constant_folding_in_index_analysis];
        const auto & proj_columns = projection->metadata->getColumns();
        if (!projection->with_parent_part_offset && !proj_columns.has("_part_offset"))
        {
            const auto & proj_metadata = projection->metadata;
            filters_on_offsets
                = (!proj_columns.has("_part")
                   && MergeTreeDataSelectExecutor::buildKeyConditionFromPartOffset(predicate_dag, proj_metadata, skip_folding, context))
                || (!proj_columns.has("_part_starting_offset")
                    && MergeTreeDataSelectExecutor::buildKeyConditionFromTotalOffset(predicate_dag, proj_metadata, skip_folding, context));
        }
    }

    /// read the setting from the context of the read, as the optimizer does
    const auto & read_settings = read_step->getContext()->getSettingsRef();
    const std::string_view relaxing_setting = !read_settings[Setting::prefer_optimize_projection] ? ""
        : force_requested ? "force_optimize_projection" : "prefer_optimize_projection";

    result.status = WhatIfCandidateResult::Applicable;

    if (settings.empirical)
    {
        if (tryEstimateProjection(
                result, *projection, filters_on_offsets, relaxing_setting, read_step, baseline_parts, analysis.selected_marks,
                settings.projection_scan_budget_rows, weigh, context))
            return result;
        result.empirical_status = WhatIfCandidateResult::Unsupported;
    }
    else
    {
        /// without data, the optimizer still decides if the projection has anything to serve
        auto scenario = std::make_shared<HypotheticalProjection>(projection->clone());
        weigh(scenario);
        if (scenario->outcome.nothing_to_serve && relaxing_setting.empty())
        {
            result.status = WhatIfCandidateResult::NotApplicable;
            result.not_applicable_reason = scenario->outcome.reason;
            return result;
        }
        result.empirical_status = WhatIfCandidateResult::Disabled;
    }

    result.estimate_source = WhatIfCandidateResult::ApplicabilityOnly;
    return result;
}

}
