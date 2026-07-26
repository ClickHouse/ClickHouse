#include <Storages/StorageMergeTreeCodecBlockCounts.h>

#include <Access/Common/AccessFlags.h>
#include <Access/EnabledRowPolicies.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <Compression/ICompressionCodec.h>
#include <Compression/getCompressionCodecForFile.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/StorageID.h>
#include <Parsers/IAST.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int ACCESS_DENIED;
extern const int BAD_ARGUMENTS;
}

/// StorageMergeTreeCodecBlockCounts -> ReadFromMergeTreeCodecBlockCounts -> MergeTreeCodecBlockCountsSource
///            analyzer                             planner                             executor
///       snapshots the parts            compiles WHERE over names           filters names, reads survivors

namespace
{

const String PART_NAME_COLUMN = "part_name";
const String COLUMN_COLUMN = "column";
const String SUBSTREAM_COLUMN = "substream";
const String DATA_COMPRESSED_BYTES_COLUMN = "data_compressed_bytes";
const String DATA_UNCOMPRESSED_BYTES_COLUMN = "data_uncompressed_bytes";
const String CODEC_BLOCK_COUNTS_COLUMN = "codec_block_counts";

Map codecCountsToField(const std::map<String, UInt64> & counts)
{
    Map result;
    result.reserve(counts.size());
    for (const auto & [name, count] : counts)
        result.emplace_back(Tuple({name, count}));
    return result;
}

struct SubstreamInfo
{
    std::optional<UInt64> compressed;
    std::optional<UInt64> uncompressed;
    Map codecs;
};

/// For each part, builds a block of key rows (part_name, column, substream) and runs keys_filter over it.
/// The surviving rows become the output. Their `.bin`s are opened only when `codec_block_counts` is selected.
class MergeTreeCodecBlockCountsSource final : public ISource
{
public:
    MergeTreeCodecBlockCountsSource(
        SharedHeader header_, MergeTreeData::DataPartsVector data_parts_, ExpressionActionsPtr keys_filter_, ReadSettings read_settings_)
        : ISource(header_)
        , header(std::move(header_))
        , data_parts(std::move(data_parts_))
        , keys_filter(std::move(keys_filter_))
        , read_settings(std::move(read_settings_))
    {
        auto position_of = [&](const String & name) -> std::optional<size_t>
        { return header->has(name) ? std::optional<size_t>(header->getPositionByName(name)) : std::nullopt; };

        part_name_pos = position_of(PART_NAME_COLUMN);
        column_pos = position_of(COLUMN_COLUMN);
        substream_pos = position_of(SUBSTREAM_COLUMN);
        compressed_pos = position_of(DATA_COMPRESSED_BYTES_COLUMN);
        uncompressed_pos = position_of(DATA_UNCOMPRESSED_BYTES_COLUMN);
        codec_counts_pos = position_of(CODEC_BLOCK_COUNTS_COLUMN);
    }

    String getName() const override { return "MergeTreeCodecBlockCounts"; }

protected:
    Chunk generate() override
    {
        /// One part per call. generate is called repeatedly until it returns an empty chunk.
        /// Thus we skip parts with empty ColumnsSubstreams (e.g. Compact without substream marks) and parts
        /// where keys_filter drops every row, instead of returning an empty chunk for them.
        while (part_index < data_parts.size())
        {
            const auto & part = data_parts[part_index++];
            /// TODO: mutating an old Wide part that has no columns_substreams.txt leaves the rewritten part without one too.
            /// Yet the rewritten columns may hold adaptively-selected codecs. This part is omitted here until next merge records substreams.
            if (part->getColumnsSubstreams().empty())
                continue;

            /// The pushed-down WHERE drops rows before anything is read. The surviving rows drive the reads.
            Block keys = makeKeysBlock(part);
            if (keys_filter)
                VirtualColumnUtils::filterBlockWithExpression(keys_filter, keys);

            const size_t num_rows = keys.rows();
            if (num_rows == 0)
                continue;

            const auto & columns = *keys.getByName(COLUMN_COLUMN).column;
            const auto & substreams = *keys.getByName(SUBSTREAM_COLUMN).column;

            MutableColumns result = header->cloneEmptyColumns();

            for (size_t row = 0; row < num_rows; ++row)
            {
                const String substream(substreams.getDataAt(row));
                SubstreamInfo info = computeForSubstream(part, substream);

                if (part_name_pos)
                    result[*part_name_pos]->insert(part->name);
                if (column_pos)
                    result[*column_pos]->insertFrom(columns, row);
                if (substream_pos)
                    result[*substream_pos]->insertFrom(substreams, row);
                if (codec_counts_pos)
                    result[*codec_counts_pos]->insert(info.codecs);

                /// Sizes are Nullable: the value, or a NULL `Field` for Compact / no `.bin`.
                if (compressed_pos)
                    result[*compressed_pos]->insert(info.compressed ? Field(*info.compressed) : Field());
                if (uncompressed_pos)
                    result[*uncompressed_pos]->insert(info.uncompressed ? Field(*info.uncompressed) : Field());
            }

            return Chunk(std::move(result), num_rows);
        }

        return {};
    }

private:
    /// The part's key columns, one row per (column, substream) with a `.bin`-recorded substream, i.e. per future output row.
    Block makeKeysBlock(const MergeTreeDataPartPtr & part) const
    {
        auto column_column = ColumnString::create();
        auto substream_column = ColumnString::create();

        const auto & columns_substreams = part->getColumnsSubstreams();
        size_t column_position = 0;
        for (const auto & column : part->getColumns())
        {
            for (const auto & substream : columns_substreams.getColumnSubstreams(column_position))
            {
                column_column->insert(column.name);
                substream_column->insert(substream);
            }
            ++column_position;
        }

        const size_t num_rows = column_column->size();
        const auto string_type = std::make_shared<DataTypeString>();
        return Block{
            {string_type->createColumnConst(num_rows, part->name), string_type, PART_NAME_COLUMN},
            {std::move(column_column), string_type, COLUMN_COLUMN},
            {std::move(substream_column), string_type, SUBSTREAM_COLUMN},
        };
    }

    /// Per-substream sizes (metadata only) and, when requested, codec block counts (reads `.bin`).
    /// Empty for Compact parts: their columns share one `data.bin`, so there is no per-stream `.bin`.
    SubstreamInfo computeForSubstream(const MergeTreeDataPartPtr & part, const String & substream)
    {
        if (isCompactPart(part))
            return {};

        auto filename = IMergeTreeDataPart::getStreamNameOrHash(substream, ".bin", part->checksums);
        if (!filename)
            return {};

        SubstreamInfo info;
        if (auto it = part->checksums.files.find(*filename + ".bin"); it != part->checksums.files.end())
        {
            info.compressed = it->second.file_size;
            info.uncompressed = it->second.uncompressed_size;
        }

        /// Gate this because the `.bin` walk is expensive.
        if (codec_counts_pos)
            info.codecs = walkBin(part, *filename);

        return info;
    }

    /// Counts the codec of every compressed block in the resolved `.bin` (reads the data file).
    Map walkBin(const MergeTreeDataPartPtr & part, const String & filename)
    {
        auto read_buffer = part->getDataPartStorage().readFile(filename + ".bin", read_settings, std::nullopt);
        std::map<String, UInt64> counts;
        while (!read_buffer->eof())
        {
            UInt32 size_compressed = 0;
            UInt32 size_decompressed = 0;
            auto codec = getCompressionCodecForFile(*read_buffer, size_compressed, size_decompressed, true);
            ++counts[codec->getCodecDesc()->formatForLogging()];
        }
        return codecCountsToField(counts);
    }

    SharedHeader header;
    MergeTreeData::DataPartsVector data_parts;
    ExpressionActionsPtr keys_filter;
    ReadSettings read_settings;

    /// Positions of the output columns in the header, nullopt when the query didn't select them.
    /// Unselected columns aren't built, and a nullopt codec_counts_pos also skips the `.bin` walk.
    std::optional<size_t> part_name_pos;
    std::optional<size_t> column_pos;
    std::optional<size_t> substream_pos;
    std::optional<size_t> compressed_pos;
    std::optional<size_t> uncompressed_pos;
    std::optional<size_t> codec_counts_pos;

    size_t part_index = 0;
};

/// During plan optimisation, applyFilters receives the query's WHERE and compiles it's relecant part into keys_filter for the source.
class ReadFromMergeTreeCodecBlockCounts : public SourceStepWithFilter
{
public:
    ReadFromMergeTreeCodecBlockCounts(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        SharedHeader sample_block,
        MergeTreeData::DataPartsVector data_parts_,
        StorageID source_table_id_)
        : SourceStepWithFilter(std::move(sample_block), column_names_, query_info_, storage_snapshot_, context_)
        , data_parts(std::move(data_parts_))
        , source_table_id(std::move(source_table_id_))
    {
    }

    std::string getName() const override { return "ReadFromMergeTreeCodecBlockCounts"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void applyFilters(ActionDAGNodes added_filter_nodes) override;

private:
    MergeTreeData::DataPartsVector data_parts;
    StorageID source_table_id;
    ExpressionActionsPtr keys_filter;
};

void ReadFromMergeTreeCodecBlockCounts::applyFilters(ActionDAGNodes added_filter_nodes)
{
    SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));

    if (!filter_actions_dag)
        return;

    Block block_to_filter{
        {{}, std::make_shared<DataTypeString>(), PART_NAME_COLUMN},
        {{}, std::make_shared<DataTypeString>(), COLUMN_COLUMN},
        {{}, std::make_shared<DataTypeString>(), SUBSTREAM_COLUMN},
    };

    auto dag = VirtualColumnUtils::splitFilterDagForAllowedInputs(filter_actions_dag->getOutputs().at(0), &block_to_filter, context);
    if (dag)
        keys_filter = VirtualColumnUtils::buildFilterExpression(std::move(*dag), context);
}

void ReadFromMergeTreeCodecBlockCounts::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    LOG_DEBUG(
        getLogger("StorageMergeTreeCodecBlockCounts"),
        "Reading codec block counts from {} parts of table {}{}",
        data_parts.size(),
        source_table_id.getNameForLogs(),
        keys_filter ? " with filter pushdown" : "");

    pipeline.init(
        Pipe(std::make_shared<MergeTreeCodecBlockCountsSource>(getOutputHeader(), data_parts, keys_filter, context->getReadSettings())));
}

}

StorageMergeTreeCodecBlockCounts::StorageMergeTreeCodecBlockCounts(
    const StorageID & table_id_, StoragePtr source_table_, const ColumnsDescription & columns_)
    : IStorage(table_id_)
    , source_table(std::move(source_table_))
{
    const auto * merge_tree = dynamic_cast<const MergeTreeData *>(source_table.get());
    if (!merge_tree)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Storage MergeTreeCodecBlockCounts expected MergeTree table, got: {}", source_table->getName());

    /// `system.parts_columns` lists patch parts, so this function does too.
    data_parts = merge_tree->getDataPartsVectorForInternalUsage(
        {MergeTreeData::DataPartState::Active}, {MergeTreeData::DataPartKind::Regular, MergeTreeData::DataPartKind::Patch});
    std::erase_if(data_parts, [](const MergeTreeData::DataPartPtr & part) { return part->isEmpty(); });

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);
}

void StorageMergeTreeCodecBlockCounts::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);

    const auto source_metadata = source_table->getInMemoryMetadataPtr(context, false);
    const auto source_storage_id = source_table->getStorageID();
    context->checkAccess(AccessType::SELECT, source_storage_id, source_metadata->getColumns().getNamesOfPhysical());

    auto sample_block = std::make_shared<const Block>(storage_snapshot->getSampleBlockForColumns(column_names));

    /// A row policy denies only `codec_block_counts`, whose counts aggregate all rows of a part, including rows the policy should hide.
    /// The other columns are metadata that `system.parts_columns` reports regardless of row policies.
    if (sample_block->has(CODEC_BLOCK_COUNTS_COLUMN))
    {
        auto row_policy_filter = context->getRowPolicyFilter(
            source_storage_id.getDatabaseName(), source_storage_id.getTableName(), RowPolicyFilterType::SELECT_FILTER);
        if (row_policy_filter && !row_policy_filter->isAlwaysTrue())
            throw Exception(
                ErrorCodes::ACCESS_DENIED,
                "Cannot read column `{}` from `mergeTreeCodecBlockCounts` because a row policy is applied on table {}. "
                "The counts would cover rows the policy hides",
                CODEC_BLOCK_COUNTS_COLUMN,
                source_storage_id.getNameForLogs());
    }

    /// The parts reference the source table's MergeTreeData without owning it.
    query_plan.addStorageHolder(source_table);

    query_plan.addStep(
        std::make_unique<ReadFromMergeTreeCodecBlockCounts>(
            column_names, query_info, storage_snapshot, std::move(context), std::move(sample_block), data_parts, source_storage_id));
}

}
