#include <Storages/StorageMergeTreeIndex.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/NestedUtils.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeMarksLoader.h>
#include <Storages/VirtualColumnUtils.h>
#include <Access/Common/AccessFlags.h>
#include <Common/CurrentThread.h>
#include <Common/HashTable/HashSet.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/escapeForFileName.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/ISource.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool use_streaming_marks_compression;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
}

class MergeTreeIndexSource final : public ISource, WithContext
{
public:
    MergeTreeIndexSource(
        SharedHeader header_,
        SharedHeader index_header_,
        SharedHeader minmax_header_,
        MergeTreeData::DataPartsVector data_parts_,
        ContextPtr context_,
        bool with_marks_,
        StorageMetadataHandle source_metadata_)
        : ISource(header_)
        , WithContext(context_)
        , header(std::move(header_))
        , index_header(std::move(index_header_))
        , minmax_header(std::move(minmax_header_))
        , data_parts(std::move(data_parts_))
        , with_marks(with_marks_)
        , source_metadata(std::move(source_metadata_))
    {
    }

    String getName() const override { return "MergeTreeIndex"; }

protected:
    Chunk generate() override
    {
        if (part_index >= data_parts.size())
            return {};

        auto component_guard = Coordination::setCurrentComponent("MergeTreeIndexSource::generate");

        const auto & part = data_parts[part_index];
        const auto & index_granularity = part->index_granularity;

        /// Both caches below describe one part, so drop what the previous part left.
        part_streams_by_logical_name.reset();
        marks_loaders.clear();

        size_t num_columns = header->columns();
        size_t num_rows = index_granularity->getMarksCount();

        const auto & part_name_column = StorageMergeTreeIndex::part_name_column;
        const auto & mark_number_column = StorageMergeTreeIndex::mark_number_column;
        const auto & rows_in_granule_column = StorageMergeTreeIndex::rows_in_granule_column;
        IMergeTreeDataPart::IndexPtr index_ptr;

        Columns result_columns(num_columns);
        for (size_t pos = 0; pos < num_columns; ++pos)
        {
            const auto & column_name = header->getByPosition(pos).name;
            const auto & column_type = header->getByPosition(pos).type;

            if (index_header->has(column_name))
            {
                if (!index_ptr)
                    index_ptr = part->getIndex();

                size_t index_position = index_header->getPositionByName(column_name);

                /// Some of the columns from suffix of primary index may be not loaded
                /// according to setting 'primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns'.
                if (index_position < index_ptr->size())
                {
                    result_columns[pos] = index_ptr->at(index_position);
                }
                else
                {
                    auto index_column = column_type->createColumnConstWithDefaultValue(num_rows);
                    result_columns[pos] = index_column->convertToFullColumnIfConst();
                }
            }
            else if (minmax_header && minmax_header->has(column_name))
            {
                FieldRef min_value(Null{});
                FieldRef max_value(Null{});

                size_t minmax_pos = minmax_header->getPositionByName(column_name);
                const auto & hyperrectangle = part->getMinMaxIndex()->hyperrectangle;
                if (minmax_pos < hyperrectangle.size())
                {
                    min_value = hyperrectangle[minmax_pos].left;
                    max_value = hyperrectangle[minmax_pos].right;
                }

                auto column = column_type->createColumnConst(num_rows, Tuple{std::move(min_value), std::move(max_value)});
                result_columns[pos] = column->convertToFullColumnIfConst();
            }
            else if (column_name == part_name_column.name)
            {
                auto column = column_type->createColumnConst(num_rows, part->name);
                result_columns[pos] = column->convertToFullColumnIfConst();
            }
            else if (column_name == mark_number_column.name)
            {
                auto column = column_type->createColumn();
                auto & data = assert_cast<ColumnUInt64 &>(*column).getData();

                data.resize(num_rows);
                std::iota(data.begin(), data.end(), 0);

                result_columns[pos] = std::move(column);
            }
            else if (column_name == rows_in_granule_column.name)
            {
                auto column = column_type->createColumn();
                auto & data = assert_cast<ColumnUInt64 &>(*column).getData();

                data.resize(num_rows);
                for (size_t i = 0; i < num_rows; ++i)
                    data[i] = index_granularity->getMarkRows(i);

                result_columns[pos] = std::move(column);
            }
            else if (auto [first, second] = Nested::splitName(column_name, true); with_marks && second == "mark")
            {
                result_columns[pos] = fillMarks(part, *column_type, first);
            }
            else
            {
                throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "No such column {}", column_name);
            }
        }

        ++part_index;
        return Chunk(std::move(result_columns), num_rows);
    }

private:
    std::shared_ptr<MergeTreeMarksLoader> createMarksLoader(const MergeTreeDataPartPtr & part, const String & prefix_name, size_t num_columns)
    {
        auto info_for_read = std::make_shared<LoadedMergeTreeDataPartInfoForReader>(part, std::make_shared<AlterConversions>());
        auto local_context = getContext();

        return std::make_shared<MergeTreeMarksLoader>(
            info_for_read,
            local_context->getMarkCache().get(),
            info_for_read->getIndexGranularityInfo().getMarksFilePath(prefix_name),
            info_for_read->getMarksCount(),
            info_for_read->getIndexGranularityInfo(),
            /*save_marks_in_cache=*/ false,
            local_context->getReadSettings(),
            /*load_marks_threadpool=*/ nullptr,
            num_columns,
            local_context->getSettingsRef()[Setting::use_streaming_marks_compression]);
    }

    /// A stream as the part addresses it -- by the column's stamped ID.
    struct PartStream
    {
        NameAndTypePair column_in_part;
        ISerialization::SubstreamPath substream_path;
    };

    /// A stream name as `getFileNameForStream` derives it from a column's CURRENT logical name --
    /// never `getFileNameForStreamByColumnId`. The header's `<stream>.mark` columns are named this way
    /// (see TableFunctionMergeTreeIndex), and the name is all `fillMarks` is handed.
    using LogicalStreamName = String;

    using PartStreamByLogicalName = std::unordered_map<LogicalStreamName, PartStream>;

    PartStreamByLogicalName buildPartStreamsByLogicalName(const MergeTreeDataPartPtr & part) const
    {
        PartStreamByLogicalName streams;
        ISerialization::StreamFileNameSettings stream_file_name_settings(*part->storage.getSettings());

        for (const auto & column : source_metadata->getColumns().getAllPhysical())
        {
            auto column_in_part = part->tryGetColumnBySnapshotName(column.name, source_metadata);
            if (!column_in_part)
                continue;

            /// The header also asks for the stream carrying the column itself, which for some types
            /// (e.g. Tuple) is not among the substreams. Insert it first so that it wins over an
            /// equally named substream, matching how the header was built.
            streams.emplace(escapeForFileName(column.name), PartStream{*column_in_part, {}});

            auto serialization = part->tryGetSerialization(column_in_part->getStorageKey());
            if (!serialization)
                continue;

            serialization->enumerateStreams([&](const auto & substream_path)
            {
                auto stream_name = ISerialization::getFileNameForStream(column.name, substream_path, stream_file_name_settings);
                streams.emplace(std::move(stream_name), PartStream{*column_in_part, substream_path});
            });
        }

        return streams;
    }

    ColumnPtr fillMarks(const MergeTreeDataPartPtr & part, const IDataType & data_type, const LogicalStreamName & logical_stream_name)
    {
        size_t num_rows = part->index_granularity->getMarksCount();

        if (!part_streams_by_logical_name)
            part_streams_by_logical_name = buildPartStreamsByLogicalName(part);

        std::optional<ColumnMarksLocation> marks_location;
        if (auto it = part_streams_by_logical_name->find(logical_stream_name); it != part_streams_by_logical_name->end())
            marks_location = part->getColumnMarksLocation(it->second.column_in_part, it->second.substream_path);

        /// The part stores nothing for this stream, so the column has no marks here.
        if (!marks_location)
            return data_type.createColumnConstWithDefaultValue(num_rows)->convertToFullColumnIfConst();

        auto & marks_loader = marks_loaders[marks_location->marks_stream_name];
        if (!marks_loader)
            marks_loader = createMarksLoader(part, marks_location->marks_stream_name, marks_location->columns_in_mark);

        auto compressed = ColumnUInt64::create(num_rows);
        auto uncompressed = ColumnUInt64::create(num_rows);

        auto & compressed_data = compressed->getData();
        auto & uncompressed_data = uncompressed->getData();

        auto marks_getter = marks_loader->loadMarks();

        for (size_t i = 0; i < num_rows; ++i)
        {
            auto mark = marks_getter->getMark(i, marks_location->index_in_mark);

            compressed_data[i] = mark.offset_in_compressed_file;
            uncompressed_data[i] = mark.offset_in_decompressed_block;
        }

        auto compressed_nullable = ColumnNullable::create(std::move(compressed), ColumnUInt8::create(num_rows, static_cast<UInt8>(0)));
        auto uncompressed_nullable = ColumnNullable::create(std::move(uncompressed), ColumnUInt8::create(num_rows, static_cast<UInt8>(0)));

        return ColumnTuple::create(Columns{std::move(compressed_nullable), std::move(uncompressed_nullable)});
    }

    SharedHeader header;
    SharedHeader index_header;
    SharedHeader minmax_header;
    MergeTreeData::DataPartsVector data_parts;
    bool with_marks;
    /// Source table's live metadata, captured once at pipeline init (this source holds no
    /// StorageSnapshot of the source table). It carries the current logical names the header is built
    /// from, and resolves them to a part's columns by their stable IDs.
    StorageMetadataHandle source_metadata;

    size_t part_index = 0;

    std::optional<PartStreamByLogicalName> part_streams_by_logical_name;
    /// Keyed by marks stream name, so a Wide part gets a loader per column and a Compact part one.
    std::unordered_map<String, std::shared_ptr<MergeTreeMarksLoader>> marks_loaders;
};

const ColumnWithTypeAndName StorageMergeTreeIndex::part_name_column{std::make_shared<DataTypeString>(), "part_name"};
const ColumnWithTypeAndName StorageMergeTreeIndex::mark_number_column{std::make_shared<DataTypeUInt64>(), "mark_number"};
const ColumnWithTypeAndName StorageMergeTreeIndex::rows_in_granule_column{std::make_shared<DataTypeUInt64>(), "rows_in_granule"};
const Block StorageMergeTreeIndex::virtuals_sample_block{part_name_column, mark_number_column, rows_in_granule_column};

StorageMergeTreeIndex::StorageMergeTreeIndex(
    const StorageID & table_id_,
    const StoragePtr & source_table_,
    const ColumnsDescription & columns,
    bool with_marks_,
    bool with_minmax_)
    : StorageWithCommonVirtualColumns(table_id_)
    , source_table(source_table_)
    , with_marks(with_marks_)
    , with_minmax(with_minmax_)
{
    const auto * merge_tree = dynamic_cast<const MergeTreeData *>(source_table.get());
    if (!merge_tree)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Storage MergeTreeIndex expected MergeTree table, got: {}", source_table->getName());

    data_parts = merge_tree->getDataPartsVectorForInternalUsage();
    std::erase_if(data_parts, [](const MergeTreeData::DataPartPtr & part) { return part->isEmpty(); });

    auto primary_key_metadata = merge_tree->getInMemoryMetadataPtr(CurrentThread::tryGetQueryContext(), false);
    key_sample_block = std::make_shared<const Block>(primary_key_metadata->getPrimaryKey().sample_block);

    if (with_minmax)
    {
        Block minmax_block;
        const auto metadata_snapshot = merge_tree->getInMemoryMetadataPtr(CurrentThread::tryGetQueryContext(), false);
        const auto & partition_key = metadata_snapshot->getPartitionKey();
        for (const auto & column : MergeTreeData::getMinMaxColumns(partition_key, merge_tree->getSettings()))
            minmax_block.insert({nullptr, std::make_shared<DataTypeTuple>(DataTypes{makeNullableSafe(column.type), makeNullableSafe(column.type)}), fmt::format("minmax_{}", column.name)});
        minmax_sample_block = std::make_shared<const Block>(std::move(minmax_block));
    }

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns);
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);
}

VirtualColumnsDescription StorageMergeTreeIndex::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    return desc;
}

class ReadFromMergeTreeIndex : public SourceStepWithFilter
{
public:
    std::string getName() const override { return "ReadFromMergeTreeIndex"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    ReadFromMergeTreeIndex(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        SharedHeader sample_block,
        std::shared_ptr<StorageMergeTreeIndex> storage_)
        : SourceStepWithFilter(
            std::move(sample_block),
            column_names_,
            query_info_,
            storage_snapshot_,
            context_)
        , storage(std::move(storage_))
        , log(&Poco::Logger::get("StorageMergeTreeIndex"))
    {
    }

    void applyFilters(ActionDAGNodes added_filter_nodes) override;

private:
    std::shared_ptr<StorageMergeTreeIndex> storage;
    Poco::Logger * log;
    ExpressionActionsPtr virtual_columns_filter;
};

void ReadFromMergeTreeIndex::applyFilters(ActionDAGNodes added_filter_nodes)
{
    SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));

    if (filter_actions_dag)
    {
        Block block_to_filter
        {
            { {}, std::make_shared<DataTypeString>(), StorageMergeTreeIndex::part_name_column.name },
        };

        auto dag = VirtualColumnUtils::splitFilterDagForAllowedInputs(filter_actions_dag->getOutputs().at(0), &block_to_filter, context);
        if (dag)
            virtual_columns_filter = VirtualColumnUtils::buildFilterExpression(std::move(*dag), context);
    }
}

void StorageMergeTreeIndex::readImpl(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum,
    size_t /*max_block_size*/,
    size_t /*num_streams*/)
{
    const auto storage_metadata = source_table->getInMemoryMetadataPtr(context, false);
    const auto & storage_columns = storage_metadata->getColumns();
    Names columns_from_storage;

    for (const auto & column_name : column_names)
    {
        if (storage_columns.hasColumnOrSubcolumn(GetColumnsOptions::All, column_name))
        {
            columns_from_storage.push_back(column_name);
            continue;
        }

        if (with_marks)
        {
            auto [first, second] = Nested::splitName(column_name, true);
            auto unescaped_name = unescapeForFileName(first);

            if (second == "mark" && storage_columns.hasColumnOrSubcolumn(GetColumnsOptions::All, unescapeForFileName(unescaped_name)))
            {
                columns_from_storage.push_back(unescaped_name);
                continue;
            }
        }
    }

    context->checkAccess(AccessType::SELECT, source_table->getStorageID(), columns_from_storage);

    auto sample_block = std::make_shared<const Block>(storage_snapshot->getSampleBlockForColumns(column_names));

    auto this_ptr = std::static_pointer_cast<StorageMergeTreeIndex>(shared_from_this());

    auto reading = std::make_unique<ReadFromMergeTreeIndex>(
        column_names, query_info, storage_snapshot,
        std::move(context), std::move(sample_block), std::move(this_ptr));

    query_plan.addStep(std::move(reading));
}

void ReadFromMergeTreeIndex::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto filtered_parts = VirtualColumnUtils::filterDataPartsWithExpression(storage->data_parts, virtual_columns_filter);

    LOG_DEBUG(log, "Reading index{}{} from {} parts of table {}",
        storage->with_marks ? " with marks" : "",
        storage->with_minmax ? " with minmax_idx" : "",
        filtered_parts.size(),
        storage->source_table->getStorageID().getNameForLogs());

    /// Capture the source table's live metadata once here (this step holds no StorageSnapshot
    /// of the source table); fillMarks uses its active column-ID mapping to resolve current
    /// names to stable IDs before locating the part column by ID.
    auto source_metadata = storage->source_table->getInMemoryMetadataPtr(context, false);

    pipeline.init(Pipe(std::make_shared<MergeTreeIndexSource>(
        getOutputHeader(),
        storage->key_sample_block,
        storage->minmax_sample_block,
        std::move(filtered_parts),
        context,
        storage->with_marks,
        std::move(source_metadata))));
}

}
