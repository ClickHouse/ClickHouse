#include <Processors/QueryPlan/LazilyReadFromObjectStorage.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <Common/typeid_cast.h>
#include <Processors/Sources/LazyReadFromObjectStorageSource.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TOO_MANY_ROWS;
}

UInt64 LazyObjectStorageFileRegistry::registerFile(const ObjectInfoPtr & object_info)
{
    std::lock_guard lock(mutex);
    if (files.size() >= MAX_FILES)
        throw Exception(ErrorCodes::TOO_MANY_ROWS,
            "Too many files ({}) are read by a query with lazy materialization. "
            "Disable the query_plan_optimize_lazy_materialization_for_object_storage setting",
            files.size());
    files.push_back(object_info);
    return files.size() - 1;
}

namespace
{

std::optional<size_t> countLeafColumnsOfType(const IDataType & type)
{
    if (isObject(type) || isDynamic(type))
        return std::nullopt;
    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(&type))
        return countLeafColumnsOfType(*nullable->getNestedType());
    if (const auto * low_cardinality = typeid_cast<const DataTypeLowCardinality *>(&type))
        return countLeafColumnsOfType(*low_cardinality->getDictionaryType());
    if (const auto * array = typeid_cast<const DataTypeArray *>(&type))
        return countLeafColumnsOfType(*array->getNestedType());
    if (const auto * map = typeid_cast<const DataTypeMap *>(&type))
    {
        const auto keys = countLeafColumnsOfType(*map->getKeyType());
        const auto values = countLeafColumnsOfType(*map->getValueType());
        if (!keys || !values)
            return std::nullopt;
        return *keys + *values;
    }
    if (const auto * tuple = typeid_cast<const DataTypeTuple *>(&type))
    {
        size_t leaves = 0;
        for (const auto & element : tuple->getElements())
        {
            const auto element_leaves = countLeafColumnsOfType(*element);
            if (!element_leaves)
                return std::nullopt;
            leaves += *element_leaves;
        }
        return std::max<size_t>(leaves, 1);
    }
    return 1;
}

}

std::optional<size_t> countLeafColumns(const Block & header)
{
    size_t leaves = 0;
    for (const auto & column : header)
    {
        const auto column_leaves = countLeafColumnsOfType(*column.type);
        if (!column_leaves)
            return std::nullopt;
        leaves += *column_leaves;
    }
    return leaves;
}

ObjectStorageLazyMaterializingRows::ObjectStorageLazyMaterializingRows(LazyObjectStorageFileRegistryPtr file_registry_)
    : file_registry(std::move(file_registry_))
{
}

void ObjectStorageLazyMaterializingRows::filterRangesAndFillRows(const PaddedPODArray<UInt64> & sorted_indexes)
{
    rows_in_files.clear();

    std::lock_guard lock(file_registry->mutex);

    for (size_t i = 0; i < sorted_indexes.size();)
    {
        UInt64 file_index = sorted_indexes[i] >> LazyObjectStorageFileRegistry::ROW_INDEX_BITS;
        if (file_index >= file_registry->files.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Global row index refers to file {} while only {} files were registered",
                file_index, file_registry->files.size());

        auto rows = std::make_shared<PaddedPODArray<UInt64>>();
        while (i < sorted_indexes.size()
            && (sorted_indexes[i] >> LazyObjectStorageFileRegistry::ROW_INDEX_BITS) == file_index)
        {
            rows->push_back(sorted_indexes[i] & LazyObjectStorageFileRegistry::ROW_INDEX_MASK);
            ++i;
        }

        rows_in_files.push_back({file_registry->files[file_index], std::move(rows)});
    }

    LOG_TRACE(getLogger("ObjectStorageLazyMaterializingRows"), "Lazily reading {} rows from {} files",
        sorted_indexes.size(), rows_in_files.size());
}

LazilyReadFromObjectStorage::LazilyReadFromObjectStorage(
    SharedHeader header,
    const StorageID & storage_id_,
    ObjectStoragePtr object_storage_,
    StorageObjectStorageConfigurationPtr configuration_,
    StorageSnapshotPtr storage_snapshot_,
    const std::optional<DB::FormatSettings> & format_settings_,
    ReadFromFormatInfo info_,
    ContextPtr context_,
    size_t max_block_size_,
    size_t main_read_leaf_columns_,
    size_t max_num_streams_)
    : ISourceStep(std::move(header))
    , storage_id(storage_id_)
    , object_storage(std::move(object_storage_))
    , configuration(std::move(configuration_))
    , storage_snapshot(std::move(storage_snapshot_))
    , format_settings(format_settings_)
    , info(std::move(info_))
    , context(std::move(context_))
    , max_block_size(max_block_size_)
    , main_read_leaf_columns(main_read_leaf_columns_)
    , max_num_streams(max_num_streams_)
{
}

void LazilyReadFromObjectStorage::setLazyMaterializingRows(ObjectStorageLazyMaterializingRowsPtr lazy_materializing_rows_)
{
    lazy_materializing_rows = std::move(lazy_materializing_rows_);
}

void LazilyReadFromObjectStorage::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    if (!lazy_materializing_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "LazilyReadFromObjectStorage: lazy_materializing_rows is not set");

    auto source = std::make_shared<LazyReadFromObjectStorageSource>(
        getOutputHeader(),
        storage_id,
        object_storage,
        configuration,
        storage_snapshot,
        format_settings,
        info,
        context,
        max_block_size,
        std::min(settings.max_threads, max_num_streams),
        main_read_leaf_columns,
        lazy_materializing_rows);

    processors.emplace_back(source);
    Pipe pipe(std::move(source));
    pipeline.init(std::move(pipe));
}

void LazilyReadFromObjectStorage::describeActions(FormatSettings & settings) const
{
    const String & prefix = settings.detail_prefix;

    settings.out << prefix << "Lazily read columns: ";

    bool first = true;
    for (const auto & column : *getOutputHeader())
    {
        if (!first)
            settings.out << ", ";
        first = false;

        settings.out << column.name;
    }

    settings.out << '\n';
}

void LazilyReadFromObjectStorage::describeActions(JSONBuilder::JSONMap & map) const
{
    auto json_array = std::make_unique<JSONBuilder::JSONArray>();

    for (const auto & column : *getOutputHeader())
        json_array->add(column.name);

    map.add("Lazily read columns", std::move(json_array));
}

}
