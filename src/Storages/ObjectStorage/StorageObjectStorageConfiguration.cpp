#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>

#include <Storages/NamedCollectionsHelpers.h>
#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>
#include <Storages/ObjectStorage/StorageObjectStorageSink.h>
#include <Interpreters/Context.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

bool StorageObjectStorageConfiguration::update( ///NOLINT
    ObjectStoragePtr object_storage_ptr,
    ContextPtr context,
    bool /* if_not_updated_before */,
    bool /* check_consistent_with_previous_metadata */)
{
    IObjectStorage::ApplyNewSettingsOptions options{.allow_client_change = !isStaticConfiguration()};
    object_storage_ptr->applyNewSettings(context->getConfigRef(), getTypeName() + ".", context, options);
    return true;
}

void StorageObjectStorageConfiguration::create( ///NOLINT
    ObjectStoragePtr object_storage_ptr,
    ContextPtr context,
    const std::optional<ColumnsDescription> & /*columns*/,
    ASTPtr /*partition_by*/,
    bool /*if_not_exists*/,
    std::shared_ptr<DataLake::ICatalog> /*catalog*/,
        const StorageID & /*table_id_*/)
{
    IObjectStorage::ApplyNewSettingsOptions options{.allow_client_change = !isStaticConfiguration()};
    object_storage_ptr->applyNewSettings(context->getConfigRef(), getTypeName() + ".", context, options);
}

ReadFromFormatInfo StorageObjectStorageConfiguration::prepareReadingFromFormat(
    ObjectStoragePtr,
    const Strings & requested_columns,
    const StorageSnapshotPtr & storage_snapshot,
    bool supports_subset_of_columns,
    bool supports_tuple_elements,
    ContextPtr local_context,
    const PrepareReadingFromFormatHiveParams & hive_parameters)
{
    return DB::prepareReadingFromFormat(requested_columns, storage_snapshot, local_context, supports_subset_of_columns, supports_tuple_elements, hive_parameters);
}

std::optional<ColumnsDescription> StorageObjectStorageConfiguration::tryGetTableStructureFromMetadata() const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method tryGetTableStructureFromMetadata is not implemented for basic configuration");
}

void StorageObjectStorageConfiguration::initialize(
    StorageObjectStorageConfiguration & configuration_to_initialize,
    ASTs & engine_args,
    ContextPtr local_context,
    bool with_table_structure)
{
    if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, local_context))
        configuration_to_initialize.fromNamedCollection(*named_collection, local_context);
    else
        configuration_to_initialize.fromAST(engine_args, local_context, with_table_structure);

    if (configuration_to_initialize.isNamespaceWithGlobs())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Expression can not have wildcards inside {} name", configuration_to_initialize.getNamespaceType());

    if (configuration_to_initialize.isDataLakeConfiguration())
    {
        if (configuration_to_initialize.partition_strategy_type != PartitionStrategyFactory::StrategyType::NONE)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The `partition_strategy` argument is incompatible with data lakes");
        }
    }
    else if (configuration_to_initialize.partition_strategy_type == PartitionStrategyFactory::StrategyType::NONE)
    {
        if (configuration_to_initialize.getRawPath().hasPartitionWildcard())
        {
            // Promote to wildcard in case it is not data lake to make it backwards compatible
            configuration_to_initialize.partition_strategy_type = PartitionStrategyFactory::StrategyType::WILDCARD;
        }
    }

    if (configuration_to_initialize.format == "auto")
    {
        if (configuration_to_initialize.isDataLakeConfiguration())
        {
            configuration_to_initialize.format = "Parquet";
        }
        else
        {
            configuration_to_initialize.format
                = FormatFactory::instance()
                      .tryGetFormatFromFileName(configuration_to_initialize.isArchive() ? configuration_to_initialize.getPathInArchive() : configuration_to_initialize.getRawPath().path)
                      .value_or("auto");
        }
    }
    else
        FormatFactory::instance().checkFormatName(configuration_to_initialize.format);

    /// It might be changed on `StorageObjectStorageConfiguration::initPartitionStrategy`
    configuration_to_initialize.read_path = configuration_to_initialize.getRawPath();
    configuration_to_initialize.initialized = true;
}

void StorageObjectStorageConfiguration::initPartitionStrategy(ASTPtr partition_by, const ColumnsDescription & columns, ContextPtr context)
{
    partition_strategy = PartitionStrategyFactory::get(
        partition_strategy_type,
        partition_by,
        columns.getOrdinary(),
        context,
        format,
        getRawPath().hasGlobs(),
        getRawPath().hasPartitionWildcard(),
        partition_columns_in_data_file);

    if (partition_strategy)
    {
        read_path = partition_strategy->getPathForRead(getRawPath().path);
        LOG_DEBUG(getLogger("StorageObjectStorageConfiguration"), "Initialized partition strategy {}", magic_enum::enum_name(partition_strategy_type));
    }
}

const StorageObjectStorageConfiguration::Path & StorageObjectStorageConfiguration::getPathForRead() const
{
    return read_path;
}

StorageObjectStorageConfiguration::Path StorageObjectStorageConfiguration::getPathForWrite(const std::string & partition_id) const
{
    auto raw_path = getRawPath();

    if (!partition_strategy)
    {
        return raw_path;
    }

    return Path {partition_strategy->getPathForWrite(raw_path.path, partition_id)};
}

bool StorageObjectStorageConfiguration::Path::hasPartitionWildcard() const
{
    static const String PARTITION_ID_WILDCARD = "{_partition_id}";
    return path.find(PARTITION_ID_WILDCARD) != String::npos;
}

bool StorageObjectStorageConfiguration::Path::hasGlobsIgnorePartitionWildcard() const
{
    if (!hasPartitionWildcard())
        return hasGlobs();
    return PartitionedSink::replaceWildcards(path, "").find_first_of("*?{") != std::string::npos;
}

bool StorageObjectStorageConfiguration::Path::hasGlobs() const
{
    return path.find_first_of("*?{") != std::string::npos;
}

std::string StorageObjectStorageConfiguration::Path::cutGlobs(bool supports_partial_prefix) const
{
    if (supports_partial_prefix)
    {
        return path.substr(0, path.find_first_of("*?{"));
    }

    auto first_glob_pos = path.find_first_of("*?{");
    auto end_of_path_without_globs = path.substr(0, first_glob_pos).rfind('/');
    if (end_of_path_without_globs == std::string::npos || end_of_path_without_globs == 0)
        return "/";
    return path.substr(0, end_of_path_without_globs);
}

void StorageObjectStorageConfiguration::check(ContextPtr) const
{
    FormatFactory::instance().checkFormatName(format);
}

bool StorageObjectStorageConfiguration::isNamespaceWithGlobs() const
{
    return getNamespace().find_first_of("*?{") != std::string::npos;
}

bool StorageObjectStorageConfiguration::isPathInArchiveWithGlobs() const
{
    return getPathInArchive().find_first_of("*?{") != std::string::npos;
}

std::string StorageObjectStorageConfiguration::getPathInArchive() const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Path {} is not archive", getRawPath().path);
}

void StorageObjectStorageConfiguration::assertInitialized() const
{
    if (!initialized)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Configuration was not initialized before usage");
    }
}

void StorageObjectStorageConfiguration::addDeleteTransformers(
    ObjectInfoPtr,
    QueryPipelineBuilder &,
    const std::optional<FormatSettings> &,
    ContextPtr) const
{
}

}
