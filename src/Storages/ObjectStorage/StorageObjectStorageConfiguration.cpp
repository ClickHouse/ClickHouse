#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>

#include <Storages/NamedCollectionsHelpers.h>
#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>
#include <Storages/ObjectStorage/StorageObjectStorageSink.h>
#include <Interpreters/Context.h>
#include <Common/logger_useful.h>
#include <Common/SipHash.h>
#include <Core/Settings.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ObjectStorage/Common.h>

#include <boost/algorithm/string/replace.hpp>

namespace DB
{

namespace DataLakeStorageSetting
{
    extern const DataLakeStorageSettingsString disk;
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

void StorageObjectStorageConfiguration::update( ///NOLINT
    ObjectStoragePtr object_storage_ptr,
    ContextPtr context)
{
    IObjectStorage::ApplyNewSettingsOptions options{.allow_client_change = !isStaticConfiguration()};
    object_storage_ptr->applyNewSettings(context->getConfigRef(), getTypeName() + ".", context, options);
}

void StorageObjectStorageConfiguration::lazyInitializeIfNeeded(
    ObjectStoragePtr object_storage_ptr,
    ContextPtr context)
{
    update(object_storage_ptr, context);
}

void StorageObjectStorageConfiguration::create( ///NOLINT
    ObjectStoragePtr /*object_storage_ptr*/,
    ContextPtr /*context*/,
    const std::optional<ColumnsDescription> & /*columns*/,
    ASTPtr /*partition_by*/,
    ASTPtr /*order_by*/,
    bool /*if_not_exists*/,
    std::shared_ptr<DataLake::ICatalog> /*catalog*/,
        const StorageID & /*table_id_*/)
{
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

std::optional<ColumnsDescription> StorageObjectStorageConfiguration::tryGetTableStructureFromMetadata(ContextPtr) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method tryGetTableStructureFromMetadata is not implemented for basic configuration");
}

std::optional<DataLakeTableStateSnapshot> StorageObjectStorageConfiguration::getTableStateSnapshot(ContextPtr) const
{
    return std::nullopt;
}

std::unique_ptr<StorageInMemoryMetadata> StorageObjectStorageConfiguration::buildStorageMetadataFromState(
    const DataLakeTableStateSnapshot &, ContextPtr) const
{
    return nullptr;
}

bool StorageObjectStorageConfiguration::shouldReloadSchemaForConsistency(ContextPtr) const
{
    return false;
}


void StorageObjectStorageConfiguration::initialize(
    ASTs & engine_args,
    ContextPtr local_context,
    bool with_table_structure,
    const StorageID * table_id)
{
    std::string disk_name;
    if (isDataLakeConfiguration())
    {
        const auto & storage_settings = getDataLakeSettings();
        disk_name = storage_settings[DataLakeStorageSetting::disk].changed
            ? storage_settings[DataLakeStorageSetting::disk].value
            : "";
    }
    if (!disk_name.empty())
        fromDisk(disk_name, engine_args, local_context, with_table_structure);
    else if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, local_context, true, nullptr, table_id))
        fromNamedCollection(*named_collection, local_context);
    else
        fromAST(engine_args, local_context, with_table_structure);

    if (isNamespaceWithGlobs())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Expression can not have wildcards inside {} name", getNamespaceType());

    if (isDataLakeConfiguration())
    {
        if (getPartitionStrategyType() != PartitionStrategyFactory::StrategyType::NONE)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The `partition_strategy` argument is incompatible with data lakes");
        }
    }
    else if (getPartitionStrategyType() == PartitionStrategyFactory::StrategyType::NONE)
    {
        if (getRawPath().hasPartitionWildcard())
        {
            // Promote to wildcard in case it is not data lake to make it backwards compatible
            setPartitionStrategyType(PartitionStrategyFactory::StrategyType::WILDCARD);
        }
    }

    if (format == "auto")
    {
        if (isDataLakeConfiguration())
        {
            format = "Parquet";
        }
        else
        {
            format
                = FormatFactory::instance()
                      .tryGetFormatFromFileName(isArchive() ? getPathInArchive() : getRawPath().path)
                      .value_or("auto");
        }
    }
    else
        FormatFactory::instance().checkFormatName(format);

    /// It might be changed on `StorageObjectStorageConfiguration::initPartitionStrategy`
    /// We shouldn't set path for disk setup because path prefix is already set in used object_storage.
    if (disk_name.empty())
        read_path = getRawPath();

    initialized = true;
}

String StorageObjectStorageConfiguration::computeSchemaHash(const ColumnsDescription & columns)
{
    SipHash hash;
    auto columns_str = columns.getAllPhysical().toString();
    hash.update(columns_str.data(), columns_str.size());
    return getSipHash128AsHexString(hash);
}

void StorageObjectStorageConfiguration::setSchemaHash(const String & hash)
{
    schema_hash = hash;
    boost::replace_all(read_path.path, SCHEMA_HASH_WILDCARD, schema_hash);

    if (getPaths().size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected exactly one path when setting schema hash, got {}", getPaths().size());
    auto path = getRawPath();
    boost::replace_all(path.path, SCHEMA_HASH_WILDCARD, schema_hash);
    setRawPath(path);
    setPaths({path});
}

void StorageObjectStorageConfiguration::initPartitionStrategy(ASTPtr partition_by, const ColumnsDescription & columns, ContextPtr context)
{
    partition_strategy = PartitionStrategyFactory::get(
        partition_strategy_type,
        partition_by,
        columns.getOrdinary(),
        context,
        format,
        getRawPath().hasGlobsIgnorePlaceholders(),
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

    if (!schema_hash.empty())
        boost::replace_all(raw_path.path, SCHEMA_HASH_WILDCARD, schema_hash);

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

bool StorageObjectStorageConfiguration::Path::hasSchemaHashWildcard() const
{
    return path.find(StorageObjectStorageConfiguration::SCHEMA_HASH_WILDCARD) != String::npos;
}

bool StorageObjectStorageConfiguration::Path::hasGlobsIgnorePlaceholders() const
{
    if (!hasPartitionWildcard() && !hasSchemaHashWildcard())
        return hasGlobs();
    String cleaned = PartitionedSink::replaceWildcards(path, "");
    boost::replace_all(cleaned, StorageObjectStorageConfiguration::SCHEMA_HASH_WILDCARD, "");
    return cleaned.find_first_of("*?{") != std::string::npos;
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

void StorageObjectStorageConfiguration::check(ContextPtr)
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
    FormatParserSharedResourcesPtr,
    ContextPtr) const
{
}

void StorageObjectStorageConfiguration::initializeFromParsedArguments(const StorageParsedArguments & parsed_arguments)
{
    format = parsed_arguments.format;
    compression_method = parsed_arguments.compression_method;
    structure = parsed_arguments.structure;
    partition_strategy_type = parsed_arguments.partition_strategy_type;
    partition_columns_in_data_file = parsed_arguments.partition_columns_in_data_file;
    partition_strategy = parsed_arguments.partition_strategy;
}
}
