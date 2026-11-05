#include "config.h"
#if USE_AVRO

#include <chrono>
#include <cctype>

#include <Poco/String.h>

#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/DataLake/Common.h>
#include <Interpreters/Context.h>
#include <Processors/Sources/SourceFromSingleChunk.h>

#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ExecuteCommandArgs.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/FileNamesGenerator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergFieldParseHelpers.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergWrites.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SnapshotRefExecute.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LIMIT_EXCEEDED;
extern const int LOGICAL_ERROR;
}

namespace DataLakeStorageSetting
{
extern const DataLakeStorageSettingsBool iceberg_use_version_hint;
}

namespace Iceberg
{

namespace
{

constexpr auto MAX_TRANSACTION_RETRIES = 1000;

void validateRefName(const String & name, std::string_view command)
{
    if (name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} requires a non-empty snapshot reference name", command);

    if (name == f_main)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} cannot target the reserved Iceberg reference 'main'", command);

    for (char c : name)
    {
        const bool ok = std::isalnum(static_cast<unsigned char>(c)) || c == '_' || c == '-' || c == '.';
        if (!ok)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "{} snapshot reference name '{}' contains an invalid character; use letters, digits, '_', '-' or '.'",
                command,
                name);
    }
}

String normalizeRefType(const String & type, std::string_view command)
{
    String lowered = Poco::toLower(type);
    if (lowered != f_branch && lowered != f_tag)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} type must be 'branch' or 'tag', got '{}'", command, type);
    return lowered;
}

bool snapshotIdExists(const Poco::JSON::Object::Ptr & metadata, Int64 snapshot_id)
{
    if (!metadata->has(f_snapshots) || metadata->isNull(f_snapshots))
        return false;

    auto snapshots = metadata->getArray(f_snapshots);
    for (UInt32 i = 0; i < snapshots->size(); ++i)
    {
        auto snapshot = snapshots->getObject(i);
        if (snapshot && snapshot->has(f_metadata_snapshot_id)
            && snapshot->getValue<Int64>(f_metadata_snapshot_id) == snapshot_id)
            return true;
    }
    return false;
}

Int64 currentSnapshotIdOrThrow(const Poco::JSON::Object::Ptr & metadata, std::string_view command)
{
    if (!metadata->has(f_current_snapshot_id) || metadata->isNull(f_current_snapshot_id))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} requires a current snapshot; the table has none", command);

    Int64 snapshot_id = metadata->getValue<Int64>(f_current_snapshot_id);
    if (snapshot_id < 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} requires a current snapshot; the table has none", command);
    return snapshot_id;
}

Poco::JSON::Object::Ptr refsObject(const Poco::JSON::Object::Ptr & metadata)
{
    if (!metadata->has(f_refs) || metadata->isNull(f_refs))
        return nullptr;
    return metadata->getObject(f_refs);
}

Pipe resultPipe(const String & name, const String & type, Int64 snapshot_id, bool skipped)
{
    Block header{
        ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "name"),
        ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "type"),
        ColumnWithTypeAndName(std::make_shared<DataTypeInt64>(), "snapshot_id"),
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt8>(), "skipped"),
    };

    MutableColumns columns = header.cloneEmptyColumns();
    columns[0]->insert(name);
    columns[1]->insert(type);
    columns[2]->insert(snapshot_id);
    columns[3]->insert(UInt64(skipped ? 1 : 0));

    Chunk chunk(std::move(columns), 1);
    return Pipe(std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(std::move(header)), std::move(chunk)));
}

void touchLastUpdatedMs(Poco::JSON::Object::Ptr metadata)
{
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch());
    metadata->set(f_last_updated_ms, ms.count());
}

bool commitFileMetadata(
    Poco::JSON::Object::Ptr metadata,
    ContextPtr context,
    ObjectStoragePtr object_storage,
    const DataLakeStorageSettings & data_lake_settings,
    const PersistentTableComponents & persistent_table_components,
    const String & write_format,
    std::shared_ptr<DataLake::ICatalog> catalog,
    const String & table_name,
    Int32 last_version,
    CompressionMethod compression_method,
    LoggerPtr log)
{
    FileNamesGenerator filename_generator(
        persistent_table_components.path_resolver.getTableLocation(), false, CompressionMethod::None, write_format);
    filename_generator.setVersion(last_version + 1);
    filename_generator.setCompressionMethod(compression_method);

    std::string json_representation = stringifyJSON(metadata, 4);
    auto metadata_info = filename_generator.generateMetadataPathWithInfo();
    auto hint_path = filename_generator.generateVersionHint();
    if (!writeMetadataFileAndVersionHint(
            persistent_table_components.path_resolver,
            metadata_info,
            json_representation,
            hint_path,
            object_storage,
            context,
            data_lake_settings[DataLakeStorageSetting::iceberg_use_version_hint]))
    {
        LOG_WARNING(log, "Metadata commit conflict while updating snapshot references, retrying");
        return false;
    }

    if (catalog)
    {
        auto catalog_filename = persistent_table_components.path_resolver.resolveForCatalog(metadata_info.path);
        const auto & [namespace_name, parsed_table_name] = DataLake::parseTableName(table_name);
        if (!catalog->updateMetadata(namespace_name, parsed_table_name, catalog_filename, nullptr))
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Failed to update catalog metadata after writing a new metadata file. "
                "The table metadata may be in an inconsistent state");
        }
    }
    return true;
}

struct CreateOptions
{
    String name;
    String type;
    std::optional<Int64> snapshot_id;
    bool if_not_exists = false;
};

CreateOptions parseCreateOptions(const ASTPtr & args)
{
    static constexpr std::string_view cmd = "create_snapshot_ref";
    ExecuteCommandArgs schema(String(cmd));
    schema.addPositional("name", Field::Types::String);
    schema.addNamed("type", Field::Types::String);
    schema.addNamed("snapshot_id");
    schema.addNamed("if_not_exists");
    schema.addDefault("type", Field(String(f_branch)));
    schema.addDefault("if_not_exists", Field(UInt64(0)));

    auto parsed = schema.parse(args);
    CreateOptions options;
    options.name = parsed.getAs<String>("name");
    validateRefName(options.name, cmd);
    options.type = normalizeRefType(parsed.getAs<String>("type"), cmd);
    if (parsed.has("snapshot_id") && !parsed.get("snapshot_id").isNull())
        options.snapshot_id = fieldToInt64(parsed.get("snapshot_id"), cmd, "snapshot_id");
    options.if_not_exists = fieldToBool(parsed.get("if_not_exists"), cmd, "if_not_exists");
    return options;
}

struct DropOptions
{
    String name;
    bool if_exists = false;
};

DropOptions parseDropOptions(const ASTPtr & args)
{
    static constexpr std::string_view cmd = "drop_snapshot_ref";
    ExecuteCommandArgs schema(String(cmd));
    schema.addPositional("name", Field::Types::String);
    schema.addNamed("if_exists");
    schema.addDefault("if_exists", Field(UInt64(0)));

    auto parsed = schema.parse(args);
    DropOptions options;
    options.name = parsed.getAs<String>("name");
    validateRefName(options.name, cmd);
    options.if_exists = fieldToBool(parsed.get("if_exists"), cmd, "if_exists");
    return options;
}

}

Pipe executeCreateSnapshotRef(
    const ASTPtr & args,
    ContextPtr context,
    ObjectStoragePtr object_storage,
    const DataLakeStorageSettings & data_lake_settings,
    const PersistentTableComponents & persistent_table_components,
    const String & write_format,
    std::shared_ptr<DataLake::ICatalog> catalog,
    const String & table_name)
{
    auto options = parseCreateOptions(args);
    const bool catalog_commits_metadata = catalog && catalog->isTransactional();
    auto log = getLogger("IcebergSnapshotRef");

    int max_retries = MAX_TRANSACTION_RETRIES;
    while (--max_retries > 0)
    {
        auto [last_version, metadata_path, compression_method] = getLatestMetadataFileAndVersionWithCatalog(
            object_storage,
            catalog,
            table_name,
            persistent_table_components.table_path,
            data_lake_settings,
            persistent_table_components.metadata_cache,
            context,
            log.get(),
            persistent_table_components.table_uuid,
            persistent_table_components.metadata_compression_method);

        auto metadata = deepCopy(getMetadataJSONObject(
            metadata_path,
            object_storage,
            persistent_table_components.metadata_cache,
            context,
            log,
            compression_method,
            persistent_table_components.table_uuid));

        Int64 snapshot_id = options.snapshot_id.value_or(currentSnapshotIdOrThrow(metadata, "create_snapshot_ref"));
        if (!snapshotIdExists(metadata, snapshot_id))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "create_snapshot_ref snapshot id {} does not exist", snapshot_id);

        auto refs = refsObject(metadata);
        if (refs && refs->has(options.name))
        {
            auto existing = refs->getObject(options.name);
            Int64 existing_id = existing->getValue<Int64>(f_metadata_snapshot_id);
            String existing_type = existing->has(f_type) ? existing->getValue<String>(f_type) : String();
            if (options.if_not_exists)
                return resultPipe(options.name, existing_type.empty() ? options.type : existing_type, existing_id, true);

            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Iceberg snapshot reference '{}' already exists", options.name);
        }

        if (catalog_commits_metadata)
        {
            const auto & [namespace_name, parsed_table_name] = DataLake::parseTableName(table_name);
            auto committed = catalog->setSnapshotRef(
                namespace_name, parsed_table_name, metadata, options.name, options.type, snapshot_id);
            if (!committed)
            {
                LOG_WARNING(log, "Catalog commit conflict during create_snapshot_ref, retrying ({} retries left)", max_retries);
                continue;
            }
        }
        else
        {
            if (!metadata->has(f_refs) || metadata->isNull(f_refs))
                metadata->set(f_refs, Poco::JSON::Object::Ptr(new Poco::JSON::Object));

            Poco::JSON::Object::Ptr branch = new Poco::JSON::Object;
            branch->set(f_metadata_snapshot_id, snapshot_id);
            branch->set(f_type, options.type);
            metadata->getObject(f_refs)->set(options.name, branch);
            touchLastUpdatedMs(metadata);

            if (!commitFileMetadata(
                    metadata,
                    context,
                    object_storage,
                    data_lake_settings,
                    persistent_table_components,
                    write_format,
                    catalog,
                    table_name,
                    last_version,
                    compression_method,
                    log))
            {
                continue;
            }
        }

        persistent_table_components.invalidateMetadataCache();
        return resultPipe(options.name, options.type, snapshot_id, false);
    }

    throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Too many unsuccessful retries to create Iceberg snapshot reference '{}'", options.name);
}

Pipe executeDropSnapshotRef(
    const ASTPtr & args,
    ContextPtr context,
    ObjectStoragePtr object_storage,
    const DataLakeStorageSettings & data_lake_settings,
    const PersistentTableComponents & persistent_table_components,
    const String & write_format,
    std::shared_ptr<DataLake::ICatalog> catalog,
    const String & table_name)
{
    auto options = parseDropOptions(args);
    const bool catalog_commits_metadata = catalog && catalog->isTransactional();
    auto log = getLogger("IcebergSnapshotRef");

    int max_retries = MAX_TRANSACTION_RETRIES;
    while (--max_retries > 0)
    {
        auto [last_version, metadata_path, compression_method] = getLatestMetadataFileAndVersionWithCatalog(
            object_storage,
            catalog,
            table_name,
            persistent_table_components.table_path,
            data_lake_settings,
            persistent_table_components.metadata_cache,
            context,
            log.get(),
            persistent_table_components.table_uuid,
            persistent_table_components.metadata_compression_method);

        auto metadata = deepCopy(getMetadataJSONObject(
            metadata_path,
            object_storage,
            persistent_table_components.metadata_cache,
            context,
            log,
            compression_method,
            persistent_table_components.table_uuid));

        auto refs = refsObject(metadata);
        if (!refs || !refs->has(options.name))
        {
            if (options.if_exists)
                return resultPipe(options.name, "", 0, true);
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Iceberg snapshot reference '{}' does not exist", options.name);
        }

        auto existing = refs->getObject(options.name);
        Int64 snapshot_id = existing->getValue<Int64>(f_metadata_snapshot_id);
        String type = existing->has(f_type) ? existing->getValue<String>(f_type) : String();

        if (catalog_commits_metadata)
        {
            const auto & [namespace_name, parsed_table_name] = DataLake::parseTableName(table_name);
            auto committed = catalog->removeSnapshotRef(namespace_name, parsed_table_name, metadata, options.name);
            if (!committed)
            {
                LOG_WARNING(log, "Catalog commit conflict during drop_snapshot_ref, retrying ({} retries left)", max_retries);
                continue;
            }
        }
        else
        {
            metadata->getObject(f_refs)->remove(options.name);
            touchLastUpdatedMs(metadata);

            if (!commitFileMetadata(
                    metadata,
                    context,
                    object_storage,
                    data_lake_settings,
                    persistent_table_components,
                    write_format,
                    catalog,
                    table_name,
                    last_version,
                    compression_method,
                    log))
            {
                continue;
            }
        }

        persistent_table_components.invalidateMetadataCache();
        return resultPipe(options.name, type, snapshot_id, false);
    }

    throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Too many unsuccessful retries to drop Iceberg snapshot reference '{}'", options.name);
}

}

}

#endif
