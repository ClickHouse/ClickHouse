#include <Columns/ColumnString.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/DataLake/Common.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergWrites.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Mutations.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteTransform.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Poco/JSON/Object.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace DB::DataLakeStorageSetting
{
extern const DataLakeStorageSettingsBool iceberg_use_version_hint;
}

namespace Iceberg
{

static constexpr const char * block_datafile_path = "_path";
static constexpr const char * block_row_number = "_row_number";

struct DeleteFileWriteResult
{
    FileNamesGenerator::Result path;
    Int32 total_rows;
    Int32 total_bytes;
};

DeleteFileWriteResult writeDataFiles(
    const MutationCommands & commands,
    ContextPtr context,
    StorageMetadataPtr metadata,
    StorageID storage_id,
    ObjectStoragePtr object_storage,
    StorageObjectStorageConfigurationPtr configuration,
    FileNamesGenerator & generator,
    const std::optional<FormatSettings> & format_settings)
{
    if (commands.empty())
        return {};

    chassert(commands.size() == 1);

    auto storage_ptr = DatabaseCatalog::instance().getTable(storage_id, context);

    if (commands.front().type == MutationCommand::Type::DELETE)
    {
        MutationsInterpreter::Settings settings(true);
        settings.return_all_columns = true;
        settings.return_mutated_rows = true;

        auto interpreter = std::make_unique<MutationsInterpreter>(storage_ptr, metadata, commands, context, settings);
        auto pipeline = QueryPipelineBuilder::getPipeline(interpreter->execute());
        PullingPipelineExecutor executor(pipeline);

        auto header = interpreter->getUpdatedHeader();

        auto delete_file_info = generator.generatePositionDeleteFile();
        auto write_buffer = object_storage->writeObject(
            StoredObject(delete_file_info.path_in_storage),
            WriteMode::Rewrite,
            std::nullopt,
            DBMS_DEFAULT_BUFFER_SIZE,
            context->getWriteSettings());

        ColumnsWithTypeAndName delete_file_columns_desc;
        delete_file_columns_desc.push_back(
            ColumnWithTypeAndName(std::make_shared<DataTypeString>(), IcebergPositionDeleteTransform::data_file_path_column_name));
        delete_file_columns_desc.push_back(
            ColumnWithTypeAndName(std::make_shared<DataTypeInt64>(), IcebergPositionDeleteTransform::positions_column_name));

        Block delete_file_sample_block(delete_file_columns_desc);
        auto output_format = FormatFactory::instance().getOutputFormat(
            configuration->format, *write_buffer, delete_file_sample_block, context, format_settings);

        Block block;
        Int32 total_rows = 0;
        Int32 total_bytes = 0;
        while (executor.pull(block))
        {
            auto col_data_filename = block.getByName(block_datafile_path);
            auto col_position = block.getByName(block_row_number);

            auto col_data_filename_without_namespaces = ColumnString::create();
            for (size_t i = 0; i < col_data_filename.column->size(); ++i)
            {
                Field cur_value;
                col_data_filename.column->get(i, cur_value);

                String path_without_namespace;
                if (cur_value.safeGet<String>().starts_with(configuration->getNamespace()))
                    path_without_namespace = cur_value.safeGet<String>().substr(configuration->getNamespace().size());

                if (!path_without_namespace.starts_with(configuration->getPathForRead().path))
                {
                    if (path_without_namespace.starts_with('/'))
                        path_without_namespace = path_without_namespace.substr(1);
                    else
                        path_without_namespace = "/" + path_without_namespace;
                }
                col_data_filename_without_namespaces->insert(path_without_namespace);
            }
            col_data_filename.column = std::move(col_data_filename_without_namespaces);
            Block delete_file_block({col_data_filename, col_position});
            total_rows += delete_file_block.rows();
            output_format->write(delete_file_block);
        }

        total_bytes = static_cast<Int32>(write_buffer->count());
        output_format->flush();
        output_format->finalize();
        write_buffer->finalize();
        return DeleteFileWriteResult{
            .path = delete_file_info,
            .total_rows = total_rows,
            .total_bytes = total_bytes,
        };
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Iceberg supports only delete mutations");
}

bool writeMetadataFiles(
    const DeleteFileWriteResult & delete_filename,
    ObjectStoragePtr object_storage,
    StorageObjectStorageConfigurationPtr configuration,
    ContextPtr context,
    FileNamesGenerator & filename_generator,
    std::shared_ptr<DataLake::ICatalog> catalog,
    StorageID table_id)
{
    auto log = getLogger("IcebergMutations");
    auto [last_version, metadata_path, compression_method]
        = getLatestOrExplicitMetadataFileAndVersion(object_storage, configuration, nullptr, context, log.get());

    filename_generator.setVersion(last_version + 1);
    auto metadata = getMetadataJSONObject(metadata_path, object_storage, configuration, nullptr, context, log, compression_method);
    auto partition_spec_id = metadata->getValue<Int64>(Iceberg::f_default_spec_id);
    auto partitions_specs = metadata->getArray(Iceberg::f_partition_specs);
    Poco::JSON::Object::Ptr partititon_spec;
    for (size_t i = 0; i < partitions_specs->size(); ++i)
    {
        auto current_partition_spec = partitions_specs->getObject(static_cast<UInt32>(i));
        if (current_partition_spec->getValue<Int64>(Iceberg::f_spec_id) == partition_spec_id)
        {
            partititon_spec = current_partition_spec;
            break;
        }
    }

    auto [metadata_name, storage_metadata_name] = filename_generator.generateMetadataName();
    Int64 parent_snapshot = -1;
    if (metadata->has(Iceberg::f_current_snapshot_id))
        parent_snapshot = metadata->getValue<Int64>(Iceberg::f_current_snapshot_id);

    auto [new_snapshot, manifest_list_name, storage_manifest_list_name]
        = MetadataGenerator(metadata).generateNextMetadata(filename_generator, metadata_name, parent_snapshot, 0, 0, 0, 1, 1);

    Strings manifest_entries_in_storage;
    Strings manifest_entries;
    Int32 manifest_lengths = 0;

    {
        auto [manifest_entry_name, storage_manifest_entry_name] = filename_generator.generateManifestEntryName();
        manifest_entries_in_storage.push_back(storage_manifest_entry_name);
        manifest_entries.push_back(manifest_entry_name);

        auto buffer_manifest_entry = object_storage->writeObject(
            StoredObject(storage_manifest_entry_name),
            WriteMode::Rewrite,
            std::nullopt,
            DBMS_DEFAULT_BUFFER_SIZE,
            context->getWriteSettings());
        generateManifestFile(
            metadata,
            {},
            {},
            delete_filename.path.path_in_metadata,
            new_snapshot,
            configuration->format,
            partititon_spec,
            partition_spec_id,
            *buffer_manifest_entry,
            Iceberg::FileContentType::POSITION_DELETE);
        buffer_manifest_entry->finalize();
        manifest_lengths += buffer_manifest_entry->count();
    }

    {
        auto buffer_manifest_list = object_storage->writeObject(
            StoredObject(storage_manifest_list_name),
            WriteMode::Rewrite,
            std::nullopt,
            DBMS_DEFAULT_BUFFER_SIZE,
            context->getWriteSettings());

        generateManifestList(
            filename_generator,
            metadata,
            object_storage,
            context,
            manifest_entries,
            new_snapshot,
            manifest_lengths,
            *buffer_manifest_list,
            Iceberg::FileContentType::POSITION_DELETE);
        buffer_manifest_list->finalize();
    }

    {
        std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        Poco::JSON::Stringifier::stringify(metadata, oss, 4);
        std::string json_representation = removeEscapedSlashes(oss.str());

        auto cleanup = [&]()
        {
            for (const auto & manifest_filename_in_storage : manifest_entries_in_storage)
                object_storage->removeObjectIfExists(StoredObject(manifest_filename_in_storage));

            object_storage->removeObjectIfExists(StoredObject(storage_manifest_list_name));
        };

        if (object_storage->exists(StoredObject(storage_metadata_name)))
        {
            cleanup();
            return false;
        }

        Iceberg::writeMessageToFile(json_representation, storage_metadata_name, object_storage, context);

        if (configuration->getDataLakeSettings()[DataLakeStorageSetting::iceberg_use_version_hint].value)
        {
            auto filename_version_hint = filename_generator.generateVersionHint();
            Iceberg::writeMessageToFile(storage_metadata_name, filename_version_hint.path_in_storage, object_storage, context);
        }
        if (catalog)
        {
            String catalog_filename = metadata_name;
            if (!catalog_filename.starts_with(configuration->getTypeName()))
                catalog_filename = configuration->getTypeName() + "://" + configuration->getNamespace() + "/" + metadata_name;

            const auto & [namespace_name, table_name] = DataLake::parseTableName(table_id.getTableName());
            if (!catalog->updateMetadata(namespace_name, table_name, catalog_filename, new_snapshot))
            {
                cleanup();
                object_storage->removeObjectIfExists(StoredObject(storage_metadata_name));
                return false;
            }
        }
    }
    return true;
}

void mutate(
    const MutationCommands & commands,
    ContextPtr context,
    StorageMetadataPtr metadata,
    StorageID storage_id,
    ObjectStoragePtr object_storage,
    StorageObjectStorageConfigurationPtr configuration,
    const std::optional<FormatSettings> & format_settings,
    std::shared_ptr<DataLake::ICatalog> catalog,
    StorageID table_id)
{
    FileNamesGenerator generator(configuration->getRawPath().path, configuration->getRawPath().path, false);
    auto delete_file = writeDataFiles(commands, context, metadata, storage_id, object_storage, configuration, generator, format_settings);
    writeMetadataFiles(delete_file, object_storage, configuration, context, generator, catalog, table_id);
}

}
