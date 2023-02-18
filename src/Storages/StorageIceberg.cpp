#include "config.h"

#if USE_AWS_S3 && USE_AVRO

#    include <Storages/StorageIceberg.h>
#    include <Common/logger_useful.h>

#    include <Columns/ColumnString.h>
#    include <Columns/ColumnTuple.h>
#    include <Columns/IColumn.h>

#    include <Storages/StorageFactory.h>

#    include <Formats/FormatFactory.h>

#    include <fmt/format.h>

#    include <Processors/Formats/Impl/AvroRowInputFormat.h>

#    include <Poco/JSON/Array.h>
#    include <Poco/JSON/Object.h>
#    include <Poco/JSON/Parser.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int S3_ERROR;
    extern const int FILE_DOESNT_EXIST;
    extern const int ILLEGAL_COLUMN;
}

template <typename Configuration, typename MetadataReadHelper>
IcebergMetadataParser<Configuration, MetadataReadHelper>::IcebergMetadataParser(const Configuration & configuration_, ContextPtr context_)
    : base_configuration(configuration_), context(context_)
{
}

template <typename Configuration, typename MetadataReadHelper>
std::vector<String> IcebergMetadataParser<Configuration, MetadataReadHelper>::getFiles() const
{
    auto metadata = fetchMetadataFile();
    auto manifest_list = getManifestList(metadata);

    /// When table first created and does not have any data
    if (manifest_list.empty())
    {
        return {};
    }

    auto manifest_files = getManifestFiles(manifest_list);
    return getFilesForRead(manifest_files);
}

template <typename Configuration, typename MetadataReadHelper>
String IcebergMetadataParser<Configuration, MetadataReadHelper>::fetchMetadataFile() const
{
    /// Iceberg stores all the metadata.json in metadata directory, and the
    /// newest version has the max version name, so we should list all of them,
    /// then find the newest metadata.
    static constexpr auto meta_file_suffix = ".json";
    auto metadata_files = MetadataReadHelper::listFilesMatchSuffix(base_configuration, metadata_directory, meta_file_suffix);

    if (metadata_files.empty())
        throw Exception(
            ErrorCodes::FILE_DOESNT_EXIST, "The metadata file for Iceberg table with path {} doesn't exist", base_configuration.url.key);

    /// See comment above
    auto it = std::max_element(metadata_files.begin(), metadata_files.end());
    return *it;
}

template <typename Configuration, typename MetadataReadHelper>
String IcebergMetadataParser<Configuration, MetadataReadHelper>::getManifestList(const String & metadata_name) const
{
    auto buffer = MetadataReadHelper::createReadBuffer(metadata_name, context, base_configuration);
    String json_str;
    readJSONObjectPossiblyInvalid(json_str, *buffer);

    /// Looks like base/base/JSON.h can not parse this json file
    Poco::JSON::Parser parser;
    Poco::Dynamic::Var json = parser.parse(json_str);
    Poco::JSON::Object::Ptr object = json.extract<Poco::JSON::Object::Ptr>();

    auto current_snapshot_id = object->getValue<Int64>("current-snapshot-id");

    auto snapshots = object->get("snapshots").extract<Poco::JSON::Array::Ptr>();

    for (size_t i = 0; i < snapshots->size(); ++i)
    {
        auto snapshot = snapshots->getObject(static_cast<UInt32>(i));
        if (snapshot->getValue<Int64>("snapshot-id") == current_snapshot_id)
        {
            auto path = snapshot->getValue<String>("manifest-list");
            return std::filesystem::path(base_configuration.url.key) / metadata_directory / std::filesystem::path(path).filename();
        }
    }

    return {};
}

static MutableColumns
parseAvro(const std::unique_ptr<avro::DataFileReaderBase> & file_reader, const DataTypePtr & data_type, const String & field_name)
{
    auto deserializer = std::make_unique<AvroDeserializer>(
        Block{{data_type->createColumn(), data_type, field_name}}, file_reader->dataSchema(), true, true);
    file_reader->init();
    MutableColumns columns;
    columns.emplace_back(data_type->createColumn());

    RowReadExtension ext;
    while (file_reader->hasMore())
    {
        file_reader->decr();
        deserializer->deserializeRow(columns, file_reader->decoder(), ext);
    }
    return columns;
}

template <typename Configuration, typename MetadataReadHelper>
std::vector<String> IcebergMetadataParser<Configuration, MetadataReadHelper>::getManifestFiles(const String & manifest_list) const
{
    auto buffer = MetadataReadHelper::createReadBuffer(manifest_list, context, base_configuration);

    auto file_reader = std::make_unique<avro::DataFileReaderBase>(std::make_unique<AvroInputStreamReadBufferAdapter>(*buffer));

    static constexpr auto manifest_path = "manifest_path";

    /// The manifest_path is the first field in manifest list file,
    /// And its have String data type
    /// {'manifest_path': 'xxx', ...}
    auto data_type = AvroSchemaReader::avroNodeToDataType(file_reader->dataSchema().root()->leafAt(0));
    auto columns = parseAvro(file_reader, data_type, manifest_path);
    auto & col = columns.at(0);

    std::vector<String> res;
    if (col->getDataType() == TypeIndex::String)
    {
        const auto * col_str = typeid_cast<ColumnString *>(col.get());
        size_t col_size = col_str->size();
        for (size_t i = 0; i < col_size; ++i)
        {
            auto file_path = col_str->getDataAt(i).toView();
            /// We just need obtain the file name
            std::filesystem::path path(file_path);
            res.emplace_back(std::filesystem::path(base_configuration.url.key) / metadata_directory / path.filename());
        }

        return res;
    }
    throw Exception(
        ErrorCodes::ILLEGAL_COLUMN,
        "The parsed column from Avro file of `manifest_path` field should be String type, got {}",
        col->getFamilyName());
}

template <typename Configuration, typename MetadataReadHelper>
std::vector<String> IcebergMetadataParser<Configuration, MetadataReadHelper>::getFilesForRead(const std::vector<String> & manifest_files) const
{
    std::vector<String> keys;
    for (const auto & manifest_file : manifest_files)
    {
        auto buffer = MetadataReadHelper::createReadBuffer(manifest_file, context, base_configuration);

        auto file_reader = std::make_unique<avro::DataFileReaderBase>(std::make_unique<AvroInputStreamReadBufferAdapter>(*buffer));

        static constexpr auto manifest_path = "data_file";

        /// The data_file filed at the 3rd position of the manifest file:
        /// {'status': xx, 'snapshot_id': xx, 'data_file': {'file_path': 'xxx', ...}, ...}
        /// and it's also a nested record, so its result type is a nested Tuple
        auto data_type = AvroSchemaReader::avroNodeToDataType(file_reader->dataSchema().root()->leafAt(2));
        auto columns = parseAvro(file_reader, data_type, manifest_path);
        auto & col = columns.at(0);

        if (col->getDataType() == TypeIndex::Tuple)
        {
            auto * col_tuple = typeid_cast<ColumnTuple *>(col.get());
            auto & col_str = col_tuple->getColumnPtr(0);
            if (col_str->getDataType() == TypeIndex::String)
            {
                const auto * str_col = typeid_cast<const ColumnString *>(col_str.get());
                size_t col_size = str_col->size();
                for (size_t i = 0; i < col_size; ++i)
                {
                    auto file_path = str_col->getDataAt(i).toView();
                    /// We just obtain the partition/file name
                    std::filesystem::path path(file_path);
                    keys.emplace_back(path.parent_path().filename() / path.filename());
                }
            }
            else
            {
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "The parsed column from Avro file of `file_path` field should be String type, got {}",
                    col_str->getFamilyName());
            }
        }
        else
        {
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "The parsed column from Avro file of `data_file` field should be Tuple type, got {}",
                col->getFamilyName());
        }
    }

    return keys;
}

// generateQueryFromKeys constructs query from all parquet filenames
// for underlying StorageS3 engine
template <typename Configuration, typename MetadataReadHelper>
String IcebergMetadataParser<Configuration, MetadataReadHelper>::generateQueryFromKeys(const std::vector<String> & keys, const String &)
{
    std::string new_query = fmt::format("{{{}}}", fmt::join(keys, ","));
    return new_query;
}

template IcebergMetadataParser<StorageS3::Configuration, S3DataLakeMetadataReadHelper>::IcebergMetadataParser(
    const StorageS3::Configuration & configuration_, ContextPtr context_);

template std::vector<String> IcebergMetadataParser<StorageS3::Configuration, S3DataLakeMetadataReadHelper>::getFiles() const;

template String IcebergMetadataParser<StorageS3::Configuration, S3DataLakeMetadataReadHelper>::generateQueryFromKeys(
    const std::vector<String> & keys, const String & format);

template String IcebergMetadataParser<StorageS3::Configuration, S3DataLakeMetadataReadHelper>::fetchMetadataFile() const;

template String IcebergMetadataParser<StorageS3::Configuration, S3DataLakeMetadataReadHelper>::getManifestList(const String & metadata_name) const;

template std::vector<String>
IcebergMetadataParser<StorageS3::Configuration, S3DataLakeMetadataReadHelper>::getManifestFiles(const String & manifest_list) const;

template std::vector<String>
IcebergMetadataParser<StorageS3::Configuration, S3DataLakeMetadataReadHelper>::getFilesForRead(const std::vector<String> & manifest_files) const;

void registerStorageIceberg(StorageFactory & factory)
{
    factory.registerStorage(
        "Iceberg",
        [](const StorageFactory::Arguments & args)
        {
            auto & engine_args = args.engine_args;
            StorageS3::Configuration configuration = StorageIceberg::getConfiguration(engine_args, args.getLocalContext());

            auto format_settings = getFormatSettings(args.getContext());

            return std::make_shared<StorageIceberg>(
                configuration, args.table_id, args.columns, args.constraints, args.comment, args.getContext(), format_settings);
        },
        {
            .supports_settings = false,
            .supports_schema_inference = true,
            .source_access_type = AccessType::S3,
        });
}

}

#endif
