#include "config.h"

#if USE_AWS_S3 && USE_AVRO

#include <Common/logger_useful.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/IColumn.h>
#include <Storages/DataLakes/IcebergMetadataParser.h>
#include <Storages/DataLakes/S3MetadataReader.h>
#include <Storages/StorageS3.h>
#include <Processors/Formats/Impl/AvroRowInputFormat.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadHelpers.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
}

template <typename Configuration, typename MetadataReadHelper>
struct IcebergMetadataParser<Configuration, MetadataReadHelper>::Impl
{
    Poco::Logger * log = &Poco::Logger::get("IcebergMetadataParser");

    /**
     * Useful links:
     * - https://iceberg.apache.org/spec/
     */

    /**
     * Iceberg has two format versions, currently we support only format V1.
     *
     * Unlike DeltaLake, Iceberg has several metadata layers: `table metadata`, `manifest list` and `manifest_files`.
     * Metadata file - json file.
     * Manifest list – a file that lists manifest files; one per snapshot.
     * Manifest file – a file that lists data or delete files; a subset of a snapshot.
     * All changes to table state create a new metadata file and replace the old metadata with an atomic swap.
     */

    static constexpr auto metadata_directory = "metadata";

    /**
     * Each version of table metadata is stored in a `metadata` directory and
     * has format: v<V>.metadata.json, where V - metadata version.
     */
    String getMetadataFile(const Configuration & configuration)
    {
        static constexpr auto metadata_file_suffix = ".metadata.json";

        const auto metadata_files = MetadataReadHelper::listFiles(configuration, metadata_directory, metadata_file_suffix);
        if (metadata_files.empty())
        {
            throw Exception(
                ErrorCodes::FILE_DOESNT_EXIST,
                "The metadata file for Iceberg table with path {} doesn't exist",
                configuration.url.key);
        }

        /// Get the latest version of metadata file: v<V>.metadata.json
        return *std::max_element(metadata_files.begin(), metadata_files.end());
    }

    /**
     * In order to find out which data files to read, we need to find the `manifest list`
     * which corresponds to the latest snapshot. We find it by checking a list of snapshots
     * in metadata's "snapshots" section.
     *
     * Example of metadata.json file.
     *
     * {
     *     "format-version" : 1,
     *     "table-uuid" : "ca2965ad-aae2-4813-8cf7-2c394e0c10f5",
     *     "location" : "/iceberg_data/db/table_name",
     *     "last-updated-ms" : 1680206743150,
     *     "last-column-id" : 2,
     *     "schema" : { "type" : "struct", "schema-id" : 0, "fields" : [ {<field1_info>}, {<field2_info>}, ... ] },
     *     "current-schema-id" : 0,
     *     "schemas" : [ ],
     *     ...
     *     "current-snapshot-id" : 2819310504515118887,
     *     "refs" : { "main" : { "snapshot-id" : 2819310504515118887, "type" : "branch" } },
     *     "snapshots" : [ {
     *       "snapshot-id" : 2819310504515118887,
     *       "timestamp-ms" : 1680206743150,
     *       "summary" : {
     *         "operation" : "append", "spark.app.id" : "local-1680206733239",
     *         "added-data-files" : "1", "added-records" : "100",
     *         "added-files-size" : "1070", "changed-partition-count" : "1",
     *         "total-records" : "100", "total-files-size" : "1070", "total-data-files" : "1", "total-delete-files" : "0",
     *         "total-position-deletes" : "0", "total-equality-deletes" : "0"
     *       },
     *       "manifest-list" : "/iceberg_data/db/table_name/metadata/snap-2819310504515118887-1-c87bfec7-d36c-4075-ad04-600b6b0f2020.avro",
     *       "schema-id" : 0
     *     } ],
     *     "statistics" : [ ],
     *     "snapshot-log" : [ ... ],
     *     "metadata-log" : [ ]
     * }
     */
    struct Metadata
    {
        int format_version;
        String manifest_list;
        Strings manifest_files;
    };
    Metadata processMetadataFile(const Configuration & configuration, ContextPtr context)
    {
        const auto metadata_file_path = getMetadataFile(configuration);
        auto buf = MetadataReadHelper::createReadBuffer(metadata_file_path, context, configuration);
        String json_str;
        readJSONObjectPossiblyInvalid(json_str, *buf);

        Poco::JSON::Parser parser; /// For some reason base/base/JSON.h can not parse this json file
        Poco::Dynamic::Var json = parser.parse(json_str);
        Poco::JSON::Object::Ptr object = json.extract<Poco::JSON::Object::Ptr>();

        Metadata result;
        result.format_version = object->getValue<int>("format-version");

        auto current_snapshot_id = object->getValue<Int64>("current-snapshot-id");
        auto snapshots = object->get("snapshots").extract<Poco::JSON::Array::Ptr>();

        for (size_t i = 0; i < snapshots->size(); ++i)
        {
            const auto snapshot = snapshots->getObject(static_cast<UInt32>(i));
            if (snapshot->getValue<Int64>("snapshot-id") == current_snapshot_id)
            {
                const auto path = snapshot->getValue<String>("manifest-list");
                result.manifest_list = std::filesystem::path(configuration.url.key) / metadata_directory / std::filesystem::path(path).filename();
                break;
            }
        }
        return result;
    }

    /**
     * Manifest list has Avro as default format (and currently we support only Avro).
     * Manifest list file format of manifest list is: snap-2819310504515118887-1-c87bfec7-d36c-4075-ad04-600b6b0f2020.avro
     *
     * `manifest list` has the following contents:
     * ┌─manifest_path────────────────────────────────────────────────────────────────────────────────────────┬─manifest_length─┬─partition_spec_id─┬───added_snapshot_id─┬─added_data_files_count─┬─existing_data_files_count─┬─deleted_data_files_count─┬─partitions─┬─added_rows_count─┬─existing_rows_count─┬─deleted_rows_count─┐
     * │ /iceberg_data/db/table_name/metadata/c87bfec7-d36c-4075-ad04-600b6b0f2020-m0.avro │            5813 │                 0 │ 2819310504515118887 │                      1 │                         0 │                        0 │ []         │              100 │                   0 │                  0 │
     * └──────────────────────────────────────────────────────────────────────────────────────────────────────┴─────────────────┴───────────────────┴─────────────────────┴────────────────────────┴───────────────────────────┴──────────────────────────┴────────────┴──────────────────┴─────────────────────┴────────────────────┘
     */
    void processManifestList(Metadata & metadata, const Configuration & configuration, ContextPtr context)
    {
        auto buf = MetadataReadHelper::createReadBuffer(metadata.manifest_list, context, configuration);
        auto file_reader = std::make_unique<avro::DataFileReaderBase>(std::make_unique<AvroInputStreamReadBufferAdapter>(*buf));

        auto data_type = AvroSchemaReader::avroNodeToDataType(file_reader->dataSchema().root()->leafAt(0));
        Block header{{data_type->createColumn(), data_type, "manifest_path"}};
        auto columns = parseAvro(*file_reader, header, getFormatSettings(context));
        auto & col = columns.at(0);

        if (col->getDataType() != TypeIndex::String)
        {
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "The parsed column from Avro file of `manifest_path` field should be String type, got {}",
                col->getFamilyName());
        }

        const auto * col_str = typeid_cast<ColumnString *>(col.get());
        for (size_t i = 0; i < col_str->size(); ++i)
        {
            const auto file_path = col_str->getDataAt(i).toView();
            const auto filename = std::filesystem::path(file_path).filename();
            metadata.manifest_files.emplace_back(std::filesystem::path(configuration.url.key) / metadata_directory / filename);
        }
    }

    /**
     * Manifest file has the following format: '/iceberg_data/db/table_name/metadata/c87bfec7-d36c-4075-ad04-600b6b0f2020-m0.avro'
     *
     * `manifest file` is different in format version V1 and V2 and has the following contents:
     *                        v1     v2
     * status                 req    req
     * snapshot_id            req    opt
     * sequence_number               opt
     * file_sequence_number          opt
     * data_file              req    req
     * Example format version V1:
     * ┌─status─┬─────────snapshot_id─┬─data_file───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
     * │      1 │ 2819310504515118887 │ ('/iceberg_data/db/table_name/data/00000-1-3edca534-15a0-4f74-8a28-4733e0bf1270-00001.parquet','PARQUET',(),100,1070,67108864,[(1,233),(2,210)],[(1,100),(2,100)],[(1,0),(2,0)],[],[(1,'\0'),(2,'0')],[(1,'c'),(2,'99')],NULL,[4],0) │
     * └────────┴─────────────────────┴─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
     * Example format version V2:
     * ┌─status─┬─────────snapshot_id─┬─sequence_number─┬─file_sequence_number─┬─data_file───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
     * │      1 │ 5887006101709926452 │            ᴺᵁᴸᴸ │                 ᴺᵁᴸᴸ │ (0,'/iceberg_data/db/table_name/data/00000-1-c8045c90-8799-4eac-b957-79a0484e223c-00001.parquet','PARQUET',(),100,1070,[(1,233),(2,210)],[(1,100),(2,100)],[(1,0),(2,0)],[],[(1,'\0'),(2,'0')],[(1,'c'),(2,'99')],NULL,[4],[],0) │
     * └────────┴─────────────────────┴─────────────────┴──────────────────────┴─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
     * In case of partitioned data we'll have extra directory partition=value:
     * ─status─┬─────────snapshot_id─┬─data_file──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
     * │      1 │ 2252246380142525104 │ ('/iceberg_data/db/table_name/data/a=0/00000-1-c9535a00-2f4f-405c-bcfa-6d4f9f477235-00001.parquet','PARQUET',(0),1,631,67108864,[(1,46),(2,48)],[(1,1),(2,1)],[(1,0),(2,0)],[],[(1,'\0\0\0\0\0\0\0\0'),(2,'1')],[(1,'\0\0\0\0\0\0\0\0'),(2,'1')],NULL,[4],0) │
     * │      1 │ 2252246380142525104 │ ('/iceberg_data/db/table_name/data/a=1/00000-1-c9535a00-2f4f-405c-bcfa-6d4f9f477235-00002.parquet','PARQUET',(1),1,631,67108864,[(1,46),(2,48)],[(1,1),(2,1)],[(1,0),(2,0)],[],[(1,'\0\0\0\0\0\0\0'),(2,'2')],[(1,'\0\0\0\0\0\0\0'),(2,'2')],NULL,[4],0) │
     * │      1 │ 2252246380142525104 │ ('/iceberg_data/db/table_name/data/a=2/00000-1-c9535a00-2f4f-405c-bcfa-6d4f9f477235-00003.parquet','PARQUET',(2),1,631,67108864,[(1,46),(2,48)],[(1,1),(2,1)],[(1,0),(2,0)],[],[(1,'\0\0\0\0\0\0\0'),(2,'3')],[(1,'\0\0\0\0\0\0\0'),(2,'3')],NULL,[4],0) │
     * └────────┴─────────────────────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
     */
    Strings getFilesForRead(const Metadata & metadata, const Configuration & configuration, ContextPtr context)
    {
        NameSet keys;
        for (const auto & manifest_file : metadata.manifest_files)
        {
            auto buffer = MetadataReadHelper::createReadBuffer(manifest_file, context, configuration);
            auto file_reader = std::make_unique<avro::DataFileReaderBase>(std::make_unique<AvroInputStreamReadBufferAdapter>(*buffer));

            avro::NodePtr root_node = file_reader->dataSchema().root();
            size_t leaves_num = root_node->leaves();
            size_t expected_min_num = metadata.format_version == 1 ? 3 : 2;
            if (leaves_num < expected_min_num)
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Unexpected number of columns {}. Expected at least {}",
                    root_node->leaves(), expected_min_num);
            }

            avro::NodePtr status_node = root_node->leafAt(0);
            if (status_node->type() != avro::Type::AVRO_INT)
            {
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "The parsed column from Avro file of `status` field should be Int type, got {}",
                    magic_enum::enum_name(status_node->type()));
            }

            avro::NodePtr data_file_node = root_node->leafAt(static_cast<int>(leaves_num) - 1);
            if (data_file_node->type() != avro::Type::AVRO_RECORD)
            {
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "The parsed column from Avro file of `data_file` field should be Tuple type, got {}",
                    magic_enum::enum_name(data_file_node->type()));
            }

            auto status_col_data_type = AvroSchemaReader::avroNodeToDataType(status_node);
            auto data_col_data_type = AvroSchemaReader::avroNodeToDataType(data_file_node);
            Block header{
                {status_col_data_type->createColumn(), status_col_data_type, "status"},
                {data_col_data_type->createColumn(), data_col_data_type, "data_file"}};

            const auto columns = parseAvro(*file_reader, header, getFormatSettings(context));
            if (columns.size() != 2)
            {
                throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                                "Unexpected number of columns. Expected 2, got {}", columns.size());
            }

            if (columns.at(0)->getDataType() != TypeIndex::Int32)
            {
                throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                                "The parsed column from Avro file of `status` field should be Int32 type, got {}",
                                columns.at(0)->getFamilyName());
            }
            if (columns.at(1)->getDataType() != TypeIndex::Tuple)
            {
                throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                                "The parsed column from Avro file of `file_path` field should be Tuple type, got {}",
                                columns.at(1)->getFamilyName());
            }

            const auto status_int_column = assert_cast<ColumnInt32 *>(columns.at(0).get());
            const auto data_file_tuple_column = assert_cast<ColumnTuple *>(columns.at(1).get());

            if (status_int_column->size() != data_file_tuple_column->size())
            {
                throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                                "The parsed column from Avro file of `file_path` and `status` have different rows number: {} and {}",
                                status_int_column->size(), data_file_tuple_column->size());
            }

            const auto * data_file_name_column = metadata.format_version == 1
                ? data_file_tuple_column->getColumnPtr(0).get()
                : data_file_tuple_column->getColumnPtr(1).get();

            if (data_file_name_column->getDataType() != TypeIndex::String)
            {
                throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                                "The parsed column from Avro file of `file_path` field should be String type, got {}",
                                data_file_name_column->getFamilyName());
            }
            auto file_name_str_column = assert_cast<const ColumnString *>(data_file_name_column);

            for (size_t i = 0; i < status_int_column->size(); ++i)
            {
                const auto status = status_int_column->getInt(i);
                const auto data_path = std::string(file_name_str_column->getDataAt(i).toView());
                const auto pos = data_path.find(configuration.url.key);
                const auto file_path = data_path.substr(pos);
                if (pos == std::string::npos)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected to find {} in data path: {}", configuration.url.key, data_path);

                if (status == 2)
                {
                    LOG_TEST(log, "Processing delete file for path: {}", file_path);
                    chassert(!keys.contains(file_path));
                }
                else
                    keys.insert(file_path);
            }
        }

        return std::vector<std::string>(keys.begin(), keys.end());
    }

    MutableColumns parseAvro(
        avro::DataFileReaderBase & file_reader,
        const Block & header,
        const FormatSettings & settings)
    {
        auto deserializer = std::make_unique<AvroDeserializer>(header, file_reader.dataSchema(), true, true, settings);
        MutableColumns columns = header.cloneEmptyColumns();

        file_reader.init();
        RowReadExtension ext;
        while (file_reader.hasMore())
        {
            file_reader.decr();
            deserializer->deserializeRow(columns, file_reader.decoder(), ext);
        }
        return columns;
    }

};


template <typename Configuration, typename MetadataReadHelper>
IcebergMetadataParser<Configuration, MetadataReadHelper>::IcebergMetadataParser() : impl(std::make_unique<Impl>())
{
}

template <typename Configuration, typename MetadataReadHelper>
Strings IcebergMetadataParser<Configuration, MetadataReadHelper>::getFiles(const Configuration & configuration, ContextPtr context)
{
    auto metadata = impl->processMetadataFile(configuration, context);

    /// When table first created and does not have any data
    if (metadata.manifest_list.empty())
        return {};

    impl->processManifestList(metadata, configuration, context);
    return impl->getFilesForRead(metadata, configuration, context);
}


template IcebergMetadataParser<StorageS3::Configuration, S3DataLakeMetadataReadHelper>::IcebergMetadataParser();
template Strings IcebergMetadataParser<StorageS3::Configuration, S3DataLakeMetadataReadHelper>::getFiles(const StorageS3::Configuration & configuration, ContextPtr);

}

#endif
