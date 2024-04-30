#include <Storages/DataLakes/DeltaLakeMetadataParser.h>
#include <base/JSON.h>
#include "config.h"
#include <set>

#if USE_AWS_S3 && USE_PARQUET
#include <Storages/DataLakes/S3MetadataReader.h>
#include <Storages/StorageS3.h>
#include <parquet/file_reader.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Processors/Formats/Impl/ParquetBlockInputFormat.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Formats/FormatFactory.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnNullable.h>
#include <IO/ReadHelpers.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <parquet/arrow/reader.h>
#include <ranges>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int BAD_ARGUMENTS;
}

template <typename Configuration, typename MetadataReadHelper>
struct DeltaLakeMetadataParser<Configuration, MetadataReadHelper>::Impl
{
    /**
     * Useful links:
     *  - https://github.com/delta-io/delta/blob/master/PROTOCOL.md#data-files
     */

    /**
     * DeltaLake tables store metadata files and data files.
     * Metadata files are stored as JSON in a directory at the root of the table named _delta_log,
     * and together with checkpoints make up the log of all changes that have occurred to a table.
     *
     * Delta files are the unit of atomicity for a table,
     * and are named using the next available version number, zero-padded to 20 digits.
     * For example:
     *     ./_delta_log/00000000000000000000.json
     */
    static constexpr auto deltalake_metadata_directory = "_delta_log";
    static constexpr auto metadata_file_suffix = ".json";

    std::string withPadding(size_t version)
    {
        /// File names are zero-padded to 20 digits.
        static constexpr auto padding = 20;

        const auto version_str = toString(version);
        return std::string(padding - version_str.size(), '0') + version_str;
    }

    /**
     * A delta file, n.json, contains an atomic set of actions that should be applied to the
     * previous table state (n-1.json) in order to the construct nth snapshot of the table.
     * An action changes one aspect of the table's state, for example, adding or removing a file.
     * Note: it is not a valid json, but a list of json's, so we read it in a while cycle.
     */
    std::set<String> processMetadataFiles(const Configuration & configuration, ContextPtr context)
    {
        std::set<String> result_files;
        const auto checkpoint_version = getCheckpointIfExists(result_files, configuration, context);

        if (checkpoint_version)
        {
            auto current_version = checkpoint_version;
            while (true)
            {
                const auto filename = withPadding(++current_version) + metadata_file_suffix;
                const auto file_path = fs::path(configuration.getPath()) / deltalake_metadata_directory / filename;

                if (!MetadataReadHelper::exists(file_path, configuration))
                    break;

                processMetadataFile(file_path, result_files, configuration, context);
            }

            LOG_TRACE(
                log, "Processed metadata files from checkpoint {} to {}",
                checkpoint_version, current_version - 1);
        }
        else
        {
            const auto keys = MetadataReadHelper::listFiles(
                configuration, deltalake_metadata_directory, metadata_file_suffix);

            for (const String & key : keys)
                processMetadataFile(key, result_files, configuration, context);
        }

        return result_files;
    }

    /**
     * Example of content of a single .json metadata file:
     * "
     *     {"commitInfo":{
     *         "timestamp":1679424650713,
     *         "operation":"WRITE",
     *         "operationMetrics":{"numFiles":"1","numOutputRows":"100","numOutputBytes":"2560"},
     *         ...}
     *     {"protocol":{"minReaderVersion":2,"minWriterVersion":5}}
     *     {"metaData":{
     *         "id":"bd11ad96-bc2c-40b0-be1f-6fdd90d04459",
     *         "format":{"provider":"parquet","options":{}},
     *         "schemaString":"{...}",
     *         "partitionColumns":[],
     *         "configuration":{...},
     *         "createdTime":1679424648640}}
     *     {"add":{
     *         "path":"part-00000-ecf8ed08-d04a-4a71-a5ec-57d8bb2ab4ee-c000.parquet",
     *         "partitionValues":{},
     *         "size":2560,
     *         "modificationTime":1679424649568,
     *         "dataChange":true,
     *         "stats":"{
     *             \"numRecords\":100,
     *             \"minValues\":{\"col-6c990940-59bb-4709-8f2e-17083a82c01a\":0},
     *             \"maxValues\":{\"col-6c990940-59bb-4709-8f2e-17083a82c01a\":99},
     *             \"nullCount\":{\"col-6c990940-59bb-4709-8f2e-17083a82c01a\":0,\"col-763cd7e2-7627-4d8e-9fb7-9e85d0c8845b\":0}}"}}
     * "
     */
    void processMetadataFile(
        const String & key,
        std::set<String> & result,
        const Configuration & configuration,
        ContextPtr context)
    {
        auto buf = MetadataReadHelper::createReadBuffer(key, context, configuration);

        char c;
        while (!buf->eof())
        {
            /// May be some invalid characters before json.
            while (buf->peek(c) && c != '{')
                buf->ignore();

            if (buf->eof())
                break;

            String json_str;
            readJSONObjectPossiblyInvalid(json_str, *buf);

            if (json_str.empty())
                continue;

            const JSON json(json_str);
            if (json.has("add"))
            {
                const auto path = json["add"]["path"].getString();
                result.insert(fs::path(configuration.getPath()) / path);
            }
            else if (json.has("remove"))
            {
                const auto path = json["remove"]["path"].getString();
                result.erase(fs::path(configuration.getPath()) / path);
            }
        }
    }

    /**
     * Checkpoints in delta-lake are created each 10 commits by default.
     * Latest checkpoint is written in _last_checkpoint file: _delta_log/_last_checkpoint
     *
     * _last_checkpoint contains the following:
     * {"version":20,
     *  "size":23,
     *  "sizeInBytes":14057,
     *  "numOfAddFiles":21,
     *  "checkpointSchema":{...}}
     *
     *  We need to get "version", which is the version of the checkpoint we need to read.
     */
    size_t readLastCheckpointIfExists(const Configuration & configuration, ContextPtr context)
    {
        const auto last_checkpoint_file = fs::path(configuration.getPath()) / deltalake_metadata_directory / "_last_checkpoint";
        if (!MetadataReadHelper::exists(last_checkpoint_file, configuration))
            return 0;

        String json_str;
        auto buf = MetadataReadHelper::createReadBuffer(last_checkpoint_file, context, configuration);
        readJSONObjectPossiblyInvalid(json_str, *buf);

        const JSON json(json_str);
        const auto version = json["version"].getUInt();

        LOG_TRACE(log, "Last checkpoint file version: {}", version);
        return version;
    }

    /**
     *  The format of the checkpoint file name can take one of two forms:
     *  1. A single checkpoint file for version n of the table will be named n.checkpoint.parquet.
     *     For example:
     *         00000000000000000010.checkpoint.parquet
     *  2. A multi-part checkpoint for version n can be fragmented into p files. Fragment o of p is
     *     named n.checkpoint.o.p.parquet. For example:
     *         00000000000000000010.checkpoint.0000000001.0000000003.parquet
     *         00000000000000000010.checkpoint.0000000002.0000000003.parquet
     *         00000000000000000010.checkpoint.0000000003.0000000003.parquet
     *  TODO: Only (1) is supported, need to support (2).
     *
     *  Such checkpoint files parquet contain data with the following contents:
     *
     *  Row 1:
     *  ──────
     *  txn:      (NULL,NULL,NULL)
     *  add:      ('part-00000-1e9cd0c1-57b5-43b4-9ed8-39854287b83a-c000.parquet',{},1070,1680614340485,false,{},'{"numRecords":1,"minValues":{"col-360dade5-6d0e-4831-8467-a25d64695975":13,"col-e27b0253-569a-4fe1-8f02-f3342c54d08b":"14"},"maxValues":{"col-360dade5-6d0e-4831-8467-a25d64695975":13,"col-e27b0253-569a-4fe1-8f02-f3342c54d08b":"14"},"nullCount":{"col-360dade5-6d0e-4831-8467-a25d64695975":0,"col-e27b0253-569a-4fe1-8f02-f3342c54d08b":0}}')
     *  remove:   (NULL,NULL,NULL,NULL,{},NULL,{})
     *  metaData: (NULL,NULL,NULL,(NULL,{}),NULL,[],{},NULL)
     *  protocol: (NULL,NULL)
     *
     *  Row 2:
     *  ──────
     *  txn:      (NULL,NULL,NULL)
     *  add:      ('part-00000-8887e898-91dd-4951-a367-48f7eb7bd5fd-c000.parquet',{},1063,1680614318485,false,{},'{"numRecords":1,"minValues":{"col-360dade5-6d0e-4831-8467-a25d64695975":2,"col-e27b0253-569a-4fe1-8f02-f3342c54d08b":"3"},"maxValues":{"col-360dade5-6d0e-4831-8467-a25d64695975":2,"col-e27b0253-569a-4fe1-8f02-f3342c54d08b":"3"},"nullCount":{"col-360dade5-6d0e-4831-8467-a25d64695975":0,"col-e27b0253-569a-4fe1-8f02-f3342c54d08b":0}}')
     *  remove:   (NULL,NULL,NULL,NULL,{},NULL,{})
     *  metaData: (NULL,NULL,NULL,(NULL,{}),NULL,[],{},NULL)
     *  protocol: (NULL,NULL)
     *
     * We need to check only `add` column, `remove` column does not have intersections with `add` column.
     *  ...
     */
    #define THROW_ARROW_NOT_OK(status)                                    \
        do                                                                \
        {                                                                 \
            if (const ::arrow::Status & _s = (status); !_s.ok())          \
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Arrow error: {}", _s.ToString()); \
        } while (false)

    size_t getCheckpointIfExists(std::set<String> & result, const Configuration & configuration, ContextPtr context)
    {
        const auto version = readLastCheckpointIfExists(configuration, context);
        if (!version)
            return 0;

        const auto checkpoint_filename = withPadding(version) + ".checkpoint.parquet";
        const auto checkpoint_path = fs::path(configuration.getPath()) / deltalake_metadata_directory / checkpoint_filename;

        LOG_TRACE(log, "Using checkpoint file: {}", checkpoint_path.string());

        auto buf = MetadataReadHelper::createReadBuffer(checkpoint_path, context, configuration);
        auto format_settings = getFormatSettings(context);

        /// Force nullable, because this parquet file for some reason does not have nullable
        /// in parquet file metadata while the type are in fact nullable.
        format_settings.schema_inference_make_columns_nullable = true;
        auto columns = ParquetSchemaReader(*buf, format_settings).readSchema();

        /// Read only columns that we need.
        columns.filterColumns(NameSet{"add", "remove"});
        Block header;
        for (const auto & column : columns)
            header.insert({column.type->createColumn(), column.type, column.name});

        std::atomic<int> is_stopped{0};
        auto arrow_file = asArrowFile(*buf, format_settings, is_stopped, "Parquet", PARQUET_MAGIC_BYTES);

        std::unique_ptr<parquet::arrow::FileReader> reader;
        THROW_ARROW_NOT_OK(
            parquet::arrow::OpenFile(
                asArrowFile(*buf, format_settings, is_stopped, "Parquet", PARQUET_MAGIC_BYTES),
                arrow::default_memory_pool(),
                &reader));

        std::shared_ptr<arrow::Schema> schema;
        THROW_ARROW_NOT_OK(reader->GetSchema(&schema));

        ArrowColumnToCHColumn column_reader(
            header, "Parquet",
            format_settings.parquet.allow_missing_columns,
            /* null_as_default */true,
            format_settings.date_time_overflow_behavior,
            /* case_insensitive_column_matching */false);

        std::shared_ptr<arrow::Table> table;
        THROW_ARROW_NOT_OK(reader->ReadTable(&table));

        Chunk res = column_reader.arrowTableToCHChunk(table, reader->parquet_reader()->metadata()->num_rows());
        const auto & res_columns = res.getColumns();

        if (res_columns.size() != 2)
        {
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Unexpected number of columns: {} (having: {}, expected: {})",
                res_columns.size(), res.dumpStructure(), header.dumpStructure());
        }

        const auto * tuple_column = assert_cast<const ColumnTuple *>(res_columns[0].get());
        const auto & nullable_column = assert_cast<const ColumnNullable &>(tuple_column->getColumn(0));
        const auto & path_column = assert_cast<const ColumnString &>(nullable_column.getNestedColumn());
        for (size_t i = 0; i < path_column.size(); ++i)
        {
            const auto filename = String(path_column.getDataAt(i));
            if (filename.empty())
                continue;
            LOG_TEST(log, "Adding {}", filename);
            const auto [_, inserted] = result.insert(fs::path(configuration.getPath()) / filename);
            if (!inserted)
                throw Exception(ErrorCodes::INCORRECT_DATA, "File already exists {}", filename);
        }

        return version;
    }

    LoggerPtr log = getLogger("DeltaLakeMetadataParser");
};


template <typename Configuration, typename MetadataReadHelper>
DeltaLakeMetadataParser<Configuration, MetadataReadHelper>::DeltaLakeMetadataParser() : impl(std::make_unique<Impl>())
{
}

template <typename Configuration, typename MetadataReadHelper>
Strings DeltaLakeMetadataParser<Configuration, MetadataReadHelper>::getFiles(const Configuration & configuration, ContextPtr context)
{
    auto result = impl->processMetadataFiles(configuration, context);
    return Strings(result.begin(), result.end());
}

template DeltaLakeMetadataParser<StorageS3::Configuration, S3DataLakeMetadataReadHelper>::DeltaLakeMetadataParser();
template Strings DeltaLakeMetadataParser<StorageS3::Configuration, S3DataLakeMetadataReadHelper>::getFiles(
    const StorageS3::Configuration & configuration, ContextPtr);
}

#endif
