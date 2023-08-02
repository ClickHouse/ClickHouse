#pragma once

#include "config.h"

#if USE_AWS_S3

#include <Core/Types.h>

#include <Compression/CompressionInfo.h>

#include <Storages/IStorage.h>
#include <Storages/StorageS3Settings.h>

#include <Processors/ISource.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Poco/URI.h>
#include <Common/logger_useful.h>
#include <IO/S3/getObjectInfo.h>
#include <IO/CompressionMethod.h>
#include <Interpreters/Context.h>
#include <Interpreters/threadPoolCallbackRunner.h>
#include <Storages/Cache/SchemaCache.h>
#include <Storages/StorageConfiguration.h>

namespace Aws::S3
{
    class Client;
}

namespace DB
{

class PullingPipelineExecutor;
class StorageS3SequentialSource;
class NamedCollection;

class StorageS3Source : public ISource, WithContext
{
public:

    struct KeyWithInfo
    {
        KeyWithInfo() = default;
        KeyWithInfo(String key_, std::optional<S3::ObjectInfo> info_)
            : key(std::move(key_)), info(std::move(info_))
        {
        }

        String key;
        std::optional<S3::ObjectInfo> info;
    };

    using KeysWithInfo = std::vector<KeyWithInfo>;
    using ObjectInfos = std::unordered_map<String, S3::ObjectInfo>;
    class IIterator
    {
    public:
        virtual ~IIterator() = default;
        virtual KeyWithInfo next() = 0;
        virtual size_t getTotalSize() const = 0;

        KeyWithInfo operator ()() { return next(); }
    };

    class DisclosedGlobIterator : public IIterator
    {
    public:
        DisclosedGlobIterator(
            const S3::Client & client_,
            const S3::URI & globbed_uri_,
            ASTPtr query,
            const Block & virtual_header,
            ContextPtr context,
            ObjectInfos * object_infos = nullptr,
            Strings * read_keys_ = nullptr,
            const S3Settings::RequestSettings & request_settings_ = {});

        KeyWithInfo next() override;
        size_t getTotalSize() const override;

    private:
        class Impl;
        /// shared_ptr to have copy constructor
        std::shared_ptr<Impl> pimpl;
    };

    class KeysIterator : public IIterator
    {
    public:
        explicit KeysIterator(
            const S3::Client & client_,
            const std::string & version_id_,
            const std::vector<String> & keys_,
            const String & bucket_,
            const S3Settings::RequestSettings & request_settings_,
            ASTPtr query,
            const Block & virtual_header,
            ContextPtr context,
            ObjectInfos * object_infos = nullptr,
            Strings * read_keys = nullptr);

        KeyWithInfo next() override;
        size_t getTotalSize() const override;

    private:
        class Impl;
        /// shared_ptr to have copy constructor
        std::shared_ptr<Impl> pimpl;
    };

    class ReadTaskIterator : public IIterator
    {
    public:
        explicit ReadTaskIterator(const ReadTaskCallback & callback_) : callback(callback_) {}

        KeyWithInfo next() override { return {callback(), {}}; }

        size_t getTotalSize() const override { return 0; }

    private:
        ReadTaskCallback callback;
    };

    static Block getHeader(Block sample_block, const std::vector<NameAndTypePair> & requested_virtual_columns);

    StorageS3Source(
        const std::vector<NameAndTypePair> & requested_virtual_columns_,
        const String & format,
        String name_,
        const Block & sample_block,
        ContextPtr context_,
        std::optional<FormatSettings> format_settings_,
        const ColumnsDescription & columns_,
        UInt64 max_block_size_,
        const S3Settings::RequestSettings & request_settings_,
        String compression_hint_,
        const std::shared_ptr<const S3::Client> & client_,
        const String & bucket,
        const String & version_id,
        std::shared_ptr<IIterator> file_iterator_,
        size_t download_thread_num);

    ~StorageS3Source() override;

    String getName() const override;

    Chunk generate() override;

private:
    String name;
    String bucket;
    String version_id;
    String format;
    ColumnsDescription columns_desc;
    UInt64 max_block_size;
    S3Settings::RequestSettings request_settings;
    String compression_hint;
    std::shared_ptr<const S3::Client> client;
    Block sample_block;
    std::optional<FormatSettings> format_settings;

    struct ReaderHolder
    {
    public:
        ReaderHolder(
            String path_,
            std::unique_ptr<ReadBuffer> read_buf_,
            std::shared_ptr<IInputFormat> input_format_,
            std::unique_ptr<QueryPipeline> pipeline_,
            std::unique_ptr<PullingPipelineExecutor> reader_)
            : path(std::move(path_))
            , read_buf(std::move(read_buf_))
            , input_format(input_format_)
            , pipeline(std::move(pipeline_))
            , reader(std::move(reader_))
        {
        }

        ReaderHolder() = default;
        ReaderHolder(const ReaderHolder & other) = delete;
        ReaderHolder & operator=(const ReaderHolder & other) = delete;

        ReaderHolder(ReaderHolder && other) noexcept
        {
            *this = std::move(other);
        }

        ReaderHolder & operator=(ReaderHolder && other) noexcept
        {
            /// The order of destruction is important.
            /// reader uses pipeline, pipeline uses read_buf.
            reader = std::move(other.reader);
            pipeline = std::move(other.pipeline);
            input_format = std::move(other.input_format);
            read_buf = std::move(other.read_buf);
            path = std::move(other.path);
            return *this;
        }

        const std::unique_ptr<ReadBuffer> & getReadBuffer() const { return read_buf; }

        const std::shared_ptr<IInputFormat> & getFormat() const { return input_format; }

        explicit operator bool() const { return reader != nullptr; }
        PullingPipelineExecutor * operator->() { return reader.get(); }
        const PullingPipelineExecutor * operator->() const { return reader.get(); }
        const String & getPath() const { return path; }

    private:
        String path;
        std::unique_ptr<ReadBuffer> read_buf;
        std::shared_ptr<IInputFormat> input_format;
        std::unique_ptr<QueryPipeline> pipeline;
        std::unique_ptr<PullingPipelineExecutor> reader;
    };

    ReaderHolder reader;

    std::vector<NameAndTypePair> requested_virtual_columns;
    std::shared_ptr<IIterator> file_iterator;
    size_t download_thread_num = 1;

    Poco::Logger * log = &Poco::Logger::get("StorageS3Source");

    ThreadPool create_reader_pool;
    ThreadPoolCallbackRunner<ReaderHolder> create_reader_scheduler;
    std::future<ReaderHolder> reader_future;

    UInt64 total_rows_approx_max = 0;
    size_t total_rows_count_times = 0;
    UInt64 total_rows_approx_accumulated = 0;
    size_t total_objects_size = 0;

    /// Recreate ReadBuffer and Pipeline for each file.
    ReaderHolder createReader();
    std::future<ReaderHolder> createReaderAsync();

    std::unique_ptr<ReadBuffer> createS3ReadBuffer(const String & key, size_t object_size);
    std::unique_ptr<ReadBuffer> createAsyncS3ReadBuffer(const String & key, const ReadSettings & read_settings, size_t object_size);
};

/**
 * This class represents table engine for external S3 urls.
 * It sends HTTP GET to server when select is called and
 * HTTP PUT when insert is called.
 */
class StorageS3 : public IStorage, WithContext
{
public:
    struct Configuration : public StatelessTableEngineConfiguration
    {
        S3::URI url;
        std::shared_ptr<const S3::Client> client;
        S3::AuthSettings auth_settings;
        S3Settings::RequestSettings request_settings;
        /// If s3 configuration was passed from ast, then it is static.
        /// If from config - it can be changed with config reload.
        bool static_configuration = true;
        /// Headers from ast is a part of static configuration.
        HTTPHeaderEntries headers_from_ast;

        void appendToPath(const String & suffix) { url = S3::URI{std::filesystem::path(url.uri.toString()) / suffix}; }

        String getPath() const { return url.uri.toString(); } /// For logging
    };

    StorageS3(
        const StorageS3::Configuration & configuration_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        ContextPtr context_,
        std::optional<FormatSettings> format_settings_,
        bool distributed_processing_ = false,
        ASTPtr partition_by_ = nullptr);

    String getName() const override
    {
        return name;
    }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context) override;

    void truncate(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context, TableExclusiveLockHolder &) override;

    NamesAndTypesList getVirtuals() const override;

    bool supportsPartitionBy() const override;

    using ObjectInfos = StorageS3Source::ObjectInfos;

    static void processNamedCollectionResult(StorageS3::Configuration & configuration, const NamedCollection & collection);

    static SchemaCache & getSchemaCache(const ContextPtr & ctx);

    static StorageS3::Configuration getConfiguration(ASTs & engine_args, ContextPtr local_context, bool get_format_from_file = true);

    static ColumnsDescription getTableStructureFromData(
        StorageS3::Configuration & configuration,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr ctx,
        ObjectInfos * object_infos = nullptr);

protected:
    static StorageS3::Configuration updateConfiguration(ContextPtr local_context, const Configuration & configuration);
    static void updateConfiguration(ContextPtr, Configuration &);

private:
    friend class StorageS3Cluster;
    friend class TableFunctionS3Cluster;

    Configuration s3_configuration;
    std::vector<String> keys;
    NamesAndTypesList virtual_columns;
    Block virtual_block;

    String format_name;
    String compression_method;
    String name;
    const bool distributed_processing;
    std::optional<FormatSettings> format_settings;
    ASTPtr partition_by;
    bool is_key_with_globs = false;

    ObjectInfos object_infos;

    static std::shared_ptr<StorageS3Source::IIterator> createFileIterator(
        const Configuration & s3_configuration,
        const std::vector<String> & keys,
        bool is_key_with_globs,
        bool distributed_processing,
        ContextPtr local_context,
        ASTPtr query,
        const Block & virtual_block,
        ObjectInfos * object_infos = nullptr,
        Strings * read_keys = nullptr);

    static ColumnsDescription getTableStructureFromDataImpl(
        const String & format,
        const Configuration & s3_configuration,
        const String & compression_method,
        bool is_key_with_globs,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr ctx,
        ObjectInfos * object_infos = nullptr);

    bool supportsSubcolumns() const override;

    bool supportsSubsetOfColumns() const override;

    static std::optional<ColumnsDescription> tryGetColumnsFromCache(
        const Strings::const_iterator & begin,
        const Strings::const_iterator & end,
        const Configuration & s3_configuration,
        ObjectInfos * object_infos,
        const String & format_name,
        const std::optional<FormatSettings> & format_settings,
        const ContextPtr & ctx);

    static void addColumnsToCache(
        const Strings & keys,
        const Configuration & s3_configuration,
        const ColumnsDescription & columns,
        const String & format_name,
        const std::optional<FormatSettings> & format_settings,
        const ContextPtr & ctx);
};

}

#endif
