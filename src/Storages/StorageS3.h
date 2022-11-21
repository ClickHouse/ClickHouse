#pragma once

#include "config.h"

#if USE_AWS_S3

#include <Core/Types.h>

#include <Compression/CompressionInfo.h>

#include <Storages/IStorage.h>
#include <Storages/StorageS3Settings.h>

#include <Processors/ISource.h>
#include <Poco/URI.h>
#include <Common/logger_useful.h>
#include <IO/S3Common.h>
#include <IO/CompressionMethod.h>
#include <Interpreters/Context.h>
#include <Storages/ExternalDataSourceConfiguration.h>
#include <Storages/Cache/SchemaCache.h>

namespace Aws::S3
{
    class S3Client;
}

namespace DB
{

class PullingPipelineExecutor;
class StorageS3SequentialSource;
class StorageS3Source : public ISource, WithContext
{
public:
    class DisclosedGlobIterator
    {
    public:
        DisclosedGlobIterator(
            const Aws::S3::S3Client & client_,
            const S3::URI & globbed_uri_,
            ASTPtr query,
            const Block & virtual_header,
            ContextPtr context,
            std::unordered_map<String, S3::ObjectInfo> * object_infos = nullptr,
            Strings * read_keys_ = nullptr,
            const S3Settings::RequestSettings & request_settings_ = {});

        String next();

    private:
        class Impl;
        /// shared_ptr to have copy constructor
        std::shared_ptr<Impl> pimpl;
    };

    class KeysIterator
    {
    public:
        explicit KeysIterator(
            const std::vector<String> & keys_, const String & bucket_, ASTPtr query, const Block & virtual_header, ContextPtr context);
        String next();

    private:
        class Impl;
        /// shared_ptr to have copy constructor
        std::shared_ptr<Impl> pimpl;
    };

    using IteratorWrapper = std::function<String()>;

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
        const std::shared_ptr<const Aws::S3::S3Client> & client_,
        const String & bucket,
        const String & version_id,
        std::shared_ptr<IteratorWrapper> file_iterator_,
        size_t download_thread_num,
        const std::unordered_map<String, S3::ObjectInfo> & object_infos_);

    String getName() const override;

    Chunk generate() override;

    void onCancel() override;

private:
    String name;
    String bucket;
    String version_id;
    String file_path;
    String format;
    ColumnsDescription columns_desc;
    UInt64 max_block_size;
    S3Settings::RequestSettings request_settings;
    String compression_hint;
    std::shared_ptr<const Aws::S3::S3Client> client;
    Block sample_block;
    std::optional<FormatSettings> format_settings;


    std::unique_ptr<ReadBuffer> read_buf;
    std::unique_ptr<QueryPipeline> pipeline;
    std::unique_ptr<PullingPipelineExecutor> reader;
    /// onCancel and generate can be called concurrently
    std::mutex reader_mutex;
    std::vector<NameAndTypePair> requested_virtual_columns;
    std::shared_ptr<IteratorWrapper> file_iterator;
    size_t download_thread_num = 1;

    Poco::Logger * log = &Poco::Logger::get("StorageS3Source");

    std::unordered_map<String, S3::ObjectInfo> object_infos;

    /// Recreate ReadBuffer and Pipeline for each file.
    bool initialize();

    std::unique_ptr<ReadBuffer> createS3ReadBuffer(const String & key);
};

/**
 * This class represents table engine for external S3 urls.
 * It sends HTTP GET to server when select is called and
 * HTTP PUT when insert is called.
 */
class StorageS3 : public IStorage, WithContext
{
public:
    StorageS3(
        const StorageS3Configuration & configuration_,
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

    static StorageS3Configuration getConfiguration(ASTs & engine_args, ContextPtr local_context);

    static ColumnsDescription getTableStructureFromData(
        const StorageS3Configuration & configuration,
        bool distributed_processing,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr ctx,
        std::unordered_map<String, S3::ObjectInfo> * object_infos = nullptr);

    static void processNamedCollectionResult(StorageS3Configuration & configuration, const std::vector<std::pair<String, ASTPtr>> & key_value_args);

    struct S3Configuration
    {
        const S3::URI uri;
        std::shared_ptr<const Aws::S3::S3Client> client;

        S3::AuthSettings auth_settings;
        S3Settings::RequestSettings request_settings;

        /// If s3 configuration was passed from ast, then it is static.
        /// If from config - it can be changed with config reload.
        bool static_configuration = true;

        /// Headers from ast is a part of static configuration.
        HeaderCollection headers_from_ast;

        S3Configuration(
            const String & url_,
            const S3::AuthSettings & auth_settings_,
            const S3Settings::RequestSettings & request_settings_,
            const HeaderCollection & headers_from_ast_)
            : uri(S3::URI(url_))
            , auth_settings(auth_settings_)
            , request_settings(request_settings_)
            , static_configuration(!auth_settings_.access_key_id.empty())
            , headers_from_ast(headers_from_ast_) {}
    };

    static SchemaCache & getSchemaCache(const ContextPtr & ctx);

private:
    friend class StorageS3Cluster;
    friend class TableFunctionS3Cluster;
    friend class StorageHudi;
    friend class StorageDelta;

    S3Configuration s3_configuration;
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

    std::unordered_map<String, S3::ObjectInfo> object_infos;

    static void updateS3Configuration(ContextPtr, S3Configuration &);

    static std::shared_ptr<StorageS3Source::IteratorWrapper> createFileIterator(
        const S3Configuration & s3_configuration,
        const std::vector<String> & keys,
        bool is_key_with_globs,
        bool distributed_processing,
        ContextPtr local_context,
        ASTPtr query,
        const Block & virtual_block,
        std::unordered_map<String, S3::ObjectInfo> * object_infos = nullptr,
        Strings * read_keys = nullptr);

    static ColumnsDescription getTableStructureFromDataImpl(
        const String & format,
        const S3Configuration & s3_configuration,
        const String & compression_method,
        bool distributed_processing,
        bool is_key_with_globs,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr ctx,
        std::unordered_map<String, S3::ObjectInfo> * object_infos = nullptr);

    bool supportsSubsetOfColumns() const override;

    static std::optional<ColumnsDescription> tryGetColumnsFromCache(
        const Strings::const_iterator & begin,
        const Strings::const_iterator & end,
        const S3Configuration & s3_configuration,
        std::unordered_map<String, S3::ObjectInfo> * object_infos,
        const String & format_name,
        const std::optional<FormatSettings> & format_settings,
        const ContextPtr & ctx);

    static void addColumnsToCache(
        const Strings & keys,
        const S3Configuration & s3_configuration,
        const ColumnsDescription & columns,
        const String & format_name,
        const std::optional<FormatSettings> & format_settings,
        const ContextPtr & ctx);
};

}

#endif
