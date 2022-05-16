#pragma once

#include <Common/config.h>

#if USE_AWS_S3

#include <Core/Types.h>

#include <Compression/CompressionInfo.h>

#include <Storages/IStorage.h>
#include <Storages/StorageS3Settings.h>

#include <Processors/Sources/SourceWithProgress.h>
#include <Poco/URI.h>
#include <Common/logger_useful.h>
#include <IO/S3Common.h>
#include <IO/CompressionMethod.h>
#include <Interpreters/Context.h>
#include <Storages/ExternalDataSourceConfiguration.h>

namespace Aws::S3
{
    class S3Client;
}

namespace DB
{

class PullingPipelineExecutor;
class StorageS3SequentialSource;
class StorageS3Source : public SourceWithProgress, WithContext
{
public:
    class DisclosedGlobIterator
    {
        public:
            DisclosedGlobIterator(Aws::S3::S3Client &, const S3::URI &);
            String next();
        private:
            class Impl;
            /// shared_ptr to have copy constructor
            std::shared_ptr<Impl> pimpl;
    };

    class KeysIterator
    {
        public:
            explicit KeysIterator(const std::vector<String> & keys_);
            String next();

        private:
            class Impl;
            /// shared_ptr to have copy constructor
            std::shared_ptr<Impl> pimpl;
    };

    class ReadTasksIterator
    {
    public:
        ReadTasksIterator(const std::vector<String> & read_tasks_, const ReadTaskCallback & new_read_tasks_callback_);
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
        UInt64 max_single_read_retries_,
        String compression_hint_,
        const std::shared_ptr<Aws::S3::S3Client> & client_,
        const String & bucket,
        const String & version_id,
        std::shared_ptr<IteratorWrapper> file_iterator_,
        size_t download_thread_num);

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
    UInt64 max_single_read_retries;
    String compression_hint;
    std::shared_ptr<Aws::S3::S3Client> client;
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
        const S3::URI & uri,
        const String & access_key_id,
        const String & secret_access_key,
        const StorageID & table_id_,
        const String & format_name_,
        const S3Settings::ReadWriteSettings & rw_settings_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        ContextPtr context_,
        std::optional<FormatSettings> format_settings_,
        const String & compression_method_ = "",
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
        unsigned num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context) override;

    void truncate(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context, TableExclusiveLockHolder &) override;

    NamesAndTypesList getVirtuals() const override;

    bool supportsPartitionBy() const override;

    static StorageS3Configuration getConfiguration(ASTs & engine_args, ContextPtr local_context);

    static ColumnsDescription getTableStructureFromData(
        const String & format,
        const S3::URI & uri,
        const String & access_key_id,
        const String & secret_access_key,
        const String & compression_method,
        bool distributed_processing,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr ctx);

    static void processNamedCollectionResult(StorageS3Configuration & configuration, const std::vector<std::pair<String, ASTPtr>> & key_value_args);

    struct S3Configuration
    {
        const S3::URI uri;
        const String access_key_id;
        const String secret_access_key;
        std::shared_ptr<Aws::S3::S3Client> client;
        S3Settings::AuthSettings auth_settings;
        S3Settings::ReadWriteSettings rw_settings;
    };

private:
    friend class StorageS3Cluster;
    friend class TableFunctionS3Cluster;

    S3Configuration s3_configuration;
    std::vector<String> keys;
    NamesAndTypesList virtual_columns;

    String format_name;
    String compression_method;
    String name;
    const bool distributed_processing;
    std::optional<FormatSettings> format_settings;
    ASTPtr partition_by;
    bool is_key_with_globs = false;

    std::vector<String> read_tasks_used_in_schema_inference;

    static void updateS3Configuration(ContextPtr, S3Configuration &);

    static std::shared_ptr<StorageS3Source::IteratorWrapper> createFileIterator(
        const S3Configuration & s3_configuration,
        const std::vector<String> & keys,
        bool is_key_with_globs,
        bool distributed_processing,
        ContextPtr local_context,
        const std::vector<String> & read_tasks = {});

    static ColumnsDescription getTableStructureFromDataImpl(
        const String & format,
        const S3Configuration & s3_configuration,
        const String & compression_method,
        bool distributed_processing,
        bool is_key_with_globs,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr ctx,
        std::vector<String> * read_keys_in_distributed_processing = nullptr);

    bool isColumnOriented() const override;
};

}

#endif
