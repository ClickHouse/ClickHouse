#pragma once

#include <Common/config.h>

#if USE_AWS_S3

#include <Core/Types.h>

#include <Compression/CompressionInfo.h>

#include <Storages/IStorage.h>
#include <Storages/StorageS3Settings.h>

#include <Processors/Sources/SourceWithProgress.h>
#include <Poco/URI.h>
#include <base/logger_useful.h>
#include <base/shared_ptr_helper.h>
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

    using IteratorWrapper = std::function<String()>;

    static Block getHeader(Block sample_block, bool with_path_column, bool with_file_column);

    StorageS3Source(
        bool need_path,
        bool need_file,
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
        std::shared_ptr<IteratorWrapper> file_iterator_);

    String getName() const override;

    Chunk generate() override;

    void onCancel() override;

private:
    String name;
    String bucket;
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
    bool initialized = false;
    bool with_file_column = false;
    bool with_path_column = false;
    std::shared_ptr<IteratorWrapper> file_iterator;

    /// Recreate ReadBuffer and BlockInputStream for each file.
    bool initialize();
};

/**
 * This class represents table engine for external S3 urls.
 * It sends HTTP GET to server when select is called and
 * HTTP PUT when insert is called.
 */
class StorageS3 : public shared_ptr_helper<StorageS3>, public IStorage, WithContext
{
public:
    StorageS3(
        const S3::URI & uri,
        const String & access_key_id,
        const String & secret_access_key,
        const StorageID & table_id_,
        const String & format_name_,
        UInt64 max_single_read_retries_,
        UInt64 min_upload_part_size_,
        UInt64 upload_part_size_multiply_factor_,
        UInt64 upload_part_size_multiply_parts_count_threshold_,
        UInt64 max_single_part_upload_size_,
        UInt64 max_connections_,
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
        UInt64 max_connections,
        UInt64 max_single_read_retries,
        const String & compression_method,
        bool distributed_processing,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr ctx);

private:
    friend class StorageS3Cluster;
    friend class TableFunctionS3Cluster;

    struct ClientAuthentication
    {
        const S3::URI uri;
        const String access_key_id;
        const String secret_access_key;
        const UInt64 max_connections;
        std::shared_ptr<Aws::S3::S3Client> client;
        S3AuthSettings auth_settings;
    };

    ClientAuthentication client_auth;
    std::vector<String> keys;

    String format_name;
    UInt64 max_single_read_retries;
    size_t min_upload_part_size;
    size_t upload_part_size_multiply_factor;
    size_t upload_part_size_multiply_parts_count_threshold;
    size_t max_single_part_upload_size;
    String compression_method;
    String name;
    const bool distributed_processing;
    std::optional<FormatSettings> format_settings;
    ASTPtr partition_by;
    bool is_key_with_globs = false;

    static void updateClientAndAuthSettings(ContextPtr, ClientAuthentication &);

    static std::shared_ptr<StorageS3Source::IteratorWrapper> createFileIterator(const ClientAuthentication & client_auth, const std::vector<String> & keys, bool is_key_with_globs, bool distributed_processing, ContextPtr local_context);

    static ColumnsDescription getTableStructureFromDataImpl(
        const String & format,
        const ClientAuthentication & client_auth,
        UInt64 max_single_read_retries,
        const String & compression_method,
        bool distributed_processing,
        bool is_key_with_globs,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr ctx);

    bool isColumnOriented() const override;
};

}

#endif
