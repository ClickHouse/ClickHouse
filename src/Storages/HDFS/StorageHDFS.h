#pragma once

#include <Common/config.h>

#if USE_HDFS

#include <Processors/Sources/SourceWithProgress.h>
#include <Storages/IStorage.h>
#include <Poco/URI.h>
#include <base/logger_useful.h>
#include <base/shared_ptr_helper.h>

namespace DB
{
/**
 * This class represents table engine for external hdfs files.
 * Read method is supported for now.
 */
class StorageHDFS final : public shared_ptr_helper<StorageHDFS>, public IStorage, WithContext
{
    friend struct shared_ptr_helper<StorageHDFS>;
public:
    String getName() const override { return "HDFS"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context) override;

    void truncate(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr local_context,
        TableExclusiveLockHolder &) override;

    NamesAndTypesList getVirtuals() const override;

    bool supportsPartitionBy() const override { return true; }

    /// Check if the format is column-oriented.
    /// Is is useful because column oriented formats could effectively skip unknown columns
    /// So we can create a header of only required columns in read method and ask
    /// format to read only them. Note: this hack cannot be done with ordinary formats like TSV.
    bool isColumnOriented() const override;

    static ColumnsDescription getTableStructureFromData(
        const String & format,
        const String & uri,
        const String & compression_method,
        ContextPtr ctx);

protected:
    friend class HDFSSource;
    StorageHDFS(
        const String & uri_,
        const StorageID & table_id_,
        const String & format_name_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        ContextPtr context_,
        const String & compression_method_ = "",
        bool distributed_processing_ = false,
        ASTPtr partition_by = nullptr);

private:
    std::vector<const String> uris;
    String format_name;
    String compression_method;
    const bool distributed_processing;
    ASTPtr partition_by;
    bool is_path_with_globs;

    Poco::Logger * log = &Poco::Logger::get("StorageHDFS");
};

class PullingPipelineExecutor;

class HDFSSource : public SourceWithProgress, WithContext
{
public:
    class DisclosedGlobIterator
    {
        public:
            DisclosedGlobIterator(ContextPtr context_, const String & uri_);
            String next();
        private:
            class Impl;
            /// shared_ptr to have copy constructor
            std::shared_ptr<Impl> pimpl;
    };

    class URISIterator
    {
        public:
            URISIterator(const std::vector<const String> & uris_, ContextPtr context);
            String next();
        private:
            class Impl;
            /// shared_ptr to have copy constructor
            std::shared_ptr<Impl> pimpl;
    };

    using IteratorWrapper = std::function<String()>;
    using StorageHDFSPtr = std::shared_ptr<StorageHDFS>;

    static Block getHeader(
        const StorageMetadataPtr & metadata_snapshot,
        bool need_path_column,
        bool need_file_column);

    static Block getBlockForSource(
        const StorageHDFSPtr & storage,
        const StorageSnapshotPtr & storage_snapshot_,
        const ColumnsDescription & columns_description,
        bool need_path_column,
        bool need_file_column);

    HDFSSource(
        StorageHDFSPtr storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        ContextPtr context_,
        UInt64 max_block_size_,
        bool need_path_column_,
        bool need_file_column_,
        std::shared_ptr<IteratorWrapper> file_iterator_,
        ColumnsDescription columns_description_);

    String getName() const override;

    Chunk generate() override;

    void onCancel() override;

private:
    StorageHDFSPtr storage;
    StorageSnapshotPtr storage_snapshot;
    UInt64 max_block_size;
    bool need_path_column;
    bool need_file_column;
    std::shared_ptr<IteratorWrapper> file_iterator;
    ColumnsDescription columns_description;

    std::unique_ptr<ReadBuffer> read_buf;
    std::unique_ptr<QueryPipeline> pipeline;
    std::unique_ptr<PullingPipelineExecutor> reader;
    /// onCancel and generate can be called concurrently.
    std::mutex reader_mutex;
    String current_path;

    /// Recreate ReadBuffer and PullingPipelineExecutor for each file.
    bool initialize();
};
}

#endif
