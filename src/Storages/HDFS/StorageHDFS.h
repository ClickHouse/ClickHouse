#pragma once

#include "config.h"

#if USE_HDFS

#include <Processors/ISource.h>
#include <Storages/IStorage.h>
#include <Storages/Cache/SchemaCache.h>
#include <Storages/prepareReadingFromFormat.h>
#include <Storages/SelectQueryInfo.h>
#include <Poco/URI.h>

namespace DB
{

class IInputFormat;

/**
 * This class represents table engine for external hdfs files.
 * Read method is supported for now.
 */
class StorageHDFS final : public IStorage, WithContext
{
public:
    struct PathInfo
    {
        time_t last_mod_time;
        size_t size;
    };

    struct PathWithInfo
    {
        PathWithInfo() = default;
        PathWithInfo(const String & path_, const std::optional<PathInfo> & info_) : path(path_), info(info_) {}
        String path;
        std::optional<PathInfo> info;
    };

    StorageHDFS(
        const String & uri_,
        const StorageID & table_id_,
        const String & format_name_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        const ContextPtr & context_,
        const String & compression_method_ = "",
        bool distributed_processing_ = false,
        ASTPtr partition_by = nullptr);

    String getName() const override { return "HDFS"; }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context, bool async_insert) override;

    void truncate(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr local_context,
        TableExclusiveLockHolder &) override;

    bool supportsPartitionBy() const override { return true; }

    /// Check if the format is column-oriented.
    /// Is is useful because column oriented formats could effectively skip unknown columns
    /// So we can create a header of only required columns in read method and ask
    /// format to read only them. Note: this hack cannot be done with ordinary formats like TSV.
    bool supportsSubsetOfColumns(const ContextPtr & context_) const;

    bool supportsSubcolumns() const override { return true; }

    static ColumnsDescription getTableStructureFromData(
        const String & format,
        const String & uri,
        const String & compression_method,
        const ContextPtr & ctx);

    static std::pair<ColumnsDescription, String> getTableStructureAndFormatFromData(
        const String & uri,
        const String & compression_method,
        const ContextPtr & ctx);

    static SchemaCache & getSchemaCache(const ContextPtr & ctx);

    bool supportsTrivialCountOptimization(const StorageSnapshotPtr &, ContextPtr) const override { return true; }

protected:
    friend class HDFSSource;
    friend class ReadFromHDFS;

private:
    static std::pair<ColumnsDescription, String> getTableStructureAndFormatFromDataImpl(
        std::optional<String> format,
        const String & uri,
        const String & compression_method,
        const ContextPtr & ctx);

    std::vector<String> uris;
    String format_name;
    String compression_method;
    const bool distributed_processing;
    ASTPtr partition_by;
    bool is_path_with_globs;

    LoggerPtr log = getLogger("StorageHDFS");
};

class PullingPipelineExecutor;

class HDFSSource : public ISource, WithContext
{
public:
    class DisclosedGlobIterator
    {
        public:
            DisclosedGlobIterator(const String & uri_, const ActionsDAG::Node * predicate, const NamesAndTypesList & virtual_columns, const ContextPtr & context);
            StorageHDFS::PathWithInfo next();
        private:
            class Impl;
            /// shared_ptr to have copy constructor
            std::shared_ptr<Impl> pimpl;
    };

    class URISIterator
    {
        public:
            URISIterator(const std::vector<String> & uris_, const ActionsDAG::Node * predicate, const NamesAndTypesList & virtual_columns, const ContextPtr & context);
            StorageHDFS::PathWithInfo next();
        private:
            class Impl;
            /// shared_ptr to have copy constructor
            std::shared_ptr<Impl> pimpl;
    };

    using IteratorWrapper = std::function<StorageHDFS::PathWithInfo()>;
    using StorageHDFSPtr = std::shared_ptr<StorageHDFS>;

    HDFSSource(
        const ReadFromFormatInfo & info,
        StorageHDFSPtr storage_,
        const ContextPtr & context_,
        UInt64 max_block_size_,
        std::shared_ptr<IteratorWrapper> file_iterator_,
        bool need_only_count_);

    ~HDFSSource() override;

    String getName() const override;

    Chunk generate() override;

private:
    void addNumRowsToCache(const String & path, size_t num_rows);
    std::optional<size_t> tryGetNumRowsFromCache(const StorageHDFS::PathWithInfo & path_with_info);

    StorageHDFSPtr storage;
    Block block_for_format;
    NamesAndTypesList requested_columns;
    NamesAndTypesList requested_virtual_columns;
    UInt64 max_block_size;
    std::shared_ptr<IteratorWrapper> file_iterator;
    ColumnsDescription columns_description;
    bool need_only_count;
    size_t total_rows_in_file = 0;

    std::unique_ptr<ReadBuffer> read_buf;
    std::shared_ptr<IInputFormat> input_format;
    std::unique_ptr<QueryPipeline> pipeline;
    std::unique_ptr<PullingPipelineExecutor> reader;
    String current_path;
    std::optional<size_t> current_file_size;

    /// Recreate ReadBuffer and PullingPipelineExecutor for each file.
    bool initialize();
};
}

#endif
