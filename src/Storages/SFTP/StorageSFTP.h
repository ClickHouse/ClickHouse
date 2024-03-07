#pragma once

#include "config.h"

#include <Processors/ISource.h>
#include <Storages/IStorage.h>
#include <Storages/Cache/SchemaCache.h>
#include <Storages/prepareReadingFromFormat.h>
#include <Storages/SelectQueryInfo.h>
#include <Poco/URI.h>
#include <Storages/SFTP/SSHWrapper.h>

namespace DB
{

    class IInputFormat;
/**
 * This class represents table engine for external SFTP files.
 * Read method is supported for now.
 */
    class StorageSFTP final : public IStorage, WithContext
    {
    public:
        struct Configuration {
            String host;
            UInt16 port;
            String user;
            String password;
            String path;
        };
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

        StorageSFTP(
                Configuration configuration,
                const StorageID & table_id_,
                const String & format_name_,
                const ColumnsDescription & columns_,
                const ConstraintsDescription & constraints_,
                const String & comment,
                ContextPtr context_,
                const String & compression_method_ = "",
                bool distributed_processing_ = false,
                ASTPtr partition_by = nullptr);

        String getName() const override { return "SFTP"; }

        Pipe read(
                const Names & column_names,
                const StorageSnapshotPtr & storage_snapshot,
                SelectQueryInfo & query_info,
                ContextPtr context,
                QueryProcessingStage::Enum processed_stage,
                size_t max_block_size,
                size_t num_streams) override;

        NamesAndTypesList getVirtuals() const override;

        bool supportsPartitionBy() const override { return true; }

        /// Check if the format is column-oriented.
        /// Is is useful because column oriented formats could effectively skip unknown columns
        /// So we can create a header of only required columns in read method and ask
        /// format to read only them. Note: this hack cannot be done with ordinary formats like TSV.
        bool supportsSubsetOfColumns(const ContextPtr & context_) const;

        bool supportsSubcolumns() const override { return true; }

        static ColumnsDescription getTableStructureFromData(
                const String & format,
                const std::shared_ptr<SFTPWrapper> &client,
                const String & uri,
                const String & path,
                const String & compression_method,
                ContextPtr ctx);

        static SchemaCache & getSchemaCache(const ContextPtr & ctx);

        bool supportsTrivialCountOptimization() const override { return true; }

    protected:
        friend class SFTPSource;

    private:
        static std::optional<ColumnsDescription> tryGetColumnsFromCache(
                const std::shared_ptr<SFTPWrapper> &client,
                const std::vector<StorageSFTP::PathWithInfo> & paths_with_info,
                const String & uri_without_path,
                const String & format_name,
                const ContextPtr & ctx);

        static void addColumnsToCache(
                const std::vector<StorageSFTP::PathWithInfo> & paths,
                const String & uri_without_path,
                const ColumnsDescription & columns,
                const String & format_name,
                const ContextPtr & ctx);

        Configuration configuration;
        String uri;
        String format_name;
        String compression_method;
        const bool distributed_processing;
        ASTPtr partition_by;
        bool is_path_with_globs;
        NamesAndTypesList virtual_columns;
        std::shared_ptr<SFTPWrapper> client;

        Poco::Logger * log = &Poco::Logger::get("StorageSFTP");
    };

    class PullingPipelineExecutor;

    class SFTPSource : public ISource, WithContext
    {
    public:
        class DisclosedGlobIterator
        {
        public:
            DisclosedGlobIterator(
                const std::shared_ptr<SFTPWrapper> &client_,
                const String &path,
                const ActionsDAG::Node * predicate,
                const NamesAndTypesList &virtual_columns,
                const ContextPtr &context);
            StorageSFTP::PathWithInfo next();
        private:
            class Impl;
            /// shared_ptr to have copy constructor
            std::shared_ptr<Impl> pimpl;
        };

        class URISIterator
        {
        public:
            URISIterator(
                const std::shared_ptr<SFTPWrapper> &client_,
                const std::vector<String> &uris_with_paths_,
                const ActionsDAG::Node * predicate,
                const NamesAndTypesList &virtual_columns,
                const ContextPtr &context);
            StorageSFTP::PathWithInfo next();
        private:
            class Impl;
            /// shared_ptr to have copy constructor
            std::shared_ptr<Impl> pimpl;
        };

        using IteratorWrapper = std::function<StorageSFTP::PathWithInfo()>;
        using StorageSFTPPtr = std::shared_ptr<StorageSFTP>;

        SFTPSource(
                const StorageSFTP::Configuration &configuration_,
                const ReadFromFormatInfo & info,
                StorageSFTPPtr storage_,
                ContextPtr context_,
                UInt64 max_block_size_,
                std::shared_ptr<IteratorWrapper> file_iterator_,
                bool need_only_count_,
                const SelectQueryInfo & query_info_);

        String getName() const override;

        Chunk generate() override;

    private:
        void addNumRowsToCache(const String & path, size_t num_rows);
        std::optional<size_t> tryGetNumRowsFromCache(const StorageSFTP::PathWithInfo & path_with_info);

        std::shared_ptr<SFTPWrapper> client;
        StorageSFTP::Configuration configuration;
        StorageSFTPPtr storage;
        Block block_for_format;
        NamesAndTypesList requested_columns;
        NamesAndTypesList requested_virtual_columns;
        UInt64 max_block_size;
        std::shared_ptr<IteratorWrapper> file_iterator;
        ColumnsDescription columns_description;
        bool need_only_count;
        size_t total_rows_in_file = 0;
        SelectQueryInfo query_info;

        std::unique_ptr<ReadBuffer> read_buf;
        std::shared_ptr<IInputFormat> input_format;
        std::unique_ptr<QueryPipeline> pipeline;
        std::unique_ptr<PullingPipelineExecutor> reader;
        String current_path;

        /// Recreate ReadBuffer and PullingPipelineExecutor for each file.
        bool initialize();
    };
}
