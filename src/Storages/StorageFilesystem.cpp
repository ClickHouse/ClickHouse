#include <chrono>
#include <filesystem>
#include <mutex>
#include <unordered_set>
#include <base/types.h>
#include <Core/DecimalFunctions.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/filesystemHelpers.h>
#include <Common/logger_useful.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/StorageFilesystem.h>
#include <Storages/StorageFactory.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/checkAndGetLiteralArgument.h>


namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int DATABASE_ACCESS_DENIED;
    extern const int DIRECTORY_DOESNT_EXIST;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


namespace
{

/// Must match the Enum8 values in TableFunctionFilesystem::getActualTableStructure.
Int8 fileTypeToEnumValue(fs::file_type type)
{
    switch (type)
    {
        case fs::file_type::none:       return 0;
        case fs::file_type::not_found:  return 1;
        case fs::file_type::regular:    return 2;
        case fs::file_type::directory:  return 3;
        case fs::file_type::symlink:    return 4;
        case fs::file_type::block:      return 5;
        case fs::file_type::character:  return 6;
        case fs::file_type::fifo:       return 7;
        case fs::file_type::socket:     return 8;
        case fs::file_type::unknown:    return 9;
    }
    return 9; /// unknown
}

struct QueueEntry
{
    fs::directory_entry entry;
    UInt16 depth;
};

/// Compute the output depth from internal traversal depth.
/// Root entry has internal depth 0, its children 1, grandchildren 2, etc.
/// Output: 0 for root and its direct children, 1 for grandchildren, etc.
UInt16 outputDepth(UInt16 internal_depth)
{
    return internal_depth > 0 ? internal_depth - 1 : 0;
}

/// Fill a "cheap" column value (obtainable without extra syscalls) for a single entry.
void fillCheapColumnValue(const String & column_name, IColumn & column, const QueueEntry & queue_entry)
{
    const auto & file = queue_entry.entry;
    std::error_code ec;

    if (column_name == "path")
        column.insert(file.path().parent_path().string());
    else if (column_name == "name")
        column.insert(file.path().filename().string());
    else if (column_name == "depth")
        column.insert(outputDepth(queue_entry.depth));
    else if (column_name == "type")
    {
        auto status = file.status(ec);
        column.insert(ec ? Int8(9) : fileTypeToEnumValue(status.type()));
    }
    else if (column_name == "is_symlink")
    {
        column.insert(file.is_symlink(ec));
    }
}

/// Names of columns that are cheap to compute (no file I/O beyond what the directory iterator provides).
const std::vector<String> cheap_column_names = {"path", "name", "depth", "type", "is_symlink"};

const std::vector<std::pair<String, fs::perms>> permissions_columns = {
    {"owner_read", fs::perms::owner_read},
    {"owner_write", fs::perms::owner_write},
    {"owner_exec", fs::perms::owner_exec},
    {"group_read", fs::perms::group_read},
    {"group_write", fs::perms::group_write},
    {"group_exec", fs::perms::group_exec},
    {"others_read", fs::perms::others_read},
    {"others_write", fs::perms::others_write},
    {"others_exec", fs::perms::others_exec},
    {"set_gid", fs::perms::set_gid},
    {"set_uid", fs::perms::set_uid},
    {"sticky_bit", fs::perms::sticky_bit},
};


class FilesystemSource final : public ISource
{
public:
    struct PathInfo
    {
        ConcurrentBoundedQueue<QueueEntry> queue;
        std::atomic<int64_t> in_flight{0};
        std::mutex visited_mutex;
        std::unordered_set<String> visited;
        const String user_files_absolute_path_string;
        const bool need_check;

        PathInfo(String user_files_absolute_path_string_, bool need_check_)
            : queue(10000)
            , user_files_absolute_path_string(std::move(user_files_absolute_path_string_))
            , need_check(need_check_)
        {
        }
    };
    using PathInfoPtr = std::shared_ptr<PathInfo>;

    String getName() const override { return "Filesystem"; }

    FilesystemSource(
        const StorageSnapshotPtr & storage_snapshot_, UInt64 max_block_size_, PathInfoPtr path_info_, Names column_names,
        ExpressionActionsPtr filter_expression_, Block filter_sample_block_)
        : ISource(std::make_shared<const Block>(storage_snapshot_->getSampleBlockForColumns(column_names)))
        , storage_snapshot(storage_snapshot_)
        , path_info(std::move(path_info_))
        , max_block_size(max_block_size_)
        , columns_in_use(std::move(column_names))
        , filter_expression(std::move(filter_expression_))
        , filter_sample_block(std::move(filter_sample_block_))
    {
    }

    Chunk generate() override
    {
        auto names_and_types_in_use = storage_snapshot->getSampleBlockForColumns(columns_in_use).getNamesAndTypesList();

        std::unordered_map<String, MutableColumnPtr> columns_map;
        for (const auto & [name, type] : names_and_types_in_use)
            columns_map[name] = type->createColumn();

        bool need_content = columns_map.contains("content");
        size_t total_rows = 0;

        /// Collect entries in sub-batches, filter each, and accumulate matching rows.
        /// Small sub-batches ensure we return results promptly even with highly selective filters.
        /// Use a smaller batch when reading file content, as each entry may involve heavy I/O.
        const size_t sub_batch_size = need_content ? 64 : 1024;

        while (total_rows < max_block_size)
        {
            std::vector<QueueEntry> entries;
            entries.reserve(sub_batch_size);

            while (entries.size() < sub_batch_size)
            {
                if (isCancelled())
                    break;

                QueueEntry queue_entry;
                if (!popEntry(queue_entry))
                    break;

                expandDirectory(queue_entry);

                /// Decrement in_flight now: this entry has been fully processed
                /// (children queued if it was a directory). Must happen here, not later,
                /// because pop() blocks when the queue is empty but in_flight > 0.
                if (path_info->in_flight.fetch_sub(1) == 1)
                    path_info->queue.finish();

                entries.push_back(std::move(queue_entry));
            }

            if (entries.empty())
                break;

            /// Filter on cheap columns.
            std::vector<bool> mask = evaluateFilter(entries);

            /// Fill output columns for matching entries.
            for (size_t i = 0; i < entries.size(); ++i)
            {
                if (!mask[i])
                    continue;

                fillEntryColumns(entries[i], columns_map, need_content);
                ++total_rows;
            }

            /// Return as soon as we have any matching rows.
            /// This ensures prompt output with selective filters on large directory trees.
            if (total_rows > 0)
                break;
        }

        if (total_rows == 0)
            return {};

        Columns columns;
        for (const auto & [name, _] : names_and_types_in_use)
            columns.emplace_back(std::move(columns_map[name]));

        return {std::move(columns), total_rows};
    }

private:
    StorageSnapshotPtr storage_snapshot;
    PathInfoPtr path_info;
    UInt64 max_block_size;
    Names columns_in_use;
    ExpressionActionsPtr filter_expression;
    Block filter_sample_block;

    /// Thread-local overflow buffer for entries that couldn't fit in the shared queue.
    /// Acts as a DFS fallback when the bounded queue is full, preventing entry loss.
    std::vector<QueueEntry> local_overflow;

    /// Pop an entry: first from local overflow, then from the shared queue.
    /// Uses tryPop with a timeout so that isCancelled() is checked periodically.
    bool popEntry(QueueEntry & queue_entry)
    {
        /// Drain local overflow first (DFS order).
        if (!local_overflow.empty())
        {
            queue_entry = std::move(local_overflow.back());
            local_overflow.pop_back();
            return true;
        }

        while (!path_info->queue.tryPop(queue_entry, /* milliseconds= */ 100))
        {
            if (isCancelled() || path_info->queue.isFinishedAndEmpty())
                return false;
        }
        return true;
    }

    /// Expand a directory entry: push its children into the shared queue.
    /// If the queue is full, store in thread-local overflow to avoid deadlock
    /// (a single thread cannot both push and pop simultaneously).
    void expandDirectory(const QueueEntry & queue_entry)
    {
        const auto & file = queue_entry.entry;
        std::error_code ec;

        if (!file.is_directory(ec) || ec)
            return;

        /// Use a try-catch because directory_iterator increment can throw
        /// on I/O errors, and we must not leak in_flight counts.
        try
        {
            for (const auto & child : fs::directory_iterator(file, ec))
            {
                if (isCancelled())
                    return;

                fs::path child_path = fs::absolute(child.path()).lexically_normal();

                if (path_info->need_check && !fileOrSymlinkPathStartsWith(child_path.string(), path_info->user_files_absolute_path_string))
                {
                    LOG_DEBUG(getLogger("StorageFilesystem"), "Path {} is not inside user_files {}",
                        child_path.string(), path_info->user_files_absolute_path_string);
                    continue;
                }

                if (child.is_directory(ec) && child.is_symlink(ec))
                {
                    std::error_code canon_ec;
                    auto canonical = fs::canonical(child_path, canon_ec);
                    if (!canon_ec)
                    {
                        std::lock_guard lock(path_info->visited_mutex);
                        if (!path_info->visited.emplace(canonical.string()).second)
                            continue;
                    }
                }

                path_info->in_flight.fetch_add(1);
                if (!path_info->queue.tryPush(QueueEntry{child, static_cast<UInt16>(queue_entry.depth + 1)}))
                {
                    /// Queue full — store locally to avoid deadlock.
                    local_overflow.emplace_back(QueueEntry{child, static_cast<UInt16>(queue_entry.depth + 1)});
                }
            }
        }
        catch (const fs::filesystem_error & e)
        {
            /// Ignore filesystem errors during directory iteration (e.g. permission denied, removed directory).
            LOG_DEBUG(getLogger("StorageFilesystem"), "Filesystem error during directory iteration: {}", e.what());
        }
    }

    /// Evaluate the pushed-down filter on cheap columns and return a bitmask.
    /// If no filter, returns all-true mask.
    std::vector<bool> evaluateFilter(const std::vector<QueueEntry> & entries) const
    {
        std::vector<bool> mask(entries.size(), true);

        if (!filter_expression)
            return mask;

        Block cheap_block;
        for (size_t col_idx = 0; col_idx < filter_sample_block.columns(); ++col_idx)
        {
            const auto & col_info = filter_sample_block.getByPosition(col_idx);
            auto column = col_info.type->createColumn();

            for (const auto & entry : entries)
                fillCheapColumnValue(col_info.name, *column, entry);

            cheap_block.insert(ColumnWithTypeAndName(std::move(column), col_info.type, col_info.name));
        }

        filter_expression->execute(cheap_block);

        const auto & result_name = filter_expression->getSampleBlock().getByPosition(
            filter_expression->getSampleBlock().columns() - 1).name;
        const auto & result_column = cheap_block.getByName(result_name).column;

        for (size_t i = 0; i < entries.size(); ++i)
            mask[i] = result_column->getBool(i);

        return mask;
    }

    /// Fill all requested columns for a single entry.
    void fillEntryColumns(
        const QueueEntry & queue_entry,
        std::unordered_map<String, MutableColumnPtr> & columns_map,
        bool need_content) const
    {
        const auto & file = queue_entry.entry;
        std::error_code ec;

        /// Cheap columns.
        for (const auto & col_name : cheap_column_names)
            if (auto it = columns_map.find(col_name); it != columns_map.end())
                fillCheapColumnValue(col_name, *it->second, queue_entry);

        /// Size.
        if (columns_map.contains("size"))
        {
            auto is_regular = file.is_regular_file(ec);
            if (!ec && is_regular)
            {
                auto sz = file.file_size(ec);
                if (!ec)
                    columns_map["size"]->insert(sz);
                else
                {
                    columns_map["size"]->insertDefault();
                    ec.clear();
                }
            }
            else
            {
                columns_map["size"]->insertDefault();
                ec.clear();
            }
        }

        /// Modification time.
        if (columns_map.contains("modification_time"))
        {
            auto file_time = fs::last_write_time(file.path(), ec);
            if (!ec)
            {
                auto sys_time = std::chrono::file_clock::to_sys(file_time);
                auto us = std::chrono::duration_cast<std::chrono::microseconds>(sys_time.time_since_epoch()).count();
                columns_map["modification_time"]->insert(DecimalField<DateTime64>(us, 6));
            }
            else
            {
                columns_map["modification_time"]->insertDefault();
                ec.clear();
            }
        }

        /// Content.
        if (need_content)
        {
            auto is_regular = file.is_regular_file(ec);
            if (!ec && is_regular)
            {
                try
                {
                    String content;
                    ReadBufferFromFile in(file.path().string());
                    readStringUntilEOF(content, in);
                    columns_map["content"]->insert(std::move(content));
                }
                catch (...)
                {
                    columns_map["content"]->insertDefault();
                }
            }
            else
            {
                columns_map["content"]->insertDefault();
                ec.clear();
            }
        }

        /// Permissions.
        std::optional<fs::file_status> cached_status;
        for (const auto & [col_name, perm] : permissions_columns)
        {
            if (!columns_map.contains(col_name))
                continue;
            if (!cached_status)
            {
                cached_status = file.status(ec);
                if (ec)
                {
                    for (const auto & [cn, _] : permissions_columns)
                        if (columns_map.contains(cn))
                            columns_map[cn]->insertDefault();
                    ec.clear();
                    return;
                }
            }
            columns_map[col_name]->insert(static_cast<bool>(cached_status->permissions() & perm));
        }
    }
};


class ReadFromFilesystem final : public SourceStepWithFilter
{
public:
    std::string getName() const override { return "ReadFromFilesystem"; }

    ReadFromFilesystem(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        bool local_mode_,
        String path_,
        String user_files_absolute_path_string_,
        size_t max_block_size_,
        size_t num_streams_)
        : SourceStepWithFilter(
            std::make_shared<const Block>(storage_snapshot_->getSampleBlockForColumns(column_names_)),
            column_names_,
            query_info_,
            storage_snapshot_,
            context_)
        , local_mode(local_mode_)
        , path(std::move(path_))
        , user_files_absolute_path_string(std::move(user_files_absolute_path_string_))
        , max_block_size(max_block_size_)
        , num_streams(num_streams_)
    {
    }

    void applyFilters(ActionDAGNodes added_filter_nodes) override
    {
        SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));

        if (!filter_actions_dag)
            return;

        /// Build a sample block with cheap columns for filter extraction.
        Block cheap_sample;
        auto sample_block = storage_snapshot->metadata->getSampleBlock();
        for (const auto & col_name : cheap_column_names)
        {
            const auto & col = sample_block.getByName(col_name);
            cheap_sample.insert({col.type->createColumn(), col.type, col.name});
        }

        auto filter_dag = VirtualColumnUtils::splitFilterDagForAllowedInputs(
            filter_actions_dag->getOutputs().at(0), &cheap_sample, context);

        if (filter_dag)
        {
            VirtualColumnUtils::buildSetsForDAG(*filter_dag, context);
            filter_expression = VirtualColumnUtils::buildFilterExpression(std::move(*filter_dag), context);
            filter_sample_block = std::move(cheap_sample);
        }
    }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override
    {
        auto path_info = std::make_shared<FilesystemSource::PathInfo>(user_files_absolute_path_string, !local_mode);

        fs::path file_path(path);
        if (file_path.is_relative())
            file_path = fs::path(path_info->user_files_absolute_path_string) / file_path;
        file_path = fs::absolute(file_path).lexically_normal();

        if (path_info->need_check && !fileOrSymlinkPathStartsWith(file_path.string(), path_info->user_files_absolute_path_string))
            throw Exception(ErrorCodes::DATABASE_ACCESS_DENIED, "Path {} is not inside user_files {}",
                file_path.string(), path_info->user_files_absolute_path_string);

        if (!fs::exists(file_path))
            throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Directory {} doesn't exist", file_path.string());

        /// Register the root directory as visited (by canonical path) to prevent symlink cycles.
        {
            std::error_code canon_ec;
            auto canonical = fs::canonical(file_path, canon_ec);
            if (!canon_ec)
                path_info->visited.emplace(canonical.string());
        }

        path_info->in_flight.store(1);
        if (!path_info->queue.push(QueueEntry{fs::directory_entry(file_path), 0}))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot schedule a file '{}'", file_path.string());

        Pipes pipes;
        for (size_t i = 0; i < num_streams; ++i)
            pipes.emplace_back(std::make_shared<FilesystemSource>(
                storage_snapshot, max_block_size, path_info, required_source_columns,
                filter_expression, filter_sample_block));

        pipeline.init(Pipe::unitePipes(std::move(pipes)));
    }

private:
    bool local_mode;
    String path;
    String user_files_absolute_path_string;
    size_t max_block_size;
    size_t num_streams;
    ExpressionActionsPtr filter_expression;
    Block filter_sample_block;
};

}


void StorageFilesystem::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum,
    size_t max_block_size,
    size_t num_streams)
{
    query_plan.addStep(std::make_unique<ReadFromFilesystem>(
        column_names, query_info, storage_snapshot, context,
        local_mode, path, user_files_absolute_path_string,
        max_block_size, num_streams));
}

StorageFilesystem::StorageFilesystem(
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    bool local_mode_,
    String path_,
    String user_files_absolute_path_string_)
    : IStorage(table_id_)
    , local_mode(local_mode_)
    , path(std::move(path_))
    , user_files_absolute_path_string(std::move(user_files_absolute_path_string_))
{
    StorageInMemoryMetadata metadata;
    metadata.setColumns(columns_);
    metadata.setConstraints(constraints_);
    metadata.setComment(comment);
    setInMemoryMetadata(metadata);
}

Strings StorageFilesystem::getDataPaths() const
{
    return {path};
}


void registerStorageFilesystem(StorageFactory & factory)
{
    factory.registerStorage("Filesystem", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() > 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Storage Filesystem requires at most one argument: path.");

        String path;
        if (!engine_args.empty())
        {
            const auto & ast_literal = engine_args.front()->as<const ASTLiteral &>();
            if (!ast_literal.value.isNull())
                path = checkAndGetLiteralArgument<String>(ast_literal, "path");
        }

        String user_files_absolute_path_string = fs::canonical(fs::path(args.getContext()->getUserFilesPath()).string());
        bool local_mode = args.getContext()->getApplicationType() == Context::ApplicationType::LOCAL;

        return std::make_shared<StorageFilesystem>(
            args.table_id, args.columns, args.constraints, args.comment, local_mode, path,
            user_files_absolute_path_string);
    },
    {
        .source_access_type = AccessTypeObjects::Source::FILE,
    });
}
}
