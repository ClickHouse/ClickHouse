#include <chrono>
#include <filesystem>
#include <base/types.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>
#include <Processors/ISource.h>
#include <Storages/StorageFilesystem.h>
#include <Storages/StorageFactory.h>
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

class FilesystemSource final : public ISource
{
public:
    struct PathInfo
    {
        ConcurrentBoundedQueue<fs::directory_entry> queue;
        const String user_files_absolute_path_string;
        const bool need_check;
        fs::recursive_directory_iterator directory_iterator;
        std::atomic_uint32_t rows = 1;

        PathInfo(size_t num_streams, String user_files_absolute_path_string_, bool need_check_)
            : queue(num_streams * 1000000L)
            , user_files_absolute_path_string(user_files_absolute_path_string_)
            , need_check(need_check_)
            , directory_iterator(user_files_absolute_path_string)
        {
        }
    };
    using PathInfoPtr = std::shared_ptr<PathInfo>;

    String getName() const override { return "Filesystem"; }

    Chunk generate() override
    {
        size_t current_block_size = 0;
        std::unordered_map<String, MutableColumnPtr> columns_map;
        auto names_and_types_in_use = storage_snapshot->getSampleBlockForColumns(columns_in_use).getNamesAndTypesList();
        for (const auto & [name, type] : names_and_types_in_use)
        {
            columns_map[name] = type->createColumn();
        }

        fs::directory_entry file;
        while (current_block_size < max_block_size)
        {
            if (!path_info->queue.tryPop(file))
            {
                LOG_TEST(&Poco::Logger::get("StorageFilesystem"), "No data read from queue, stop processing");
                break;
            }
            current_block_size++;
            LOG_TEST(&Poco::Logger::get("StorageFilesystem"), "Processing path {} remaining queue size {}",
                file.path().string(), path_info->queue.size());

            std::error_code ec;

            if (file.is_directory(ec) && !file.is_symlink(ec) && ec.value() == 0)
            {
                for (const auto & child : fs::directory_iterator(file, ec))
                {
                    fs::path file_path(child);
                    /// Do not use fs::canonical or fs::weakly_canonical.
                    /// Otherwise it will not allow to work with symlinks in `user_files_path` directory.
                    file_path = fs::absolute(file_path).lexically_normal();

                    if (path_info->need_check && file_path.string().find(path_info->user_files_absolute_path_string) != 0)
                    {
                        LOG_DEBUG(&Poco::Logger::get("StorageFilesystem"), "Path {} is not inside user_files {}",
                            file_path.string(),path_info->user_files_absolute_path_string);
                    }
                    if (!path_info->queue.push(child))
                        LOG_WARNING(&Poco::Logger::get("StorageFilesystem"), "Too many files to process, skipping some from {}",
                            file.path().string());
                }
            }
            else
            {
                LOG_TEST(&Poco::Logger::get("StorageFilesystem"), "Not looking at children of path {}, is_dir {}, is_sym {}, ec {}",
                    file.path().string(), file.is_directory(), file.is_symlink(), ec.value());
            }
            ec.clear();

            if (columns_map.contains("type"))
            {
                columns_map["type"]->insert(toString(file.status().type()));
            }

            if (columns_map.contains("symlink"))
            {
                columns_map["symlink"]->insert(file.is_symlink());
            }

            if (columns_map.contains("path"))
            {
                columns_map["path"]->insert(file.path().string());
            }
            if (columns_map.contains("size"))
            {
                auto is_regular_file = file.is_regular_file(ec);
                auto inserted = is_regular_file ? file.file_size(ec) : 0;
                if (ec.value() == 0 && is_regular_file)
                {
                    columns_map["size"]->insert(std::move(inserted));
                }
                else
                {
                    columns_map["size"]->insertDefault();
                    ec.clear();
                }
            }

            if (columns_map.contains("modification_time"))
            {
                auto file_time = fs::last_write_time(file.path().string(), ec);
                auto sys_clock_file_time = std::chrono::file_clock::to_sys(file_time);
                auto sys_clock_in_seconds_duration = std::chrono::time_point_cast<std::chrono::seconds>(sys_clock_file_time);
                auto file_time_since_epoch = sys_clock_in_seconds_duration.time_since_epoch().count();

                if (ec.value() == 0)
                {
                    columns_map["modification_time"]->insert(file_time_since_epoch);
                }
                else
                {
                    columns_map["modification_time"]->insertDefault();
                    ec.clear();
                }
            }

            for (const auto & [column_name, perm] : permissions_columns_names)
            {
                if (!columns_map.contains(column_name))
                    continue;
                columns_map[column_name]->insert(static_cast<bool>(file.status().permissions() & perm));
            }

            if (columns_map.contains("name"))
            {
                columns_map["name"]->insert(file.path().filename().string());
            }

            LOG_TEST(&Poco::Logger::get("StorageFilesystem"), "Processed path {} columns_map. {} ", file.path().string(),
                columns_map.empty() ? -1 : columns_map.begin()->second->size());
        }

        auto num_rows = columns_map.begin() != columns_map.end() ? columns_map.begin()->second->size() : 0;

        if (num_rows == 0)
        {
            return {};
        }

        Columns columns;

        for (const auto & [name, _] : names_and_types_in_use)
        {
            columns.emplace_back(std::move(columns_map[name]));
        }

        return {std::move(columns), num_rows};
    }

    FilesystemSource(
        const StorageSnapshotPtr & metadata_snapshot_, UInt64 max_block_size_, PathInfoPtr path_info_, Names column_names)
        : ISource(metadata_snapshot_->getSampleBlockForColumns(column_names))
        , storage_snapshot(metadata_snapshot_)
        , path_info(path_info_)
        , max_block_size(max_block_size_)
        , columns_in_use(std::move(column_names))
    {
    }

private:
    StorageSnapshotPtr storage_snapshot;
    PathInfoPtr path_info;
    ColumnsDescription columns_description;
    UInt64 max_block_size;
    Names columns_in_use;

    const std::vector<std::pair<String, fs::perms>> permissions_columns_names\
    {
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
        {"sticky_bit", fs::perms::sticky_bit}
    };
};

}


Pipe StorageFilesystem::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo &,
    ContextPtr,
    QueryProcessingStage::Enum,
    size_t max_block_size,
    size_t num_streams)
{
    auto this_ptr = std::static_pointer_cast<StorageFilesystem>(shared_from_this());

    auto path_info = std::make_shared<FilesystemSource::PathInfo>(num_streams, user_files_absolute_path_string, !local_mode);

    fs::path file_path(path);
    if (file_path.is_relative())
        file_path = fs::path(path_info->user_files_absolute_path_string) / file_path;
    /// Do not use fs::canonical or fs::weakly_canonical.
    /// Otherwise it will not allow to work with symlinks in `user_files_path` directory.
    file_path = fs::absolute(file_path).lexically_normal();


    if (path_info->need_check && file_path.string().find(path_info->user_files_absolute_path_string) != 0)
        throw Exception(ErrorCodes::DATABASE_ACCESS_DENIED, "Path {} is not inside user_files {}",
            file_path.string(), path_info->user_files_absolute_path_string);

    if (!fs::exists(file_path))
    {
        throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Directory {} doesn't exist", file_path.string());
    }

    if (!path_info->queue.push(fs::directory_entry(file_path)))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot schedule a file '{}'", file_path.string());

    Pipes pipes;
    for (size_t i = 0; i < num_streams; ++i)
    {
        pipes.emplace_back(std::make_shared<FilesystemSource>(storage_snapshot, max_block_size, path_info, column_names));
    }
    auto pipe = Pipe::unitePipes(std::move(pipes));
    return pipe;
}

StorageFilesystem::StorageFilesystem(
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    bool local_mode_,
    String path_,
    String user_files_absolute_path_string_
    )
    : IStorage(table_id_), local_mode(local_mode_), path(path_), user_files_absolute_path_string(user_files_absolute_path_string_)
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

NamesAndTypesList StorageFilesystem::getVirtuals() const
{
    return {};
}


void registerStorageFilesystem(StorageFactory & factory)
{
    factory.registerStorage("Filesystem", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() > 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Storage Filesystem requires one argument: path.");

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
        .source_access_type = AccessType::FILESYSTEM,
    });
}
}
