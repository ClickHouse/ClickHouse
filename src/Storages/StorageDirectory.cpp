#include <filesystem>
#include <Processors/ISource.h>
#include <Storages/StorageDirectory.h>

namespace fs = std::filesystem;

namespace DB
{
namespace ErrorCodes
{
    extern const int DIRECTORY_DOESNT_EXIST;
}

String fileTypeToString(fs::file_type type)
{
    switch (type)
    {
        case fs::file_type::none:
            return "none";
        case fs::file_type::not_found:
            return "not found";
        case fs::file_type::regular:
            return "regular file";
        case fs::file_type::directory:
            return "directory";
        case fs::file_type::symlink:
            return "symlink";
        case fs::file_type::block:
            return "block";
        case fs::file_type::character:
            return "character";
        case fs::file_type::fifo:
            return "fifo";
        case fs::file_type::socket:
            return "socket";
        case fs::file_type::unknown:
            return "unknown";
    }
}

String permissionsToString(fs::perms perms)
{
    using permissions = fs::perms;
    String result = "rwxrwxrwx";
    for (uint32_t i = 0; i < 9; ++i)
    {
        if (!(static_cast<uint32_t>(perms) & (1 << i)))
        {
            result[8 - i] = '-';
        }
    }

    if ((perms & permissions::set_uid) != permissions::none)
    {
        result[2] = 's';
    }

    if ((perms & permissions::set_gid) != permissions::none)
    {
        result[5] = 's';
    }

    if ((perms & permissions::sticky_bit) != permissions::none)
    {
        result[8] = 't';
    }

    return result;
}

template <typename T>
class ConcurrentQueue
{
public:
    bool pull(T * res)
    {
        std::unique_lock lock(mtx);
        if (queue.empty())
        {
            if (++curr >= max_streams)
            {
                closed = true;
                lock.unlock();
                cv.notify_all();
                return false;
            }
            cv.wait(lock, [this]() { return !queue.empty() || closed; });
            --curr;
        }
        if (closed)
        {
            return false;
        }
        *res = std::move(queue.front());
        queue.pop();
        return true;
    }

    void push(T value)
    {
        std::unique_lock lock(mtx);
        queue.push(std::move(value));
        lock.unlock();
        cv.notify_all();
    }

    explicit ConcurrentQueue(int max_streams_) : max_streams(max_streams_) { }

private:
    const int max_streams;
    std::atomic_int curr = 0;
    bool closed = false;
    std::condition_variable cv;
    std::queue<T> queue;

    std::mutex mtx;
};

class StorageDirectorySource final : public ISource
{
public:
    struct PathInfo
    {
        ConcurrentQueue<fs::directory_entry> queue;

        fs::recursive_directory_iterator directory_iterator;
        std::mutex mutex;
        const String path;
        std::atomic_uint32_t rows = 1;

        PathInfo(String path_, int max_streams) : queue(max_streams), directory_iterator(path_), path(path_)
        {
            queue.push(fs::directory_entry(path));
        }
    };
    using PathInfoPtr = std::shared_ptr<PathInfo>;

    String getName() const override { return DB::String(); }

    Chunk generate() override
    {
        std::unordered_map<String, MutableColumnPtr> columns_map;
        for (const auto & name : columns_in_use)
        {
            if (columns_description.has(name))
            {
                columns_map[name] = columns_description.get(name).type->createColumn();
            }
        }

        fs::directory_entry file;
        while (path_info->queue.pull(&file))
        {
            std::error_code ec;
            if (file.is_directory(ec) && !file.is_symlink(ec) && ec.value() == 0)
            {
                for (const auto & child : fs::directory_iterator(file, ec))
                {
                    if (path_info->rows++ >= this->max_block_size)
                    {
                        break;
                    }
                    path_info->queue.push(std::move(child));
                }
            }
            ec.clear();

            if (columns_map.contains("type"))
            {
                columns_map["type"]->insert(fileTypeToString(file.status().type()));
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

            if (columns_map.contains("last_write_time"))
            {
                auto inserted = std::chrono::file_clock::to_time_t(fs::last_write_time(file.path().string(), ec));
                if (ec.value() == 0)
                {
                    columns_map["last_write_time"]->insert(std::move(inserted));
                }
                else
                {
                    columns_map["last_write_time"]->insertDefault();
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
        }

        auto num_rows = columns_map.begin() != columns_map.end() ? columns_map.begin()->second->size() : 0;

        if (num_rows == 0)
        {
            return {};
        }

        Columns columns;

        for (const auto & column : columns_description)
        {
            if (columns_map.contains(column.name))
            {
                columns.emplace_back(std::move(columns_map[column.name]));
            }
        }

        return {std::move(columns), num_rows};
    }

    StorageDirectorySource(
        const StorageMetadataPtr & metadata_snapshot_,
        UInt64 max_block_size_,
        PathInfoPtr path_info_,
        ColumnsDescription columns_description_,
        Names column_names)
        : ISource(metadata_snapshot_->getSampleBlockForColumns(column_names))
        , metadata_snapshot(metadata_snapshot_)
        , path_info(path_info_)
        , columns_description(std::move(columns_description_))
        , max_block_size(max_block_size_)
        , columns_in_use(std::move(column_names))
    {
    }

private:
    StorageMetadataPtr metadata_snapshot;
    PathInfoPtr path_info;
    ColumnsDescription columns_description;
    UInt64 max_block_size;
    Names columns_in_use;

    const std::vector<std::pair<String, fs::perms>> permissions_columns_names{
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
        {"sticky_bit", fs::perms::sticky_bit}};
};


Pipe StorageDirectory::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo &,
    ContextPtr,
    QueryProcessingStage::Enum,
    size_t max_block_size,
    unsigned int num_streams)
{
    auto this_ptr = std::static_pointer_cast<StorageDirectory>(shared_from_this());
    if (!fs::exists(path))
    {
        throw Exception("Directory " + path + " doesn't exist", ErrorCodes::DIRECTORY_DOESNT_EXIST);
    }

    auto path_info = std::make_shared<StorageDirectorySource::PathInfo>(path, num_streams);
    Pipes pipes;
    for (unsigned i = 0; i < num_streams; ++i)
    {
        pipes.emplace_back(std::make_shared<StorageDirectorySource>(
            metadata_snapshot, max_block_size, path_info, metadata_snapshot->getColumns(), column_names));
    }
    auto pipe = Pipe::unitePipes(std::move(pipes));
    return pipe;
}
StorageDirectory::StorageDirectory(
    const StorageID & table_id_,
    ColumnsDescription columns_description_,
    String path_,
    ConstraintsDescription constraints_,
    const String & comment)
    : IStorage(table_id_), path(path_)
{
    StorageInMemoryMetadata metadata;
    metadata.setColumns(columns_description_);
    metadata.setConstraints(constraints_);
    metadata.setComment(comment);
    setInMemoryMetadata(metadata);
}
Strings StorageDirectory::getDataPaths() const
{
    return {path};
}
NamesAndTypesList StorageDirectory::getVirtuals() const
{
    return {};
}
}
