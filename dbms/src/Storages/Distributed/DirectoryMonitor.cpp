#include <DataStreams/RemoteBlockOutputStream.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <Common/escapeForFileName.h>
#include <Common/setThreadName.h>
#include <Common/CurrentMetrics.h>
#include <Common/StringUtils.h>
#include <Common/ClickHouseRevision.h>
#include <Interpreters/Context.h>
#include <Storages/Distributed/DirectoryMonitor.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/CompressedReadBuffer.h>
#include <IO/Operators.h>

#include <boost/algorithm/string/find_iterator.hpp>
#include <boost/algorithm/string/finder.hpp>

#include <Poco/DirectoryIterator.h>


namespace CurrentMetrics
{
    extern const Metric DistributedSend;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_FILE_NAME;
    extern const int CHECKSUM_DOESNT_MATCH;
    extern const int TOO_LARGE_SIZE_COMPRESSED;
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
}


namespace
{
    static constexpr const std::chrono::seconds max_sleep_time{30};
    static constexpr const std::chrono::minutes decrease_error_count_period{5};

    template <typename PoolFactory>
    ConnectionPoolPtrs createPoolsForAddresses(const std::string & name, PoolFactory && factory)
    {
        ConnectionPoolPtrs pools;

        for (auto it = boost::make_split_iterator(name, boost::first_finder(",")); it != decltype(it){}; ++it)
        {
            const auto address = boost::copy_range<std::string>(*it);
            const char * address_begin = static_cast<const char*>(address.data());
            const char * address_end = address_begin + address.size();

            const char * user_pw_end = strchr(address.data(), '@');
            const char * colon = strchr(address.data(), ':');
            if (!user_pw_end || !colon)
                throw Exception{
                    "Shard address '" + address + "' does not match to 'user[:password]@host:port#default_database' pattern",
                    ErrorCodes::INCORRECT_FILE_NAME
                };

            const bool has_pw = colon < user_pw_end;
            const char * host_end = has_pw ? strchr(user_pw_end + 1, ':') : colon;
            if (!host_end)
                throw Exception{
                    "Shard address '" + address + "' does not contain port",
                    ErrorCodes::INCORRECT_FILE_NAME
                };

            const char * has_db = strchr(address.data(), '#');
            const char * port_end = has_db ? has_db : address_end;

            const auto user = unescapeForFileName(std::string(address_begin, has_pw ? colon : user_pw_end));
            const auto password = has_pw ? unescapeForFileName(std::string(colon + 1, user_pw_end)) : std::string();
            const auto host = unescapeForFileName(std::string(user_pw_end + 1, host_end));
            const auto port = parse<UInt16>(host_end + 1, port_end - (host_end + 1));
            const auto database = has_db ? unescapeForFileName(std::string(has_db + 1, address_end))
                                         : std::string();

            pools.emplace_back(factory(host, port, user, password, database));
        }

        return pools;
    }
}


StorageDistributedDirectoryMonitor::StorageDistributedDirectoryMonitor(StorageDistributed & storage, const std::string & name)
    : storage(storage), pool{createPool(name)}, path{storage.path + name + '/'}
    , current_batch_file_path{path + "current_batch.txt"}
    , default_sleep_time{storage.context.getSettingsRef().distributed_directory_monitor_sleep_time_ms.totalMilliseconds()}
    , sleep_time{default_sleep_time}
    , log{&Logger::get(getLoggerName())}
{
    const Settings & settings = storage.context.getSettingsRef();
    should_batch_inserts = settings.distributed_directory_monitor_batch_inserts;
    min_batched_block_size_rows = settings.min_insert_block_size_rows;
    min_batched_block_size_bytes = settings.min_insert_block_size_bytes;
}


StorageDistributedDirectoryMonitor::~StorageDistributedDirectoryMonitor()
{
    {
        quit = true;
        std::lock_guard<std::mutex> lock{mutex};
    }
    cond.notify_one();
    thread.join();
}


void StorageDistributedDirectoryMonitor::run()
{
    setThreadName("DistrDirMonitor");

    std::unique_lock<std::mutex> lock{mutex};

    const auto quit_requested = [this] { return quit; };

    while (!quit_requested())
    {
        auto do_sleep = true;

        try
        {
            do_sleep = !findFiles();
        }
        catch (...)
        {
            do_sleep = true;
            ++error_count;
            sleep_time = std::min(
                std::chrono::milliseconds{Int64(default_sleep_time.count() * std::exp2(error_count))},
                std::chrono::milliseconds{max_sleep_time});
            tryLogCurrentException(getLoggerName().data());
        };

        if (do_sleep)
            cond.wait_for(lock, sleep_time, quit_requested);

        const auto now = std::chrono::system_clock::now();
        if (now - last_decrease_time > decrease_error_count_period)
        {
            error_count /= 2;
            last_decrease_time = now;
        }
    }
}


ConnectionPoolPtr StorageDistributedDirectoryMonitor::createPool(const std::string & name)
{
    const auto pool_factory = [this, &name] (const std::string & host, const UInt16 port,
                                             const std::string & user, const std::string & password,
                                             const std::string & default_database)
    {
        return std::make_shared<ConnectionPool>(
            1, host, port, default_database,
            user, password,
            storage.getName() + '_' + name);
    };

    auto pools = createPoolsForAddresses(name, pool_factory);

    return pools.size() == 1 ? pools.front() : std::make_shared<ConnectionPoolWithFailover>(pools, LoadBalancing::RANDOM);
}


bool StorageDistributedDirectoryMonitor::findFiles()
{
    std::map<UInt64, std::string> files;

    Poco::DirectoryIterator end;
    for (Poco::DirectoryIterator it{path}; it != end; ++it)
    {
        const auto & file_path_str = it->path();
        Poco::Path file_path{file_path_str};

        if (!it->isDirectory() && startsWith(file_path.getExtension().data(), "bin"))
            files[parse<UInt64>(file_path.getBaseName())] = file_path_str;
    }

    if (files.empty())
        return false;

    if (should_batch_inserts)
    {
        processFilesWithBatching(files);
    }
    else
    {
        for (const auto & file : files)
        {
            if (quit)
                return true;

            processFile(file.second);
        }
    }

    return true;
}


void StorageDistributedDirectoryMonitor::processFile(const std::string & file_path)
{
    LOG_TRACE(log, "Started processing `" << file_path << '`');
    auto connection = pool->get();

    try
    {
        CurrentMetrics::Increment metric_increment{CurrentMetrics::DistributedSend};

        ReadBufferFromFile in{file_path};

        std::string insert_query;
        readStringBinary(insert_query, in);

        RemoteBlockOutputStream remote{*connection, insert_query};

        remote.writePrefix();
        remote.writePrepared(in);
        remote.writeSuffix();
    }
    catch (const Exception & e)
    {
        maybeMarkAsBroken(file_path, e);
        throw;
    }

    Poco::File{file_path}.remove();

    LOG_TRACE(log, "Finished processing `" << file_path << '`');
}

struct StorageDistributedDirectoryMonitor::Batch
{
    std::vector<UInt64> file_indices;
    size_t total_rows = 0;
    size_t total_bytes = 0;

    StorageDistributedDirectoryMonitor & parent;

    explicit Batch(StorageDistributedDirectoryMonitor & parent_) : parent(parent_) {}

    bool isEnoughSize() const
    {
        return (!parent.min_batched_block_size_rows && !parent.min_batched_block_size_bytes)
            || (parent.min_batched_block_size_rows && total_rows >= parent.min_batched_block_size_rows)
            || (parent.min_batched_block_size_bytes && total_bytes >= parent.min_batched_block_size_bytes);
    }

    void send(bool save, const std::map<UInt64, String> & file_index_to_path)
    {
        if (file_indices.empty())
            return;

        CurrentMetrics::Increment metric_increment{CurrentMetrics::DistributedSend};

        if (save)
        {
            /// For deduplication in Replicated tables to work, in case of error
            /// we must try to re-send exactly the same batches.
            /// So we save contents of the current batch into the current_batch_file_path file
            /// and truncate it afterwards if all went well.
            WriteBufferFromFile out{parent.current_batch_file_path};
            writeText(out);
        }

        auto connection = parent.pool->get();

        String insert_query;
        std::unique_ptr<RemoteBlockOutputStream> remote;
        bool first = true;

        for (UInt64 file_idx : file_indices)
        {
            ReadBufferFromFile in{file_index_to_path.at(file_idx)};
            readStringBinary(insert_query, in); /// NOTE: all files must have the same insert_query

            if (first)
            {
                first = false;
                remote = std::make_unique<RemoteBlockOutputStream>(*connection, insert_query);
                remote->writePrefix();
            }

            remote->writePrepared(in);
        }

        remote->writeSuffix();

        LOG_TRACE(parent.log, "Sent a batch of " << file_indices.size() << " files.");

        for (UInt64 file_index : file_indices)
            Poco::File{file_index_to_path.at(file_index)}.remove();
        file_indices.clear();
        total_rows = 0;
        total_bytes = 0;

        Poco::File{parent.current_batch_file_path}.setSize(0);
    }

    void writeText(WriteBuffer & out)
    {
        for (UInt64 file_idx : file_indices)
            out << file_idx << '\n';
    }

    void readText(ReadBuffer & in)
    {
        while (!in.eof())
        {
            UInt64 idx;
            in >> idx >> "\n";
            file_indices.push_back(idx);
        }
    }
};

void StorageDistributedDirectoryMonitor::processFilesWithBatching(const std::map<UInt64, std::string> & files)
{
    std::unordered_set<UInt64> file_indices_to_skip;

    if (Poco::File{current_batch_file_path}.exists())
    {
        /// Possibly, we failed to send a batch on the previous iteration. Try to send exactly the same batch.
        Batch batch{*this};
        ReadBufferFromFile in{current_batch_file_path};
        batch.readText(in);
        file_indices_to_skip.insert(batch.file_indices.begin(), batch.file_indices.end());
        batch.send(/* save = */ false, files);
    }

    std::unordered_map<String, Batch> query_to_batch;

    for (const auto & file : files)
    {
        if (quit)
            return;

        UInt64 file_idx = file.first;
        const String & file_path = file.second;

        if (file_indices_to_skip.count(file_idx))
            continue;

        ReadBufferFromFile in{file_path};
        String insert_query;
        readStringBinary(insert_query, in);

        Batch & batch = query_to_batch.try_emplace(insert_query, *this).first->second;

        CompressedReadBuffer decompressing_in(in);
        NativeBlockInputStream block_in(decompressing_in, ClickHouseRevision::get());

        size_t total_rows = 0;
        size_t total_bytes = 0;

        block_in.readPrefix();
        try
        {
            /// Determine size of the current file and check if it is not broken.
            while (Block block = block_in.read())
            {
                total_rows += block.rows();
                total_bytes += block.bytes();
            }
        }
        catch (const Exception & e)
        {
            if (maybeMarkAsBroken(file_path, e))
            {
                tryLogCurrentException(log, "File is marked broken due to");
                continue;
            }
            else
                throw;
        }
        block_in.readSuffix();

        batch.file_indices.push_back(file_idx);
        batch.total_rows += total_rows;
        batch.total_bytes += total_bytes;

        if (batch.isEnoughSize())
            batch.send(/* save = */ true, files);
    }

    for (auto & kv : query_to_batch)
    {
        Batch & batch = kv.second;
        batch.send(/* save = */ true, files);
    }

    Poco::File{current_batch_file_path}.remove();
}


bool StorageDistributedDirectoryMonitor::maybeMarkAsBroken(const std::string & file_path, const Exception &e) const
{
    const auto code = e.code();

    /// mark file as broken if necessary
    if (code == ErrorCodes::CHECKSUM_DOESNT_MATCH
        || code == ErrorCodes::TOO_LARGE_SIZE_COMPRESSED
        || code == ErrorCodes::CANNOT_READ_ALL_DATA
        || code == ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF)
    {
        const auto last_path_separator_pos = file_path.rfind('/');
        const auto & path = file_path.substr(0, last_path_separator_pos + 1);
        const auto & file_name = file_path.substr(last_path_separator_pos + 1);
        const auto & broken_path = path + "broken/";
        const auto & broken_file_path = broken_path + file_name;

        Poco::File{broken_path}.createDirectory();
        Poco::File{file_path}.renameTo(broken_file_path);

        LOG_ERROR(log, "Renamed `" << file_path << "` to `" << broken_file_path << '`');

        return true;
    }
    else
        return false;
}


std::string StorageDistributedDirectoryMonitor::getLoggerName() const
{
    return storage.name + '.' + storage.getName() + ".DirectoryMonitor";
}

}
