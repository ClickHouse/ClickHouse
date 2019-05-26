#include <DataStreams/RemoteBlockOutputStream.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <Common/escapeForFileName.h>
#include <Common/setThreadName.h>
#include <Common/CurrentMetrics.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/ClickHouseRevision.h>
#include <Common/SipHash.h>
#include <Interpreters/Context.h>
#include <Storages/Distributed/DirectoryMonitor.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <Compression/CompressedReadBuffer.h>
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
            Cluster::Address address = Cluster::Address::fromFullString(boost::copy_range<std::string>(*it));
            pools.emplace_back(factory(address));
        }

        return pools;
    }
}


StorageDistributedDirectoryMonitor::StorageDistributedDirectoryMonitor(StorageDistributed & storage, const std::string & name, const ConnectionPoolPtr & pool)
    : storage(storage), pool{pool}, path{storage.path + name + '/'}
    , current_batch_file_path{path + "current_batch.txt"}
    , default_sleep_time{storage.global_context.getSettingsRef().distributed_directory_monitor_sleep_time_ms.totalMilliseconds()}
    , sleep_time{default_sleep_time}
    , log{&Logger::get(getLoggerName())}
{
    const Settings & settings = storage.global_context.getSettingsRef();
    should_batch_inserts = settings.distributed_directory_monitor_batch_inserts;
    min_batched_block_size_rows = settings.min_insert_block_size_rows;
    min_batched_block_size_bytes = settings.min_insert_block_size_bytes;
}


StorageDistributedDirectoryMonitor::~StorageDistributedDirectoryMonitor()
{
    if (!quit)
    {
        {
            quit = true;
            std::lock_guard lock{mutex};
        }
        cond.notify_one();
        thread.join();
    }
}


void StorageDistributedDirectoryMonitor::shutdownAndDropAllData()
{
    if (!quit)
    {
        {
            quit = true;
            std::lock_guard lock{mutex};
        }
        cond.notify_one();
        thread.join();
    }

    Poco::File(path).remove(true);
}


void StorageDistributedDirectoryMonitor::run()
{
    setThreadName("DistrDirMonitor");

    std::unique_lock lock{mutex};

    const auto quit_requested = [this] { return quit.load(std::memory_order_relaxed); };

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
        }

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


ConnectionPoolPtr StorageDistributedDirectoryMonitor::createPool(const std::string & name, const StorageDistributed & storage)
{
    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(storage.global_context.getSettingsRef());
    const auto pool_factory = [&storage, &timeouts] (const Cluster::Address & address) -> ConnectionPoolPtr
    {
        const auto & cluster = storage.getCluster();
        const auto & shards_info = cluster->getShardsInfo();
        const auto & shards_addresses = cluster->getShardsAddresses();

        /// existing connections pool have a higher priority
        for (size_t shard_index = 0; shard_index < shards_info.size(); ++shard_index)
        {
            const Cluster::Addresses & replicas_addresses = shards_addresses[shard_index];

            for (size_t replica_index = 0; replica_index < replicas_addresses.size(); ++replica_index)
            {
                const Cluster::Address & replica_address = replicas_addresses[replica_index];

                if (address == replica_address)
                    return shards_info[shard_index].per_replica_pools[replica_index];
            }
        }

        return std::make_shared<ConnectionPool>(
            1, address.host_name, address.port, address.default_database, address.user, address.password, timeouts,
            storage.getName() + '_' + address.user, Protocol::Compression::Enable, address.secure);
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

        Settings insert_settings;
        std::string insert_query;
        readQueryAndSettings(in, insert_settings, insert_query);

        RemoteBlockOutputStream remote{*connection, insert_query, &insert_settings};

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

void StorageDistributedDirectoryMonitor::readQueryAndSettings(
    ReadBuffer & in, Settings & insert_settings, std::string & insert_query) const
{
    UInt64 magic_number_or_query_size;

    readVarUInt(magic_number_or_query_size, in);

    if (magic_number_or_query_size == UInt64(DBMS_DISTRIBUTED_SENDS_MAGIC_NUMBER))
    {
        insert_settings.deserialize(in);
        readVarUInt(magic_number_or_query_size, in);
    }
    insert_query.resize(magic_number_or_query_size);
    in.readStrict(insert_query.data(), magic_number_or_query_size);
}

struct StorageDistributedDirectoryMonitor::BatchHeader
{
    Settings settings;
    String query;
    Block sample_block;

    BatchHeader(Settings settings_, String query_, Block sample_block_)
        : settings(std::move(settings_))
        , query(std::move(query_))
        , sample_block(std::move(sample_block_))
    {
    }

    bool operator==(const BatchHeader & other) const
    {
        return settings == other.settings && query == other.query &&
               blocksHaveEqualStructure(sample_block, other.sample_block);
    }

    struct Hash
    {
        size_t operator()(const BatchHeader & batch_header) const
        {
            SipHash hash_state;
            hash_state.update(batch_header.query.data(), batch_header.query.size());

            size_t num_columns = batch_header.sample_block.columns();
            for (size_t i = 0; i < num_columns; ++i)
            {
                const String & type_name = batch_header.sample_block.getByPosition(i).type->getName();
                hash_state.update(type_name.data(), type_name.size());
            }

            return hash_state.get64();
        }
    };
};

struct StorageDistributedDirectoryMonitor::Batch
{
    std::vector<UInt64> file_indices;
    size_t total_rows = 0;
    size_t total_bytes = 0;
    bool recovered = false;

    StorageDistributedDirectoryMonitor & parent;
    const std::map<UInt64, String> & file_index_to_path;

    Batch(
        StorageDistributedDirectoryMonitor & parent_,
        const std::map<UInt64, String> & file_index_to_path_)
        : parent(parent_), file_index_to_path(file_index_to_path_)
    {}

    bool isEnoughSize() const
    {
        return (!parent.min_batched_block_size_rows && !parent.min_batched_block_size_bytes)
            || (parent.min_batched_block_size_rows && total_rows >= parent.min_batched_block_size_rows)
            || (parent.min_batched_block_size_bytes && total_bytes >= parent.min_batched_block_size_bytes);
    }

    void send()
    {
        if (file_indices.empty())
            return;

        CurrentMetrics::Increment metric_increment{CurrentMetrics::DistributedSend};

        if (!recovered)
        {
            /// For deduplication in Replicated tables to work, in case of error
            /// we must try to re-send exactly the same batches.
            /// So we save contents of the current batch into the current_batch_file_path file
            /// and truncate it afterwards if all went well.
            WriteBufferFromFile out{parent.current_batch_file_path};
            writeText(out);
        }

        auto connection = parent.pool->get();

        bool batch_broken = false;
        try
        {
            Settings insert_settings;
            String insert_query;
            std::unique_ptr<RemoteBlockOutputStream> remote;
            bool first = true;

            for (UInt64 file_idx : file_indices)
            {
                auto file_path = file_index_to_path.find(file_idx);
                if (file_path == file_index_to_path.end())
                {
                    LOG_ERROR(parent.log, "Failed to send batch: file with index " << file_idx << " is absent");
                    batch_broken = true;
                    break;
                }

                ReadBufferFromFile in(file_path->second);
                parent.readQueryAndSettings(in, insert_settings, insert_query);

                if (first)
                {
                    first = false;
                    remote = std::make_unique<RemoteBlockOutputStream>(*connection, insert_query, &insert_settings);
                    remote->writePrefix();
                }

                remote->writePrepared(in);
            }

            remote->writeSuffix();
        }
        catch (const Exception & e)
        {
            if (isFileBrokenErrorCode(e.code()))
            {
                tryLogCurrentException(parent.log, "Failed to send batch due to");
                batch_broken = true;
            }
            else
                throw;
        }

        if (!batch_broken)
        {
            LOG_TRACE(parent.log, "Sent a batch of " << file_indices.size() << " files.");

            for (UInt64 file_index : file_indices)
                Poco::File{file_index_to_path.at(file_index)}.remove();
        }
        else
        {
            LOG_ERROR(parent.log, "Marking a batch of " << file_indices.size() << " files as broken.");

            for (UInt64 file_idx : file_indices)
            {
                auto file_path = file_index_to_path.find(file_idx);
                if (file_path != file_index_to_path.end())
                    parent.markAsBroken(file_path->second);
            }
        }

        file_indices.clear();
        total_rows = 0;
        total_bytes = 0;
        recovered = false;

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
        recovered = true;
    }
};


void StorageDistributedDirectoryMonitor::processFilesWithBatching(const std::map<UInt64, std::string> & files)
{
    std::unordered_set<UInt64> file_indices_to_skip;

    if (Poco::File{current_batch_file_path}.exists())
    {
        /// Possibly, we failed to send a batch on the previous iteration. Try to send exactly the same batch.
        Batch batch(*this, files);
        ReadBufferFromFile in{current_batch_file_path};
        batch.readText(in);
        file_indices_to_skip.insert(batch.file_indices.begin(), batch.file_indices.end());
        batch.send();
    }

    std::unordered_map<BatchHeader, Batch, BatchHeader::Hash> header_to_batch;

    for (const auto & file : files)
    {
        if (quit)
            return;

        UInt64 file_idx = file.first;
        const String & file_path = file.second;

        if (file_indices_to_skip.count(file_idx))
            continue;

        size_t total_rows = 0;
        size_t total_bytes = 0;
        Block sample_block;
        Settings insert_settings;
        String insert_query;
        try
        {
            /// Determine metadata of the current file and check if it is not broken.
            ReadBufferFromFile in{file_path};
            readQueryAndSettings(in, insert_settings, insert_query);

            CompressedReadBuffer decompressing_in(in);
            NativeBlockInputStream block_in(decompressing_in, ClickHouseRevision::get());
            block_in.readPrefix();

            while (Block block = block_in.read())
            {
                total_rows += block.rows();
                total_bytes += block.bytes();

                if (!sample_block)
                    sample_block = block.cloneEmpty();
            }
            block_in.readSuffix();
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

        BatchHeader batch_header(std::move(insert_settings), std::move(insert_query), std::move(sample_block));
        Batch & batch = header_to_batch.try_emplace(batch_header, *this, files).first->second;

        batch.file_indices.push_back(file_idx);
        batch.total_rows += total_rows;
        batch.total_bytes += total_bytes;

        if (batch.isEnoughSize())
            batch.send();
    }

    for (auto & kv : header_to_batch)
    {
        Batch & batch = kv.second;
        batch.send();
    }

    Poco::File{current_batch_file_path}.remove();
}

bool StorageDistributedDirectoryMonitor::isFileBrokenErrorCode(int code)
{
    return code == ErrorCodes::CHECKSUM_DOESNT_MATCH
        || code == ErrorCodes::TOO_LARGE_SIZE_COMPRESSED
        || code == ErrorCodes::CANNOT_READ_ALL_DATA
        || code == ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF;
}

void StorageDistributedDirectoryMonitor::markAsBroken(const std::string & file_path) const
{
    const auto last_path_separator_pos = file_path.rfind('/');
    const auto & base_path = file_path.substr(0, last_path_separator_pos + 1);
    const auto & file_name = file_path.substr(last_path_separator_pos + 1);
    const auto & broken_path = base_path + "broken/";
    const auto & broken_file_path = broken_path + file_name;

    Poco::File{broken_path}.createDirectory();
    Poco::File{file_path}.renameTo(broken_file_path);

    LOG_ERROR(log, "Renamed `" << file_path << "` to `" << broken_file_path << '`');
}

bool StorageDistributedDirectoryMonitor::maybeMarkAsBroken(const std::string & file_path, const Exception & e) const
{
    /// mark file as broken if necessary
    if (isFileBrokenErrorCode(e.code()))
    {
        markAsBroken(file_path);
        return true;
    }
    else
        return false;
}

std::string StorageDistributedDirectoryMonitor::getLoggerName() const
{
    return storage.table_name + '.' + storage.getName() + ".DirectoryMonitor";
}

}
