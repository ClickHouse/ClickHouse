#include <DataStreams/RemoteBlockOutputStream.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <Common/escapeForFileName.h>
#include <Common/CurrentMetrics.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/SipHash.h>
#include <Common/quoteString.h>
#include <Common/hex.h>
#include <common/StringRef.h>
#include <Common/ActionBlocker.h>
#include <Interpreters/Context.h>
#include <Interpreters/Cluster.h>
#include <Storages/Distributed/DirectoryMonitor.h>
#include <Storages/StorageDistributed.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromFile.h>
#include <Compression/CompressedReadBuffer.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/Operators.h>

#include <boost/algorithm/string/find_iterator.hpp>
#include <boost/algorithm/string/finder.hpp>

#include <Poco/DirectoryIterator.h>


namespace CurrentMetrics
{
    extern const Metric DistributedSend;
    extern const Metric DistributedFilesToInsert;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int UNKNOWN_CODEC;
    extern const int CANNOT_DECOMPRESS;
    extern const int CHECKSUM_DOESNT_MATCH;
    extern const int TOO_LARGE_SIZE_COMPRESSED;
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
}


namespace
{
    constexpr const std::chrono::minutes decrease_error_count_period{5};

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

    void assertChecksum(CityHash_v1_0_2::uint128 expected, CityHash_v1_0_2::uint128 calculated)
    {
        if (expected != calculated)
        {
            String message = "Checksum of extra info doesn't match: corrupted data."
                " Reference: " + getHexUIntLowercase(expected.first) + getHexUIntLowercase(expected.second)
                + ". Actual: " + getHexUIntLowercase(calculated.first) + getHexUIntLowercase(calculated.second)
                + ".";
            throw Exception(message, ErrorCodes::CHECKSUM_DOESNT_MATCH);
        }
    }

}


StorageDistributedDirectoryMonitor::StorageDistributedDirectoryMonitor(
    StorageDistributed & storage_, std::string path_, ConnectionPoolPtr pool_, ActionBlocker & monitor_blocker_, BackgroundSchedulePool & bg_pool)
    : storage(storage_)
    , pool(std::move(pool_))
    , path{path_ + '/'}
    , should_batch_inserts(storage.global_context.getSettingsRef().distributed_directory_monitor_batch_inserts)
    , min_batched_block_size_rows(storage.global_context.getSettingsRef().min_insert_block_size_rows)
    , min_batched_block_size_bytes(storage.global_context.getSettingsRef().min_insert_block_size_bytes)
    , current_batch_file_path{path + "current_batch.txt"}
    , default_sleep_time{storage.global_context.getSettingsRef().distributed_directory_monitor_sleep_time_ms.totalMilliseconds()}
    , sleep_time{default_sleep_time}
    , max_sleep_time{storage.global_context.getSettingsRef().distributed_directory_monitor_max_sleep_time_ms.totalMilliseconds()}
    , log{&Poco::Logger::get(getLoggerName())}
    , monitor_blocker(monitor_blocker_)
    , metric_pending_files(CurrentMetrics::DistributedFilesToInsert, 0)
{
    task_handle = bg_pool.createTask(getLoggerName() + "/Bg", [this]{ run(); });
    task_handle->activateAndSchedule();
}


StorageDistributedDirectoryMonitor::~StorageDistributedDirectoryMonitor()
{
    if (!quit)
    {
        quit = true;
        task_handle->deactivate();
    }
}

void StorageDistributedDirectoryMonitor::flushAllData()
{
    if (quit)
        return;

    std::unique_lock lock{mutex};

    const auto & files = getFiles();
    if (!files.empty())
    {
        processFiles(files);

        /// Update counters
        getFiles();
    }
}

void StorageDistributedDirectoryMonitor::shutdownAndDropAllData()
{
    if (!quit)
    {
        quit = true;
        task_handle->deactivate();
    }

    Poco::File(path).remove(true);
}


void StorageDistributedDirectoryMonitor::run()
{
    std::unique_lock lock{mutex};

    bool do_sleep = false;
    while (!quit)
    {
        do_sleep = true;

        const auto & files = getFiles();
        if (files.empty())
            break;

        if (!monitor_blocker.isCancelled())
        {
            try
            {
                do_sleep = !processFiles(files);

                std::unique_lock metrics_lock(metrics_mutex);
                last_exception = std::exception_ptr{};
            }
            catch (...)
            {
                std::unique_lock metrics_lock(metrics_mutex);

                do_sleep = true;
                ++error_count;
                sleep_time = std::min(
                    std::chrono::milliseconds{Int64(default_sleep_time.count() * std::exp2(error_count))},
                    max_sleep_time);
                tryLogCurrentException(getLoggerName().data());
                last_exception = std::current_exception();
            }
        }
        else
        {
            LOG_DEBUG(log, "Skipping send data over distributed table.");
        }

        const auto now = std::chrono::system_clock::now();
        if (now - last_decrease_time > decrease_error_count_period)
        {
            std::unique_lock metrics_lock(metrics_mutex);

            error_count /= 2;
            last_decrease_time = now;
        }

        if (do_sleep)
            break;
    }

    /// Update counters
    getFiles();

    if (!quit && do_sleep)
        task_handle->scheduleAfter(sleep_time.count());
}


ConnectionPoolPtr StorageDistributedDirectoryMonitor::createPool(const std::string & name, const StorageDistributed & storage)
{
    const auto pool_factory = [&storage] (const Cluster::Address & address) -> ConnectionPoolPtr
    {
        const auto & cluster = storage.getCluster();
        const auto & shards_info = cluster->getShardsInfo();
        const auto & shards_addresses = cluster->getShardsAddresses();

        /// check new format shard{shard_index}_number{number_index}
        if (address.shard_index != 0)
        {
            return shards_info[address.shard_index - 1].per_replica_pools[address.replica_index - 1];
        }

        /// existing connections pool have a higher priority
        for (size_t shard_index = 0; shard_index < shards_info.size(); ++shard_index)
        {
            const Cluster::Addresses & replicas_addresses = shards_addresses[shard_index];

            for (size_t replica_index = 0; replica_index < replicas_addresses.size(); ++replica_index)
            {
                const Cluster::Address & replica_address = replicas_addresses[replica_index];

                if (address.user == replica_address.user &&
                    address.password == replica_address.password &&
                    address.host_name == replica_address.host_name &&
                    address.port == replica_address.port &&
                    address.default_database == replica_address.default_database &&
                    address.secure == replica_address.secure)
                {
                    return shards_info[shard_index].per_replica_pools[replica_index];
                }
            }
        }

        return std::make_shared<ConnectionPool>(
            1, /* max_connections */
            address.host_name,
            address.port,
            address.default_database,
            address.user,
            address.password,
            address.cluster,
            address.cluster_secret,
            storage.getName() + '_' + address.user, /* client */
            Protocol::Compression::Enable,
            address.secure);
    };

    auto pools = createPoolsForAddresses(name, pool_factory);

    const auto settings = storage.global_context.getSettings();
    return pools.size() == 1 ? pools.front() : std::make_shared<ConnectionPoolWithFailover>(pools,
        settings.load_balancing,
        settings.distributed_replica_error_half_life.totalSeconds(),
        settings.distributed_replica_error_cap);
}


std::map<UInt64, std::string> StorageDistributedDirectoryMonitor::getFiles()
{
    std::map<UInt64, std::string> files;
    size_t new_bytes_count = 0;

    Poco::DirectoryIterator end;
    for (Poco::DirectoryIterator it{path}; it != end; ++it)
    {
        const auto & file_path_str = it->path();
        Poco::Path file_path{file_path_str};

        if (!it->isDirectory() && startsWith(file_path.getExtension(), "bin"))
        {
            files[parse<UInt64>(file_path.getBaseName())] = file_path_str;
            new_bytes_count += Poco::File(file_path).getSize();
        }
    }

    metric_pending_files.changeTo(files.size());

    {
        std::unique_lock metrics_lock(metrics_mutex);
        files_count = files.size();
        bytes_count = new_bytes_count;
    }

    return files;
}
bool StorageDistributedDirectoryMonitor::processFiles(const std::map<UInt64, std::string> & files)
{
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
    LOG_TRACE(log, "Started processing `{}`", file_path);
    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(storage.global_context.getSettingsRef());

    try
    {
        CurrentMetrics::Increment metric_increment{CurrentMetrics::DistributedSend};

        ReadBufferFromFile in{file_path};

        Settings insert_settings;
        std::string insert_query;
        ClientInfo client_info;

        readHeader(in, insert_settings, insert_query, client_info, log);

        auto connection = pool->get(timeouts, &insert_settings);

        RemoteBlockOutputStream remote{*connection, timeouts, insert_query, insert_settings, client_info};

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
    metric_pending_files.sub();

    LOG_TRACE(log, "Finished processing `{}`", file_path);
}

void StorageDistributedDirectoryMonitor::readHeader(
    ReadBuffer & in, Settings & insert_settings, std::string & insert_query, ClientInfo & client_info, Poco::Logger * log)
{
    UInt64 query_size;
    readVarUInt(query_size, in);

    if (query_size == DBMS_DISTRIBUTED_SIGNATURE_HEADER)
    {
        /// Read the header as a string.
        String header;
        readStringBinary(header, in);

        /// Check the checksum of the header.
        CityHash_v1_0_2::uint128 checksum;
        readPODBinary(checksum, in);
        assertChecksum(checksum, CityHash_v1_0_2::CityHash128(header.data(), header.size()));

        /// Read the parts of the header.
        ReadBufferFromString header_buf(header);

        UInt64 initiator_revision;
        readVarUInt(initiator_revision, header_buf);
        if (DBMS_TCP_PROTOCOL_VERSION < initiator_revision)
        {
            LOG_WARNING(log, "ClickHouse shard version is older than ClickHouse initiator version. It may lack support for new features.");
        }

        readStringBinary(insert_query, header_buf);
        insert_settings.read(header_buf);

        if (header_buf.hasPendingData())
            client_info.read(header_buf, initiator_revision);

        /// Add handling new data here, for example:
        /// if (header_buf.hasPendingData())
        ///    readVarUInt(my_new_data, header_buf);

        return;
    }

    if (query_size == DBMS_DISTRIBUTED_SIGNATURE_HEADER_OLD_FORMAT)
    {
        insert_settings.read(in, SettingsWriteFormat::BINARY);
        readStringBinary(insert_query, in);
        return;
    }

    insert_query.resize(query_size);
    in.readStrict(insert_query.data(), query_size);
}

struct StorageDistributedDirectoryMonitor::BatchHeader
{
    Settings settings;
    String query;
    ClientInfo client_info;
    Block sample_block;

    BatchHeader(Settings settings_, String query_, ClientInfo client_info_, Block sample_block_)
        : settings(std::move(settings_))
        , query(std::move(query_))
        , client_info(std::move(client_info_))
        , sample_block(std::move(sample_block_))
    {
    }

    bool operator==(const BatchHeader & other) const
    {
        return settings == other.settings && query == other.query && client_info.query_kind == other.client_info.query_kind
            && blocksHaveEqualStructure(sample_block, other.sample_block);
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

            /// Temporary file is required for atomicity.
            String tmp_file{parent.current_batch_file_path + ".tmp"};

            if (Poco::File{tmp_file}.exists())
                LOG_ERROR(parent.log, "Temporary file {} exists. Unclean shutdown?", backQuote(tmp_file));

            {
                WriteBufferFromFile out{tmp_file, O_WRONLY | O_TRUNC | O_CREAT};
                writeText(out);
            }

            Poco::File{tmp_file}.renameTo(parent.current_batch_file_path);
        }
        auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(parent.storage.global_context.getSettingsRef());
        auto connection = parent.pool->get(timeouts);

        bool batch_broken = false;
        try
        {
            Settings insert_settings;
            String insert_query;
            ClientInfo client_info;
            std::unique_ptr<RemoteBlockOutputStream> remote;
            bool first = true;

            for (UInt64 file_idx : file_indices)
            {
                auto file_path = file_index_to_path.find(file_idx);
                if (file_path == file_index_to_path.end())
                {
                    LOG_ERROR(parent.log, "Failed to send batch: file with index {} is absent", file_idx);
                    batch_broken = true;
                    break;
                }

                ReadBufferFromFile in(file_path->second);
                parent.readHeader(in, insert_settings, insert_query, client_info, parent.log);

                if (first)
                {
                    first = false;
                    remote = std::make_unique<RemoteBlockOutputStream>(*connection, timeouts, insert_query, insert_settings, client_info);
                    remote->writePrefix();
                }

                remote->writePrepared(in);
            }

            if (remote)
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
            LOG_TRACE(parent.log, "Sent a batch of {} files.", file_indices.size());

            for (UInt64 file_index : file_indices)
                Poco::File{file_index_to_path.at(file_index)}.remove();
        }
        else
        {
            LOG_ERROR(parent.log, "Marking a batch of {} files as broken.", file_indices.size());

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

class DirectoryMonitorBlockInputStream : public IBlockInputStream
{
public:
    explicit DirectoryMonitorBlockInputStream(const String & file_name)
        : in(file_name)
        , decompressing_in(in)
        , block_in(decompressing_in, DBMS_TCP_PROTOCOL_VERSION)
        , log{&Poco::Logger::get("DirectoryMonitorBlockInputStream")}
    {
        Settings insert_settings;
        String insert_query;
        ClientInfo client_info;
        StorageDistributedDirectoryMonitor::readHeader(in, insert_settings, insert_query, client_info, log);

        block_in.readPrefix();
        first_block = block_in.read();
        header = first_block.cloneEmpty();
    }

    String getName() const override { return "DirectoryMonitor"; }

protected:
    Block getHeader() const override { return header; }
    Block readImpl() override
    {
        if (first_block)
            return std::move(first_block);

        return block_in.read();
    }

    void readSuffix() override { block_in.readSuffix(); }

private:
    ReadBufferFromFile in;
    CompressedReadBuffer decompressing_in;
    NativeBlockInputStream block_in;

    Block first_block;
    Block header;

    Poco::Logger * log;
};

BlockInputStreamPtr StorageDistributedDirectoryMonitor::createStreamFromFile(const String & file_name)
{
    return std::make_shared<DirectoryMonitorBlockInputStream>(file_name);
}

bool StorageDistributedDirectoryMonitor::scheduleAfter(size_t ms)
{
    if (quit)
        return false;
    return task_handle->scheduleAfter(ms, false);
}

StorageDistributedDirectoryMonitor::Status StorageDistributedDirectoryMonitor::getStatus() const
{
    std::unique_lock metrics_lock(metrics_mutex);

    return Status{
        path,
        last_exception,
        error_count,
        files_count,
        bytes_count,
        monitor_blocker.isCancelled(),
    };
}

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
        metric_pending_files.sub(batch.file_indices.size());
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
        ClientInfo client_info;
        try
        {
            /// Determine metadata of the current file and check if it is not broken.
            ReadBufferFromFile in{file_path};
            readHeader(in, insert_settings, insert_query, client_info, log);

            CompressedReadBuffer decompressing_in(in);
            NativeBlockInputStream block_in(decompressing_in, DBMS_TCP_PROTOCOL_VERSION);
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

        BatchHeader batch_header(std::move(insert_settings), std::move(insert_query), std::move(client_info), std::move(sample_block));
        Batch & batch = header_to_batch.try_emplace(batch_header, *this, files).first->second;

        batch.file_indices.push_back(file_idx);
        batch.total_rows += total_rows;
        batch.total_bytes += total_bytes;

        if (batch.isEnoughSize())
        {
            batch.send();
            metric_pending_files.sub(batch.file_indices.size());
        }
    }

    for (auto & kv : header_to_batch)
    {
        Batch & batch = kv.second;
        batch.send();
        metric_pending_files.sub(batch.file_indices.size());
    }

    /// current_batch.txt will not exist if there was no send
    /// (this is the case when all batches that was pending has been marked as pending)
    if (Poco::File{current_batch_file_path}.exists())
        Poco::File{current_batch_file_path}.remove();
}

bool StorageDistributedDirectoryMonitor::isFileBrokenErrorCode(int code)
{
    return code == ErrorCodes::CHECKSUM_DOESNT_MATCH
        || code == ErrorCodes::TOO_LARGE_SIZE_COMPRESSED
        || code == ErrorCodes::CANNOT_READ_ALL_DATA
        || code == ErrorCodes::UNKNOWN_CODEC
        || code == ErrorCodes::CANNOT_DECOMPRESS
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

    LOG_ERROR(log, "Renamed `{}` to `{}`", file_path, broken_file_path);
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
    return storage.getStorageID().getFullTableName() + ".DirectoryMonitor";
}

void StorageDistributedDirectoryMonitor::updatePath(const std::string & new_path)
{
    task_handle->deactivate();
    std::lock_guard lock{mutex};

    {
        std::unique_lock metrics_lock(metrics_mutex);
        path = new_path;
    }
    current_batch_file_path = path + "current_batch.txt";

    task_handle->activateAndSchedule();
}

}
