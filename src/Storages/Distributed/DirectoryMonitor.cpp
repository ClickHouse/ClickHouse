#include <QueryPipeline/RemoteInserter.h>
#include <Formats/NativeReader.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Common/CurrentMetrics.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/SipHash.h>
#include <Common/quoteString.h>
#include <Common/hex.h>
#include <Common/ActionBlocker.h>
#include <Common/formatReadable.h>
#include <Common/Stopwatch.h>
#include <base/StringRef.h>
#include <Interpreters/Context.h>
#include <Interpreters/Cluster.h>
#include <Storages/Distributed/DirectoryMonitor.h>
#include <Storages/Distributed/Defines.h>
#include <Storages/StorageDistributed.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromFile.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CheckingCompressedReadBuffer.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/ConnectionTimeoutsContext.h>
#include <IO/Operators.h>
#include <Disks/IDisk.h>
#include <boost/algorithm/string/find_iterator.hpp>
#include <boost/algorithm/string/finder.hpp>
#include <boost/range/adaptor/indexed.hpp>
#include <filesystem>


namespace CurrentMetrics
{
    extern const Metric DistributedSend;
    extern const Metric DistributedFilesToInsert;
    extern const Metric BrokenDistributedFilesToInsert;
}

namespace fs = std::filesystem;

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
    extern const int EMPTY_DATA_PASSED;
    extern const int INCORRECT_FILE_NAME;
    extern const int MEMORY_LIMIT_EXCEEDED;
    extern const int DISTRIBUTED_BROKEN_BATCH_INFO;
    extern const int DISTRIBUTED_BROKEN_BATCH_FILES;
    extern const int TOO_MANY_PARTS;
    extern const int TOO_MANY_BYTES;
    extern const int TOO_MANY_ROWS_OR_BYTES;
    extern const int TOO_MANY_PARTITIONS;
    extern const int DISTRIBUTED_TOO_MANY_PENDING_BYTES;
    extern const int ARGUMENT_OUT_OF_BOUND;
}


namespace
{
    constexpr const std::chrono::minutes decrease_error_count_period{5};

    template <typename PoolFactory>
    ConnectionPoolPtrs createPoolsForAddresses(const std::string & name, PoolFactory && factory, const Cluster::ShardsInfo & shards_info, Poco::Logger * log)
    {
        ConnectionPoolPtrs pools;

        auto make_connection = [&](const Cluster::Address & address)
        {
            try
            {
                pools.emplace_back(factory(address));
            }
            catch (const Exception & e)
            {
                if (e.code() == ErrorCodes::INCORRECT_FILE_NAME)
                {
                    tryLogCurrentException(log);
                    return;
                }
                throw;
            }
        };

        for (auto it = boost::make_split_iterator(name, boost::first_finder(",")); it != decltype(it){}; ++it)
        {
            const std::string & dirname = boost::copy_range<std::string>(*it);
            Cluster::Address address = Cluster::Address::fromFullString(dirname);
            if (address.shard_index && dirname.ends_with("_all_replicas"))
            {
                if (address.shard_index > shards_info.size())
                {
                    LOG_ERROR(log, "No shard with shard_index={} ({})", address.shard_index, name);
                    continue;
                }

                const auto & shard_info = shards_info[address.shard_index - 1];
                size_t replicas = shard_info.per_replica_pools.size();

                for (size_t replica_index = 1; replica_index <= replicas; ++replica_index)
                {
                    address.replica_index = replica_index;
                    make_connection(address);
                }
            }
            else
                make_connection(address);
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

    struct DistributedHeader
    {
        UInt64 revision = 0;
        Settings insert_settings;
        std::string insert_query;
        ClientInfo client_info;

        /// .bin file cannot have zero rows/bytes.
        size_t rows = 0;
        size_t bytes = 0;

        /// dumpStructure() of the header -- obsolete
        std::string block_header_string;
        Block block_header;
    };

    DistributedHeader readDistributedHeader(ReadBufferFromFile & in, Poco::Logger * log)
    {
        DistributedHeader distributed_header;

        UInt64 query_size;
        readVarUInt(query_size, in);

        if (query_size == DBMS_DISTRIBUTED_SIGNATURE_HEADER)
        {
            /// Read the header as a string.
            String header_data;
            readStringBinary(header_data, in);

            /// Check the checksum of the header.
            CityHash_v1_0_2::uint128 checksum;
            readPODBinary(checksum, in);
            assertChecksum(checksum, CityHash_v1_0_2::CityHash128(header_data.data(), header_data.size()));

            /// Read the parts of the header.
            ReadBufferFromString header_buf(header_data);

            readVarUInt(distributed_header.revision, header_buf);
            if (DBMS_TCP_PROTOCOL_VERSION < distributed_header.revision)
            {
                LOG_WARNING(log, "ClickHouse shard version is older than ClickHouse initiator version. It may lack support for new features.");
            }

            readStringBinary(distributed_header.insert_query, header_buf);
            distributed_header.insert_settings.read(header_buf);

            if (header_buf.hasPendingData())
                distributed_header.client_info.read(header_buf, distributed_header.revision);

            if (header_buf.hasPendingData())
            {
                readVarUInt(distributed_header.rows, header_buf);
                readVarUInt(distributed_header.bytes, header_buf);
                readStringBinary(distributed_header.block_header_string, header_buf);
            }

            if (header_buf.hasPendingData())
            {
                NativeReader header_block_in(header_buf, distributed_header.revision);
                distributed_header.block_header = header_block_in.read();
                if (!distributed_header.block_header)
                    throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
                        "Cannot read header from the {} batch. Data was written with protocol version {}, current version: {}",
                            in.getFileName(), distributed_header.revision, DBMS_TCP_PROTOCOL_VERSION);
            }

            /// Add handling new data here, for example:
            ///
            /// if (header_buf.hasPendingData())
            ///     readVarUInt(my_new_data, header_buf);
            ///
            /// And note that it is safe, because we have checksum and size for header.

            return distributed_header;
        }

        if (query_size == DBMS_DISTRIBUTED_SIGNATURE_HEADER_OLD_FORMAT)
        {
            distributed_header.insert_settings.read(in, SettingsWriteFormat::BINARY);
            readStringBinary(distributed_header.insert_query, in);
            return distributed_header;
        }

        distributed_header.insert_query.resize(query_size);
        in.readStrict(distributed_header.insert_query.data(), query_size);

        return distributed_header;
    }

    /// 'remote_error' argument is used to decide whether some errors should be
    /// ignored or not, in particular:
    ///
    /// - ATTEMPT_TO_READ_AFTER_EOF should not be ignored
    ///   if we receive it from remote (receiver), since:
    ///   - the sender will got ATTEMPT_TO_READ_AFTER_EOF when the client just go away,
    ///     i.e. server had been restarted
    ///   - since #18853 the file will be checked on the sender locally, and
    ///     if there is something wrong with the file itself, we will receive
    ///     ATTEMPT_TO_READ_AFTER_EOF not from the remote at first
    ///     and mark batch as broken.
    bool isFileBrokenErrorCode(int code, bool remote_error)
    {
        return code == ErrorCodes::CHECKSUM_DOESNT_MATCH
            || code == ErrorCodes::EMPTY_DATA_PASSED
            || code == ErrorCodes::TOO_LARGE_SIZE_COMPRESSED
            || code == ErrorCodes::CANNOT_READ_ALL_DATA
            || code == ErrorCodes::UNKNOWN_CODEC
            || code == ErrorCodes::CANNOT_DECOMPRESS
            || code == ErrorCodes::DISTRIBUTED_BROKEN_BATCH_INFO
            || code == ErrorCodes::DISTRIBUTED_BROKEN_BATCH_FILES
            || (!remote_error && code == ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF);
    }

    /// Can the batch be split and send files from batch one-by-one instead?
    bool isSplittableErrorCode(int code, bool remote)
    {
        return code == ErrorCodes::MEMORY_LIMIT_EXCEEDED
            /// FunctionRange::max_elements and similar
            || code == ErrorCodes::ARGUMENT_OUT_OF_BOUND
            || code == ErrorCodes::TOO_MANY_PARTS
            || code == ErrorCodes::TOO_MANY_BYTES
            || code == ErrorCodes::TOO_MANY_ROWS_OR_BYTES
            || code == ErrorCodes::TOO_MANY_PARTITIONS
            || code == ErrorCodes::DISTRIBUTED_TOO_MANY_PENDING_BYTES
            || code == ErrorCodes::DISTRIBUTED_BROKEN_BATCH_INFO
            || isFileBrokenErrorCode(code, remote)
        ;
    }

    SyncGuardPtr getDirectorySyncGuard(bool dir_fsync, const DiskPtr & disk, const String & path)
    {
        if (dir_fsync)
            return disk->getDirectorySyncGuard(path);
        return nullptr;
    }

    void writeAndConvert(RemoteInserter & remote, const DistributedHeader & distributed_header, ReadBufferFromFile & in)
    {
        CompressedReadBuffer decompressing_in(in);
        NativeReader block_in(decompressing_in, distributed_header.revision);

        while (Block block = block_in.read())
        {
            auto converting_dag = ActionsDAG::makeConvertingActions(
                block.cloneEmpty().getColumnsWithTypeAndName(),
                remote.getHeader().getColumnsWithTypeAndName(),
                ActionsDAG::MatchColumnsMode::Name);

            auto converting_actions = std::make_shared<ExpressionActions>(std::move(converting_dag));
            converting_actions->execute(block);
            remote.write(block);
        }
    }

    void writeRemoteConvert(
        const DistributedHeader & distributed_header,
        RemoteInserter & remote,
        bool compression_expected,
        ReadBufferFromFile & in,
        Poco::Logger * log)
    {
        if (!remote.getHeader())
        {
            CheckingCompressedReadBuffer checking_in(in);
            remote.writePrepared(checking_in);
            return;
        }

        /// This is old format, that does not have header for the block in the file header,
        /// applying ConvertingBlockInputStream in this case is not a big overhead.
        ///
        /// Anyway we can get header only from the first block, which contain all rows anyway.
        if (!distributed_header.block_header)
        {
            LOG_TRACE(log, "Processing batch {} with old format (no header)", in.getFileName());

            writeAndConvert(remote, distributed_header, in);
            return;
        }

        if (!blocksHaveEqualStructure(distributed_header.block_header, remote.getHeader()))
        {
            LOG_WARNING(log,
                "Structure does not match (remote: {}, local: {}), implicit conversion will be done",
                remote.getHeader().dumpStructure(), distributed_header.block_header.dumpStructure());

            writeAndConvert(remote, distributed_header, in);
            return;
        }

        /// If connection does not use compression, we have to uncompress the data.
        if (!compression_expected)
        {
            writeAndConvert(remote, distributed_header, in);
            return;
        }

        if (distributed_header.revision != remote.getServerRevision())
        {
            writeAndConvert(remote, distributed_header, in);
            return;
        }

        /// Otherwise write data as it was already prepared (more efficient path).
        CheckingCompressedReadBuffer checking_in(in);
        remote.writePrepared(checking_in);
    }

    uint64_t doubleToUInt64(double d)
    {
        if (d >= double(std::numeric_limits<uint64_t>::max()))
            return std::numeric_limits<uint64_t>::max();
        return static_cast<uint64_t>(d);
    }
}


StorageDistributedDirectoryMonitor::StorageDistributedDirectoryMonitor(
    StorageDistributed & storage_,
    const DiskPtr & disk_,
    const std::string & relative_path_,
    ConnectionPoolPtr pool_,
    ActionBlocker & monitor_blocker_,
    BackgroundSchedulePool & bg_pool)
    : storage(storage_)
    , pool(std::move(pool_))
    , disk(disk_)
    , relative_path(relative_path_)
    , path(fs::path(disk->getPath()) / relative_path / "")
    , should_batch_inserts(storage.getDistributedSettingsRef().monitor_batch_inserts)
    , split_batch_on_failure(storage.getDistributedSettingsRef().monitor_split_batch_on_failure)
    , dir_fsync(storage.getDistributedSettingsRef().fsync_directories)
    , min_batched_block_size_rows(storage.getContext()->getSettingsRef().min_insert_block_size_rows)
    , min_batched_block_size_bytes(storage.getContext()->getSettingsRef().min_insert_block_size_bytes)
    , current_batch_file_path(path + "current_batch.txt")
    , default_sleep_time(storage.getDistributedSettingsRef().monitor_sleep_time_ms.totalMilliseconds())
    , sleep_time(default_sleep_time)
    , max_sleep_time(storage.getDistributedSettingsRef().monitor_max_sleep_time_ms.totalMilliseconds())
    , log(&Poco::Logger::get(getLoggerName()))
    , monitor_blocker(monitor_blocker_)
    , metric_pending_files(CurrentMetrics::DistributedFilesToInsert, 0)
    , metric_broken_files(CurrentMetrics::BrokenDistributedFilesToInsert, 0)
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

    std::lock_guard lock{mutex};

    const auto & files = getFiles();
    if (!files.empty())
    {
        processFiles(files);

        /// Update counters.
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

    auto dir_sync_guard = getDirectorySyncGuard(dir_fsync, disk, relative_path);
    fs::remove_all(path);
}


void StorageDistributedDirectoryMonitor::run()
{
    std::lock_guard lock{mutex};

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

                std::lock_guard status_lock(status_mutex);
                status.last_exception = std::exception_ptr{};
            }
            catch (...)
            {
                std::lock_guard status_lock(status_mutex);

                do_sleep = true;
                ++status.error_count;

                UInt64 q = doubleToUInt64(std::exp2(status.error_count));
                std::chrono::milliseconds new_sleep_time(default_sleep_time.count() * q);
                if (new_sleep_time.count() < 0)
                    sleep_time = max_sleep_time;
                else
                    sleep_time = std::min(new_sleep_time, max_sleep_time);

                tryLogCurrentException(getLoggerName().data());
                status.last_exception = std::current_exception();
            }
        }
        else
        {
            LOG_DEBUG(log, "Skipping send data over distributed table.");
        }

        const auto now = std::chrono::system_clock::now();
        if (now - last_decrease_time > decrease_error_count_period)
        {
            std::lock_guard status_lock(status_mutex);

            status.error_count /= 2;
            last_decrease_time = now;
        }

        if (do_sleep)
            break;
    }

    /// Update counters.
    getFiles();

    if (!quit && do_sleep)
        task_handle->scheduleAfter(sleep_time.count());
}


ConnectionPoolPtr StorageDistributedDirectoryMonitor::createPool(const std::string & name, const StorageDistributed & storage)
{
    const auto pool_factory = [&storage, &name] (const Cluster::Address & address) -> ConnectionPoolPtr
    {
        const auto & cluster = storage.getCluster();
        const auto & shards_info = cluster->getShardsInfo();
        const auto & shards_addresses = cluster->getShardsAddresses();

        /// Check new format shard{shard_index}_replica{replica_index}
        /// (shard_index and replica_index starts from 1).
        if (address.shard_index != 0)
        {
            if (!address.replica_index)
                throw Exception(ErrorCodes::INCORRECT_FILE_NAME,
                    "Wrong replica_index={} ({})", address.replica_index, name);

            if (address.shard_index > shards_info.size())
                throw Exception(ErrorCodes::INCORRECT_FILE_NAME,
                    "No shard with shard_index={} ({})", address.shard_index, name);

            const auto & shard_info = shards_info[address.shard_index - 1];
            if (address.replica_index > shard_info.per_replica_pools.size())
                throw Exception(ErrorCodes::INCORRECT_FILE_NAME,
                    "No shard with replica_index={} ({})", address.replica_index, name);

            return shard_info.per_replica_pools[address.replica_index - 1];
        }

        /// Existing connections pool have a higher priority.
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

    auto pools = createPoolsForAddresses(name, pool_factory, storage.getCluster()->getShardsInfo(), storage.log);

    const auto settings = storage.getContext()->getSettings();
    return pools.size() == 1 ? pools.front() : std::make_shared<ConnectionPoolWithFailover>(pools,
        settings.load_balancing,
        settings.distributed_replica_error_half_life.totalSeconds(),
        settings.distributed_replica_error_cap);
}


std::map<UInt64, std::string> StorageDistributedDirectoryMonitor::getFiles()
{
    std::map<UInt64, std::string> files;
    size_t new_bytes_count = 0;

    fs::directory_iterator end;
    for (fs::directory_iterator it{path}; it != end; ++it)
    {
        const auto & file_path_str = it->path();
        if (!it->is_directory() && startsWith(fs::path(file_path_str).extension(), ".bin"))
        {
            files[parse<UInt64>(fs::path(file_path_str).stem())] = file_path_str;
            new_bytes_count += fs::file_size(fs::path(file_path_str));
        }
    }

    {
        std::lock_guard status_lock(status_mutex);

        if (status.files_count != files.size())
            LOG_TRACE(log, "Files set to {} (was {})", files.size(), status.files_count);
        if (status.bytes_count != new_bytes_count)
            LOG_TRACE(log, "Bytes set to {} (was {})", new_bytes_count, status.bytes_count);

        metric_pending_files.changeTo(files.size());
        status.files_count = files.size();
        status.bytes_count = new_bytes_count;
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
    Stopwatch watch;
    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(storage.getContext()->getSettingsRef());

    try
    {
        CurrentMetrics::Increment metric_increment{CurrentMetrics::DistributedSend};

        ReadBufferFromFile in(file_path);
        const auto & distributed_header = readDistributedHeader(in, log);

        LOG_DEBUG(log, "Started processing `{}` ({} rows, {} bytes)", file_path,
            formatReadableQuantity(distributed_header.rows),
            formatReadableSizeWithBinarySuffix(distributed_header.bytes));

        auto connection = pool->get(timeouts, &distributed_header.insert_settings);
        RemoteInserter remote{*connection, timeouts,
            distributed_header.insert_query,
            distributed_header.insert_settings,
            distributed_header.client_info};
        bool compression_expected = connection->getCompression() == Protocol::Compression::Enable;
        writeRemoteConvert(distributed_header, remote, compression_expected, in, log);
        remote.onFinish();
    }
    catch (Exception & e)
    {
        e.addMessage(fmt::format("While sending {}", file_path));
        maybeMarkAsBroken(file_path, e);
        throw;
    }

    auto dir_sync_guard = getDirectorySyncGuard(dir_fsync, disk, relative_path);
    markAsSend(file_path);
    LOG_TRACE(log, "Finished processing `{}` (took {} ms)", file_path, watch.elapsedMilliseconds());
}

struct StorageDistributedDirectoryMonitor::BatchHeader
{
    Settings settings;
    String query;
    ClientInfo client_info;
    Block header;

    BatchHeader(Settings settings_, String query_, ClientInfo client_info_, Block header_)
        : settings(std::move(settings_))
        , query(std::move(query_))
        , client_info(std::move(client_info_))
        , header(std::move(header_))
    {
    }

    bool operator==(const BatchHeader & other) const
    {
        return std::tie(settings, query, client_info.query_kind) ==
               std::tie(other.settings, other.query, other.client_info.query_kind) &&
               blocksHaveEqualStructure(header, other.header);
    }

    struct Hash
    {
        size_t operator()(const BatchHeader & batch_header) const
        {
            SipHash hash_state;
            hash_state.update(batch_header.query.data(), batch_header.query.size());
            batch_header.header.updateHash(hash_state);
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

    bool split_batch_on_failure = true;
    bool fsync = false;
    bool dir_fsync = false;

    Batch(
        StorageDistributedDirectoryMonitor & parent_,
        const std::map<UInt64, String> & file_index_to_path_)
        : parent(parent_)
        , file_index_to_path(file_index_to_path_)
        , split_batch_on_failure(parent.split_batch_on_failure)
        , fsync(parent.storage.getDistributedSettingsRef().fsync_after_insert)
        , dir_fsync(parent.dir_fsync)
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

        Stopwatch watch;

        LOG_DEBUG(parent.log, "Sending a batch of {} files ({} rows, {} bytes).", file_indices.size(),
            formatReadableQuantity(total_rows),
            formatReadableSizeWithBinarySuffix(total_bytes));

        if (!recovered)
        {
            /// For deduplication in Replicated tables to work, in case of error
            /// we must try to re-send exactly the same batches.
            /// So we save contents of the current batch into the current_batch_file_path file
            /// and truncate it afterwards if all went well.

            /// Temporary file is required for atomicity.
            String tmp_file{parent.current_batch_file_path + ".tmp"};

            auto dir_sync_guard = getDirectorySyncGuard(dir_fsync, parent.disk, parent.relative_path);
            if (fs::exists(tmp_file))
                LOG_ERROR(parent.log, "Temporary file {} exists. Unclean shutdown?", backQuote(tmp_file));

            {
                WriteBufferFromFile out{tmp_file, O_WRONLY | O_TRUNC | O_CREAT};
                writeText(out);

                out.finalize();
                if (fsync)
                    out.sync();
            }

            fs::rename(tmp_file, parent.current_batch_file_path);
        }
        auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(parent.storage.getContext()->getSettingsRef());
        auto connection = parent.pool->get(timeouts);

        bool batch_broken = false;
        bool batch_marked_as_broken = false;
        try
        {
            try
            {
                sendBatch(*connection, timeouts);
            }
            catch (const Exception & e)
            {
                if (split_batch_on_failure && isSplittableErrorCode(e.code(), e.isRemoteException()))
                {
                    tryLogCurrentException(parent.log, "Trying to split batch due to");
                    sendSeparateFiles(*connection, timeouts);
                }
                else
                    throw;
            }
        }
        catch (Exception & e)
        {
            if (isFileBrokenErrorCode(e.code(), e.isRemoteException()))
            {
                tryLogCurrentException(parent.log, "Failed to send batch due to");
                batch_broken = true;
                if (!e.isRemoteException() && e.code() == ErrorCodes::DISTRIBUTED_BROKEN_BATCH_FILES)
                    batch_marked_as_broken = true;
            }
            else
            {
                std::vector<std::string> files(file_index_to_path.size());
                for (const auto && file_info : file_index_to_path | boost::adaptors::indexed())
                    files[file_info.index()] = file_info.value().second;
                e.addMessage(fmt::format("While sending batch {}", fmt::join(files, "\n")));

                throw;
            }
        }

        if (!batch_broken)
        {
            LOG_TRACE(parent.log, "Sent a batch of {} files (took {} ms).", file_indices.size(), watch.elapsedMilliseconds());

            auto dir_sync_guard = getDirectorySyncGuard(dir_fsync, parent.disk, parent.relative_path);
            for (UInt64 file_index : file_indices)
                parent.markAsSend(file_index_to_path.at(file_index));
        }
        else if (!batch_marked_as_broken)
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

        fs::resize_file(parent.current_batch_file_path, 0);
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

private:
    void sendBatch(Connection & connection, const ConnectionTimeouts & timeouts)
    {
        std::unique_ptr<RemoteInserter> remote;

        for (UInt64 file_idx : file_indices)
        {
            auto file_path = file_index_to_path.find(file_idx);
            if (file_path == file_index_to_path.end())
                throw Exception(ErrorCodes::DISTRIBUTED_BROKEN_BATCH_INFO,
                    "Failed to send batch: file with index {} is absent", file_idx);

            ReadBufferFromFile in(file_path->second);
            const auto & distributed_header = readDistributedHeader(in, parent.log);

            if (!remote)
            {
                remote = std::make_unique<RemoteInserter>(connection, timeouts,
                    distributed_header.insert_query,
                    distributed_header.insert_settings,
                    distributed_header.client_info);
            }
            bool compression_expected = connection.getCompression() == Protocol::Compression::Enable;
            writeRemoteConvert(distributed_header, *remote, compression_expected, in, parent.log);
        }

        if (remote)
            remote->onFinish();
    }

    void sendSeparateFiles(Connection & connection, const ConnectionTimeouts & timeouts)
    {
        size_t broken_files = 0;

        for (UInt64 file_idx : file_indices)
        {
            auto file_path = file_index_to_path.find(file_idx);
            if (file_path == file_index_to_path.end())
            {
                LOG_ERROR(parent.log, "Failed to send one file from batch: file with index {} is absent", file_idx);
                ++broken_files;
                continue;
            }

            try
            {
                ReadBufferFromFile in(file_path->second);
                const auto & distributed_header = readDistributedHeader(in, parent.log);

                RemoteInserter remote(connection, timeouts,
                    distributed_header.insert_query,
                    distributed_header.insert_settings,
                    distributed_header.client_info);
                bool compression_expected = connection.getCompression() == Protocol::Compression::Enable;
                writeRemoteConvert(distributed_header, remote, compression_expected, in, parent.log);
                remote.onFinish();
            }
            catch (Exception & e)
            {
                e.addMessage(fmt::format("While sending {}", file_path->second));
                parent.maybeMarkAsBroken(file_path->second, e);
                ++broken_files;
            }
        }

        if (broken_files)
            throw Exception(ErrorCodes::DISTRIBUTED_BROKEN_BATCH_FILES,
                "Failed to send {} files", broken_files);
    }
};

class DirectoryMonitorSource : public SourceWithProgress
{
public:

    struct Data
    {
        std::unique_ptr<ReadBufferFromFile> in;
        std::unique_ptr<CompressedReadBuffer> decompressing_in;
        std::unique_ptr<NativeReader> block_in;

        Poco::Logger * log = nullptr;

        Block first_block;

        explicit Data(const String & file_name)
        {
            in = std::make_unique<ReadBufferFromFile>(file_name);
            decompressing_in = std::make_unique<CompressedReadBuffer>(*in);
            log = &Poco::Logger::get("DirectoryMonitorSource");

            auto distributed_header = readDistributedHeader(*in, log);
            block_in = std::make_unique<NativeReader>(*decompressing_in, distributed_header.revision);

            first_block = block_in->read();
        }

        Data(Data &&) = default;
    };

    explicit DirectoryMonitorSource(const String & file_name)
        : DirectoryMonitorSource(Data(file_name))
    {
    }

    explicit DirectoryMonitorSource(Data data_)
        : SourceWithProgress(data_.first_block.cloneEmpty())
        , data(std::move(data_))
    {
    }

    String getName() const override { return "DirectoryMonitorSource"; }

protected:
    Chunk generate() override
    {
        if (data.first_block)
        {
            size_t num_rows = data.first_block.rows();
            Chunk res(data.first_block.getColumns(), num_rows);
            data.first_block.clear();
            return res;
        }

        auto block = data.block_in->read();
        if (!block)
            return {};

        size_t num_rows = block.rows();
        return Chunk(block.getColumns(), num_rows);
    }

private:
    Data data;
};

std::shared_ptr<ISource> StorageDistributedDirectoryMonitor::createSourceFromFile(const String & file_name)
{
    return std::make_shared<DirectoryMonitorSource>(file_name);
}

bool StorageDistributedDirectoryMonitor::addAndSchedule(size_t file_size, size_t ms)
{
    if (quit)
        return false;

    {
        std::lock_guard status_lock(status_mutex);
        metric_pending_files.add();
        status.bytes_count += file_size;
        ++status.files_count;
    }

    return task_handle->scheduleAfter(ms, false);
}

StorageDistributedDirectoryMonitor::Status StorageDistributedDirectoryMonitor::getStatus()
{
    std::lock_guard status_lock(status_mutex);
    Status current_status{status, path, monitor_blocker.isCancelled()};
    return current_status;
}

void StorageDistributedDirectoryMonitor::processFilesWithBatching(const std::map<UInt64, std::string> & files)
{
    std::unordered_set<UInt64> file_indices_to_skip;

    if (fs::exists(current_batch_file_path))
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

        if (file_indices_to_skip.contains(file_idx))
            continue;

        size_t total_rows = 0;
        size_t total_bytes = 0;
        Block header;
        DistributedHeader distributed_header;
        try
        {
            /// Determine metadata of the current file and check if it is not broken.
            ReadBufferFromFile in{file_path};
            distributed_header = readDistributedHeader(in, log);

            if (distributed_header.rows)
            {
                total_rows += distributed_header.rows;
                total_bytes += distributed_header.bytes;
            }

            if (distributed_header.block_header)
                header = distributed_header.block_header;

            if (!total_rows || !header)
            {
                LOG_DEBUG(log, "Processing batch {} with old format (no header/rows)", in.getFileName());

                CompressedReadBuffer decompressing_in(in);
                NativeReader block_in(decompressing_in, distributed_header.revision);

                while (Block block = block_in.read())
                {
                    total_rows += block.rows();
                    total_bytes += block.bytes();

                    if (!header)
                        header = block.cloneEmpty();
                }
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

        BatchHeader batch_header(
            std::move(distributed_header.insert_settings),
            std::move(distributed_header.insert_query),
            std::move(distributed_header.client_info),
            std::move(header)
        );
        Batch & batch = header_to_batch.try_emplace(batch_header, *this, files).first->second;

        batch.file_indices.push_back(file_idx);
        batch.total_rows += total_rows;
        batch.total_bytes += total_bytes;

        if (batch.isEnoughSize())
        {
            batch.send();
        }
    }

    for (auto & kv : header_to_batch)
    {
        Batch & batch = kv.second;
        batch.send();
    }

    {
        auto dir_sync_guard = getDirectorySyncGuard(dir_fsync, disk, relative_path);

        /// current_batch.txt will not exist if there was no send
        /// (this is the case when all batches that was pending has been marked as pending)
        if (fs::exists(current_batch_file_path))
            fs::remove(current_batch_file_path);
    }
}

void StorageDistributedDirectoryMonitor::markAsBroken(const std::string & file_path)
{
    const auto last_path_separator_pos = file_path.rfind('/');
    const auto & base_path = file_path.substr(0, last_path_separator_pos + 1);
    const auto & file_name = file_path.substr(last_path_separator_pos + 1);
    const String & broken_path = fs::path(base_path) / "broken/";
    const String & broken_file_path = fs::path(broken_path) / file_name;

    fs::create_directory(broken_path);

    auto dir_sync_guard = getDirectorySyncGuard(dir_fsync, disk, relative_path);
    auto broken_dir_sync_guard = getDirectorySyncGuard(dir_fsync, disk, fs::path(relative_path) / "broken/");

    {
        std::lock_guard status_lock(status_mutex);

        size_t file_size = fs::file_size(file_path);

        --status.files_count;
        status.bytes_count -= file_size;

        ++status.broken_files_count;
        status.broken_bytes_count += file_size;

        metric_broken_files.add();
    }

    fs::rename(file_path, broken_file_path);
    LOG_ERROR(log, "Renamed `{}` to `{}`", file_path, broken_file_path);
}

void StorageDistributedDirectoryMonitor::markAsSend(const std::string & file_path)
{
    size_t file_size = fs::file_size(file_path);

    {
        std::lock_guard status_lock(status_mutex);
        metric_pending_files.sub();
        --status.files_count;
        status.bytes_count -= file_size;
    }

    fs::remove(file_path);
}

bool StorageDistributedDirectoryMonitor::maybeMarkAsBroken(const std::string & file_path, const Exception & e)
{
    /// Mark file as broken if necessary.
    if (isFileBrokenErrorCode(e.code(), e.isRemoteException()))
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

void StorageDistributedDirectoryMonitor::updatePath(const std::string & new_relative_path)
{
    task_handle->deactivate();
    std::lock_guard lock{mutex};

    {
        std::lock_guard status_lock(status_mutex);
        relative_path = new_relative_path;
        path = fs::path(disk->getPath()) / relative_path / "";
    }
    current_batch_file_path = path + "current_batch.txt";

    task_handle->activateAndSchedule();
}

}
