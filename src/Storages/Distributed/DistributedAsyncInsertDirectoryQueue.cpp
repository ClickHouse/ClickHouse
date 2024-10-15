#include <Storages/Distributed/DistributedAsyncInsertBatch.h>
#include <Storages/Distributed/DistributedAsyncInsertHeader.h>
#include <Storages/Distributed/DistributedAsyncInsertHelpers.h>
#include <Storages/Distributed/DistributedAsyncInsertDirectoryQueue.h>
#include <Storages/StorageDistributed.h>
#include <QueryPipeline/RemoteInserter.h>
#include <Formats/NativeReader.h>
#include <Processors/ISource.h>
#include <Interpreters/Context.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ConnectionTimeouts.h>
#include <Compression/CompressedReadBuffer.h>
#include <Disks/IDisk.h>
#include <Common/CurrentMetrics.h>
#include <Common/StringUtils.h>
#include <Common/SipHash.h>
#include <Common/quoteString.h>
#include <Common/ProfileEvents.h>
#include <Common/ActionBlocker.h>
#include <Common/formatReadable.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>
#include <Compression/CheckingCompressedReadBuffer.h>
#include <IO/Operators.h>
#include <base/hex.h>
#include <boost/algorithm/string/find_iterator.hpp>
#include <boost/algorithm/string/finder.hpp>
#include <boost/range/adaptor/indexed.hpp>
#include <filesystem>


namespace CurrentMetrics
{
    extern const Metric DistributedSend;
    extern const Metric DistributedFilesToInsert;
    extern const Metric BrokenDistributedFilesToInsert;
    extern const Metric DistributedBytesToInsert;
    extern const Metric BrokenDistributedBytesToInsert;
}

namespace ProfileEvents
{
    extern const Event DistributedAsyncInsertionFailures;
}

namespace fs = std::filesystem;

namespace DB
{
namespace Setting
{
    extern const SettingsBool distributed_insert_skip_read_only_replicas;
    extern const SettingsSeconds distributed_replica_error_half_life;
    extern const SettingsUInt64 distributed_replica_error_cap;
    extern const SettingsLoadBalancing load_balancing;
    extern const SettingsUInt64 min_insert_block_size_bytes;
    extern const SettingsUInt64 min_insert_block_size_rows;
}

namespace ErrorCodes
{
    extern const int INCORRECT_FILE_NAME;
    extern const int LOGICAL_ERROR;
}


namespace
{

template <typename PoolFactory>
ConnectionPoolPtrs createPoolsForAddresses(const Cluster::Addresses & addresses, PoolFactory && factory, LoggerPtr log)
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

    for (const auto & address : addresses)
        make_connection(address);

    return pools;
}

uint64_t doubleToUInt64(double d)
{
    if (d >= static_cast<double>(std::numeric_limits<uint64_t>::max()))
        return std::numeric_limits<uint64_t>::max();
    return static_cast<uint64_t>(d);
}

}


DistributedAsyncInsertDirectoryQueue::DistributedAsyncInsertDirectoryQueue(
    StorageDistributed & storage_,
    const DiskPtr & disk_,
    const std::string & relative_path_,
    ConnectionPoolWithFailoverPtr pool_,
    ActionBlocker & monitor_blocker_,
    BackgroundSchedulePool & bg_pool)
    : storage(storage_)
    , pool(std::move(pool_))
    , disk(disk_)
    , relative_path(relative_path_)
    , path(fs::path(disk->getPath()) / relative_path / "")
    , broken_relative_path(fs::path(relative_path) / "broken")
    , broken_path(fs::path(path) / "broken" / "")
    , should_batch_inserts(storage.getDistributedSettingsRef().background_insert_batch)
    , split_batch_on_failure(storage.getDistributedSettingsRef().background_insert_split_batch_on_failure)
    , dir_fsync(storage.getDistributedSettingsRef().fsync_directories)
    , min_batched_block_size_rows(storage.getContext()->getSettingsRef()[Setting::min_insert_block_size_rows])
    , min_batched_block_size_bytes(storage.getContext()->getSettingsRef()[Setting::min_insert_block_size_bytes])
    , current_batch_file_path(path + "current_batch.txt")
    , pending_files(std::numeric_limits<size_t>::max())
    , default_sleep_time(storage.getDistributedSettingsRef().background_insert_sleep_time_ms.totalMilliseconds())
    , sleep_time(default_sleep_time)
    , max_sleep_time(storage.getDistributedSettingsRef().background_insert_max_sleep_time_ms.totalMilliseconds())
    , log(getLogger(getLoggerName()))
    , monitor_blocker(monitor_blocker_)
    , metric_pending_bytes(CurrentMetrics::DistributedBytesToInsert, 0)
    , metric_pending_files(CurrentMetrics::DistributedFilesToInsert, 0)
    , metric_broken_bytes(CurrentMetrics::BrokenDistributedBytesToInsert, 0)
    , metric_broken_files(CurrentMetrics::BrokenDistributedFilesToInsert, 0)
{
    fs::create_directory(broken_path);

    initializeFilesFromDisk();

    task_handle = bg_pool.createTask(getLoggerName() + "/Bg", [this]{ run(); });
    task_handle->activateAndSchedule();
}


DistributedAsyncInsertDirectoryQueue::~DistributedAsyncInsertDirectoryQueue()
{
    if (!pending_files.isFinished())
    {
        pending_files.clearAndFinish();
        task_handle->deactivate();
    }
}

void DistributedAsyncInsertDirectoryQueue::flushAllData(const SettingsChanges & settings_changes)
{
    if (pending_files.isFinished())
        return;

    std::lock_guard lock{mutex};
    if (!hasPendingFiles())
        return;
    processFiles(settings_changes);
}

void DistributedAsyncInsertDirectoryQueue::shutdownAndDropAllData()
{
    if (!pending_files.isFinished())
    {
        pending_files.clearAndFinish();
        task_handle->deactivate();
    }

    auto dir_sync_guard = getDirectorySyncGuard(relative_path);
    fs::remove_all(path);
}

void DistributedAsyncInsertDirectoryQueue::shutdownWithoutFlush()
{
    /// It's incompatible with should_batch_inserts
    /// because processFilesWithBatching may push to the queue after shutdown
    chassert(!should_batch_inserts);
    pending_files.finish();
    task_handle->deactivate();
}


void DistributedAsyncInsertDirectoryQueue::run()
{
    constexpr const std::chrono::minutes decrease_error_count_period{5};

    std::lock_guard lock{mutex};

    bool do_sleep = false;
    while (!pending_files.isFinished())
    {
        do_sleep = true;

        if (!hasPendingFiles())
            break;

        if (!monitor_blocker.isCancelled())
        {
            try
            {
                processFiles();
                /// No errors while processing existing files.
                /// Let's see maybe there are more files to process.
                do_sleep = false;

                const auto now = std::chrono::system_clock::now();
                if (now - last_decrease_time > decrease_error_count_period)
                {
                    std::lock_guard status_lock(status_mutex);

                    status.error_count /= 2;
                    last_decrease_time = now;
                }
            }
            catch (...)
            {
                tryLogCurrentException(getLoggerName().data());

                UInt64 q = doubleToUInt64(std::exp2(status.error_count));
                std::chrono::milliseconds new_sleep_time(default_sleep_time.count() * q);
                if (new_sleep_time.count() < 0)
                    sleep_time = max_sleep_time;
                else
                    sleep_time = std::min(new_sleep_time, max_sleep_time);

                do_sleep = true;
            }
        }
        else
            LOG_TEST(LogFrequencyLimiter(log, 30), "Skipping send data over distributed table.");

        if (do_sleep)
            break;
    }

    if (!pending_files.isFinished() && do_sleep)
        task_handle->scheduleAfter(sleep_time.count());
}


ConnectionPoolWithFailoverPtr DistributedAsyncInsertDirectoryQueue::createPool(const Cluster::Addresses & addresses, const StorageDistributed & storage)
{
    const auto pool_factory = [&storage] (const Cluster::Address & address) -> ConnectionPoolPtr
    {
        const auto & cluster = storage.getCluster();
        const auto & shards_info = cluster->getShardsInfo();
        const auto & shards_addresses = cluster->getShardsAddresses();

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
            address.proto_send_chunked,
            address.proto_recv_chunked,
            address.quota_key,
            address.cluster,
            address.cluster_secret,
            storage.getName() + '_' + address.user, /* client */
            Protocol::Compression::Enable,
            address.secure);
    };

    auto pools = createPoolsForAddresses(addresses, pool_factory, storage.log);

    const auto & settings = storage.getContext()->getSettingsRef();
    return std::make_shared<ConnectionPoolWithFailover>(
        std::move(pools),
        settings[Setting::load_balancing],
        settings[Setting::distributed_replica_error_half_life].totalSeconds(),
        settings[Setting::distributed_replica_error_cap]);
}

bool DistributedAsyncInsertDirectoryQueue::hasPendingFiles() const
{
    return fs::exists(current_batch_file_path) || !current_file.empty() || !pending_files.empty();
}

void DistributedAsyncInsertDirectoryQueue::addFile(const std::string & file_path)
{
    if (!pending_files.push(fs::absolute(file_path).string()))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot schedule a file '{}'", file_path);
}

void DistributedAsyncInsertDirectoryQueue::initializeFilesFromDisk()
{
    /// NOTE: This method does not requires to hold status_mutex (because this
    /// object is not in the list that the caller may iterate over), hence, no
    /// TSA annotations in the header file.

    fs::directory_iterator end;

    /// Initialize pending files
    {
        size_t bytes_count = 0;

        for (fs::directory_iterator it{path}; it != end; ++it)
        {
            const auto & file_path = it->path();
            const auto & base_name = file_path.stem().string();
            if (!it->is_directory() && startsWith(fs::path(file_path).extension(), ".bin") && parse<UInt64>(base_name))
            {
                const std::string & file_path_str = file_path.string();
                addFile(file_path_str);
                bytes_count += fs::file_size(file_path);
            }
            else if (base_name != "tmp" && base_name != "broken")
            {
                /// It is OK to log current_batch.txt here too (useful for debugging).
                LOG_WARNING(log, "Unexpected file {} in {}", file_path.string(), path);
            }
        }

        LOG_TRACE(log, "Files set to {}", pending_files.size());
        LOG_TRACE(log, "Bytes set to {}", bytes_count);

        metric_pending_bytes.changeTo(bytes_count);
        metric_pending_files.changeTo(pending_files.size());
        status.files_count = pending_files.size();
        status.bytes_count = bytes_count;
    }

    /// Initialize broken files
    {
        size_t broken_bytes_count = 0;
        size_t broken_files = 0;

        for (fs::directory_iterator it{broken_path}; it != end; ++it)
        {
            const auto & file_path = it->path();
            if (!it->is_directory() && startsWith(fs::path(file_path).extension(), ".bin") && parse<UInt64>(file_path.stem()))
                broken_bytes_count += fs::file_size(file_path);
            else
                LOG_WARNING(log, "Unexpected file {} in {}", file_path.string(), broken_path);
        }

        LOG_TRACE(log, "Broken files set to {}", broken_files);
        LOG_TRACE(log, "Broken bytes set to {}", broken_bytes_count);

        metric_broken_files.changeTo(broken_files);
        metric_broken_bytes.changeTo(broken_bytes_count);
        status.broken_files_count = broken_files;
        status.broken_bytes_count = broken_bytes_count;
    }
}
void DistributedAsyncInsertDirectoryQueue::processFiles(const SettingsChanges & settings_changes)
try
{
    if (should_batch_inserts)
        processFilesWithBatching(settings_changes);
    else
    {
        /// Process unprocessed file.
        if (!current_file.empty())
            processFile(current_file, settings_changes);

        while (!pending_files.isFinished() && pending_files.tryPop(current_file))
            processFile(current_file, settings_changes);
    }

    std::lock_guard status_lock(status_mutex);
    status.last_exception = std::exception_ptr{};
}
catch (...)
{
    ProfileEvents::increment(ProfileEvents::DistributedAsyncInsertionFailures);

    std::lock_guard status_lock(status_mutex);

    ++status.error_count;
    status.last_exception = std::current_exception();
    status.last_exception_time = std::chrono::system_clock::now();

    throw;
}

void DistributedAsyncInsertDirectoryQueue::processFile(std::string & file_path, const SettingsChanges & settings_changes)
{
    OpenTelemetry::TracingContextHolderPtr thread_trace_context;

    Stopwatch watch;
    try
    {
        CurrentMetrics::Increment metric_increment{CurrentMetrics::DistributedSend};

        ReadBufferFromFile in(file_path);
        const auto & distributed_header = DistributedAsyncInsertHeader::read(in, log);
        thread_trace_context = distributed_header.createTracingContextHolder(
            __PRETTY_FUNCTION__,
            storage.getContext()->getOpenTelemetrySpanLog());

        Settings insert_settings = distributed_header.insert_settings;
        insert_settings.applyChanges(settings_changes);

        auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(insert_settings);
        auto results = pool->getManyCheckedForInsert(timeouts, insert_settings, PoolMode::GET_ONE, storage.remote_storage.getQualifiedName());
        auto result = pool->getValidTryResult(results, insert_settings[Setting::distributed_insert_skip_read_only_replicas]);
        auto connection = std::move(result.entry);

        LOG_DEBUG(log, "Sending `{}` to {} ({} rows, {} bytes)",
            file_path,
            connection->getDescription(),
            formatReadableQuantity(distributed_header.rows),
            formatReadableSizeWithBinarySuffix(distributed_header.bytes));

        RemoteInserter remote{*connection, timeouts,
            distributed_header.insert_query,
            insert_settings,
            distributed_header.client_info};
        bool compression_expected = connection->getCompression() == Protocol::Compression::Enable;
        writeRemoteConvert(distributed_header, remote, compression_expected, in, log);
        remote.onFinish();
    }
    catch (Exception & e)
    {
        if (thread_trace_context)
            thread_trace_context->root_span.addAttribute(std::current_exception());

        e.addMessage(fmt::format("While sending {}", file_path));
        if (isDistributedSendBroken(e.code(), e.isRemoteException()))
        {
            markAsBroken(file_path);
            file_path.clear();
        }
        throw;
    }
    catch (...)
    {
        if (thread_trace_context)
            thread_trace_context->root_span.addAttribute(std::current_exception());

        throw;
    }

    auto dir_sync_guard = getDirectorySyncGuard(relative_path);
    markAsSend(file_path);
    LOG_TRACE(log, "Finished processing `{}` (took {} ms)", file_path, watch.elapsedMilliseconds());
    file_path.clear();
}

struct DistributedAsyncInsertDirectoryQueue::BatchHeader
{
    Settings settings;
    String query;
    ClientInfo client_info;
    Block header;

    BatchHeader(Settings settings_, String query_, ClientInfo client_info_, Block header_)
        : settings(settings_)
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

bool DistributedAsyncInsertDirectoryQueue::addFileAndSchedule(const std::string & file_path, size_t file_size, size_t ms)
{
    /// NOTE: It is better not to throw in this case, since the file is already
    /// on disk (see DistributedSink), and it will be processed next time.
    if (pending_files.isFinished())
    {
        LOG_DEBUG(log, "File {} had not been scheduled, since the table had been detached", file_path);
        return false;
    }

    addFile(file_path);

    {
        std::lock_guard lock(status_mutex);
        metric_pending_files.add();
        metric_pending_bytes.add(file_size);
        status.bytes_count += file_size;
        ++status.files_count;
    }

    return task_handle->scheduleAfter(ms, false);
}

DistributedAsyncInsertDirectoryQueue::Status DistributedAsyncInsertDirectoryQueue::getStatus()
{
    std::lock_guard status_lock(status_mutex);
    Status current_status{status, path, monitor_blocker.isCancelled()};
    return current_status;
}

void DistributedAsyncInsertDirectoryQueue::processFilesWithBatching(const SettingsChanges & settings_changes)
{
    /// Possibly, we failed to send a batch on the previous iteration. Try to send exactly the same batch.
    if (fs::exists(current_batch_file_path))
    {
        LOG_DEBUG(log, "Restoring the batch");

        DistributedAsyncInsertBatch batch(*this);
        batch.deserialize();

        /// In case of recovery it is possible that some of files will be
        /// missing, if server had been restarted abnormally
        /// (between unlink(*.bin) and unlink(current_batch.txt)).
        ///
        /// But current_batch_file_path should be removed anyway, since if some
        /// file was missing, then the batch is not complete and there is no
        /// point in trying to pretend that it will not break deduplication.
        if (batch.valid())
            batch.send(settings_changes);

        auto dir_sync_guard = getDirectorySyncGuard(relative_path);
        fs::remove(current_batch_file_path);
    }

    std::unordered_map<BatchHeader, DistributedAsyncInsertBatch, BatchHeader::Hash> header_to_batch;

    std::string file_path;

    try
    {
        while (pending_files.tryPop(file_path))
        {
            if (!fs::exists(file_path))
            {
                LOG_WARNING(log, "File {} does not exist, likely due to current_batch.txt processing", file_path);
                continue;
            }

            size_t total_rows = 0;
            size_t total_bytes = 0;
            Block header;
            DistributedAsyncInsertHeader distributed_header;
            try
            {
                /// Determine metadata of the current file and check if it is not broken.
                ReadBufferFromFile in{file_path};
                distributed_header = DistributedAsyncInsertHeader::read(in, log);

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
                if (isDistributedSendBroken(e.code(), e.isRemoteException()))
                {
                    markAsBroken(file_path);
                    tryLogCurrentException(log, "File is marked broken due to");
                    continue;
                }
                throw;
            }

            BatchHeader batch_header(
                std::move(distributed_header.insert_settings),
                std::move(distributed_header.insert_query),
                std::move(distributed_header.client_info),
                std::move(header)
            );
            DistributedAsyncInsertBatch & batch = header_to_batch.try_emplace(batch_header, *this).first->second;

            batch.files.push_back(file_path);
            batch.total_rows += total_rows;
            batch.total_bytes += total_bytes;

            if (batch.isEnoughSize())
            {
                batch.send(settings_changes);
            }
        }

        for (auto & kv : header_to_batch)
        {
            DistributedAsyncInsertBatch & batch = kv.second;
            batch.send(settings_changes);
        }
    }
    catch (...)
    {
        /// Revert uncommitted files.
        for (const auto & [_, batch] : header_to_batch)
        {
            for (const auto & file : batch.files)
            {
                if (!pending_files.pushFront(file))
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot re-schedule a file '{}'", file);
            }
        }
        /// Rethrow exception
        throw;
    }

    {
        auto dir_sync_guard = getDirectorySyncGuard(relative_path);

        /// current_batch.txt will not exist if there was no send
        /// (this is the case when all batches that was pending has been marked as pending)
        if (fs::exists(current_batch_file_path))
            fs::remove(current_batch_file_path);
    }
}

void DistributedAsyncInsertDirectoryQueue::markAsBroken(const std::string & file_path)
{
    const String & broken_file_path = fs::path(broken_path) / fs::path(file_path).filename();

    auto dir_sync_guard = getDirectorySyncGuard(relative_path);
    auto broken_dir_sync_guard = getDirectorySyncGuard(broken_relative_path);

    {
        std::lock_guard status_lock(status_mutex);

        size_t file_size = fs::file_size(file_path);

        --status.files_count;
        status.bytes_count -= file_size;

        ++status.broken_files_count;
        status.broken_bytes_count += file_size;

        metric_broken_files.add();
        metric_broken_bytes.add(file_size);
    }

    fs::rename(file_path, broken_file_path);
    LOG_ERROR(log, "Renamed `{}` to `{}`", file_path, broken_file_path);
}

void DistributedAsyncInsertDirectoryQueue::markAsSend(const std::string & file_path)
{
    size_t file_size = fs::file_size(file_path);

    {
        std::lock_guard status_lock(status_mutex);
        metric_pending_files.sub();
        metric_pending_bytes.sub(file_size);
        --status.files_count;
        status.bytes_count -= file_size;
    }

    fs::remove(file_path);
}

SyncGuardPtr DistributedAsyncInsertDirectoryQueue::getDirectorySyncGuard(const std::string & dir_path)
{
    if (dir_fsync)
        return disk->getDirectorySyncGuard(dir_path);
    return nullptr;
}

std::string DistributedAsyncInsertDirectoryQueue::getLoggerName() const
{
    return storage.getStorageID().getFullTableName() + ".DistributedInsertQueue." + disk->getName();
}

void DistributedAsyncInsertDirectoryQueue::updatePath(const std::string & new_relative_path)
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
