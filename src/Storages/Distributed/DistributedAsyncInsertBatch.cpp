#include <Storages/Distributed/DistributedAsyncInsertBatch.h>
#include <Storages/Distributed/DistributedAsyncInsertHelpers.h>
#include <Storages/Distributed/DistributedAsyncInsertHeader.h>
#include <Storages/Distributed/DistributedAsyncInsertDirectoryQueue.h>
#include <Storages/Distributed/DistributedSettings.h>
#include <Storages/StorageDistributed.h>
#include <QueryPipeline/RemoteInserter.h>
#include <Common/CurrentMetrics.h>
#include <base/defines.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromFile.h>

namespace CurrentMetrics
{
    extern const Metric DistributedSend;
}

namespace DB
{
namespace Setting
{
    extern const SettingsBool distributed_insert_skip_read_only_replicas;
}

namespace DistributedSetting
{
    extern const DistributedSettingsBool fsync_after_insert;
}

namespace ErrorCodes
{
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
        || isDistributedSendBroken(code, remote)
    ;
}

DistributedAsyncInsertBatch::DistributedAsyncInsertBatch(DistributedAsyncInsertDirectoryQueue & parent_)
    : parent(parent_)
    , split_batch_on_failure(parent.split_batch_on_failure)
    , fsync(parent.storage.getDistributedSettingsRef()[DistributedSetting::fsync_after_insert])
    , dir_fsync(parent.dir_fsync)
{}

bool DistributedAsyncInsertBatch::isEnoughSize() const
{
    return (!parent.min_batched_block_size_rows && !parent.min_batched_block_size_bytes)
        || (parent.min_batched_block_size_rows && total_rows >= parent.min_batched_block_size_rows)
        || (parent.min_batched_block_size_bytes && total_bytes >= parent.min_batched_block_size_bytes);
}

void DistributedAsyncInsertBatch::send(const SettingsChanges & settings_changes)
{
    if (files.empty())
        return;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::DistributedSend};

    Stopwatch watch;

    if (!recovered)
    {
        /// For deduplication in Replicated tables to work, in case of error
        /// we must try to re-send exactly the same batches.
        /// So we save contents of the current batch into the current_batch_file_path file
        /// and truncate it afterwards if all went well.
        serialize();
    }

    bool batch_broken = false;
    bool batch_marked_as_broken = false;
    try
    {
        try
        {
            sendBatch(settings_changes);
        }
        catch (const Exception & e)
        {
            if (split_batch_on_failure && files.size() > 1 && isSplittableErrorCode(e.code(), e.isRemoteException()))
            {
                tryLogCurrentException(parent.log, "Trying to split batch due to");
                sendSeparateFiles(settings_changes);
            }
            else
                throw;
        }
    }
    catch (Exception & e)
    {
        if (isDistributedSendBroken(e.code(), e.isRemoteException()))
        {
            tryLogCurrentException(parent.log, "Failed to send batch due to");
            batch_broken = true;
            if (!e.isRemoteException() && e.code() == ErrorCodes::DISTRIBUTED_BROKEN_BATCH_FILES)
                batch_marked_as_broken = true;
        }
        else
        {
            e.addMessage(fmt::format("While sending a batch of {} files, files: {}", files.size(), fmt::join(files, "\n")));
            throw;
        }
    }

    if (!batch_broken)
    {
        LOG_TRACE(parent.log, "Sent a batch of {} files (took {} ms).", files.size(), watch.elapsedMilliseconds());

        auto dir_sync_guard = parent.getDirectorySyncGuard(parent.relative_path);
        for (const auto & file : files)
            parent.markAsSend(file);
    }
    else if (!batch_marked_as_broken)
    {
        LOG_ERROR(parent.log, "Marking a batch of {} files as broken, files: {}", files.size(), fmt::join(files, "\n"));

        for (const auto & file : files)
            parent.markAsBroken(file);
    }

    files.clear();
    total_rows = 0;
    total_bytes = 0;
    recovered = false;

    std::filesystem::resize_file(parent.current_batch_file_path, 0);
}

void DistributedAsyncInsertBatch::serialize()
{
    /// Temporary file is required for atomicity.
    String tmp_file{parent.current_batch_file_path + ".tmp"};

    auto dir_sync_guard = parent.getDirectorySyncGuard(parent.relative_path);
    if (std::filesystem::exists(tmp_file))
        LOG_ERROR(parent.log, "Temporary file {} exists. Unclean shutdown?", backQuote(tmp_file));

    {
        WriteBufferFromFile out{tmp_file, O_WRONLY | O_TRUNC | O_CREAT};
        writeText(out);

        out.finalize();
        if (fsync)
            out.sync();
    }

    std::filesystem::rename(tmp_file, parent.current_batch_file_path);
}

void DistributedAsyncInsertBatch::deserialize()
{
    ReadBufferFromFile in{parent.current_batch_file_path};
    readText(in);
}

bool DistributedAsyncInsertBatch::valid()
{
    chassert(!files.empty());

    bool res = true;
    for (const auto & file : files)
    {
        if (!fs::exists(file))
        {
            LOG_WARNING(parent.log, "File {} does not exist, likely due abnormal shutdown", file);
            res = false;
        }
    }
    return res;
}

void DistributedAsyncInsertBatch::writeText(WriteBuffer & out)
{
    for (const auto & file : files)
    {
        UInt64 file_index = parse<UInt64>(std::filesystem::path(file).stem());
        out << file_index << '\n';
    }
}

void DistributedAsyncInsertBatch::readText(ReadBuffer & in)
{
    while (!in.eof())
    {
        UInt64 idx;
        in >> idx >> "\n";
        files.push_back(std::filesystem::absolute(fmt::format("{}/{}.bin", parent.path, idx)).string());

        ReadBufferFromFile header_buffer(files.back());
        const DistributedAsyncInsertHeader & header = DistributedAsyncInsertHeader::read(header_buffer, parent.log);
        total_bytes += total_bytes;

        if (header.rows)
        {
            total_rows += header.rows;
            total_bytes += header.bytes;
        }
    }

    recovered = true;
}

void DistributedAsyncInsertBatch::sendBatch(const SettingsChanges & settings_changes)
{
    IConnectionPool::Entry connection;
    std::unique_ptr<RemoteInserter> remote;
    bool compression_expected = false;

    /// Since the batch is sent as a whole (in case of failure, the whole batch
    /// will be repeated), we need to mark the whole batch as failed in case of
    /// error).
    std::vector<OpenTelemetry::TracingContextHolderPtr> tracing_contexts;
    UInt64 batch_start_time = clock_gettime_ns();

    try
    {
        for (const auto & file : files)
        {
            ReadBufferFromFile in(file);
            const auto & distributed_header = DistributedAsyncInsertHeader::read(in, parent.log);

            tracing_contexts.emplace_back(distributed_header.createTracingContextHolder(
                __PRETTY_FUNCTION__,
                parent.storage.getContext()->getOpenTelemetrySpanLog()));
            tracing_contexts.back()->root_span.addAttribute("clickhouse.distributed_batch_start_time", batch_start_time);

            if (!remote)
            {
                Settings insert_settings = distributed_header.insert_settings;
                insert_settings.applyChanges(settings_changes);

                auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(insert_settings);
                auto results = parent.pool->getManyCheckedForInsert(timeouts, insert_settings, PoolMode::GET_ONE, parent.storage.remote_storage.getQualifiedName());
                auto result = parent.pool->getValidTryResult(results, insert_settings[Setting::distributed_insert_skip_read_only_replicas]);
                connection = std::move(result.entry);
                compression_expected = connection->getCompression() == Protocol::Compression::Enable;

                LOG_DEBUG(parent.log, "Sending a batch of {} files to {} ({} rows, {} bytes).",
                    files.size(),
                    connection->getDescription(),
                    formatReadableQuantity(total_rows),
                    formatReadableSizeWithBinarySuffix(total_bytes));

                remote = std::make_unique<RemoteInserter>(*connection, timeouts,
                    distributed_header.insert_query,
                    insert_settings,
                    distributed_header.client_info);
            }
            writeRemoteConvert(distributed_header, *remote, compression_expected, in, parent.log);
        }

        if (remote)
            remote->onFinish();
    }
    catch (...)
    {
        try
        {
            for (auto & tracing_context : tracing_contexts)
                tracing_context->root_span.addAttribute(std::current_exception());
        }
        catch (...)
        {
            tryLogCurrentException(parent.log, "Cannot append exception to tracing context");
        }
        throw;
    }
}

void DistributedAsyncInsertBatch::sendSeparateFiles(const SettingsChanges & settings_changes)
{
    size_t broken_files = 0;

    for (const auto & file : files)
    {
        OpenTelemetry::TracingContextHolderPtr trace_context;

        try
        {
            ReadBufferFromFile in(file);
            const auto & distributed_header = DistributedAsyncInsertHeader::read(in, parent.log);

            Settings insert_settings = distributed_header.insert_settings;
            insert_settings.applyChanges(settings_changes);

            // This function is called in a separated thread, so we set up the trace context from the file
            trace_context = distributed_header.createTracingContextHolder(
                __PRETTY_FUNCTION__,
                parent.storage.getContext()->getOpenTelemetrySpanLog());

            auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(insert_settings);
            auto results = parent.pool->getManyCheckedForInsert(timeouts, insert_settings, PoolMode::GET_ONE, parent.storage.remote_storage.getQualifiedName());
            auto result = parent.pool->getValidTryResult(results, insert_settings[Setting::distributed_insert_skip_read_only_replicas]);
            auto connection = std::move(result.entry);
            bool compression_expected = connection->getCompression() == Protocol::Compression::Enable;

            RemoteInserter remote(*connection, timeouts,
                distributed_header.insert_query,
                insert_settings,
                distributed_header.client_info);

            writeRemoteConvert(distributed_header, remote, compression_expected, in, parent.log);
            remote.onFinish();
        }
        catch (Exception & e)
        {
            trace_context->root_span.addAttribute(std::current_exception());

            if (isDistributedSendBroken(e.code(), e.isRemoteException()))
            {
                parent.markAsBroken(file);
                ++broken_files;
            }
        }
    }

    if (broken_files)
        throw Exception(ErrorCodes::DISTRIBUTED_BROKEN_BATCH_FILES,
            "Failed to send {} files", broken_files);
}

}
