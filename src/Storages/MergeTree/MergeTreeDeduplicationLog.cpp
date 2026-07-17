#include <filesystem>
#include <Disks/IDisk.h>
#include <Disks/DiskObjectStorage/DiskObjectStorage.h>
#include <Disks/WriteMode.h>
#include <Disks/supportWritingWithAppend.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteHelpers.h>
#include <Storages/MergeTree/MergeTreeDeduplicationLog.h>
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
}

namespace
{

void writeRecord(const MergeTreeDeduplicationLogRecord & record, WriteBuffer & out)
{
    writeIntText(static_cast<uint8_t>(record.operation), out);
    writeChar('\t', out);
    writeString(record.part_name, out);
    writeChar('\t', out);
    writeString(record.block_id, out);
    writeChar('\n', out);
    out.next();
}

void readRecord(MergeTreeDeduplicationLogRecord & record, ReadBuffer & in)
{
    uint8_t op = 0;
    readIntText(op, in);
    record.operation = static_cast<MergeTreeDeduplicationOp>(op);
    assertChar('\t', in);
    readString(record.part_name, in);
    assertChar('\t', in);
    readString(record.block_id, in);
    assertChar('\n', in);
}


std::string getLogPath(const std::string & prefix, size_t number)
{
    std::filesystem::path path(prefix);
    path /= std::filesystem::path(std::string{"deduplication_log_"} + std::to_string(number) + ".txt");
    return path;
}

size_t getLogNumber(const std::string & path_str)
{
    std::filesystem::path path(path_str);
    std::string filename = path.stem();
    Strings filename_parts;
    boost::split(filename_parts, filename, boost::is_any_of("_"));

    return parse<size_t>(filename_parts[2]);
}

}

MergeTreeDeduplicationLog::MergeTreeDeduplicationLog(
    const std::string & logs_dir_, size_t deduplication_window_, const MergeTreeDataFormatVersion & format_version_, DiskPtr disk_)
    : logs_dir(logs_dir_)
    , deduplication_window(deduplication_window_)
    , rotate_interval(deduplication_window_ * 2) /// actually it doesn't matter
    , format_version(format_version_)
    , deduplication_map(deduplication_window)
    , disk(disk_)
    , disk_supports_writing_with_append(supportWritingWithAppend(disk))
{
    if (deduplication_window != 0 && !disk->existsDirectory(logs_dir))
        disk->createDirectories(logs_dir);
}

void MergeTreeDeduplicationLog::load()
{
    if (!disk->existsDirectory(logs_dir))
    {
        if (auto * object_storage = dynamic_cast<DiskObjectStorage *>(disk.get()))
        {
            // MetadataStorageType::Plain does not have directory concept. When checking `logs_dir` existence, it might return false.
            if (object_storage->getMetadataStorage()->getType() != MetadataStorageType::Plain)
                return;
        }
    }

    for (auto it = disk->iterateDirectory(logs_dir); it->isValid(); it->next())
    {
        const auto & path = it->path();
        auto log_number = getLogNumber(path);
        existing_logs[log_number] = {path, 0};
    }

    /// We should know which logs are exist even in case
    /// of deduplication_window = 0
    if (!existing_logs.empty())
        current_log_number = existing_logs.rbegin()->first;

    if (deduplication_window != 0)
    {
        /// Order important, we load history from the begging to the end.
        /// Collect every record from all logs first (in chronological order), then
        /// replay them together: a CANCEL record can refer to an ADD in an earlier
        /// log file, so the (ADD, CANCEL) pairs of rolled-back inserts can only be
        /// cancelled out once the whole history is known. `record_log_numbers` keeps
        /// each record's originating log number so `applyRecords` can recompute the
        /// per-file `entries_count` from only the surviving records.
        std::vector<MergeTreeDeduplicationLogRecord> records;
        std::vector<size_t> record_log_numbers;
        for (auto & [log_number, desc] : existing_logs)
        {
            try
            {
                loadSingleLog(desc.path, log_number, records, record_log_numbers);
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__, "Error while loading MergeTree deduplication log on path " + desc.path);
            }
        }

        applyRecords(records, record_log_numbers);

        /// Start new log, drop previous
        rotateAndDropIfNeeded();

        /// Can happen in case we have unfinished log
        if (!current_writer)
            current_writer = disk->writeFile(existing_logs.rbegin()->second.path, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append);
    }
}

void MergeTreeDeduplicationLog::loadSingleLog(
    const std::string & path,
    size_t log_number,
    std::vector<MergeTreeDeduplicationLogRecord> & records,
    std::vector<size_t> & record_log_numbers)
{
    auto read_buf = disk->readFile(path, getReadSettings());

    while (!read_buf->eof())
    {
        MergeTreeDeduplicationLogRecord record;
        readRecord(record, *read_buf);
        records.push_back(std::move(record));
        /// Kept in lockstep with `records` (pushed together even if a later read
        /// throws) so every record can be attributed back to this log file.
        record_log_numbers.push_back(log_number);
    }
}

void MergeTreeDeduplicationLog::applyRecords(
    const std::vector<MergeTreeDeduplicationLogRecord> & records,
    const std::vector<size_t> & record_log_numbers)
{
    /// First, cancel out the (ADD, CANCEL) and (DROP, CANCEL) pairs left behind
    /// by operations that failed and rolled back. Each CANCEL record cancels the
    /// most recent preceding, not-yet-cancelled ADD or DROP of the same block id -
    /// which is exactly the record the failed operation wrote, because the
    /// rollback writes the CANCEL records immediately after the failed batch
    /// under the same lock, with no other operation in between. Dropping both
    /// records means the transient record never touches the in-memory map on
    /// replay: a rolled-back ADD neither publishes its block id nor consumes a
    /// deduplication-window slot (which could otherwise evict an unrelated,
    /// still-active block before the CANCEL is seen), and a rolled-back DROP does
    /// not erase a block id that stayed published in the live map.
    std::vector<bool> cancelled(records.size(), false);
    std::unordered_map<std::string_view, std::vector<size_t>, StringViewHash> pending_indices;
    for (size_t i = 0; i < records.size(); ++i)
    {
        const auto & record = records[i];
        if (record.operation == MergeTreeDeduplicationOp::CANCEL)
        {
            /// The CANCEL record itself is never replayed.
            cancelled[i] = true;
            auto it = pending_indices.find(record.block_id);
            if (it != pending_indices.end() && !it->second.empty())
            {
                cancelled[it->second.back()] = true;
                it->second.pop_back();
            }
        }
        else
        {
            pending_indices[record.block_id].push_back(i);
        }
    }

    /// Recompute each log's `entries_count` from only the records that survive
    /// cancel-pair elimination. `dropOutdatedLogs` sums these counts from the
    /// newest log backwards to decide which older logs are redundant; a cancelled
    /// pair contributes nothing to the reconstructed map, so counting its raw
    /// records would let a failed multi-block operation inflate the counts and
    /// wrongly drop an older log that still holds live block ids - after which a
    /// restart forgets those committed blocks. Counting only survivors keeps the
    /// retention accounting in step with what a replay actually reconstructs.
    for (auto & log : existing_logs)
        log.second.entries_count = 0;
    for (size_t i = 0; i < records.size(); ++i)
        if (!cancelled[i])
            existing_logs.at(record_log_numbers[i]).entries_count++;

    /// Now replay the surviving records exactly as they happened live: ADD inserts
    /// (evicting the oldest entry when the map is full), DROP erases.
    for (size_t i = 0; i < records.size(); ++i)
    {
        if (cancelled[i])
            continue;

        const auto & record = records[i];
        if (record.operation == MergeTreeDeduplicationOp::DROP)
            deduplication_map.erase(record.block_id);
        else
            deduplication_map.insert(record.block_id, MergeTreePartInfo::fromPartName(record.part_name, format_version));
    }
}

void MergeTreeDeduplicationLog::rotate()
{
    /// We don't deduplicate anything so we don't need any writers
    if (deduplication_window == 0)
        return;

    /// Open the writer for the new log file first, before touching any state.
    /// If this throws (e.g. a transient I/O error, or an injected fault), nothing
    /// has changed: `current_writer` still points to the previous, non-finalized
    /// writer, so the log remains usable and the operation can be retried later.
    /// Previously the new writer was created only after the old one had been
    /// finalized, so a failure here left `current_writer` pointing to a finalized
    /// buffer, and the next write (e.g. from the background cleanup thread) aborted
    /// with the "Cannot write to finalized buffer" logical error.
    size_t new_log_number = current_log_number + 1;
    auto new_path = getLogPath(logs_dir, new_log_number);
    auto new_writer = disk->writeFile(new_path, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite);

    /// The new writer is ready; now finalize the previous one and switch over.
    /// `current_writer` can already be canceled here - e.g. `addPart` rolling back
    /// a failed insert calls `rotate` after a failed `writeRecord` left it canceled.
    /// `finalize` disallows calling it on a canceled buffer (it throws a logical
    /// error, which aborts the process in debug and sanitizer builds), so skip it
    /// in that case: a canceled buffer has nothing left to flush or sync anyway.
    std::exception_ptr finalize_error;
    try
    {
        if (current_writer && !current_writer->isCanceled())
        {
            current_writer->finalize();
            current_writer->sync();
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__, "Error while finalizing MergeTree deduplication log on path " + existing_logs[current_log_number].path + "; the last " + DB::toString(existing_logs[current_log_number].entries_count) + " records may not have reached durable storage");
        /// A no-op in both possible states (a failed `finalize` cancels the buffer
        /// itself, and `cancel` does nothing on a finalized buffer whose `sync`
        /// failed), but keeps the writer safely destructible no matter what threw.
        if (current_writer)
            current_writer->cancel();
        finalize_error = std::current_exception();
    }

    current_log_number = new_log_number;
    existing_logs.emplace(current_log_number, MergeTreeDeduplicationLogNameDescription{new_path, 0});
    current_writer = std::move(new_writer);

    /// A failure to finalize or sync the previous log file means the records
    /// written to it may never have reached durable storage, so the failure must
    /// propagate: `addPart` publishes block IDs - and its caller commits the part -
    /// only if the rotation succeeds, and a committed insert whose only `ADD`
    /// record is lost would be forgotten after a restart, wrongly accepting - and
    /// duplicating - a retry of the same block. Rethrow only after switching over
    /// to the new writer, so the log itself stays usable and `addPart` can still
    /// write compensating `DROP` records during its rollback.
    if (finalize_error)
        std::rethrow_exception(finalize_error);
}

void MergeTreeDeduplicationLog::dropOutdatedLogs()
{
    size_t current_sum = 0;
    size_t remove_from_value = 0;
    /// Go from end to the beginning
    for (auto itr = existing_logs.rbegin(); itr != existing_logs.rend(); ++itr)
    {
        if (current_sum > deduplication_window)
        {
            /// We have more logs than required, all older files (including current) can be dropped
            remove_from_value = itr->first;
            break;
        }

        auto & description = itr->second;
        current_sum += description.entries_count;
    }

    /// If we found some logs to drop
    if (remove_from_value != 0)
    {
        /// Go from the beginning to the end and drop all outdated logs
        for (auto itr = existing_logs.begin(); itr != existing_logs.end();)
        {
            size_t number = itr->first;
            disk->removeFile(itr->second.path);
            itr = existing_logs.erase(itr);
            if (remove_from_value == number)
                break;
        }
    }

}

void MergeTreeDeduplicationLog::rotateAndDropIfNeeded()
{
    /// If we don't have logs at all or already have enough records in current
    /// For the disk that doesn't support writing with append, we can't append logs to the last file.
    if (existing_logs.empty() || existing_logs[current_log_number].entries_count >= rotate_interval || !disk_supports_writing_with_append)
    {
        rotate();
        dropOutdatedLogs();
    }
}

std::vector<MergeTreeDeduplicationLog::AddPartResult> MergeTreeDeduplicationLog::addPart(const std::vector<std::string> & block_ids, const MergeTreePartInfo & part_info)
{
    std::lock_guard lock(state_mutex);

    /// We support zero case because user may want to disable deduplication with
    /// ALTER MODIFY SETTING query. It's much more simpler to handle zero case
    /// here then destroy whole object, check for null pointer from different
    /// threads and so on.
    if (deduplication_window == 0)
        return {};

    std::vector<MergeTreeDeduplicationLog::AddPartResult> result;

    /// If we already have this block let's deduplicate it
    for (const auto & block_id : block_ids)
    {
        if (deduplication_map.contains(block_id))
        {
            auto info = deduplication_map.get(block_id);
            result.emplace_back(info, block_id);
        }
    }

    if (!result.empty())
        return result;

    if (stopped)
    {
        throw Exception(ErrorCodes::ABORTED, "Storage has been shutdown when we add this part.");
    }

    chassert(current_writer != nullptr);

    /// Writing the ADD records must be all-or-nothing. If anything below throws,
    /// the caller aborts the insert before the part is committed (MergeTreeSink
    /// commits the part only after addPart returns), so a block ID left published
    /// in `deduplication_map` here would wrongly deduplicate - and silently drop -
    /// a client retry of the same insert, even though the original part never
    /// became active. `deduplication_map.insert` is therefore deferred below,
    /// until the durable writes and the rotation both succeeded: it also evicts
    /// the oldest entry once the map is at capacity, and that eviction cannot be
    /// undone by erasing only the block IDs this call published, so mutating the
    /// map on a path that might still fail would silently narrow the
    /// deduplication window for unrelated, already-active parts.
    size_t written = 0;
    /// All ADD records below go to the log that is current right now: no rotation
    /// happens until the rotateAndDropIfNeeded() after the loop. Remember it so the
    /// rollback can undo their retention count even if that rotation (or a failed
    /// write that cancels the writer) has since moved `current_log_number` on.
    const size_t add_log_number = current_log_number;
    try
    {
        for (const auto & block_id : block_ids)
        {
            /// Create new record
            MergeTreeDeduplicationLogRecord record;
            record.operation = MergeTreeDeduplicationOp::ADD;
            record.part_name = part_info.getPartNameAndCheckFormat(format_version);
            record.block_id = block_id;
            /// Write it to disk
            writeRecord(record, *current_writer);
            /// We have one more record in current log
            existing_logs[current_log_number].entries_count++;
            ++written;
        }
        /// Rotate and drop old logs if needed
        rotateAndDropIfNeeded();
    }
    catch (...)
    {
        /// Best effort: write compensating CANCEL records for the block IDs that
        /// were durably written above, so that replaying the log on server startup
        /// does not publish the rolled back block IDs either. A CANCEL cancels the
        /// matching ADD on replay - as opposed to a DROP, which would still replay
        /// the transient ADD first and could evict an unrelated, still-active block
        /// from the bounded in-memory map before erasing the rolled-back one.
        try
        {
            /// If the exception came from `writeRecord` failing to write one of the
            /// ADD records above, `current_writer` is now canceled: `next()` cancels
            /// the buffer on any `nextImpl()` exception, and a canceled buffer
            /// refuses further writes. Rotate to a fresh writer in that case;
            /// `rotate` skips finalizing an already canceled `current_writer` and
            /// always opens a new log file. If the exception came from
            /// `rotateAndDropIfNeeded` instead, `current_writer` is live: either
            /// still the previous writer (opening the new log file failed, and
            /// nothing changed) or a fresh one (the new log file was opened, but
            /// finalizing the previous one failed and `rotate` rethrew that failure
            /// after switching over), so the CANCEL records below go to it directly.
            if (current_writer->isCanceled())
                rotate();

            for (size_t i = 0; i < written; ++i)
            {
                MergeTreeDeduplicationLogRecord record;
                record.operation = MergeTreeDeduplicationOp::CANCEL;
                record.part_name = part_info.getPartNameAndCheckFormat(format_version);
                record.block_id = block_ids[i];
                writeRecord(record, *current_writer);
            }

            /// The rolled-back ADD records above are cancelled by these CANCEL
            /// records and survive neither the in-memory map nor a replay, so they
            /// must not count towards log retention: leaving them (and the CANCEL
            /// records) in `entries_count` would let dropOutdatedLogs treat them as
            /// consumed deduplication-window slots and drop an older log that still
            /// holds live block ids - after which a restart forgets those committed
            /// blocks. Undo the count of the ADD records (added above, always to
            /// `add_log_number`) and do not count the CANCEL records, so the live
            /// accounting matches what a replay of these logs reconstructs. Done
            /// only after the CANCEL records were written, so a failure to persist
            /// them (handled below) leaves the on-disk ADD records still counted.
            existing_logs.at(add_log_number).entries_count -= written;
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__,
                "Cannot write compensating records to the deduplication log after a failed insertion; "
                "a duplicate of this insert may be deduplicated wrongly after a server restart");
        }

        throw;
    }

    /// Everything is durable now; publish into the in-memory map.
    for (const auto & block_id : block_ids)
        deduplication_map.insert(block_id, part_info);

    return {};
}

void MergeTreeDeduplicationLog::dropPart(const MergeTreePartInfo & drop_part_info)
{
    std::lock_guard lock(state_mutex);

    /// We support zero case because user may want to disable deduplication with
    /// ALTER MODIFY SETTING query. It's much more simpler to handle zero case
    /// here then destroy whole object, check for null pointer from different
    /// threads and so on.
    if (deduplication_window == 0)
        return;

    if (stopped)
    {
        throw Exception(ErrorCodes::ABORTED, "Storage has been shutdown when we drop this part.");
    }

    chassert(current_writer != nullptr);

    /// Collect every block id covered by the dropped part before touching the log
    /// or the in-memory map. Writing the DROP records and erasing the block ids
    /// must be all-or-nothing, the same contract as addPart: rotateAndDropIfNeeded
    /// can now rethrow a failure to finalize or fsync the previous log file, and a
    /// plain writeRecord can throw too. Erasing eagerly, one block id at a time,
    /// before that failure-prone boundary (as the code used to) left the in-memory
    /// map in a partial state when the loop was interrupted partway - some block
    /// ids of the dropped part erased, the rest still published - which the caller
    /// (StorageMergeTree::dropPartNoWaitNoThrow) cannot repair: it has already taken
    /// the part out of the active set and never retries the drop.
    std::vector<std::string> block_ids;
    std::vector<std::string> part_names;
    for (const auto & node : deduplication_map)
    {
        /// Part is covered by the dropped part, so it must leave deduplication history.
        if (drop_part_info.contains(node.value))
        {
            block_ids.push_back(node.key);
            part_names.push_back(node.value.getPartNameAndCheckFormat(format_version));
        }
    }

    if (block_ids.empty())
        return;

    /// Write all the DROP records first. If a write or the rotation throws partway,
    /// no block id has been erased yet, so every covered block id stays published in
    /// memory - the deduplicating (safe) direction. `writeRecord` flushes every
    /// record, though, so the records written before the failure may already be
    /// durable, and replaying such a prefix on startup would erase some of the
    /// covered block ids while the live map kept them all. The rollback below
    /// therefore writes a compensating CANCEL record for each DROP record that was
    /// written, mirroring addPart, so a replay reconstructs the same all-or-nothing
    /// state the live map kept.
    size_t written = 0;
    /// All DROP records below go to the log that is current right now: no rotation
    /// happens until the rotateAndDropIfNeeded() after the loop. Remember it so the
    /// rollback can undo their retention count even if a failed write (which cancels
    /// the writer) has since moved `current_log_number` on.
    const size_t drop_log_number = current_log_number;
    try
    {
        for (size_t i = 0; i < block_ids.size(); ++i)
        {
            MergeTreeDeduplicationLogRecord record;
            record.operation = MergeTreeDeduplicationOp::DROP;
            record.part_name = part_names[i];
            record.block_id = block_ids[i];
            /// Write it to disk
            writeRecord(record, *current_writer);
            /// We have one more record on disk
            existing_logs[current_log_number].entries_count++;
            ++written;
        }

        /// Rotate before erasing from the in-memory map: if the rotation rethrows a
        /// failure to finalize or fsync the previous log file, the DROP records just
        /// written may not be durable, so leaving every block id published keeps the
        /// map all-or-nothing and never wrongly forgets a block id whose DROP was lost.
        rotateAndDropIfNeeded();
    }
    catch (...)
    {
        /// Best effort: cancel out the DROP records that were durably written above,
        /// so that a replay on server startup does not erase their block ids either -
        /// they all stayed published in the in-memory map. See the analogous rollback
        /// in addPart for why the writer may need rotating to a fresh one first and
        /// why the cancelled records must not count towards log retention.
        try
        {
            if (current_writer->isCanceled())
                rotate();

            for (size_t i = 0; i < written; ++i)
            {
                MergeTreeDeduplicationLogRecord record;
                record.operation = MergeTreeDeduplicationOp::CANCEL;
                record.part_name = part_names[i];
                record.block_id = block_ids[i];
                writeRecord(record, *current_writer);
            }

            existing_logs.at(drop_log_number).entries_count -= written;
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__,
                "Cannot write compensating records to the deduplication log after a failed drop; "
                "some of the dropped block ids may wrongly stop deduplicating after a server restart");
        }

        throw;
    }

    /// Everything is durable now; erase the dropped block ids from the map.
    for (const auto & block_id : block_ids)
        deduplication_map.erase(block_id);
}

void MergeTreeDeduplicationLog::setDeduplicationWindowSize(size_t deduplication_window_)
{
    std::lock_guard lock(state_mutex);

    if (stopped)
        return;

    deduplication_window = deduplication_window_;
    rotate_interval = deduplication_window * 2;

    /// If settings was set for the first time with ALTER MODIFY SETTING query
    if (deduplication_window != 0 && !disk->existsDirectory(logs_dir))
        disk->createDirectories(logs_dir);

    deduplication_map.setMaxSize(deduplication_window);
    rotateAndDropIfNeeded();

    /// Can happen in case we have unfinished log
    if (!current_writer)
        current_writer = disk->writeFile(existing_logs.rbegin()->second.path, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append);
}


void MergeTreeDeduplicationLog::shutdown()
{
    std::lock_guard lock(state_mutex);
    if (stopped)
        return;

    stopped = true;
    if (current_writer)
    {
        /// `current_writer` can already be canceled here - e.g. after a failed
        /// insert whose rollback could not reopen a fresh writer. `finalize`
        /// disallows calling it on a canceled buffer (it throws a logical error,
        /// which aborts the process in debug and sanitizer builds), so just drop
        /// it in that case: a canceled buffer has nothing left to flush.
        if (current_writer->isCanceled())
        {
            current_writer.reset();
        }
        else
        {
            /// If an error has occurred during finalize, we'd like to have the exception set for reset.
            /// Otherwise, we'll be in a situation when a finalization didn't happen, and we didn't get
            /// any error, causing logical error (see ~MemoryBuffer()).
            try
            {
                current_writer->finalize();
                current_writer.reset();
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
                current_writer->cancel();
                current_writer.reset();
            }
        }
    }
}

MergeTreeDeduplicationLog::~MergeTreeDeduplicationLog()
{
    shutdown();
}

}
