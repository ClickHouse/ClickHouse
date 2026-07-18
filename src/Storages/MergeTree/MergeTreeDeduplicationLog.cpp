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
        /// replay them together: a rollback record can refer to an ADD or DROP in
        /// an earlier log file, so the record pairs of rolled-back operations can
        /// only be cancelled out once the whole history is known.
        /// `record_log_numbers` keeps each record's originating log number so
        /// `applyRecords` can recompute the per-file record counts - the raw
        /// `entries_count` from every record and `effective_entries_count` from only
        /// the surviving ones.
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
        /// Kept in lockstep with `records` so `applyRecords`, which indexes the two
        /// in parallel, can attribute every record back to this log file. The two
        /// appends must be atomic: if this second one throws (e.g. std::bad_alloc
        /// while loading a large log) after the first succeeded, the vectors would
        /// go out of sync and `applyRecords` would read past the end of
        /// `record_log_numbers`. `load` catches the exception and still replays what
        /// was read, so undo the just-pushed record here to keep the sizes equal.
        /// `pop_back` on a non-empty vector never throws.
        try
        {
            record_log_numbers.push_back(log_number);
        }
        catch (...)
        {
            records.pop_back();
            throw;
        }
    }
}

void MergeTreeDeduplicationLog::applyRecords(
    const std::vector<MergeTreeDeduplicationLogRecord> & records,
    const std::vector<size_t> & record_log_numbers)
{
    /// First, cancel out the record pairs left behind by operations that failed
    /// and rolled back: (ADD, DROP with the reserved cancelled-add part name) for
    /// a failed insert and (DROP, CANCEL) for a failed part drop. Each rollback
    /// record cancels the most recent preceding, not-yet-cancelled ADD or DROP of
    /// the same block id - which is exactly the record the failed operation
    /// wrote, because the rollback writes its records immediately after the
    /// failed batch under the same lock, with no other operation in between.
    /// Dropping both records means the transient record never touches the
    /// in-memory map on replay: a rolled-back ADD neither publishes its block id
    /// nor consumes a deduplication-window slot (which could otherwise evict an
    /// unrelated, still-active block before the rollback record is seen), and a
    /// rolled-back DROP does not erase a block id that stayed published in the
    /// live map. An older server replays the rollback records themselves with
    /// the correct net effect instead (see MergeTreeDeduplicationOp), so the
    /// encoding needs no format version.
    std::vector<bool> cancelled(records.size(), false);
    std::unordered_map<std::string_view, std::vector<size_t>, StringViewHash> pending_indices;
    for (size_t i = 0; i < records.size(); ++i)
    {
        const auto & record = records[i];
        const bool is_rollback = record.operation == MergeTreeDeduplicationOp::CANCEL
            || (record.operation == MergeTreeDeduplicationOp::DROP && record.part_name == DEDUPLICATION_LOG_CANCELLED_ADD_PART_NAME);
        if (is_rollback)
        {
            /// The rollback record itself is never replayed.
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

    /// Recompute each log's two counts from the records just read. The raw
    /// `entries_count` counts every record: it drives rotation and compaction, so a
    /// log full of rolled-back pairs still rotates once it reaches the raw threshold
    /// instead of growing without bound. The `effective_entries_count` counts only
    /// the records that survive cancel-pair elimination: `dropOutdatedLogs` sums it
    /// from the newest log backwards to decide which older logs are redundant, and a
    /// cancelled pair contributes nothing to the reconstructed map, so counting its
    /// raw records there would let a failed multi-block operation inflate the sums
    /// and wrongly drop an older log that still holds live block ids - after which a
    /// restart forgets those committed blocks. Splitting the two keeps retention in
    /// step with what a replay reconstructs while rotation stays bounded by the
    /// physical log size.
    for (auto & log : existing_logs)
    {
        log.second.entries_count = 0;
        log.second.effective_entries_count = 0;
    }
    for (size_t i = 0; i < records.size(); ++i)
    {
        auto & description = existing_logs.at(record_log_numbers[i]);
        ++description.entries_count;
        if (!cancelled[i])
            ++description.effective_entries_count;
    }

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

    /// Register the new log file before finalizing the old writer. This is the only
    /// remaining bookkeeping step that can throw - `std::map::emplace` allocates a
    /// node - and it must not run after the old writer has been finalized: if it did
    /// and threw, `current_writer` would still point at the finalized old writer, and
    /// the next `writeRecord` would abort with the very "Cannot write to finalized
    /// buffer" logical error this change eliminates (and one no rollback path can
    /// detect, since the buffer is finalized, not canceled). Doing it here, while the
    /// old writer is still live, keeps `rotate` all-or-nothing: a throw leaves the log
    /// fully usable and the operation retryable, because `emplace` has no effect when
    /// it throws, so the switch-over below happens either in full or not at all.
    existing_logs.emplace(new_log_number, MergeTreeDeduplicationLogNameDescription{new_path, 0});

    /// The new writer is ready and registered; now finalize the previous one and
    /// switch over. `current_writer` can already be canceled here - e.g. `addPart`
    /// rolling back a failed insert calls `rotate` after a failed `writeRecord` left
    /// it canceled. `finalize` disallows calling it on a canceled buffer (it throws a
    /// logical error, which aborts the process in debug and sanitizer builds), so skip
    /// it in that case: a canceled buffer has nothing left to flush or sync anyway.
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

    /// Switch over to the new writer. Both statements are non-throwing (an integer
    /// store and a unique_ptr move that destroys the old, already finalized-or-canceled
    /// writer), so once the bookkeeping above has succeeded the switch always completes.
    current_log_number = new_log_number;
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
        /// Retention is decided by the effective (surviving-record) coverage, not the
        /// raw record count: the cancelled pairs of a rolled-back operation reconstruct
        /// nothing on replay, so counting them here could drop an older log that still
        /// holds committed block ids. Rotation, in contrast, uses the raw count.
        current_sum += description.effective_entries_count;
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
    /// Rotation uses the raw `entries_count` (every physical record, including
    /// rollback records), so a log dominated by rolled-back pairs - whose effective
    /// coverage is zero - still rotates once it reaches the threshold rather than
    /// growing without bound.
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

    /// Adding a part must be all-or-nothing: both the durable ADD records and the
    /// in-memory publication either all take effect or none do. If anything below
    /// throws, the caller aborts the insert before the part is committed
    /// (MergeTreeSink commits the part only after addPart returns), so a block ID
    /// left published in `deduplication_map` here would wrongly deduplicate - and
    /// silently drop - a client retry of the same insert, even though the part never
    /// became active.
    ///
    /// Publication is split so that nothing which can throw runs after the records are
    /// durable. The block IDs are inserted into the map up front but WITHOUT evicting
    /// the oldest entries (`insertWithoutEviction`): that insertion is the only part
    /// of publishing that allocates - and so can throw - so doing it before the
    /// durable writes means a failure aborts with nothing on disk and a rollback that
    /// only has to `erase` what it published, which never allocates (so it cannot
    /// throw) and never drops an unrelated, still-active block ID (nothing was
    /// evicted). Once the writes and the rotation have both succeeded, `trimToMaxSize`
    /// enforces the deduplication window; it only pops the oldest entries, so it
    /// cannot throw at a point where the insert could no longer be rolled back.
    size_t published = 0;
    size_t written = 0;
    /// All ADD records below go to the log that is current right now: no rotation
    /// happens until the rotateAndDropIfNeeded() after the loop. Remember it so the
    /// rollback can undo their retention count even if that rotation (or a failed
    /// write that cancels the writer) has since moved `current_log_number` on.
    const size_t add_log_number = current_log_number;
    try
    {
        /// Publish into the in-memory map first, without eviction (see above).
        for (const auto & block_id : block_ids)
        {
            deduplication_map.insertWithoutEviction(block_id, part_info);
            ++published;
        }

        for (const auto & block_id : block_ids)
        {
            /// Create new record
            MergeTreeDeduplicationLogRecord record;
            record.operation = MergeTreeDeduplicationOp::ADD;
            record.part_name = part_info.getPartNameAndCheckFormat(format_version);
            record.block_id = block_id;
            /// Write it to disk
            writeRecord(record, *current_writer);
            /// One more record physically in the current log (raw growth) and, unless
            /// this insert rolls back below, one more record that survives a replay.
            ++existing_logs[current_log_number].entries_count;
            ++existing_logs[current_log_number].effective_entries_count;
            ++written;
        }
        /// Rotate and drop old logs if needed
        rotateAndDropIfNeeded();
    }
    catch (...)
    {
        /// Undo the in-memory publication. `erase` never allocates, so this cannot
        /// throw, and because the entries were inserted without eviction it restores
        /// the map exactly - it never drops an unrelated, still-active block ID.
        for (size_t i = 0; i < published; ++i)
            deduplication_map.erase(block_ids[i]);

        /// Best effort: write compensating records for the block IDs that were
        /// durably written above, so that replaying the log on server startup does
        /// not publish the rolled back block IDs either. The compensation is a
        /// DROP record carrying the reserved cancelled-add part name: replay
        /// recognizes the marker and cancels the (ADD, DROP) pair out entirely -
        /// a plain DROP would still replay the transient ADD first and could evict
        /// an unrelated, still-active block from the bounded in-memory map before
        /// erasing the rolled-back one - while an older server, which knows no
        /// marker, still replays the record as the erase that unpublishes the
        /// never-committed block id, keeping a downgrade safe.
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
            /// after switching over), so the compensating records below go to it
            /// directly.
            if (current_writer->isCanceled())
                rotate();

            for (size_t i = 0; i < written; ++i)
            {
                MergeTreeDeduplicationLogRecord record;
                record.operation = MergeTreeDeduplicationOp::DROP;
                record.part_name = DEDUPLICATION_LOG_CANCELLED_ADD_PART_NAME;
                record.block_id = block_ids[i];
                writeRecord(record, *current_writer);
                /// The compensating record is physically on disk, so it counts towards
                /// raw log growth (in whatever log `current_writer` points at now -
                /// possibly a fresh one, if the writer was canceled and rotated above),
                /// which keeps rotation honest even for a log full of rolled-back pairs.
                /// It never survives a replay, so it adds nothing to effective coverage.
                ++existing_logs[current_log_number].entries_count;
            }

            /// The rolled-back ADD records above are cancelled by these rollback
            /// records and survive neither the in-memory map nor a replay, so they
            /// must not count towards log retention: leaving them in
            /// `effective_entries_count` would let dropOutdatedLogs treat them as
            /// consumed deduplication-window slots and drop an older log that still
            /// holds live block ids - after which a restart forgets those committed
            /// blocks. Undo only the effective count of the ADD records (added above,
            /// always to `add_log_number`); their raw count stays, so retention
            /// shrinks while rotation still accounts for the physical growth. Done
            /// only after the rollback records were written, so a failure to persist
            /// them (handled below) leaves the ADDs counted as surviving - matching
            /// what a replay of just their records would then reconstruct.
            existing_logs.at(add_log_number).effective_entries_count -= written;
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__,
                "Cannot write compensating records to the deduplication log after a failed insertion; "
                "a duplicate of this insert may be deduplicated wrongly after a server restart");
        }

        throw;
    }

    /// Everything is durable now; enforce the deduplication window. Trimming only
    /// pops the oldest entries, so - unlike a plain insert, which allocates - it
    /// cannot throw here, where the durably recorded insert could no longer be rolled
    /// back.
    deduplication_map.trimToMaxSize();

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
    /// state the live map kept. (The CANCEL carries the real part name, so an older
    /// server replays it as the insert that restores the block id - the same net
    /// effect; see MergeTreeDeduplicationOp.)
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
            /// One more record physically on disk (raw growth) and, unless this drop
            /// rolls back below, one more record that survives a replay.
            ++existing_logs[current_log_number].entries_count;
            ++existing_logs[current_log_number].effective_entries_count;
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
                /// Counts towards raw log growth (see the analogous rollback in
                /// addPart), but never survives a replay, so not towards effective
                /// coverage.
                ++existing_logs[current_log_number].entries_count;
            }

            existing_logs.at(drop_log_number).effective_entries_count -= written;
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
