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
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int CORRUPTED_DATA;
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


/// Hash of a (block id, part name) pair, for pairing a CANCEL with the exact
/// DROP generation it undoes in applyRecords.
struct StringViewPairHash
{
    size_t operator()(const std::pair<std::string_view, std::string_view> & pair) const
    {
        return StringViewHash{}(pair.first) * 0x9e3779b97f4a7c15ULL + StringViewHash{}(pair.second);
    }
};

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

/// The unfinished-compaction marker lives in the log directory as the log file with
/// number 0, which no real log can have (rotation and compaction only ever create
/// `current_log_number + 1`, starting from 1). See markUnfinishedCompaction.
std::string getCompactionMarkerPath(const std::string & prefix)
{
    return getLogPath(prefix, 0);
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
        /// Log number 0 is the unfinished-compaction marker, not a log: no real log
        /// ever gets that number (see markUnfinishedCompaction). It must never be
        /// replayed or counted as history; discardHistoryAfterUnfinishedCompaction
        /// below acts on it instead.
        if (log_number == 0)
            continue;
        existing_logs[log_number] = {path, 0};
    }

    /// If the previous run died between starting a compaction and restoring a provably
    /// consistent on-disk state, the files just collected may replay to a wrong
    /// deduplication state; discard them instead of replaying them. Skipped when
    /// deduplication is disabled: nothing is replayed or written then, and the marker
    /// stays for the next load with deduplication enabled to act on.
    if (deduplication_window != 0)
        discardHistoryAfterUnfinishedCompaction();

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

        /// Drop the empty log files a previous run's rotations left at the end of the
        /// history. Without append support every rotation - including the one below in
        /// load - starts a fresh file, and dropOutdatedLogs can never reclaim a
        /// zero-record file that sits after the file holding the live state (a normal
        /// committed file or a compaction snapshot), because retention only drops an
        /// oldest prefix. So without this a restart-only cycle (no new operations) would
        /// leak one empty file per restart and make every future load replay O(number
        /// of restarts) files. With append support the last file is reopened and reused
        /// instead, so no empty files pile up and this is unnecessary.
        if (!disk_supports_writing_with_append)
            removeTrailingEmptyLogs();

        /// Start new log, drop previous
        rotateAndDropIfNeeded();

        /// Can happen in case we have unfinished log
        if (!current_writer)
            current_writer = disk->writeFile(existing_logs.rbegin()->second.path, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append);

        /// A previous run may have left a lot of rolled-back record pairs that
        /// dropOutdatedLogs cannot reclaim; compact them away so replaying this log on
        /// every future restart stays bounded by the deduplication window, not by the
        /// number of past failures.
        compactIfNeeded();
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
    /// record cancels the most recent preceding, not-yet-cancelled record OF THE
    /// EXACT GENERATION IT UNDOES - a cancelled-add DROP undoes an ADD of the
    /// same block id, a CANCEL undoes a real DROP of the same block id AND the
    /// same part name (a CANCEL carries the part name of the DROP it rolls back,
    /// so the exact generation is known; the cancelled-add marker occupies the
    /// part name field with the sentinel, so an ADD can only be matched by block
    /// id - which is sufficient, see below). Matching precisely matters: a
    /// rollback record can survive on disk while the very record it was meant to
    /// undo did not (for example, on the failed-drop + failed-sync path the DROP
    /// that a CANCEL undoes may never have reached durable storage). Pairing
    /// only by block id would then let the rollback record latch onto an older
    /// committed record and cancel it instead - forgetting a still published
    /// block id, resurrecting a dropped one, or, when a block id was reused
    /// across part generations (ADD partA, DROP partA, ADD partB, lost DROP
    /// partB, CANCEL partB), cancelling the older generation's committed DROP so
    /// the replayed map keeps partA where the live map kept partB - after which
    /// dropping partB no longer clears the block id and a legitimate reinsert is
    /// wrongly deduplicated. Restricting each rollback record to its exact
    /// target leaves the unrelated committed record untouched: the surviving
    /// rollback record is simply a stray that reconstructs no state on its own.
    /// Matching an ADD by block id alone is safe because a second ADD of the
    /// same block id can only be written once the live map no longer holds the
    /// block id (the insert would have been deduplicated otherwise), so any
    /// older surviving ADD of that block id is followed by a surviving DROP that
    /// erases it on replay regardless of whether the marker latched onto it.
    /// Under normal operation the
    /// rollback record is always found next to its match, because the rollback
    /// writes its records immediately after the failed batch under the same lock,
    /// with no other operation in between. Dropping both records of a pair means
    /// the transient records never touch the in-memory map on replay: a
    /// rolled-back ADD neither publishes its block id nor consumes a
    /// deduplication-window slot (which could otherwise evict an unrelated, still
    /// active block before the rollback record is seen), and a rolled-back DROP
    /// does not erase a block id that stayed published in the live map. An older
    /// server replays the rollback records themselves with the correct net effect
    /// instead (see MergeTreeDeduplicationOp), so the encoding needs no format
    /// version.
    std::vector<bool> cancelled(records.size(), false);
    std::unordered_map<std::string_view, std::vector<size_t>, StringViewHash> pending_adds;
    std::unordered_map<std::pair<std::string_view, std::string_view>, std::vector<size_t>, StringViewPairHash> pending_drops;
    for (size_t i = 0; i < records.size(); ++i)
    {
        const auto & record = records[i];
        const bool cancels_add = record.operation == MergeTreeDeduplicationOp::DROP
            && record.part_name == DEDUPLICATION_LOG_CANCELLED_ADD_PART_NAME;
        const bool cancels_drop = record.operation == MergeTreeDeduplicationOp::CANCEL;
        if (cancels_add)
        {
            /// The rollback record itself is never replayed. Only ever consume a
            /// pending ADD of the same block id.
            cancelled[i] = true;
            auto it = pending_adds.find(record.block_id);
            if (it != pending_adds.end() && !it->second.empty())
            {
                cancelled[it->second.back()] = true;
                it->second.pop_back();
            }
        }
        else if (cancels_drop)
        {
            /// The rollback record itself is never replayed. Only ever consume a
            /// pending DROP of the same block id and the same part name - the
            /// exact record generation this CANCEL was written to undo.
            cancelled[i] = true;
            auto it = pending_drops.find({record.block_id, record.part_name});
            if (it != pending_drops.end() && !it->second.empty())
            {
                cancelled[it->second.back()] = true;
                it->second.pop_back();
            }
        }
        else if (record.operation == MergeTreeDeduplicationOp::DROP)
        {
            pending_drops[{record.block_id, record.part_name}].push_back(i);
        }
        else
        {
            pending_adds[record.block_id].push_back(i);
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

void MergeTreeDeduplicationLog::removeTrailingEmptyLogs()
{
    /// Remove the zero-record log files that sit at the end of the history, newest
    /// first, stopping at the first file that still holds records (see the call site in
    /// load for why they accumulate without append support and why dropOutdatedLogs
    /// cannot reclaim them). Collect the numbers first, then erase, so the map is not
    /// mutated while it is walked.
    std::vector<size_t> empty_tail;
    for (auto it = existing_logs.rbegin(); it != existing_logs.rend(); ++it)
    {
        if (it->second.entries_count != 0)
            break;
        empty_tail.push_back(it->first);
    }

    for (size_t number : empty_tail)
    {
        auto it = existing_logs.find(number);
        try
        {
            disk->removeFile(it->second.path);
        }
        catch (...)
        {
            /// Best effort: keep this file (and, since we walk newest to oldest, every
            /// older one too) - an empty file replays as no-ops, so leaving it behind is
            /// harmless.
            tryLogCurrentException(__PRETTY_FUNCTION__, "Cannot remove an empty deduplication log file " + it->second.path);
            break;
        }
        existing_logs.erase(it);
    }

    /// Keep current_log_number pointing at a file that still exists (the newest
    /// surviving one), so rotate() numbers the next file correctly and
    /// rotateAndDropIfNeeded never inserts a phantom entry for a removed number through
    /// operator[]. If every file was empty and removed, leave it at the previous
    /// maximum so numbering keeps increasing and never collides with a removed file.
    if (!existing_logs.empty())
        current_log_number = existing_logs.rbegin()->first;
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

    /// A failed compaction can leave an orphan snapshot pending neutralization at
    /// exactly this number (`current_log_number + 1`; see prepareToWrite). The Rewrite
    /// above has truncated it away and the path now belongs to a live log file, so it
    /// must not be neutralized later - that would empty a legitimate log.
    orphan_logs_pending_neutralization.erase(new_path);

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

void MergeTreeDeduplicationLog::compactIfNeeded()
{
    if (deduplication_window == 0)
        return;

    size_t total_raw = 0;
    size_t total_effective = 0;
    for (const auto & [number, description] : existing_logs)
    {
        total_raw += description.entries_count;
        total_effective += description.effective_entries_count;
    }

    /// The gap between the two counts is exactly the records left behind by rolled-back
    /// operations: their ADD/DROP records and the compensating records that cancel them
    /// reconstruct nothing on replay, so they raise the raw count without raising the
    /// effective coverage (in normal operation the two are equal and this is zero).
    /// dropOutdatedLogs cannot reclaim those records - a rollback record cancels a
    /// record in an older file that is still retained for other, live block ids, and
    /// retention only drops an oldest prefix - so under repeated transient write or
    /// rotation failures the retained files, and the records load must replay, would
    /// otherwise grow without bound. Once more than a couple of rotation intervals of
    /// such garbage has piled up, rewrite the live state into a single fresh file;
    /// tolerating some of it first keeps a sporadic failure from triggering a full
    /// rewrite.
    if (total_raw <= total_effective + 2 * rotate_interval)
        return;

    compact();
}

void MergeTreeDeduplicationLog::compact()
{
    /// Snapshot the entire live deduplication state into a single fresh log file and
    /// drop every older file. The in-memory map already holds exactly the records that
    /// survive rollback-pair elimination, so a fresh ADD-per-entry log written in the
    /// map's insertion order replays - evicting in the same order - to the identical
    /// state; it is therefore safe to discard the whole history and start from the
    /// snapshot. This reclaims the cancelled record pairs that dropOutdatedLogs cannot
    /// (see compactIfNeeded).
    ///
    /// Best effort: on any failure the existing files and writer are left untouched, so
    /// the log stays correct and usable and only the space optimization is skipped. The
    /// snapshot is finalized (object storage only makes a file durable on finalize) and
    /// the writer for the next operation prepared before any old file is removed, so a
    /// throw can never leave the live state only in files that are about to be deleted.
    const size_t snapshot_log_number = current_log_number + 1;
    const auto snapshot_path = getLogPath(logs_dir, snapshot_log_number);
    const size_t snapshot_size = deduplication_map.size();

    /// Persist the fact that a compaction is in flight before creating or removing
    /// anything, so the failure barrier survives a restart. The process-local
    /// `orphan_logs_pending_neutralization` dies with the process: without the marker,
    /// a restart while any file this compaction touched is still stale would replay
    /// that file as ordinary history and could silently deduplicate wrongly. With the
    /// marker still active, the next load discards the history instead (see
    /// markUnfinishedCompaction). If the marker cannot be made durable, do not
    /// compact - a half-done compaction without the marker is exactly the unprotected
    /// state it exists to prevent - and lose nothing but the space optimization.
    if (!markUnfinishedCompaction())
        return;

    /// The writer the next operation appends to. On an append-capable disk that is the
    /// finalized snapshot file itself, reopened for appending. On a disk without append
    /// support the snapshot file cannot be reopened that way, so it stays a finalized,
    /// durable file and the next operation starts in its own fresh, empty file - the
    /// same regime rotate() uses on such a disk, where every operation already writes a
    /// new file. Either way the retained history is reduced to the snapshot, so repeated
    /// rolled-back operations can no longer grow the log files - or the load-time replay -
    /// without bound on any disk (previously compaction was skipped entirely without
    /// append support, so on e.g. s3_plain_rewritable the rollback records still piled up).
    const bool reopen_snapshot = disk_supports_writing_with_append;
    const size_t writer_log_number = reopen_snapshot ? snapshot_log_number : snapshot_log_number + 1;
    const auto writer_path = reopen_snapshot ? snapshot_path : getLogPath(logs_dir, writer_log_number);

    std::unique_ptr<WriteBufferFromFileBase> new_writer;
    try
    {
        {
            auto snapshot_writer = disk->writeFile(snapshot_path, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite);
            for (const auto & node : deduplication_map)
            {
                MergeTreeDeduplicationLogRecord record;
                record.operation = MergeTreeDeduplicationOp::ADD;
                record.part_name = node.value.getPartNameAndCheckFormat(format_version);
                record.block_id = node.key;
                writeRecord(record, *snapshot_writer);
            }
            snapshot_writer->finalize();
            snapshot_writer->sync();
        }

        /// Open the writer for the next operation (reopen the snapshot on an append-capable
        /// disk, a fresh file otherwise) and register the snapshot - and, without append
        /// support, that fresh file too - the only remaining steps that can throw, while
        /// the old writer and files are still live, so a failure here changes nothing.
        new_writer = disk->writeFile(
            writer_path, DBMS_DEFAULT_BUFFER_SIZE, reopen_snapshot ? WriteMode::Append : WriteMode::Rewrite);
        existing_logs.emplace(snapshot_log_number, MergeTreeDeduplicationLogNameDescription{snapshot_path, snapshot_size, snapshot_size});
        if (!reopen_snapshot)
            existing_logs.emplace(writer_log_number, MergeTreeDeduplicationLogNameDescription{writer_path, 0, 0});
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__, "Cannot compact the MergeTree deduplication log; keeping the existing log files");
        /// Discard whatever was set up so no orphan file is left for load to read. The
        /// snapshot may already be durable at a HIGHER log number than the older files the
        /// server keeps appending to, so it cannot simply be forgotten: left behind, load
        /// would replay it last - after the older files that by then hold newer committed
        /// block ids - and its stale ADD records would resurrect evicted block ids and
        /// forget committed ones. neutralizeOrphanLog removes it, or, if that fails,
        /// overwrites it with an empty file that replays as a no-op no matter its position.
        if (new_writer)
            new_writer->cancel();
        existing_logs.erase(snapshot_log_number);
        if (!reopen_snapshot)
            existing_logs.erase(writer_log_number);
        /// If a file can neither be removed nor emptied, it must not be forgotten: the
        /// server would keep appending newer committed records to the older, lower-
        /// numbered file, and the next restart would replay the stale higher-numbered
        /// snapshot last, silently forgetting them. Record it and have every subsequent
        /// operation retry the neutralization - failing closed until it succeeds - in
        /// prepareToWrite.
        if (!neutralizeOrphanLog(snapshot_path))
            orphan_logs_pending_neutralization.insert(snapshot_path);
        /// writer_path == snapshot_path when the snapshot is reopened, so only a separate
        /// fresh writer file needs its own cleanup.
        if (!reopen_snapshot && !neutralizeOrphanLog(writer_path))
            orphan_logs_pending_neutralization.insert(writer_path);
        /// The failed compaction is fully rolled back once nothing is pending, so the
        /// history is consistent again and the marker can go. While something IS
        /// pending the marker must stay active: it is what makes a restart discard the
        /// suspect history instead of replaying it. Clearing it is then tied to
        /// draining the pending set in prepareToWrite.
        if (orphan_logs_pending_neutralization.empty())
            clearCompactionMarker();
        return;
    }

    /// Point of no return: the snapshot is durable and registered, and the new writer
    /// is open. Everything below is non-throwing - `cancel` is noexcept, per-file
    /// removal is guarded, and the switch-over is an integer store and a unique_ptr move.
    /// The old writer's records are all captured in the snapshot, so discard it.
    if (current_writer)
        current_writer->cancel();

    for (auto it = existing_logs.begin(); it != existing_logs.end();)
    {
        if (it->first == snapshot_log_number || it->first == writer_log_number)
        {
            ++it;
            continue;
        }
        try
        {
            disk->removeFile(it->second.path);
            it = existing_logs.erase(it);
        }
        catch (...)
        {
            /// Leaving the old file behind is NOT harmless. The snapshot has a higher log
            /// number, so load replays it after the lingering file - which reconstructs the
            /// same SET of block ids as the snapshot but not necessarily their FIFO order,
            /// and that order decides which block is evicted next. Replaying the old file
            /// first and then the snapshot can therefore rebuild the right block ids in the
            /// wrong order, so the next insert after a restart evicts a different committed
            /// block than the live process would. Neutralize the file the same way an orphan
            /// snapshot is handled: overwrite it with an empty log (after retrying the
            /// removal once), which replays as a no-op wherever it sits, so the snapshot
            /// alone determines the reloaded state and its order.
            tryLogCurrentException(
                __PRETTY_FUNCTION__, "Cannot remove an outdated deduplication log file during compaction; will empty it instead");
            /// If it can neither be removed nor emptied, its stale record order stays on
            /// disk, so record it and retry - failing new operations closed until it is
            /// neutralized - in prepareToWrite.
            if (!neutralizeOrphanLog(it->second.path))
                orphan_logs_pending_neutralization.insert(it->second.path);
            it = existing_logs.erase(it);
        }
    }

    current_log_number = writer_log_number;
    current_writer = std::move(new_writer);

    /// The compaction is complete. Unless some old file could not be neutralized - in
    /// which case the marker must survive so a restart discards the suspect history,
    /// and prepareToWrite clears it once the pending set drains - deactivate the
    /// marker; if even that fails, prepareToWrite retries and fails operations closed,
    /// because a restart with an active marker discards everything written after it.
    if (orphan_logs_pending_neutralization.empty())
        clearCompactionMarker();
}

bool MergeTreeDeduplicationLog::neutralizeOrphanLog(const std::string & path)
{
    /// Try to remove the orphan file first (see the header for why leaving a durable,
    /// higher-numbered snapshot behind would corrupt the next replay).
    try
    {
        disk->removeFileIfExists(path);
        return true;
    }
    catch (...)
    {
        tryLogCurrentException(
            __PRETTY_FUNCTION__,
            "Cannot remove an orphan file left by a failed deduplication log compaction: " + path
                + "; will overwrite it with an empty file instead");
    }

    /// Removal failed, so overwrite the file with an empty one: an empty log replays as a
    /// no-op wherever it sits, so it can no longer resurrect evicted or drop committed
    /// block ids on the next restart. Rewrite creates the file afresh, so even a partial
    /// snapshot is truncated away, and finalize makes the emptiness durable on object
    /// storage. If this fails as well the disk is unwritable, so report the failure: the
    /// caller must not carry on as if the on-disk history were consistent, but record the
    /// file as pending and fail closed until a retry neutralizes it (see prepareToWrite).
    try
    {
        auto empty_writer = disk->writeFile(path, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite);
        empty_writer->finalize();
        empty_writer->sync();
        return true;
    }
    catch (...)
    {
        tryLogCurrentException(
            __PRETTY_FUNCTION__,
            "Cannot overwrite an orphan file left by a failed deduplication log compaction with an empty file: " + path
                + "; a restart may replay stale records from it");
        return false;
    }
}

bool MergeTreeDeduplicationLog::markUnfinishedCompaction()
{
    try
    {
        auto out = disk->writeFile(getCompactionMarkerPath(logs_dir), DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite);
        /// The marker's one record is a no-op on every server version, so a server
        /// from before the marker existed replays this file as an ordinary, harmless
        /// log: a DROP (carrying the rollback part-name marker, which such a server
        /// never parses) of a block id that can never exist erases nothing.
        MergeTreeDeduplicationLogRecord record;
        record.operation = MergeTreeDeduplicationOp::DROP;
        record.part_name = DEDUPLICATION_LOG_CANCELLED_ADD_PART_NAME;
        record.block_id = "unfinished_compaction";
        writeRecord(record, *out);
        out->finalize();
        out->sync();
        return true;
    }
    catch (...)
    {
        tryLogCurrentException(
            __PRETTY_FUNCTION__, "Cannot persist the unfinished-compaction marker of the deduplication log; skipping the compaction");
        /// The failed write may still have left a non-empty file behind, and a
        /// non-empty marker reads as active - a restart would discard the history.
        /// Clear it (or, if that fails too, keep failing operations closed until a
        /// retry clears it in prepareToWrite).
        clearCompactionMarker();
        return false;
    }
}

bool MergeTreeDeduplicationLog::clearCompactionMarker()
{
    /// Removing the file - or, failing that, truncating it to an empty one - both
    /// deactivate the marker: only an existing, non-empty marker is active.
    if (neutralizeOrphanLog(getCompactionMarkerPath(logs_dir)))
    {
        compaction_marker_pending_clear = false;
        return true;
    }

    compaction_marker_pending_clear = true;
    return false;
}

void MergeTreeDeduplicationLog::discardHistoryAfterUnfinishedCompaction()
{
    const auto marker_path = getCompactionMarkerPath(logs_dir);
    if (!disk->existsFile(marker_path))
        return;

    if (disk->getFileSize(marker_path) == 0)
    {
        /// The marker was already deactivated by overwriting it with an empty file
        /// (its removal had failed); try to reclaim the leftover, but an empty marker
        /// is inactive either way.
        try
        {
            disk->removeFileIfExists(marker_path);
        }
        catch (...) /// NOLINT(bugprone-empty-catch): Ok, see above - an empty marker is already inactive.
        {
        }
        return;
    }

    LOG_WARNING(
        getLogger("MergeTreeDeduplicationLog"),
        "The previous run left an active unfinished-compaction marker ({}), so the deduplication log files may replay to an "
        "inconsistent state. Discarding the deduplication history: recent duplicate inserts may be accepted again, but no insert "
        "will be deduplicated wrongly against stale history.",
        marker_path);

    for (auto it = existing_logs.begin(); it != existing_logs.end();)
    {
        if (!neutralizeOrphanLog(it->second.path))
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "An unfinished deduplication log compaction left the history inconsistent, and the log file {} can neither be "
                "removed nor emptied; refusing to load the deduplication log because replaying it could deduplicate wrongly",
                it->second.path);

        /// The file is no longer a hazard: gone, or emptied in place. An emptied file
        /// stays registered and replays as a no-op.
        if (disk->existsFile(it->second.path))
        {
            it->second.entries_count = 0;
            it->second.effective_entries_count = 0;
            ++it;
        }
        else
            it = existing_logs.erase(it);
    }

    if (!clearCompactionMarker())
        throw Exception(
            ErrorCodes::CORRUPTED_DATA,
            "The unfinished-compaction marker of the deduplication log ({}) can neither be removed nor emptied; refusing to load "
            "the deduplication log because every record written now would be discarded by the next restart",
            marker_path);
}

void MergeTreeDeduplicationLog::prepareToWrite()
{
    /// Retry neutralizing the files a failed compaction left behind (see the header).
    /// While one remains on disk with its stale content intact, a restart would replay
    /// it - after or before files holding the live state, either way reconstructing
    /// wrong deduplication history - so refuse to write any new record until the disk
    /// recovers far enough to neutralize them all: failing the operation loudly here is
    /// recoverable (the caller retries), silently deduplicating wrongly after a restart
    /// is not.
    bool marker_still_active = compaction_marker_pending_clear;
    for (auto it = orphan_logs_pending_neutralization.begin(); it != orphan_logs_pending_neutralization.end();)
    {
        if (!neutralizeOrphanLog(*it))
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Deduplication log contains a stale file {} left by a failed compaction that can neither be removed nor emptied; "
                "refusing to write new records because a restart would replay inconsistent deduplication history",
                *it);
        it = orphan_logs_pending_neutralization.erase(it);
        /// While files were pending, the failed compaction's on-disk marker was kept
        /// active on purpose - a restart had to discard the suspect history. Now that
        /// they are all neutralized the history is consistent again, so the marker
        /// must be cleared below, or a restart would still throw the history away.
        marker_still_active = true;
    }

    if (marker_still_active && !clearCompactionMarker())
        throw Exception(
            ErrorCodes::CORRUPTED_DATA,
            "The unfinished-compaction marker of the deduplication log ({}) can neither be removed nor emptied; refusing to write "
            "new records because a restart would discard them together with the rest of the history",
            getCompactionMarkerPath(logs_dir));

    /// Heal a canceled writer. A failed write cancels the buffer; the rollback of that
    /// failed operation rotates to a fresh writer, but the rotation can itself fail and
    /// leave the canceled writer in place - and writes to a canceled buffer throw. Rotate
    /// up front, before this operation writes anything, so the first retry after the
    /// disk recovers succeeds instead of failing once more just to heal the writer.
    if (current_writer && current_writer->isCanceled())
        rotate();
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

    /// Repair any damage a previously failed operation or compaction left behind -
    /// a canceled writer, or a stale file that still needs neutralizing - before
    /// writing anything, so this operation either starts from a consistent state or
    /// fails cleanly here, with nothing written, and can be retried.
    prepareToWrite();

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

                /// The ADD record this cancels (written above, always to
                /// `add_log_number`) no longer survives a replay: the (ADD, DROP-marker)
                /// pair is elided. So the ADD must stop counting towards that log's
                /// effective coverage - otherwise dropOutdatedLogs treats it as a
                /// consumed deduplication-window slot and can drop an older log that
                /// still holds live block ids, after which a restart forgets those
                /// committed blocks. Decrement once per successfully written compensating
                /// record, right after it is durable, rather than once after the whole
                /// loop: if a later writeRecord throws, only the ADDs whose compensating
                /// record did reach disk are discounted, which is exactly what a replay
                /// of the partially written rollback stream reconstructs. A single
                /// post-loop decrement would instead discount every ADD even when only a
                /// prefix of the compensating records was persisted, inflating
                /// retention's view of the surviving coverage.
                --existing_logs.at(add_log_number).effective_entries_count;
            }
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

    /// Reclaim the record pairs left behind by any rolled-back operations once enough
    /// of them have piled up (best effort, never throws), so a burst of transient
    /// failures cannot grow the retained log without bound.
    compactIfNeeded();

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

    /// See addPart: heal a canceled writer and retry any pending neutralization before
    /// writing anything, so the drop either starts from a consistent state or fails
    /// cleanly with nothing written.
    prepareToWrite();

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

                /// The DROP record this CANCEL cancels (written above, always to
                /// `drop_log_number`) no longer survives a replay, so decrement its
                /// effective coverage once per successfully written CANCEL - right after
                /// it is durable - so a mid-loop failure discounts only the DROPs whose
                /// CANCEL reached disk, matching what a replay of the partially written
                /// rollback reconstructs (see the analogous rollback in addPart).
                --existing_logs.at(drop_log_number).effective_entries_count;
            }
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

    /// Reclaim the record pairs left behind by any rolled-back operations once enough
    /// of them have piled up (best effort, never throws).
    compactIfNeeded();
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
