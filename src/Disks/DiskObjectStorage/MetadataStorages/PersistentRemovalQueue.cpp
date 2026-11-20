#include <Disks/DiskObjectStorage/MetadataStorages/PersistentRemovalQueue.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteHelpers.h>

#include <Core/Defines.h>

#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/logger_useful.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int FAULT_INJECTED;
}

namespace FailPoints
{
    extern const char persistent_removal_queue_fail_append[];
}

PersistentRemovalQueue::PersistentRemovalQueue(DiskPtr disk_, const std::string & directory_, size_t compaction_threshold_)
    : disk(std::move(disk_))
    , directory(directory_)
    , log_path(fs::path(directory_) / "blobs_to_remove.log")
    , compaction_threshold(compaction_threshold_)
    , log(getLogger("PersistentRemovalQueue"))
{
}

void PersistentRemovalQueue::load()
{
    if (!disk->existsDirectory(directory))
        disk->createDirectory(directory);

    if (readLog())
        compactLog();

    LOG_INFO(log, "Loaded {} blobs pending removal from {}", queue.size(), log_path);
}

bool PersistentRemovalQueue::readLog()
{
    auto buf = disk->readFileIfExists(log_path, ReadSettings{});
    if (!buf)
        return false;

    bool needs_compaction = false;

    /// Read version header.
    UInt32 version = 0;
    try
    {
        readBinaryLittleEndian(version, *buf);
    }
    catch (...)
    {
        LOG_WARNING(log, "Truncated version header in {}, starting with empty queue: {}", log_path, getCurrentExceptionMessage(false));
        return true;
    }

    if (version > CURRENT_VERSION)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported removal log version: {}", version);

    if (version != CURRENT_VERSION)
        needs_compaction = true;

    while (!buf->eof())
    {
        try
        {
            /// Binary format (V0): entry_type (UInt8) | remote_path (binary string) | local_path (binary string) | bytes_size (UInt64 LE)
            UInt8 entry_type = 0;
            readBinaryLittleEndian(entry_type, *buf);

            StoredObject object;
            readStringBinary(object.remote_path, *buf);
            readStringBinary(object.local_path, *buf);
            readBinaryLittleEndian(object.bytes_size, *buf);

            if (entry_type == EntryType::ADD)
            {
                queue.submitForRemoval({std::move(object)});
            }
            else if (entry_type == EntryType::REMOVED)
            {
                queue.markAsRemoved({object});
                ++stale_entries;
            }
            else
            {
                LOG_WARNING(log, "Skipping unknown entry type '{}' in {}", static_cast<UInt16>(entry_type), log_path);
            }
        }
        catch (...)
        {
            /// Truncated entry from a partial write (e.g. crash mid-append). Stop reading — everything
            /// loaded so far is valid. Compaction will rewrite the file cleanly.
            LOG_WARNING(log, "Truncated entry in {}, stopping: {}", log_path, getCurrentExceptionMessage(false));
            needs_compaction = true;
            break;
        }
    }

    return needs_compaction;
}

void PersistentRemovalQueue::submitForRemoval(const StoredObjects & blobs)
{
    if (blobs.empty())
        return;

    try
    {
        appendToLog(EntryType::ADD, blobs);
    }
    catch (...)
    {
        LOG_ERROR(log, "Failed to persist removal queue entry: {}. "
            "Blobs are tracked in memory but may become orphaned on crash.",
            getCurrentExceptionMessage(false));
    }

    queue.submitForRemoval(blobs);
}

int64_t PersistentRemovalQueue::markAsRemoved(const StoredObjects & blobs)
{
    if (blobs.empty())
        return 0;

    /// Persist REMOVED entries to WAL before erasing from in-memory queue,
    /// so that on WAL failure the blobs remain tracked and will be retried.
    appendToLog(EntryType::REMOVED, blobs);

    /// Erase from in-memory queue before compaction, so that `compactLog`
    /// (which snapshots the queue via `takeFirst(0)`) does not re-add these blobs
    /// as ADD entries in the compacted file — otherwise a restart after compaction
    /// would resurrect them as zombie pending-removal entries.
    auto removed_count = queue.markAsRemoved(blobs);

    stale_entries += blobs.size();
    if (stale_entries >= compaction_threshold)
        compactLog();

    return removed_count;
}

void PersistentRemovalQueue::appendToLog(EntryType entry_type, const StoredObjects & blobs)
{
    if (log_needs_repair)
    {
        compactLog();
        log_needs_repair = false;
    }

    try
    {
        appendToLogImpl(entry_type, blobs);
    }
    catch (...)
    {
        log_needs_repair = true;
        throw;
    }
}

void PersistentRemovalQueue::appendToLogImpl(EntryType entry_type, const StoredObjects & blobs)
{
    bool file_exists = disk->existsFile(log_path);
    auto buf = disk->writeFile(log_path, /* buf_size */ DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append, /* settings */ {});

    fiu_do_on(FailPoints::persistent_removal_queue_fail_append,
    {
        throw Exception(ErrorCodes::FAULT_INJECTED, "persistent_removal_queue_fail_append");
    });

    /// Write version header if this is a new file.
    /// An empty or corrupt file cannot exist here: `load` and a failed append make `compactLog` rewrite it with a valid header.
    if (!file_exists)
    {
        UInt32 version = CURRENT_VERSION;
        writeBinaryLittleEndian(version, *buf);
    }

    for (const auto & blob : blobs)
    {
        UInt8 type = entry_type;
        writeBinaryLittleEndian(type, *buf);
        writeStringBinary(blob.remote_path, *buf);
        writeStringBinary(blob.local_path, *buf);
        writeBinaryLittleEndian(blob.bytes_size, *buf);
    }
    buf->finalize();
    buf->sync();
}

void PersistentRemovalQueue::compactLog()
{
    const auto tmp_path = log_path + ".tmp";

    auto buf = disk->writeFile(tmp_path, /* buf_size */ DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, /* settings */ {});

    UInt32 version = CURRENT_VERSION;
    writeBinaryLittleEndian(version, *buf);

    for (const auto & blob : queue.takeFirst(0))
    {
        UInt8 type = EntryType::ADD;
        writeBinaryLittleEndian(type, *buf);
        writeStringBinary(blob.remote_path, *buf);
        writeStringBinary(blob.local_path, *buf);
        writeBinaryLittleEndian(blob.bytes_size, *buf);
    }
    buf->finalize();
    buf->sync();

    disk->replaceFile(tmp_path, log_path);
    stale_entries = 0;
}

}
