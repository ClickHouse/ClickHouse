#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>

#include <Disks/IDiskTransaction.h>
#include <Disks/SingleDiskVolume.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <IO/PackedFilesReader.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadPipeline.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/MergeTreeIndicesSerialization.h>
#include <Common/FailPoint.h>
#include <Common/typeid_cast.h>

#include <set>
#include <vector>

namespace DB
{

namespace FailPoints
{
    extern const char part_storage_fail_commit_transaction[];
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int FAULT_INJECTED;
}

DataPartStorageOnDiskFull::DataPartStorageOnDiskFull(VolumePtr volume_, std::string root_path_, std::string part_dir_)
    : DataPartStorageOnDiskBase(std::move(volume_), std::move(root_path_), std::move(part_dir_))
{
}

DataPartStorageOnDiskFull::DataPartStorageOnDiskFull(
    VolumePtr volume_, std::string root_path_, std::string part_dir_, DiskTransactionPtr transaction_)
    : DataPartStorageOnDiskBase(std::move(volume_), std::move(root_path_), std::move(part_dir_), std::move(transaction_))
{
}

MutableDataPartStoragePtr DataPartStorageOnDiskFull::create(
    VolumePtr volume_, std::string root_path_, std::string part_dir_, bool /*initialize_*/) const
{
    return std::make_shared<DataPartStorageOnDiskFull>(std::move(volume_), std::move(root_path_), std::move(part_dir_));
}

MutableDataPartStoragePtr DataPartStorageOnDiskFull::getProjection(const std::string & name, bool use_parent_transaction) // NOLINT
{
    /// Not arena-scoped: most callers use this only as a short-lived filesystem handle (CHECK TABLE,
    /// mutation hardlink/copy, existence probes). The part-lifetime projection storage is created via
    /// `getProjectionPartBuilder`, which scopes the arena itself.
    return std::shared_ptr<DataPartStorageOnDiskFull>(new DataPartStorageOnDiskFull(volume, std::string(fs::path(root_path) / part_dir), name, use_parent_transaction ? transaction : nullptr));
}

DataPartStoragePtr DataPartStorageOnDiskFull::getProjection(const std::string & name) const
{
    return std::make_shared<DataPartStorageOnDiskFull>(volume, std::string(fs::path(root_path) / part_dir), name);
}

bool DataPartStorageOnDiskFull::exists() const
{
    auto path = fs::path(root_path) / part_dir;
    /// CA read-your-writes: a part dir being assembled by this transaction (e.g. a carried-forward
    /// projection dir staged into the open whole-part txn) is not on committed metadata yet. Mirrors
    /// existsDirectory at directory granularity for the part's OWN directory.
    if (transaction && transaction->hasInFlightDirectory(path))
        return true;
    return volume->getDisk()->existsDirectory(path);
}

bool DataPartStorageOnDiskFull::existsFile(const std::string & name) const
{
    auto path = fs::path(root_path) / part_dir / name;
    /// B59: a part still being assembled by this transaction can have staged-but-uncommitted files
    /// (e.g. projection temp blocks on a content-addressed disk). Consult the held transaction first.
    if (transaction && transaction->tryGetInFlightFileSize(path).has_value())
        return true;
    if (looksLikePackedSkipIndexFile(name))
    {
        if (auto reader = getSkipIndicesPackedReader(); reader && reader->exists(name))
            return true;
    }
    return volume->getDisk()->existsFile(path);
}

bool DataPartStorageOnDiskFull::existsDirectory(const std::string & name) const
{
    auto path = fs::path(root_path) / part_dir / name;
    /// CA read-your-writes: a part still being assembled by this transaction can have a staged-but-uncommitted
    /// directory (e.g. a carried-forward projection hardlinked into the open whole-part txn) that committed
    /// metadata cannot see yet. Mirrors existsFile (B59) at directory granularity.
    if (transaction && transaction->hasInFlightDirectory(path))
        return true;
    return volume->getDisk()->existsDirectory(path);
}

class DataPartStorageIteratorOnDisk final : public IDataPartStorageIterator
{
public:
    DataPartStorageIteratorOnDisk(DiskPtr disk_, DirectoryIteratorPtr it_)
        : disk(std::move(disk_)), it(std::move(it_))
    {
    }

    void next() override { it->next(); }
    bool isValid() const override { return it->isValid(); }
    bool isFile() const override { return isValid() && disk->existsFile(it->path()); }
    std::string name() const override { return it->name(); }
    std::string path() const override { return it->path(); }

private:
    DiskPtr disk;
    DirectoryIteratorPtr it;
};

/// CA read-your-writes directory enumeration: a merged view of the committed disk entries PLUS the
/// immediate children this transaction has STAGED under the part dir (deduplicated). Used so
/// loadProjections' withPartFormatFromDisk can iterate a staged-but-uncommitted projection directory and
/// find its mark file. Mirrors existsFile/existsDirectory (B59) at the enumeration level; the committed
/// entries dominate (a name present both on disk and staged appears once).
class DataPartStorageMergedIterator final : public IDataPartStorageIterator
{
public:
    DataPartStorageMergedIterator(DiskPtr disk_, std::string dir_path_, std::vector<std::string> names_)
        : disk(std::move(disk_)), dir_path(std::move(dir_path_)), names(std::move(names_))
    {
    }

    void next() override { ++pos; }
    bool isValid() const override { return pos < names.size(); }
    std::string name() const override { return names[pos]; }
    std::string path() const override { return fs::path(dir_path) / names[pos]; }
    bool isFile() const override { return isValid() && disk->existsFile(path()); }

private:
    DiskPtr disk;
    std::string dir_path;
    std::vector<std::string> names;
    size_t pos = 0;
};

DataPartStorageIteratorPtr DataPartStorageOnDiskFull::iterate() const
{
    auto dir_path = fs::path(root_path) / part_dir;
    if (transaction)
    {
        if (auto staged = transaction->listInFlightDirectory(dir_path); !staged.empty())
        {
            /// Union the committed entries with the staged children (set semantics, committed dominates).
            std::set<std::string> names(staged.begin(), staged.end());
            if (volume->getDisk()->existsDirectory(dir_path))
                for (auto it = volume->getDisk()->iterateDirectory(dir_path); it->isValid(); it->next())
                    names.insert(it->name());
            return std::make_unique<DataPartStorageMergedIterator>(
                volume->getDisk(), dir_path, std::vector<std::string>(names.begin(), names.end()));
        }
    }

    return std::make_unique<DataPartStorageIteratorOnDisk>(
        volume->getDisk(),
        volume->getDisk()->iterateDirectory(dir_path));
}

Poco::Timestamp DataPartStorageOnDiskFull::getFileLastModified(const String & file_name) const
{
    return volume->getDisk()->getLastModified(fs::path(root_path) / part_dir / file_name);
}

size_t DataPartStorageOnDiskFull::getFileSize(const String & file_name) const
{
    auto path = fs::path(root_path) / part_dir / file_name;
    /// B59: see existsFile — the merge stats the staged temp files before reading them back.
    if (transaction)
        if (auto size = transaction->tryGetInFlightFileSize(path))
            return *size;
    if (looksLikePackedSkipIndexFile(file_name))
    {
        if (auto reader = getSkipIndicesPackedReader(); reader && reader->exists(file_name))
            return reader->getFileSize(file_name);
    }
    return volume->getDisk()->getFileSize(path);
}

UInt32 DataPartStorageOnDiskFull::getRefCount(const String & file_name) const
{
    return volume->getDisk()->getRefCount(fs::path(root_path) / part_dir / file_name);
}

std::vector<std::string> DataPartStorageOnDiskFull::getRemotePaths(const std::string & file_name) const
{
    const std::string path = fs::path(root_path) / part_dir / file_name;

    /// B59: a file staged by this transaction resolves to its already-uploaded blob object(s) before commit.
    /// A mutable per-part file intentionally does NOT resolve here (tryGetInFlightStorageObjects returns
    /// nullopt → falls through): it has no blob object and must be read via tryReadFileInFlight. The merge
    /// reads projection column blocks (blob-backed) through this path, not mutable files.
    StoredObjects objects;
    if (transaction)
        if (auto inflight = transaction->tryGetInFlightStorageObjects(path))
            objects = std::move(*inflight);
    if (objects.empty())
        objects = volume->getDisk()->getStorageObjects(path);

    std::vector<std::string> remote_paths;
    remote_paths.reserve(objects.size());

    for (const auto & object : objects)
        remote_paths.push_back(object.remote_path);

    return remote_paths;
}

String DataPartStorageOnDiskFull::getUniqueId() const
{
    auto disk = volume->getDisk();
    if (!disk->supportZeroCopyReplication())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Disk {} doesn't support zero-copy replication", disk->getName());

    return disk->getUniqueId(fs::path(getRelativePath()) / "checksums.txt");
}

void DataPartStorageOnDiskFull::prepareRead(
    const std::string & name,
    const ReadSettings & settings,
    std::optional<size_t> read_hint,
    ReadPipeline & pipeline) const
{
    auto path = fs::path(root_path) / part_dir / name;

    /// B59: read-your-writes for a part still being assembled by this transaction. A projection
    /// spill-and-merge reads its own temp blocks back before the parent part's single commit; on a
    /// content-addressed disk those files are staged in the transaction (blob uploaded, no ref yet),
    /// so the committed metadata path can't see them. If the held transaction resolves the file
    /// in-flight, serve it via a custom pipeline source that reads through the transaction. Gated on
    /// `transaction != nullptr` so committed-part reads (no open transaction) are unchanged.
    if (transaction)
    {
        StoredObjects inflight_objects;
        if (auto objs = transaction->tryGetInFlightStorageObjects(path))
            inflight_objects = std::move(*objs);
        else if (auto size = transaction->tryGetInFlightFileSize(path))
            /// Mutable per-part file staged inline (no blob object); synthesize a placeholder so the
            /// single-object pipeline is satisfied — the custom creator below ignores it and reads the
            /// inline bytes through the transaction.
            inflight_objects = StoredObjects{StoredObject(path, path, *size)};

        if (!inflight_objects.empty())
        {
            /// Safe to capture the raw transaction pointer: no cache/gather/async stage is added on this
            /// branch, so the custom source is consumed synchronously inside build() during this read and
            /// the pointer is never retained past it.
            auto * tx = transaction.get();
            pipeline.setSource(
                [tx, path](const StoredObject &, const ReadSettings & read_settings, bool /*use_external_buffer*/, bool /*restrict_seek*/)
                {
                    return tx->tryReadFileInFlight(path, read_settings, std::nullopt);
                },
                std::move(inflight_objects),
                settings);
            return;
        }
    }

    if (looksLikePackedSkipIndexFile(name))
    {
        if (auto reader = getSkipIndicesPackedReader(); reader && reader->exists(name))
        {
            /// Packed substreams skip the disk's normal pipeline (filesystem cache,
            /// async prefetch, etc.) and read through PackedFilesReader::readFile, which
            /// opens the archive via the underlying disk and wraps the result with
            /// ReadBufferFromFileView at the right offset. The archive's current location is
            /// captured here and passed in, so the reader holds no path of its own.
            auto disk = volume->getDisk();
            String archive_path = fs::path(root_path) / part_dir / String(SKIP_INDICES_PACKED_FILENAME);
            ReadPipeline::BufferCreator creator =
                [reader, disk, archive_path, name, read_hint](const StoredObject &, const ReadSettings & s, bool, bool)
                {
                    return reader->readFile(disk, archive_path, name, s, read_hint);
                };
            pipeline.setSource(std::move(creator), StoredObjects{StoredObject{}}, settings);
            return;
        }
    }

    volume->getDisk()->prepareRead(path, settings, read_hint, pipeline);
}

std::unique_ptr<ReadBufferFromFileBase> DataPartStorageOnDiskFull::readFileIfExists(
    const std::string & name,
    const ReadSettings & settings,
    std::optional<size_t> read_hint) const
{
    auto path = fs::path(root_path) / part_dir / name;
    /// B59: serve a file staged by this transaction (uploaded blob or inline mutable bytes) before commit.
    /// This direct delegate bypasses prepareRead, so the in-flight guard must be repeated here; it is the
    /// only path that reaches the inline-mutable case via a returned buffer.
    if (transaction)
        if (auto rb = transaction->tryReadFileInFlight(path, settings, read_hint))
            return rb;
    if (looksLikePackedSkipIndexFile(name))
    {
        if (auto reader = getSkipIndicesPackedReader(); reader && reader->exists(name))
            return reader->readFile(
                volume->getDisk(),
                fs::path(root_path) / part_dir / String(SKIP_INDICES_PACKED_FILENAME),
                name, settings, read_hint);
    }
    return volume->getDisk()->readFileIfExists(path, settings, read_hint);
}

std::unique_ptr<WriteBufferFromFileBase> DataPartStorageOnDiskFull::writeFile(
    const String & name,
    size_t buf_size,
    WriteMode mode,
    const WriteSettings & settings)
{
    if (transaction)
        return transaction->writeFile(fs::path(root_path) / part_dir / name, buf_size, mode, settings);
    return volume->getDisk()->writeFile(fs::path(root_path) / part_dir / name, buf_size, mode, settings);
}

void DataPartStorageOnDiskFull::createFile(const String & name)
{
    executeWriteOperation([&](auto & disk) { disk.createFile(fs::path(root_path) / part_dir / name); });
}

void DataPartStorageOnDiskFull::moveFile(const String & from_name, const String & to_name)
{
    executeWriteOperation([&](auto & disk)
    {
        auto relative_path = fs::path(root_path) / part_dir;
        disk.moveFile(relative_path / from_name, relative_path / to_name);
    });
}

void DataPartStorageOnDiskFull::replaceFile(const String & from_name, const String & to_name)
{
    executeWriteOperation([&](auto & disk)
    {
        auto relative_path = fs::path(root_path) / part_dir;
        disk.replaceFile(relative_path / from_name, relative_path / to_name);
    });
}

void DataPartStorageOnDiskFull::removeFile(const String & name)
{
    executeWriteOperation([&](auto & disk) { disk.removeFile(fs::path(root_path) / part_dir / name); });
}

void DataPartStorageOnDiskFull::removeFileIfExists(const String & name)
{
    executeWriteOperation([&](auto & disk) { disk.removeFileIfExists(fs::path(root_path) / part_dir / name); });
}

void DataPartStorageOnDiskFull::createHardLinkFrom(const IDataPartStorage & source, const std::string & from, const std::string & to)
{
    const auto * source_on_disk = typeid_cast<const DataPartStorageOnDiskFull *>(&source);
    if (!source_on_disk)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot create hardlink from different storage. Expected DataPartStorageOnDiskFull, got {}",
            typeid(source).name());

    executeWriteOperation([&](auto & disk)
    {
        disk.createHardLink(
            fs::path(source_on_disk->getRelativePath()) / from,
            fs::path(root_path) / part_dir / to);
    });
}

void DataPartStorageOnDiskFull::copyFileFrom(const IDataPartStorage & source, const std::string & from, const std::string & to)
{
    const auto * source_on_disk = typeid_cast<const DataPartStorageOnDiskFull *>(&source);
    if (!source_on_disk)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot create copy file from different storage. Expected DataPartStorageOnDiskFull, got {}",
            typeid(source).name());

    /// Copying files between different disks is
    /// not supported in disk transactions.
    source_on_disk->getDisk()->copyFile(
        fs::path(source_on_disk->getRelativePath()) / from,
        *volume->getDisk(),
        fs::path(root_path) / part_dir / to,
        getReadSettings());
}

void DataPartStorageOnDiskFull::createProjection(const std::string & name)
{
    executeWriteOperation([&](auto & disk) { disk.createDirectory(fs::path(root_path) / part_dir / name); });
}

void DataPartStorageOnDiskFull::beginTransaction()
{
    /// A borrowed projection sub-part shares the PARENT part's whole-part transaction (on a
    /// content-addressed disk a part is one atomic unit: one manifest + one ref). It must not open its
    /// own — riding the parent transaction is the point (B58) — so begin is a no-op here. This
    /// centralizes the rule the 6 merge/mutate call sites used to duplicate as
    /// `if (!isContentAddressed()) beginTransaction()`.
    if (has_shared_transaction)
        return;

    if (transaction)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Uncommitted transaction already exists");

    transaction = volume->getDisk()->createTransaction();
}

void DataPartStorageOnDiskFull::commitTransaction()
{
    /// The mirror of beginTransaction: a borrowed projection sub-part rides the parent's transaction and
    /// is published by the parent's single commit. Committing here would be committing someone else's
    /// transaction, so it is a no-op.
    if (has_shared_transaction)
        return;

    if (!transaction)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no uncommitted transaction");

    /// Regression gate for the part-durability-before-Keeper-commit invariant: lets a test fail the
    /// close of the PART's deferred disk transaction specifically (autocommit one-shot disk ops are
    /// not affected, unlike disk_object_storage_fail_commit_metadata_transaction).
    fiu_do_on(FailPoints::part_storage_fail_commit_transaction,
    {
        throw Exception(ErrorCodes::FAULT_INJECTED, "part_storage_fail_commit_transaction");
    });

    transaction->commit();
    transaction.reset();
}

}
