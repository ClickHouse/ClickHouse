#include <Storages/MergeTree/DataPartStorageOnDiskPacked.h>

#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadBufferFromFileView.h>
#include <IO/ReadPipeline.h>
#include <IO/WriteSettings.h>
#include <IO/ReadHelpers.h>
#include <IO/copyData.h>
#include <Disks/SingleDiskVolume.h>
#include <Disks/IDiskTransaction.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndicesSerialization.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int NOT_INITIALIZED;
    extern const int FILE_DOESNT_EXIST;
}

DataPartStorageOnDiskPacked::DataPartStorageOnDiskPacked(
    VolumePtr volume_,
    std::string root_path_,
    std::string part_dir_,
    DiskTransactionPtr transaction_,
    const ReadSettings & read_settings_,
    bool initialize)
    : DataPartStorageOnDiskBase(std::move(volume_), std::move(root_path_), std::move(part_dir_), std::move(transaction_))
{
    if (initialize)
        resetReader(read_settings_);
    if (transaction)
        resetWriterFromTransaction();
}

DataPartStorageOnDiskPacked::DataPartStorageOnDiskPacked(
    VolumePtr volume_,
    std::string root_path_,
    std::string part_dir_,
    const ReadSettings & read_settings_,
    bool initialize_)
    : DataPartStorageOnDiskBase(std::move(volume_), std::move(root_path_), std::move(part_dir_))
{
    auto component_guard = Coordination::setCurrentComponent("DataPartStorageOnDiskPacked::DataPartStorageOnDiskPacked");
    if (initialize_)
        resetReader(read_settings_);
}

MutableDataPartStoragePtr DataPartStorageOnDiskPacked::create(
    VolumePtr volume_, std::string root_path_, std::string part_dir_, bool initialize_) const // NOLINT
{
    return std::make_shared<DataPartStorageOnDiskPacked>(std::move(volume_), std::move(root_path_), std::move(part_dir_), getReadSettings(), initialize_);
}

MutableDataPartStoragePtr DataPartStorageOnDiskPacked::getProjection(const std::string & name, bool use_parent_transaction) // NOLINT
{
    return std::shared_ptr<DataPartStorageOnDiskPacked>(new DataPartStorageOnDiskPacked(volume, fs::path(root_path) / part_dir, name, use_parent_transaction ? transaction : nullptr, getReadSettings()));
}

MutableDataPartStoragePtr DataPartStorageOnDiskPacked::getProjectionNoInitialize(const std::string & name, bool use_parent_transaction) // NOLINT
{
    return std::shared_ptr<DataPartStorageOnDiskPacked>(new DataPartStorageOnDiskPacked(volume, fs::path(root_path) / part_dir, name, use_parent_transaction ? transaction : nullptr, getReadSettings(), false));
}

DataPartStoragePtr DataPartStorageOnDiskPacked::getProjection(const std::string & name) const
{
    return std::make_shared<DataPartStorageOnDiskPacked>(volume, std::string(fs::path(root_path) / part_dir), name, getReadSettings());
}

bool DataPartStorageOnDiskPacked::exists() const
{
    return volume->getDisk()->existsFileOrDirectory(fs::path(root_path) / part_dir);
}

bool DataPartStorageOnDiskPacked::existsDirectory(const std::string & file_name) const
{
    if (isWrittenSeparately(file_name))
        return volume->getDisk()->existsDirectory(fs::path(root_path) / part_dir / file_name);
    return false;
}

class DataPartStorageIteratorOnDiskPacked final : public IDataPartStorageIterator
{
public:
    DataPartStorageIteratorOnDiskPacked(
        Names files_, DiskPtr disk_,
        const String & root_path_, const String & part_dir_)
        : files(std::move(files_))
        , files_it(files.begin())
        , disk(std::move(disk_))
        , dir_it(disk->iterateDirectory(fs::path(root_path_) / part_dir_))
        , part_dir(part_dir_)
    {
    }

    void next() override
    {
        if (files_it == files.end())
        {
            skipDataFile();
            dir_it->next();
            return;
        }

        ++files_it;
    }

    bool isValid() const override
    {
        if (files_it == files.end())
        {
            skipDataFile();
            return dir_it->isValid();
        }

        return true;
    }

    bool isFile() const override
    {
        if (files_it == files.end())
        {
            skipDataFile();
            return dir_it->isValid() && disk->existsFile(dir_it->path());
        }

        return true;
    }

    std::string name() const override
    {
        if (files_it == files.end())
        {
            skipDataFile();
            return dir_it->name();
        }

        return *files_it;
    }

    std::string path() const override
    {
        if (files_it == files.end())
        {
            skipDataFile();
            return dir_it->path();
        }

        return fs::path(part_dir) / *files_it;
    }

private:
    void skipDataFile() const
    {
        if (dir_it->isValid()
            && dir_it->name() == DataPartStorageOnDiskPacked::DATA_FILE_NAME)
            dir_it->next();
    }

    Names files;
    Names::const_iterator files_it;

    DiskPtr disk;
    DirectoryIteratorPtr dir_it;
    String part_dir;
};

DataPartStorageIteratorPtr DataPartStorageOnDiskPacked::iterate() const
{
    auto files = reader ? reader->getFileNames() : Names{};
    return std::make_unique<DataPartStorageIteratorOnDiskPacked>(std::move(files), volume->getDisk(), root_path, part_dir);
}

Poco::Timestamp DataPartStorageOnDiskPacked::getFileLastModified(const String & file_name) const
{
    auto disk = volume->getDisk();
    if (isWrittenSeparately(file_name))
        return disk->getLastModified(fs::path(root_path) / part_dir / file_name);

    if (!reader)
        throw Exception(ErrorCodes::NOT_INITIALIZED,
            "Cannot get modification time for file {} because reader is not initialized", file_name);

    if (!reader->exists(file_name))
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST,
            "File {} does not exist in packed storage", file_name);

    return disk->getLastModified(getRelativeDataPath());
}

bool DataPartStorageOnDiskPacked::existsFile(const std::string & file_name) const
{
    /// Packed skip-index substreams live inside the per-part skp_idx.packed archive (which is
    /// itself a virtual file inside data.packed). The outer data.packed reader's index only
    /// knows about skp_idx.packed as a single blob, not about its inner virtual files — so we
    /// must consult the skip-indices overlay before falling through to the outer reader.
    if (looksLikePackedSkipIndexFile(file_name))
    {
        if (const auto * skip_reader = getSkipIndicesPackedReader(); skip_reader && skip_reader->exists(file_name))
            return true;
    }

    auto disk = volume->getDisk();
    if (isWrittenSeparately(file_name))
        return disk->existsFile(fs::path(root_path) / part_dir / file_name);

    if (!reader)
        return false;

    return disk->existsFile(getRelativeDataPath()) && reader->exists(file_name);
}

size_t DataPartStorageOnDiskPacked::getFileSize(const String & file_name) const
{
    /// See existsFile() for why we consult the skip-indices overlay first.
    if (looksLikePackedSkipIndexFile(file_name))
    {
        if (const auto * skip_reader = getSkipIndicesPackedReader(); skip_reader && skip_reader->exists(file_name))
            return skip_reader->getFileSize(file_name);
    }

    auto disk = volume->getDisk();
    if (isWrittenSeparately(file_name))
        return disk->getFileSize(fs::path(root_path) / part_dir / file_name);

    /// The archive itself is a physical file on disk; its size can be read directly
    /// without going through the packed index (which only knows individual file sizes).
    if (file_name == DATA_FILE_NAME)
        return disk->getFileSize(fs::path(root_path) / part_dir / DATA_FILE_NAME);

    if (!disk->existsFile(getRelativeDataPath()))
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File {} does not exist, cannot get file size of {}", getRelativeDataPath(), file_name);

    if (!reader)
        throw Exception(ErrorCodes::NOT_INITIALIZED,
            "Cannot read file {} because reader is not initialized", file_name);

    return reader->getFileSize(file_name);
}

PackedFilesIO::FileOffset DataPartStorageOnDiskPacked::getFileOffsetAndSize(const std::string & file_name) const
{
    auto disk = volume->getDisk();

    if (file_name == DATA_FILE_NAME)
        return {0, disk->getFileSize(fs::path(root_path) / part_dir / DATA_FILE_NAME)};

    if (isWrittenSeparately(file_name))
        return {0, disk->getFileSize(fs::path(root_path) / part_dir / file_name)};

    if (!disk->existsFile(getRelativeDataPath()))
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File {} does not exist, cannot get file size of {}", getRelativeDataPath(), file_name);

    if (!reader)
        throw Exception(ErrorCodes::NOT_INITIALIZED,
            "Cannot read file {} because reader is not initialized", file_name);

    return reader->getFileOffsetAndSize(file_name);
}

UInt32 DataPartStorageOnDiskPacked::getRefCount(const String & file_name) const
{
    if (isWrittenSeparately(file_name))
        return volume->getDisk()->getRefCount(fs::path(root_path) / part_dir / file_name);

    return reader ? volume->getDisk()->getRefCount(getRelativeDataPath()) : 0;
}

std::vector<std::string> DataPartStorageOnDiskPacked::getRemotePaths(const std::string & file_name) const
{
    std::string file_path = getRelativeDataPath();

    if (isWrittenSeparately(file_name))
        file_path = fs::path(root_path) / part_dir / file_name;

    auto objects = volume->getDisk()->getStorageObjects(file_path);

    std::vector<std::string> remote_paths;
    remote_paths.reserve(objects.size());

    for (const auto & object : objects)
        remote_paths.push_back(object.remote_path);

    return remote_paths;
}

String DataPartStorageOnDiskPacked::getUniqueId() const
{
    auto disk = volume->getDisk();
    if (!disk->supportZeroCopyReplication())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Disk {} doesn't support zero-copy replication", disk->getName());

    return disk->getUniqueId(fs::path(getRelativePath()) / DATA_FILE_NAME);
}

void DataPartStorageOnDiskPacked::prepareRead(
    const std::string & name,
    const ReadSettings & settings,
    std::optional<size_t> read_hint,
    ReadPipeline & pipeline) const
{
    /// See existsFile() for why we consult the skip-indices overlay first. We can't just call
    /// skip_reader->readFile here — the skip-indices reader's data_file_name points at the
    /// storage-relative path ".../skp_idx.packed" which doesn't exist as a standalone file in
    /// packed-part storage (skp_idx.packed is itself a virtual file inside data.packed). So
    /// compose: get the virtual file's offset+size from the inner overlay, then sub-view the
    /// outer reader's bytes for skp_idx.packed.
    if (looksLikePackedSkipIndexFile(name))
    {
        if (const auto * skip_reader = getSkipIndicesPackedReader(); skip_reader && skip_reader->exists(name))
        {
            if (!reader)
                throw Exception(ErrorCodes::NOT_INITIALIZED,
                    "Cannot read packed skip-index file {} because outer reader is not initialized", name);

            auto inner = skip_reader->getFileOffsetAndSize(name);
            ReadPipeline::BufferCreator creator =
                [this, file_name = name, inner](const StoredObject &, const ReadSettings & s, bool, bool)
                    -> std::unique_ptr<ReadBufferFromFileBase>
                {
                    auto outer_buf = reader->readFile(String(SKIP_INDICES_PACKED_FILENAME), s, std::nullopt);
                    return std::make_unique<ReadBufferFromFileView>(std::move(outer_buf), file_name, inner.offset, inner.offset + inner.size);
                };
            pipeline.setSource(std::move(creator), StoredObjects{StoredObject{}}, settings);
            return;
        }
    }

    if (isWrittenSeparately(name))
    {
        volume->getDisk()->prepareRead(fs::path(root_path) / part_dir / name, settings, read_hint, pipeline);
        return;
    }

    if (!reader)
        throw Exception(ErrorCodes::NOT_INITIALIZED, "Cannot read file, because reader is not initialized");

    /// Packed files are read via PackedFilesReader which handles archive offsets internally.
    /// Wrap it as a CustomSource so the pipeline can build it.
    pipeline.setSource(
        [this, file_name = name, read_hint](const StoredObject &, const ReadSettings & read_settings, bool, bool)
            -> std::unique_ptr<ReadBufferFromFileBase>
        {
            return reader->readFile(file_name, read_settings, read_hint);
        },
        StoredObjects{StoredObject(name, "", reader->getFileSize(name))},
        settings);
}

void DataPartStorageOnDiskPacked::rename(
    std::string new_root_path,
    std::string new_part_dir,
    LoggerPtr log,
    bool remove_new_dir_if_exists,
    bool fsync_part_dir)
{
    DataPartStorageOnDiskBase::rename(std::move(new_root_path), std::move(new_part_dir), log, remove_new_dir_if_exists, fsync_part_dir);
    resetReader(getReadSettings());
}

void DataPartStorageOnDiskPacked::changeRootPath(const std::string & from_root, const std::string & to_root)
{
    DataPartStorageOnDiskBase::changeRootPath(from_root, to_root);
    resetReader(getReadSettings());
}

std::unique_ptr<WriteBufferFromFileBase> DataPartStorageOnDiskPacked::writeFile(
    const String & name,
    size_t buf_size,
    WriteMode mode,
    const WriteSettings & settings)
{
    if (isWrittenSeparately(name))
    {
        auto file_path = fs::path(root_path) / part_dir / name;
        if (transaction)
            return transaction->writeFile(file_path, buf_size, mode, settings);

        return volume->getDisk()->writeFile(file_path, buf_size, mode, settings);
    }

    if (writer)
        return writer->writeFile(name, settings);

    throw Exception(ErrorCodes::NOT_INITIALIZED, "Cannot write file {} because writer is not initialized", name);
}

void DataPartStorageOnDiskPacked::createFile(const String &)
{
}

void DataPartStorageOnDiskPacked::moveFile(const String & from_name, const String & to_name)
{
    executeBinaryWriteOperation(from_name, to_name,
        [](auto & disk, const auto & from_path, const auto & to_path)
        {
            disk.moveFile(from_path, to_path);
        });
}

void DataPartStorageOnDiskPacked::replaceFile(const String & from_name, const String & to_name)
{
    executeBinaryWriteOperation(from_name, to_name,
        [](auto & disk, const auto & from_path, const auto & to_path)
        {
            disk.replaceFile(from_path, to_path);
        });
}

void DataPartStorageOnDiskPacked::removeFile(const String & name)
{
    executeUnaryWriteOperation(name,
        [](auto & disk, const auto & path)
        {
            disk.removeFile(path);
        });
}

void DataPartStorageOnDiskPacked::removeFileIfExists(const String & name)
{
    executeUnaryWriteOperation(name,
        [](auto & disk, const auto & path)
        {
            disk.removeFileIfExists(path);
        });
}

void DataPartStorageOnDiskPacked::createHardLinkFrom(const IDataPartStorage &, const std::string &, const std::string &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "DataPartStorageOnDiskPacked does not support creating hardlinks");
}

void DataPartStorageOnDiskPacked::copyFileFrom(const IDataPartStorage &, const std::string &, const std::string &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "DataPartStorageOnDiskPacked does not support copying files");
}

void DataPartStorageOnDiskPacked::createProjection(const std::string & name)
{
    executeWriteOperation([&](auto & disk) { disk.createDirectory(fs::path(root_path) / part_dir / name); });
}

void DataPartStorageOnDiskPacked::beginTransaction()
{
    if (transaction || writer)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Uncommitted{}transaction already exists", has_shared_transaction ? " shared " : " ");

    transaction = volume->getDisk()->createTransaction();
    resetWriterFromTransaction();
}

void DataPartStorageOnDiskPacked::precommitTransaction()
{
    if (!transaction || (!writer && !is_precommitted))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no uncommitted transaction");

    if (!is_precommitted)
    {
        finalizeWriter();
        is_precommitted = true;
    }
}

void DataPartStorageOnDiskPacked::commitTransaction()
{
    if (!transaction || (!writer && !is_precommitted))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no uncommitted transaction");

    if (has_shared_transaction)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot commit shared transaction");

    if (!is_precommitted)
        finalizeWriter();

    transaction->commit();

    if (!reader)
        reader.emplace(volume->getDisk(), getRelativeDataPath(), getReadSettings());

    transaction.reset();
    is_precommitted = false;
}

#if CLICKHOUSE_CLOUD
TransactionCommitOutcomeVariant DataPartStorageOnDiskPacked::tryCommitTransaction(const TransactionCommitOptionsVariant & options)
{
    if (!transaction || (!writer && !is_precommitted))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no uncommitted transaction");

    if (has_shared_transaction)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot commit shared transaction");

    if (!is_precommitted)
    {
        finalizeWriter();
        is_precommitted = true;
    }

    auto result = transaction->tryCommit(options);

    if (!mayRetryCommit(options, result))
    {
        if (!reader && isSuccessfulOutcome(result))
            reader.emplace(volume->getDisk(), getRelativeDataPath(), getReadSettings());

        transaction.reset();
        is_precommitted = false;
    }

    return result;
}

void DataPartStorageOnDiskPacked::undoTransaction()
{
    if (!transaction || (!writer && !is_precommitted))
        return;

    if (has_shared_transaction)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot commit shared transaction");

    transaction->undo();
    transaction.reset();
    is_precommitted = false;
}
#endif

String DataPartStorageOnDiskPacked::getRelativeDataPath() const
{
    return fs::path(root_path) / part_dir / DATA_FILE_NAME;
}

bool DataPartStorageOnDiskPacked::isWrittenSeparately(const String & file_name) const
{
    if (files_written_separately.contains(file_name))
        return true;

    auto path = fs::path(file_name);
    return path.extension() == ".proj"
        || path.extension() == ".tmp_proj"
        || (path.extension() == ".tmp" && files_written_separately.contains(path.stem()));
}

void DataPartStorageOnDiskPacked::resetReader(const ReadSettings & read_settings)
{
    if (reader)
    {
        /// Just update data path for the reader.
        auto index = reader->getIndex();
        reader.emplace(volume->getDisk(), getRelativeDataPath(), index);
    }
    else
    {
        /// Initialize reader from index of data file.
        auto data_path = getRelativeDataPath();
        if (volume->getDisk()->existsFile(data_path))
            reader.emplace(volume->getDisk(), data_path, read_settings);
    }

    /// The skip-indices overlay (if any) is now stale relative to the new path/reader. Drop it
    /// so the next access reprobes via the override below, which routes through the (just
    /// refreshed) outer reader. Doing this eagerly avoids subtle bugs where a seeded overlay
    /// from before resetReader keeps serving stale offsets.
    {
        std::lock_guard lock(skip_indices_packed_mutex);
        skip_indices_packed_probed = false;
        skip_indices_packed_reader.reset();
    }
}

const PackedFilesReader * DataPartStorageOnDiskPacked::getSkipIndicesPackedReader() const
{
    {
        std::lock_guard lock(skip_indices_packed_mutex);
        if (skip_indices_packed_probed)
            return skip_indices_packed_reader.get();
    }

    auto component_guard = Coordination::setCurrentComponent("DataPartStorageOnDiskPacked::getSkipIndicesPackedReader");

    /// skp_idx.packed is a virtual file inside data.packed on packed-part storage; the base-class
    /// disk->existsFile probe would always miss it. Route through the outer reader.
    if (reader && reader->exists(String(SKIP_INDICES_PACKED_FILENAME)))
    {
        auto inner_archive_buf = reader->readFile(String(SKIP_INDICES_PACKED_FILENAME), getReadSettings(), std::nullopt);
        auto inner_index = PackedFilesReader::readIndex(*inner_archive_buf);
        seedSkipIndicesPackedReader(inner_index);
    }
    else
    {
        std::lock_guard lock(skip_indices_packed_mutex);
        skip_indices_packed_probed = true;
    }

    std::lock_guard lock(skip_indices_packed_mutex);
    return skip_indices_packed_reader.get();
}

void DataPartStorageOnDiskPacked::resetWriterFromTransaction()
{
    writer.emplace();
    is_precommitted = false;
}

void DataPartStorageOnDiskPacked::finalizeWriter()
{
    chassert(writer);

    /// Data is unchanged.
    if (!writer->hasModifiedFiles())
        return;

    PackedFilesIO::Index old_index;
    bool file_is_rewriten = false;

    if (reader)
    {
        old_index = reader->getIndex();
        file_is_rewriten = !old_index.empty();
        writer->applyMetadataChanges(old_index);

        /// Merge old data with changes from writer.
        for (const auto & [name, _] : old_index)
        {
            /// File is modified, will rewrite it.
            if (writer->isWritten(name))
                continue;

            auto in = reader->readFile(name, {}, {});
            auto out = writer->writeFile(name);

            copyData(*in, *out);
            out->finalize();
        }
    }
    else
    {
        writer->applyMetadataChanges(old_index);
    }

    auto out_buffer_getter = [&](String serialised_data, const WriteSettings & settings, bool need_sync)
    {
        if (file_is_rewriten)
        {
            /// replace file to avoid modification of hardlinked files
            auto buf = transaction->writeFile(
                getRelativeDataPath() + ".tmp", DBMS_DEFAULT_BUFFER_SIZE,
                WriteMode::Rewrite, settings);
            buf->write(serialised_data.data(), serialised_data.size());
            buf->finalize();
            if (need_sync)
                buf->sync();

            transaction->replaceFile(getRelativeDataPath() + ".tmp", getRelativeDataPath());
        }
        else
        {
            auto buf = transaction->writeFile(
                getRelativeDataPath(), DBMS_DEFAULT_BUFFER_SIZE,
                WriteMode::Rewrite, settings);
            buf->write(serialised_data.data(), serialised_data.size());
            buf->finalize();
            if (need_sync)
                buf->sync();
        }

    };

    auto new_index = writer->finalize(std::move(out_buffer_getter), preferred_file_order);
    reader.emplace(volume->getDisk(), getRelativeDataPath(), new_index);
    writer.reset();
}

template <typename Op>
void DataPartStorageOnDiskPacked::executeUnaryWriteOperation(const String & file_name, Op && op)
{
    if (isWrittenSeparately(file_name))
    {
        executeWriteOperation([&](auto & disk)
        {
            op(disk, fs::path(root_path) / part_dir / file_name);
        });
        return;
    }

    if (writer)
    {
        op(*writer, file_name);
        return;
    }

    throw Exception(ErrorCodes::NOT_INITIALIZED,
        "Cannot modify file {} because writer is not initialized", file_name);
}

template <typename Op>
void DataPartStorageOnDiskPacked::executeBinaryWriteOperation(const String & from_name, const String & to_name, Op && op)
{
    bool is_from_separate = isWrittenSeparately(from_name);
    bool is_to_separate = isWrittenSeparately(to_name);

    if (is_from_separate && is_to_separate)
    {
        auto relative_path = fs::path(root_path) / part_dir;
        executeWriteOperation([&](auto & disk)
        {
            op(disk, relative_path / from_name, relative_path / to_name);
        });
        return;
    }

    if (is_from_separate || is_to_separate)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cannot modify files {} and {}, because one of them is written separately and one is not",
            from_name, to_name);
    }

    if (writer)
    {
        op(*writer, from_name, to_name);
        return;
    }

    throw Exception(ErrorCodes::NOT_INITIALIZED,
        "Cannot modify files {} and {} because writer is not initialized", from_name, to_name);
}

NameSet DataPartStorageOnDiskPacked::getActualFileNamesOnDisk(const NameSet & file_names) const
{
    NameSet actual_file_names;
    for (const auto & name : file_names)
    {
        if (isWrittenSeparately(name))
            actual_file_names.insert(name);
        else
            actual_file_names.insert(DATA_FILE_NAME);
    }

    return actual_file_names;
}

String DataPartStorageOnDiskPacked::getActualFileNameOnDisk(const String & file_name) const
{
    if (isWrittenSeparately(file_name))
        return file_name;
    else
        return DATA_FILE_NAME;
}

MutableDataPartStoragePtr DataPartStorageOnDiskPacked::freeze(
    const std::string & to,
    const std::string & dir_path,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    std::function<void(const DiskPtr &)> save_metadata_callback,
    const ClonePartParams & params) const
{
    auto disk = volume->getDisk();
    auto src_disk = volume->getDisk();

    auto single_disk_volume = std::make_shared<SingleDiskVolume>(disk->getName(), disk, 0);

    std::shared_ptr<DataPartStorageOnDiskPacked> dest_storage;

    bool to_detached = dir_path.starts_with(std::string_view((fs::path(MergeTreeData::DETACHED_DIR_NAME) / "").string()));
    if (params.external_transaction)
    {
        dest_storage = std::shared_ptr<DataPartStorageOnDiskPacked>(new DataPartStorageOnDiskPacked(single_disk_volume, to, dir_path, params.external_transaction, read_settings));
        params.external_transaction->createDirectories(dest_storage->getRelativePath());
    }
    else
    {
        dest_storage = std::make_shared<DataPartStorageOnDiskPacked>(single_disk_volume, to, dir_path, read_settings, !to_detached);
        disk->createDirectories(dest_storage->getRelativePath());
    }


    bool need_commit = false;
    if (!to_detached && (params.copy_instead_of_hardlink
        || params.metadata_version_to_write.has_value()
        || params.files_to_copy_instead_of_hardlinks.contains(DATA_FILE_NAME)
        || !params.keep_metadata_version))
    {
        if (!dest_storage->transaction)
        {
            dest_storage->beginTransaction();
            need_commit = true;
        }

        auto files = reader->getFileNames();
        for (const auto & file : files)
        {
            auto is_metadata_version = (file == IMergeTreeDataPart::METADATA_VERSION_FILE_NAME);
            if (is_metadata_version)
            {
                if (!params.keep_metadata_version)
                    continue;

                if (params.metadata_version_to_write.has_value())
                {
                    auto write_buf = dest_storage->writeFile(file, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, write_settings);
                    writeIntText(*params.metadata_version_to_write, *write_buf);
                    write_buf->finalize();
                }
            }
            else
            {
                auto read_buf = reader->readFile(file, read_settings, {});
                auto write_buf = dest_storage->writeFile(file, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, write_settings);
                copyData(*read_buf, *write_buf);
                write_buf->finalize();
            }
        }
    }
    else if (disk->existsFile(getRelativeDataPath()))
    {
        if (params.external_transaction)
            params.external_transaction->createHardLink(getRelativeDataPath(), dest_storage->getRelativeDataPath());
        else
            disk->copyFile(getRelativeDataPath(), *disk, dest_storage->getRelativeDataPath(), read_settings);
    }

    std::vector<std::string> all_files;
    src_disk->listFiles(getRelativePath(), all_files);
    for (const auto & file : all_files)
    {
         if (src_disk->existsDirectory(fs::path(getRelativePath()) / file))
         {
             auto projection_storage = getProjection(file);
             auto params_copy = params;
             params_copy.external_transaction = dest_storage->transaction;
             projection_storage->freeze(dest_storage->getRelativePath(), file, read_settings, write_settings, save_metadata_callback, params);
         }
    }

    if (need_commit)
        dest_storage->commitTransaction();
    else if (dest_storage->writer)
        dest_storage->finalizeWriter();

    if (!params.external_transaction && !to_detached)
        dest_storage->resetReader(getReadSettings());

    if (save_metadata_callback)
        save_metadata_callback(disk);

    return dest_storage;
}

MutableDataPartStoragePtr DataPartStorageOnDiskPacked::freezeRemote(
    const std::string & to,
    const std::string & dir_path,
    const DiskPtr & dst_disk,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    std::function<void(const DiskPtr &)> save_metadata_callback,
    const ClonePartParams & params) const
{
    auto src_disk = volume->getDisk();

    auto single_disk_volume = std::make_shared<SingleDiskVolume>(dst_disk->getName(), dst_disk, 0);

    std::shared_ptr<DataPartStorageOnDiskPacked> dest_storage;

    bool to_detached = dir_path.starts_with(std::string_view((fs::path(MergeTreeData::DETACHED_DIR_NAME) / "").string()));
    if (params.external_transaction)
    {
        dest_storage = std::shared_ptr<DataPartStorageOnDiskPacked>(new DataPartStorageOnDiskPacked(single_disk_volume, to, dir_path, params.external_transaction, read_settings));
        params.external_transaction->createDirectories(dest_storage->getRelativePath());
    }
    else
    {
        dest_storage = std::make_shared<DataPartStorageOnDiskPacked>(single_disk_volume, to, dir_path, read_settings, !to_detached);
        dst_disk->createDirectories(dest_storage->getRelativePath());
    }

    bool need_commit = false;
    if (!to_detached && (params.copy_instead_of_hardlink
        || params.metadata_version_to_write.has_value()
        || params.files_to_copy_instead_of_hardlinks.contains(DATA_FILE_NAME)
        || !params.keep_metadata_version))
    {
        if (!dest_storage->transaction)
        {
            dest_storage->beginTransaction();
            need_commit = true;
        }

        auto files = reader->getFileNames();
        std::vector<std::string> all_files;
        src_disk->listFiles(getRelativePath(), all_files);
        for (const auto & file : files)
        {
            auto is_metadata_version = (file == IMergeTreeDataPart::METADATA_VERSION_FILE_NAME);
            if (is_metadata_version)
            {
                if (!params.keep_metadata_version)
                    continue;

                if (params.metadata_version_to_write.has_value())
                {
                    auto write_buf = dest_storage->writeFile(file, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, write_settings);
                    writeIntText(*params.metadata_version_to_write, *write_buf);
                    write_buf->finalize();
                }
            }
            else
            {
                auto read_buf = reader->readFile(file, read_settings, {});
                auto write_buf = dest_storage->writeFile(file, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, write_settings);
                copyData(*read_buf, *write_buf);
                write_buf->finalize();
            }
        }
    }
    else if (src_disk->existsFile(getRelativeDataPath()))
    {
        if (params.external_transaction)
        {
            if (dst_disk->getDataSourceDescription() == src_disk->getDataSourceDescription() && dst_disk->getMetadataStorage().get() == src_disk->getMetadataStorage().get())
            {
                params.external_transaction->copyFile(getRelativeDataPath(), dest_storage->getRelativeDataPath(), read_settings, write_settings);
            }
            else
            {
                /// Transactions doesn't support copy between different metadata storages, so doing it manually
                auto write_buf = transaction->writeFile(dest_storage->getRelativeDataPath(), DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, write_settings);
                auto read_buf = src_disk->readFile(getRelativeDataPath(), read_settings);
                copyData(*read_buf, *write_buf);
                write_buf->finalize();
            }
        }
        else
        {
            src_disk->copyFile(getRelativeDataPath(), *dst_disk, dest_storage->getRelativeDataPath(), read_settings, write_settings);
        }
    }

    std::vector<std::string> all_files;
    src_disk->listFiles(getRelativePath(), all_files);
    for (const auto & file : all_files)
    {
         if (src_disk->existsDirectory(fs::path(getRelativePath()) / file))
         {
             auto projection_storage = getProjection(file);
             auto params_copy = params;
             params_copy.external_transaction = dest_storage->transaction;
             projection_storage->freeze(dest_storage->getRelativePath(), file, read_settings, write_settings, save_metadata_callback, params);
         }
    }

    if (need_commit)
        dest_storage->commitTransaction();
    else if (dest_storage->writer)
        dest_storage->finalizeWriter();

    if (!params.external_transaction)
        dest_storage->resetReader(getReadSettings());

    if (save_metadata_callback)
        save_metadata_callback(dst_disk);

    return dest_storage;
}

#if CLICKHOUSE_CLOUD
void DataPartStorageOnDiskPacked::serializeAuxiliaryInfo(WriteBuffer & out) const
{
    if (!reader)
        throw Exception(ErrorCodes::NOT_INITIALIZED,
            "Cannot serialize part header because reader is not initialized");

    out << "part header: \n";
    PackedFilesWriter::writePackedIndex(out, reader->getIndex());
    out << "\n";
}

void DataPartStorageOnDiskPacked::deserializeAuxiliaryInfo(ReadBuffer & in)
{
    if (reader)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Reader is already initialized");

    in >> "part header: \n";
    auto index = PackedFilesReader::readIndex(in);
    reader.emplace(volume->getDisk(), getRelativeDataPath(), index);
    in >> "\n";
}
#endif

}
