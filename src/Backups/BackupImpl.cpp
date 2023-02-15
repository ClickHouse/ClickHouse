#include <Backups/BackupImpl.h>
#include <Backups/BackupFactory.h>
#include <Backups/BackupEntryFromMemory.h>
#include <Backups/BackupIO.h>
#include <Backups/IBackupEntry.h>
#include <Backups/BackupCoordinationLocal.h>
#include <Backups/BackupCoordinationRemote.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/hex.h>
#include <Common/quoteString.h>
#include <Common/XMLUtils.h>
#include <Interpreters/Context.h>
#include <IO/Archives/IArchiveReader.h>
#include <IO/Archives/IArchiveWriter.h>
#include <IO/Archives/createArchiveReader.h>
#include <IO/Archives/createArchiveWriter.h>
#include <IO/ConcatSeekableReadBuffer.h>
#include <IO/HashingReadBuffer.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadHelpers.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/DOM/DOMParser.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BACKUP_NOT_FOUND;
    extern const int BACKUP_ALREADY_EXISTS;
    extern const int BACKUP_VERSION_NOT_SUPPORTED;
    extern const int BACKUP_DAMAGED;
    extern const int NO_BASE_BACKUP;
    extern const int WRONG_BASE_BACKUP;
    extern const int BACKUP_ENTRY_ALREADY_EXISTS;
    extern const int BACKUP_ENTRY_NOT_FOUND;
    extern const int BACKUP_IS_EMPTY;
    extern const int FAILED_TO_SYNC_BACKUP_OR_RESTORE;
    extern const int LOGICAL_ERROR;
}

namespace
{
    const int INITIAL_BACKUP_VERSION = 1;
    const int CURRENT_BACKUP_VERSION = 1;

    using SizeAndChecksum = IBackup::SizeAndChecksum;
    using FileInfo = IBackupCoordination::FileInfo;

    String hexChecksum(UInt128 checksum)
    {
        return getHexUIntLowercase(checksum);
    }

    UInt128 unhexChecksum(const String & checksum)
    {
        constexpr size_t num_chars_in_checksum = sizeof(UInt128) * 2;
        if (checksum.size() != num_chars_in_checksum)
            throw Exception(ErrorCodes::BACKUP_DAMAGED, "Unexpected size of checksum: {}, must be {}", checksum.size(), num_chars_in_checksum);
        return unhexUInt<UInt128>(checksum.data());
    }

    String formatSizeAndChecksum(const SizeAndChecksum & size_and_checksum)
    {
        return hexChecksum(size_and_checksum.second) + std::to_string(size_and_checksum.first);
    }

    /// We store entries' file names in the backup without leading slashes.
    String removeLeadingSlash(const String & path)
    {
        if (path.starts_with('/'))
            return path.substr(1);
        return path;
    }
}


class BackupImpl::BackupEntryFromBackupImpl : public IBackupEntry
{
public:
    BackupEntryFromBackupImpl(
        const std::shared_ptr<const BackupImpl> & backup_,
        const String & archive_suffix_,
        const String & data_file_name_,
        UInt64 size_,
        const UInt128 checksum_,
        BackupEntryPtr base_backup_entry_ = {})
        : backup(backup_), archive_suffix(archive_suffix_), data_file_name(data_file_name_), size(size_), checksum(checksum_),
          base_backup_entry(std::move(base_backup_entry_))
    {
    }

    std::unique_ptr<SeekableReadBuffer> getReadBuffer() const override
    {
        std::unique_ptr<SeekableReadBuffer> read_buffer;
        if (backup->use_archives)
            read_buffer = backup->getArchiveReader(archive_suffix)->readFile(data_file_name);
        else
            read_buffer = backup->reader->readFile(data_file_name);
        if (base_backup_entry)
        {
            size_t base_size = base_backup_entry->getSize();
            read_buffer = std::make_unique<ConcatSeekableReadBuffer>(
                base_backup_entry->getReadBuffer(), base_size, std::move(read_buffer), size - base_size);
        }
        return read_buffer;
    }

    UInt64 getSize() const override { return size; }
    std::optional<UInt128> getChecksum() const override { return checksum; }

    String getFilePath() const override
    {
        return data_file_name;
    }

    DiskPtr tryGetDiskIfExists() const override
    {
        return nullptr;
    }

    DataSourceDescription getDataSourceDescription() const override
    {
        return backup->reader->getDataSourceDescription();
    }


private:
    const std::shared_ptr<const BackupImpl> backup;
    const String archive_suffix;
    const String data_file_name;
    const UInt64 size;
    const UInt128 checksum;
    BackupEntryPtr base_backup_entry;
};


BackupImpl::BackupImpl(
    const String & backup_name_for_logging_,
    const ArchiveParams & archive_params_,
    const std::optional<BackupInfo> & base_backup_info_,
    std::shared_ptr<IBackupReader> reader_,
    const ContextPtr & context_)
    : backup_name_for_logging(backup_name_for_logging_)
    , archive_params(archive_params_)
    , use_archives(!archive_params.archive_name.empty())
    , open_mode(OpenMode::READ)
    , reader(std::move(reader_))
    , is_internal_backup(false)
    , coordination(std::make_shared<BackupCoordinationLocal>())
    , version(INITIAL_BACKUP_VERSION)
    , base_backup_info(base_backup_info_)
{
    open(context_);
}


BackupImpl::BackupImpl(
    const String & backup_name_for_logging_,
    const ArchiveParams & archive_params_,
    const std::optional<BackupInfo> & base_backup_info_,
    std::shared_ptr<IBackupWriter> writer_,
    const ContextPtr & context_,
    bool is_internal_backup_,
    const std::shared_ptr<IBackupCoordination> & coordination_,
    const std::optional<UUID> & backup_uuid_,
    bool deduplicate_files_)
    : backup_name_for_logging(backup_name_for_logging_)
    , archive_params(archive_params_)
    , use_archives(!archive_params.archive_name.empty())
    , open_mode(OpenMode::WRITE)
    , writer(std::move(writer_))
    , is_internal_backup(is_internal_backup_)
    , coordination(coordination_)
    , uuid(backup_uuid_)
    , version(CURRENT_BACKUP_VERSION)
    , base_backup_info(base_backup_info_)
    , deduplicate_files(deduplicate_files_)
    , log(&Poco::Logger::get("BackupImpl"))
{
    open(context_);
}


BackupImpl::~BackupImpl()
{
    try
    {
        close();
    }
    catch (...)
    {
        DB::tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void BackupImpl::open(const ContextPtr & context)
{
    std::lock_guard lock{mutex};

    if (open_mode == OpenMode::WRITE)
    {
        timestamp = std::time(nullptr);
        if (!uuid)
            uuid = UUIDHelpers::generateV4();
        lock_file_name = use_archives ? (archive_params.archive_name + ".lock") : ".lock";
        writing_finalized = false;

        /// Check that we can write a backup there and create the lock file to own this destination.
        checkBackupDoesntExist();
        if (!is_internal_backup)
            createLockFile();
        checkLockFile(true);
    }

    if (open_mode == OpenMode::READ)
        readBackupMetadata();

    if (base_backup_info)
    {
        BackupFactory::CreateParams params;
        params.backup_info = *base_backup_info;
        params.open_mode = OpenMode::READ;
        params.context = context;
        base_backup = BackupFactory::instance().createBackup(params);

        if (open_mode == OpenMode::WRITE)
        {
            base_backup_uuid = base_backup->getUUID();
        }
        else if (base_backup_uuid != base_backup->getUUID())
        {
            throw Exception(
                ErrorCodes::WRONG_BASE_BACKUP,
                "Backup {}: The base backup {} has different UUID ({} != {})",
                backup_name_for_logging,
                base_backup->getNameForLogging(),
                toString(base_backup->getUUID()),
                (base_backup_uuid ? toString(*base_backup_uuid) : ""));
        }
    }
}

void BackupImpl::close()
{
    std::lock_guard lock{mutex};
    closeArchives();

    if (!is_internal_backup && writer && !writing_finalized)
        removeAllFilesAfterFailure();

    writer.reset();
    reader.reset();
    coordination.reset();
}

void BackupImpl::closeArchives()
{
    archive_readers.clear();
    for (auto & archive_writer : archive_writers)
        archive_writer = {"", nullptr};
}

size_t BackupImpl::getNumFiles() const
{
    std::lock_guard lock{mutex};
    return num_files;
}

UInt64 BackupImpl::getTotalSize() const
{
    std::lock_guard lock{mutex};
    return total_size;
}

size_t BackupImpl::getNumEntries() const
{
    std::lock_guard lock{mutex};
    return num_entries;
}

UInt64 BackupImpl::getSizeOfEntries() const
{
    std::lock_guard lock{mutex};
    return size_of_entries;
}

UInt64 BackupImpl::getUncompressedSize() const
{
    std::lock_guard lock{mutex};
    return uncompressed_size;
}

UInt64 BackupImpl::getCompressedSize() const
{
    std::lock_guard lock{mutex};
    return compressed_size;
}

size_t BackupImpl::getNumReadFiles() const
{
    std::lock_guard lock{mutex};
    return num_read_files;
}

UInt64 BackupImpl::getNumReadBytes() const
{
    std::lock_guard lock{mutex};
    return num_read_bytes;
}

void BackupImpl::writeBackupMetadata()
{
    assert(!is_internal_backup);

    Poco::AutoPtr<Poco::Util::XMLConfiguration> config{new Poco::Util::XMLConfiguration()};
    config->setInt("version", CURRENT_BACKUP_VERSION);
    config->setBool("deduplicate_files", deduplicate_files);
    config->setString("timestamp", toString(LocalDateTime{timestamp}));
    config->setString("uuid", toString(*uuid));

    auto all_file_infos = coordination->getAllFileInfos();

    if (base_backup_info)
    {
        bool base_backup_in_use = false;
        for (const auto & info : all_file_infos)
        {
            if (info.base_size)
                base_backup_in_use = true;
        }

        if (base_backup_in_use)
        {
            config->setString("base_backup", base_backup_info->toString());
            config->setString("base_backup_uuid", toString(*base_backup_uuid));
        }
    }

    num_files = all_file_infos.size();
    total_size = 0;
    num_entries = 0;
    size_of_entries = 0;

    for (size_t i = 0; i != all_file_infos.size(); ++i)
    {
        const auto & info = all_file_infos[i];
        String prefix = i ? "contents.file[" + std::to_string(i) + "]." : "contents.file.";
        config->setString(prefix + "name", info.file_name);
        config->setUInt64(prefix + "size", info.size);

        if (info.size)
        {
            config->setString(prefix + "checksum", hexChecksum(info.checksum));
            if (info.base_size)
            {
                config->setBool(prefix + "use_base", true);
                if (info.base_size != info.size)
                {
                    config->setUInt64(prefix + "base_size", info.base_size);
                    config->setString(prefix + "base_checksum", hexChecksum(info.base_checksum));
                }
            }
            if (!info.data_file_name.empty() && (info.data_file_name != info.file_name))
                config->setString(prefix + "data_file", info.data_file_name);
            if (!info.archive_suffix.empty())
                config->setString(prefix + "archive_suffix", info.archive_suffix);
            if (info.pos_in_archive != static_cast<size_t>(-1))
                config->setUInt64(prefix + "pos_in_archive", info.pos_in_archive);
        }

        total_size += info.size;
        bool has_entry = !deduplicate_files || (info.size && (info.size != info.base_size) && (info.data_file_name.empty() || (info.data_file_name == info.file_name)));
        if (has_entry)
        {
            ++num_entries;
            size_of_entries += info.size - info.base_size;
        }
    }

    std::ostringstream stream; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    config->save(stream);
    String str = stream.str();

    checkLockFile(true);

    std::unique_ptr<WriteBuffer> out;
    if (use_archives)
        out = getArchiveWriter("")->writeFile(".backup");
    else
        out = writer->writeFile(".backup");
    out->write(str.data(), str.size());
    out->finalize();

    uncompressed_size = size_of_entries + str.size();
}


void BackupImpl::readBackupMetadata()
{
    using namespace XMLUtils;

    std::unique_ptr<ReadBuffer> in;
    if (use_archives)
    {
        if (!reader->fileExists(archive_params.archive_name))
            throw Exception(ErrorCodes::BACKUP_NOT_FOUND, "Backup {} not found", backup_name_for_logging);
        setCompressedSize();
        in = getArchiveReader("")->readFile(".backup");
    }
    else
    {
        if (!reader->fileExists(".backup"))
            throw Exception(ErrorCodes::BACKUP_NOT_FOUND, "Backup {} not found", backup_name_for_logging);
        in = reader->readFile(".backup");
    }

    String str;
    readStringUntilEOF(str, *in);
    Poco::XML::DOMParser dom_parser;
    Poco::AutoPtr<Poco::XML::Document> config = dom_parser.parseMemory(str.data(), str.size());
    const Poco::XML::Node * config_root = getRootNode(config);

    version = getInt(config_root, "version");
    if ((version < INITIAL_BACKUP_VERSION) || (version > CURRENT_BACKUP_VERSION))
        throw Exception(
            ErrorCodes::BACKUP_VERSION_NOT_SUPPORTED, "Backup {}: Version {} is not supported", backup_name_for_logging, version);

    timestamp = parse<::LocalDateTime>(getString(config_root, "timestamp")).to_time_t();
    uuid = parse<UUID>(getString(config_root, "uuid"));

    if (config_root->getNodeByPath("base_backup") && !base_backup_info)
        base_backup_info = BackupInfo::fromString(getString(config_root, "base_backup"));

    if (config_root->getNodeByPath("base_backup_uuid"))
        base_backup_uuid = parse<UUID>(getString(config_root, "base_backup_uuid"));

    num_files = 0;
    total_size = 0;
    num_entries = 0;
    size_of_entries = 0;

    const auto * contents = config_root->getNodeByPath("contents");
    for (const Poco::XML::Node * child = contents->firstChild(); child; child = child->nextSibling())
    {
        if (child->nodeName() == "file")
        {
            const Poco::XML::Node * file_config = child;
            FileInfo info;
            info.file_name = getString(file_config, "name");
            info.size = getUInt64(file_config, "size");
            if (info.size)
            {
                info.checksum = unhexChecksum(getString(file_config, "checksum"));

                bool use_base = getBool(file_config, "use_base", false);
                info.base_size = getUInt64(file_config, "base_size", use_base ? info.size : 0);
                if (info.base_size)
                    use_base = true;

                if (info.base_size > info.size)
                {
                    throw Exception(
                        ErrorCodes::BACKUP_DAMAGED,
                        "Backup {}: Base size must not be greater than the size of entry {}",
                        backup_name_for_logging,
                        quoteString(info.file_name));
                }

                if (use_base)
                {
                    if (info.base_size == info.size)
                        info.base_checksum = info.checksum;
                    else
                        info.base_checksum = unhexChecksum(getString(file_config, "base_checksum"));
                }

                if (info.size > info.base_size)
                {
                    info.data_file_name = getString(file_config, "data_file", info.file_name);
                    info.archive_suffix = getString(file_config, "archive_suffix", "");
                    info.pos_in_archive = getUInt64(file_config, "pos_in_archive", static_cast<UInt64>(-1));
                }
            }

            coordination->addFileInfo(info);

            ++num_files;
            total_size += info.size;
            bool has_entry = !deduplicate_files || (info.size && (info.size != info.base_size) && (info.data_file_name.empty() || (info.data_file_name == info.file_name)));
            if (has_entry)
            {
                ++num_entries;
                size_of_entries += info.size - info.base_size;
            }
        }
    }

    uncompressed_size = size_of_entries + str.size();
    compressed_size = uncompressed_size;
    if (!use_archives)
        setCompressedSize();
}

void BackupImpl::checkBackupDoesntExist() const
{
    String file_name_to_check_existence;
    if (use_archives)
        file_name_to_check_existence = archive_params.archive_name;
    else
        file_name_to_check_existence = ".backup";

    if (writer->fileExists(file_name_to_check_existence))
        throw Exception(ErrorCodes::BACKUP_ALREADY_EXISTS, "Backup {} already exists", backup_name_for_logging);

    /// Check that no other backup (excluding internal backups) is writing to the same destination.
    if (!is_internal_backup)
    {
        assert(!lock_file_name.empty());
        if (writer->fileExists(lock_file_name))
            throw Exception(ErrorCodes::BACKUP_ALREADY_EXISTS, "Backup {} is being written already", backup_name_for_logging);
    }
}

void BackupImpl::createLockFile()
{
    /// Internal backup must not create the lock file (it should be created by the initiator).
    assert(!is_internal_backup);

    assert(uuid);
    auto out = writer->writeFile(lock_file_name);
    writeUUIDText(*uuid, *out);
    out->finalize();
}

bool BackupImpl::checkLockFile(bool throw_if_failed) const
{
    if (!lock_file_name.empty() && uuid && writer->fileContentsEqual(lock_file_name, toString(*uuid)))
        return true;

    if (throw_if_failed)
    {
        if (!writer->fileExists(lock_file_name))
        {
            throw Exception(
                ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE,
                "Lock file {} suddenly disappeared while writing backup {}",
                lock_file_name,
                backup_name_for_logging);
        }

        throw Exception(
            ErrorCodes::BACKUP_ALREADY_EXISTS, "A concurrent backup writing to the same destination {} detected", backup_name_for_logging);
    }
    return false;
}

void BackupImpl::removeLockFile()
{
    if (is_internal_backup)
        return; /// Internal backup must not remove the lock file (it's still used by the initiator).

    if (checkLockFile(false))
        writer->removeFile(lock_file_name);
}

Strings BackupImpl::listFiles(const String & directory, bool recursive) const
{
    std::lock_guard lock{mutex};
    auto adjusted_dir = removeLeadingSlash(directory);
    return coordination->listFiles(adjusted_dir, recursive);
}

bool BackupImpl::hasFiles(const String & directory) const
{
    std::lock_guard lock{mutex};
    auto adjusted_dir = removeLeadingSlash(directory);
    return coordination->hasFiles(adjusted_dir);
}

bool BackupImpl::fileExists(const String & file_name) const
{
    std::lock_guard lock{mutex};
    auto adjusted_path = removeLeadingSlash(file_name);
    return coordination->getFileInfo(adjusted_path).has_value();
}

bool BackupImpl::fileExists(const SizeAndChecksum & size_and_checksum) const
{
    std::lock_guard lock{mutex};
    return coordination->getFileInfo(size_and_checksum).has_value();
}

UInt64 BackupImpl::getFileSize(const String & file_name) const
{
    std::lock_guard lock{mutex};
    auto adjusted_path = removeLeadingSlash(file_name);
    auto info = coordination->getFileInfo(adjusted_path);
    if (!info)
    {
        throw Exception(
            ErrorCodes::BACKUP_ENTRY_NOT_FOUND,
            "Backup {}: Entry {} not found in the backup",
            backup_name_for_logging,
            quoteString(file_name));
    }
    return info->size;
}

UInt128 BackupImpl::getFileChecksum(const String & file_name) const
{
    std::lock_guard lock{mutex};
    auto adjusted_path = removeLeadingSlash(file_name);
    auto info = coordination->getFileInfo(adjusted_path);
    if (!info)
    {
        throw Exception(
            ErrorCodes::BACKUP_ENTRY_NOT_FOUND,
            "Backup {}: Entry {} not found in the backup",
            backup_name_for_logging,
            quoteString(file_name));
    }
    return info->checksum;
}

SizeAndChecksum BackupImpl::getFileSizeAndChecksum(const String & file_name) const
{
    std::lock_guard lock{mutex};
    auto adjusted_path = removeLeadingSlash(file_name);
    auto info = coordination->getFileInfo(adjusted_path);
    if (!info)
    {
        throw Exception(
            ErrorCodes::BACKUP_ENTRY_NOT_FOUND,
            "Backup {}: Entry {} not found in the backup",
            backup_name_for_logging,
            quoteString(file_name));
    }
    return {info->size, info->checksum};
}

BackupEntryPtr BackupImpl::readFile(const String & file_name) const
{
    return readFile(getFileSizeAndChecksum(file_name));
}

BackupEntryPtr BackupImpl::readFile(const SizeAndChecksum & size_and_checksum) const
{
    std::lock_guard lock{mutex};
    if (open_mode != OpenMode::READ)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Backup is not opened for reading");

    ++num_read_files;
    num_read_bytes += size_and_checksum.first;

    if (!size_and_checksum.first)
    {
        /// Entry's data is empty.
        return std::make_unique<BackupEntryFromMemory>(nullptr, 0, UInt128{0, 0});
    }

    auto info_opt = coordination->getFileInfo(size_and_checksum);
    if (!info_opt)
    {
        throw Exception(
            ErrorCodes::BACKUP_ENTRY_NOT_FOUND,
            "Backup {}: Entry {} not found in the backup",
            backup_name_for_logging,
            formatSizeAndChecksum(size_and_checksum));
    }

    const auto & info = *info_opt;

    if (!info.base_size)
    {
        /// Data goes completely from this backup, the base backup isn't used.
        return std::make_unique<BackupEntryFromBackupImpl>(
            std::static_pointer_cast<const BackupImpl>(shared_from_this()), info.archive_suffix, info.data_file_name, info.size, info.checksum);
    }

    if (!base_backup)
    {
        throw Exception(
            ErrorCodes::NO_BASE_BACKUP,
            "Backup {}: Entry {} is marked to be read from a base backup, but there is no base backup specified",
            backup_name_for_logging, formatSizeAndChecksum(size_and_checksum));
    }

    if (!base_backup->fileExists(std::pair(info.base_size, info.base_checksum)))
    {
        throw Exception(
            ErrorCodes::WRONG_BASE_BACKUP,
            "Backup {}: Entry {} is marked to be read from a base backup, but doesn't exist there",
            backup_name_for_logging, formatSizeAndChecksum(size_and_checksum));
    }

    auto base_entry = base_backup->readFile(std::pair{info.base_size, info.base_checksum});

    if (info.size == info.base_size)
    {
        /// Data goes completely from the base backup (nothing goes from this backup).
        return base_entry;
    }

    {
        /// The beginning of the data goes from the base backup,
        /// and the ending goes from this backup.
        return std::make_unique<BackupEntryFromBackupImpl>(
            static_pointer_cast<const BackupImpl>(shared_from_this()), info.archive_suffix, info.data_file_name, info.size, info.checksum, std::move(base_entry));
    }
}

namespace
{

std::optional<SizeAndChecksum> getInfoAboutFileFromBaseBackupIfExists(std::shared_ptr<const IBackup> base_backup, const std::string & file_path)
{
    if (base_backup && base_backup->fileExists(file_path))
        return std::pair{base_backup->getFileSize(file_path), base_backup->getFileChecksum(file_path)};

    return std::nullopt;
}

enum class CheckBackupResult
{
    HasPrefix,
    HasFull,
    HasNothing,
};

CheckBackupResult checkBaseBackupForFile(const SizeAndChecksum & base_backup_info, const FileInfo & new_entry_info)
{
    /// We cannot reuse base backup because our file is smaller
    /// than file stored in previous backup
    if (new_entry_info.size < base_backup_info.first)
        return CheckBackupResult::HasNothing;

    if (base_backup_info.first == new_entry_info.size)
        return CheckBackupResult::HasFull;

    return CheckBackupResult::HasPrefix;

}

struct ChecksumsForNewEntry
{
    UInt128 full_checksum;
    UInt128 prefix_checksum;
};

/// Calculate checksum for backup entry if it's empty.
/// Also able to calculate additional checksum of some prefix.
ChecksumsForNewEntry calculateNewEntryChecksumsIfNeeded(BackupEntryPtr entry, size_t prefix_size)
{
    if (prefix_size > 0)
    {
        auto read_buffer = entry->getReadBuffer();
        HashingReadBuffer hashing_read_buffer(*read_buffer);
        hashing_read_buffer.ignore(prefix_size);
        auto prefix_checksum = hashing_read_buffer.getHash();
        if (entry->getChecksum() == std::nullopt)
        {
            hashing_read_buffer.ignoreAll();
            auto full_checksum = hashing_read_buffer.getHash();
            return ChecksumsForNewEntry{full_checksum, prefix_checksum};
        }
        else
        {
            return ChecksumsForNewEntry{*(entry->getChecksum()), prefix_checksum};
        }
    }
    else
    {
        if (entry->getChecksum() == std::nullopt)
        {
            auto read_buffer = entry->getReadBuffer();
            HashingReadBuffer hashing_read_buffer(*read_buffer);
            hashing_read_buffer.ignoreAll();
            return ChecksumsForNewEntry{hashing_read_buffer.getHash(), 0};
        }
        else
        {
            return ChecksumsForNewEntry{*(entry->getChecksum()), 0};
        }
    }
}

}

void BackupImpl::writeFile(const String & file_name, BackupEntryPtr entry)
{
    if (open_mode != OpenMode::WRITE)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Backup is not opened for writing");

    if (writing_finalized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Backup is already finalized");

    std::string from_file_name = "memory buffer";
    if (auto fname = entry->getFilePath(); !fname.empty())
        from_file_name = "file " + fname;
    LOG_TRACE(log, "Writing backup for file {} from {}", file_name, from_file_name);

    auto adjusted_path = removeLeadingSlash(file_name);

    if (coordination->getFileInfo(adjusted_path))
    {
        throw Exception(
                        ErrorCodes::BACKUP_ENTRY_ALREADY_EXISTS, "Backup {}: Entry {} already exists",
                        backup_name_for_logging, quoteString(file_name));
    }

    FileInfo info
    {
        .file_name = adjusted_path,
        .size = entry->getSize(),
        .base_size = 0,
        .base_checksum = 0,
    };

    {
        std::lock_guard lock{mutex};
        ++num_files;
        total_size += info.size;
    }

    /// Empty file, nothing to backup
    if (info.size == 0 && deduplicate_files)
    {
        coordination->addFileInfo(info);
        return;
    }

    std::optional<SizeAndChecksum> base_backup_file_info = getInfoAboutFileFromBaseBackupIfExists(base_backup, adjusted_path);

    /// We have info about this file in base backup
    /// If file has no checksum -- calculate and fill it.
    if (base_backup_file_info.has_value())
    {
        LOG_TRACE(log, "File {} found in base backup, checking for equality", adjusted_path);
        CheckBackupResult check_base = checkBaseBackupForFile(*base_backup_file_info, info);

        /// File with the same name but smaller size exist in previous backup
        if (check_base == CheckBackupResult::HasPrefix)
        {
            auto checksums = calculateNewEntryChecksumsIfNeeded(entry, base_backup_file_info->first);
            info.checksum = checksums.full_checksum;

            /// We have prefix of this file in backup with the same checksum.
            /// In ClickHouse this can happen for StorageLog for example.
            if (checksums.prefix_checksum == base_backup_file_info->second)
            {
                LOG_TRACE(log, "File prefix of {} in base backup, will write rest part of file to current backup", adjusted_path);
                info.base_size = base_backup_file_info->first;
                info.base_checksum = base_backup_file_info->second;
            }
            else
            {
                LOG_TRACE(log, "Prefix checksum of file {} doesn't match with checksum in base backup", adjusted_path);
            }
        }
        else
        {
            /// We have full file or have nothing, first of all let's get checksum
            /// of current file
            auto checksums = calculateNewEntryChecksumsIfNeeded(entry, 0);
            info.checksum = checksums.full_checksum;

            if (info.checksum == base_backup_file_info->second)
            {
                LOG_TRACE(log, "Found whole file {} in base backup", adjusted_path);
                assert(check_base == CheckBackupResult::HasFull);
                assert(info.size == base_backup_file_info->first);

                info.base_size = base_backup_file_info->first;
                info.base_checksum = base_backup_file_info->second;
                /// Actually we can add this info to coordination and exist,
                /// but we intentionally don't do it, otherwise control flow
                /// of this function will be very complex.
            }
            else
            {
                LOG_TRACE(log, "Whole file {} in base backup doesn't match by checksum", adjusted_path);
            }
        }
    }
    else /// We don't have info about this file_name (sic!) in base backup,
         /// however file could be renamed, so we will check one more time using size and checksum
    {

        LOG_TRACE(log, "Nothing found for file {} in base backup", adjusted_path);
        auto checksums = calculateNewEntryChecksumsIfNeeded(entry, 0);
        info.checksum = checksums.full_checksum;
    }

    /// Maybe we have a copy of this file in the backup already.
    if (coordination->getFileInfo(std::pair{info.size, info.checksum}) && deduplicate_files)
    {
        LOG_TRACE(log, "File {} already exist in current backup, adding reference", adjusted_path);
        coordination->addFileInfo(info);
        return;
    }

    /// On the previous lines we checked that backup for file with adjusted_name exist in previous backup.
    /// However file can be renamed, but has the same size and checksums, let's check for this case.
    if (base_backup && base_backup->fileExists(std::pair{info.size, info.checksum}))
    {

        LOG_TRACE(log, "File {} doesn't exist in current backup, but we have file with same size and checksum", adjusted_path);
        info.base_size = info.size;
        info.base_checksum = info.checksum;

        coordination->addFileInfo(info);
        return;
    }

    /// All "short paths" failed. We don't have this file in previous or existing backup
    /// or have only prefix of it in previous backup. Let's go long path.

    info.data_file_name = info.file_name;

    if (use_archives)
    {
        std::lock_guard lock{mutex};
        info.archive_suffix = current_archive_suffix;
    }

    bool is_data_file_required;
    coordination->addFileInfo(info, is_data_file_required);
    if (!is_data_file_required && deduplicate_files)
    {
        LOG_TRACE(log, "File {} doesn't exist in current backup, but we have file with same size and checksum", adjusted_path);
        return; /// We copy data only if it's a new combination of size & checksum.
    }
    auto writer_description = writer->getDataSourceDescription();
    auto reader_description = entry->getDataSourceDescription();

    /// We need to copy whole file without archive, we can do it faster
    /// if source and destination are compatible
    if (!use_archives && writer->supportNativeCopy(reader_description))
    {
        /// Should be much faster than writing data through server.
        LOG_TRACE(log, "Will copy file {} using native copy", adjusted_path);

        /// NOTE: `mutex` must be unlocked here otherwise writing will be in one thread maximum and hence slow.

        writer->copyFileNative(entry->tryGetDiskIfExists(), entry->getFilePath(), info.base_size, info.size - info.base_size, info.data_file_name);
    }
    else
    {
        LOG_TRACE(log, "Will copy file {}", adjusted_path);

        bool has_entries = false;
        {
            std::lock_guard lock{mutex};
            has_entries = num_entries > 0;
        }
        if (!has_entries)
            checkLockFile(true);

        if (use_archives)
        {
            LOG_TRACE(log, "Adding file {} to archive", adjusted_path);

            /// An archive must be written strictly in one thread, so it's correct to lock the mutex for all the time we're writing the file
            /// to the archive.
            std::lock_guard lock{mutex};

            String archive_suffix = current_archive_suffix;
            bool next_suffix = false;
            if (current_archive_suffix.empty() && is_internal_backup)
                next_suffix = true;
            /*if (archive_params.max_volume_size && current_archive_writer
                && (current_archive_writer->getTotalSize() + size - base_size > archive_params.max_volume_size))
                next_suffix = true;*/
            if (next_suffix)
                current_archive_suffix = coordination->getNextArchiveSuffix();

            if (info.archive_suffix != current_archive_suffix)
            {
                info.archive_suffix = current_archive_suffix;
                coordination->updateFileInfo(info);
            }
            auto out = getArchiveWriter(current_archive_suffix)->writeFile(info.data_file_name);
            auto read_buffer = entry->getReadBuffer();
            if (info.base_size != 0)
                read_buffer->seek(info.base_size, SEEK_SET);
            copyData(*read_buffer, *out);
            out->finalize();
        }
        else
        {
            auto create_read_buffer = [entry] { return entry->getReadBuffer(); };

            /// NOTE: `mutex` must be unlocked here otherwise writing will be in one thread maximum and hence slow.
            writer->copyDataToFile(create_read_buffer, info.base_size, info.size - info.base_size, info.data_file_name);
        }
    }

    {
        std::lock_guard lock{mutex};
        ++num_entries;
        size_of_entries += info.size - info.base_size;
        uncompressed_size += info.size - info.base_size;
    }
}


void BackupImpl::finalizeWriting()
{
    std::lock_guard lock{mutex};
    if (open_mode != OpenMode::WRITE)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Backup is not opened for writing");

    if (writing_finalized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Backup is already finalized");

    if (!coordination->hasFiles(""))
        throw Exception(ErrorCodes::BACKUP_IS_EMPTY, "Backup must not be empty");

    if (!is_internal_backup)
    {
        LOG_TRACE(log, "Finalizing backup {}", backup_name_for_logging);
        writeBackupMetadata();
        closeArchives();
        setCompressedSize();
        removeLockFile();
        LOG_TRACE(log, "Finalized backup {}", backup_name_for_logging);
    }

    writing_finalized = true;
}


void BackupImpl::setCompressedSize()
{
    if (use_archives)
        compressed_size = writer ? writer->getFileSize(archive_params.archive_name) : reader->getFileSize(archive_params.archive_name);
    else
        compressed_size = uncompressed_size;
}


String BackupImpl::getArchiveNameWithSuffix(const String & suffix) const
{
    return archive_params.archive_name + (suffix.empty() ? "" : ".") + suffix;
}

std::shared_ptr<IArchiveReader> BackupImpl::getArchiveReader(const String & suffix) const
{
    auto it = archive_readers.find(suffix);
    if (it != archive_readers.end())
        return it->second;
    String archive_name_with_suffix = getArchiveNameWithSuffix(suffix);
    size_t archive_size = reader->getFileSize(archive_name_with_suffix);
    auto new_archive_reader = createArchiveReader(archive_params.archive_name, [reader=reader, archive_name_with_suffix]{ return reader->readFile(archive_name_with_suffix); },
        archive_size);
    new_archive_reader->setPassword(archive_params.password);
    archive_readers.emplace(suffix, new_archive_reader);
    return new_archive_reader;
}

std::shared_ptr<IArchiveWriter> BackupImpl::getArchiveWriter(const String & suffix)
{
    for (const auto & archive_writer : archive_writers)
    {
        if ((suffix == archive_writer.first) && archive_writer.second)
            return archive_writer.second;
    }

    String archive_name_with_suffix = getArchiveNameWithSuffix(suffix);
    auto new_archive_writer = createArchiveWriter(archive_params.archive_name, writer->writeFile(archive_name_with_suffix));
    new_archive_writer->setPassword(archive_params.password);
    new_archive_writer->setCompression(archive_params.compression_method, archive_params.compression_level);
    size_t pos = suffix.empty() ? 0 : 1;
    archive_writers[pos] = {suffix, new_archive_writer};

    return new_archive_writer;
}


void BackupImpl::removeAllFilesAfterFailure()
{
    if (is_internal_backup)
        return; /// Let the initiator remove unnecessary files.

    try
    {
        LOG_INFO(log, "Removing all files of backup {} after failure", backup_name_for_logging);

        Strings files_to_remove;
        if (use_archives)
        {
            files_to_remove.push_back(archive_params.archive_name);
            for (const auto & suffix : coordination->getAllArchiveSuffixes())
            {
                String archive_name_with_suffix = getArchiveNameWithSuffix(suffix);
                files_to_remove.push_back(std::move(archive_name_with_suffix));
            }
        }
        else
        {
            files_to_remove.push_back(".backup");
            for (const auto & file_info : coordination->getAllFileInfos())
                files_to_remove.push_back(file_info.data_file_name);
        }

        if (!checkLockFile(false))
            return;

        writer->removeFiles(files_to_remove);
        removeLockFile();
    }
    catch (...)
    {
        DB::tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
