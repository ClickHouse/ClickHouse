#include <Backups/BackupImpl.h>
#include <Backups/BackupFactory.h>
#include <Backups/BackupFileInfo.h>
#include <Backups/BackupIO.h>
#include <Backups/IBackupEntry.h>
#include <Common/ProfileEvents.h>
#include <Common/StringUtils.h>
#include <base/hex.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Common/XMLUtils.h>
#include <Interpreters/Context.h>
#include <IO/Archives/IArchiveReader.h>
#include <IO/Archives/IArchiveWriter.h>
#include <IO/Archives/createArchiveReader.h>
#include <IO/Archives/createArchiveWriter.h>
#include <IO/ConcatSeekableReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <IO/copyData.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/DOM/DOMParser.h>


namespace ProfileEvents
{
    extern const Event BackupsOpenedForRead;
    extern const Event BackupsOpenedForWrite;
    extern const Event BackupReadMetadataMicroseconds;
    extern const Event BackupWriteMetadataMicroseconds;
}

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
    extern const int BACKUP_ENTRY_NOT_FOUND;
    extern const int BACKUP_IS_EMPTY;
    extern const int CANNOT_RESTORE_TO_NONENCRYPTED_DISK;
    extern const int FAILED_TO_SYNC_BACKUP_OR_RESTORE;
    extern const int LOGICAL_ERROR;
}

namespace
{
    const int INITIAL_BACKUP_VERSION = 1;
    const int CURRENT_BACKUP_VERSION = 1;

    using SizeAndChecksum = IBackup::SizeAndChecksum;

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


BackupImpl::BackupImpl(
    const BackupInfo & backup_info_,
    const ArchiveParams & archive_params_,
    const std::optional<BackupInfo> & base_backup_info_,
    std::shared_ptr<IBackupReader> reader_,
    const ContextPtr & context_,
    bool is_internal_backup_,
    bool use_same_s3_credentials_for_base_backup_,
    bool use_same_password_for_base_backup_)
    : backup_info(backup_info_)
    , backup_name_for_logging(backup_info.toStringForLogging())
    , use_archive(!archive_params_.archive_name.empty())
    , archive_params(archive_params_)
    , open_mode(OpenMode::READ)
    , reader(std::move(reader_))
    , context(context_)
    , is_internal_backup(is_internal_backup_)
    , version(INITIAL_BACKUP_VERSION)
    , base_backup_info(base_backup_info_)
    , use_same_s3_credentials_for_base_backup(use_same_s3_credentials_for_base_backup_)
    , use_same_password_for_base_backup(use_same_password_for_base_backup_)
    , log(getLogger("BackupImpl"))
{
    open();
}


BackupImpl::BackupImpl(
    const BackupInfo & backup_info_,
    const ArchiveParams & archive_params_,
    const std::optional<BackupInfo> & base_backup_info_,
    std::shared_ptr<IBackupWriter> writer_,
    const ContextPtr & context_,
    bool is_internal_backup_,
    const std::shared_ptr<IBackupCoordination> & coordination_,
    const std::optional<UUID> & backup_uuid_,
    bool deduplicate_files_,
    bool use_same_s3_credentials_for_base_backup_,
    bool use_same_password_for_base_backup_)
    : backup_info(backup_info_)
    , backup_name_for_logging(backup_info.toStringForLogging())
    , use_archive(!archive_params_.archive_name.empty())
    , archive_params(archive_params_)
    , open_mode(OpenMode::WRITE)
    , writer(std::move(writer_))
    , context(context_)
    , is_internal_backup(is_internal_backup_)
    , coordination(coordination_)
    , uuid(backup_uuid_)
    , version(CURRENT_BACKUP_VERSION)
    , base_backup_info(base_backup_info_)
    , deduplicate_files(deduplicate_files_)
    , use_same_s3_credentials_for_base_backup(use_same_s3_credentials_for_base_backup_)
    , use_same_password_for_base_backup(use_same_password_for_base_backup_)
    , log(getLogger("BackupImpl"))
{
    open();
}


BackupImpl::~BackupImpl()
{
    if ((open_mode == OpenMode::WRITE) && !is_internal_backup && !writing_finalized && !std::uncaught_exceptions() && !std::current_exception())
    {
        /// It is suspicious to destroy BackupImpl without finalization while writing a backup when there is no exception.
        LOG_ERROR(log, "BackupImpl is not finalized when destructor is called. Stack trace: {}", StackTrace().toString());
        chassert(false && "BackupImpl is not finalized when destructor is called.");
    }

    try
    {
        close();
    }
    catch (...)
    {
        DB::tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void BackupImpl::open()
{
    std::lock_guard lock{mutex};

    if (open_mode == OpenMode::READ)
    {
        ProfileEvents::increment(ProfileEvents::BackupsOpenedForRead);
        LOG_INFO(log, "Reading backup: {}", backup_name_for_logging);
    }
    else
    {
        ProfileEvents::increment(ProfileEvents::BackupsOpenedForWrite);
        LOG_INFO(log, "Writing backup: {}", backup_name_for_logging);
        timestamp = std::time(nullptr);
        if (!uuid)
            uuid = UUIDHelpers::generateV4();
        lock_file_name = use_archive ? (archive_params.archive_name + ".lock") : ".lock";
        lock_file_before_first_file_checked = false;
        writing_finalized = false;

        /// Check that we can write a backup there and create the lock file to own this destination.
        checkBackupDoesntExist();
        if (!is_internal_backup)
            createLockFile();
        checkLockFile(true);
    }

    if (use_archive)
        openArchive();

    if (open_mode == OpenMode::READ)
        readBackupMetadata();

    if ((open_mode == OpenMode::WRITE) && base_backup_info)
        base_backup_uuid = getBaseBackupUnlocked()->getUUID();
}

void BackupImpl::close()
{
    std::lock_guard lock{mutex};
    closeArchive(/* finalize= */ false);
    writer.reset();
    reader.reset();
    coordination.reset();
}

void BackupImpl::openArchive()
{
    if (!use_archive)
        return;

    const String & archive_name = archive_params.archive_name;

    if (open_mode == OpenMode::READ)
    {
        if (!reader->fileExists(archive_name))
            throw Exception(ErrorCodes::BACKUP_NOT_FOUND, "Backup {} not found", backup_name_for_logging);
        size_t archive_size = reader->getFileSize(archive_name);
        archive_reader = createArchiveReader(archive_name, [my_reader = reader, archive_name]{ return my_reader->readFile(archive_name); }, archive_size);
        archive_reader->setPassword(archive_params.password);
    }
    else
    {
        archive_writer = createArchiveWriter(archive_name, writer->writeFile(archive_name));
        archive_writer->setPassword(archive_params.password);
        archive_writer->setCompression(archive_params.compression_method, archive_params.compression_level);
    }
}

void BackupImpl::closeArchive(bool finalize)
{
    if (finalize && archive_writer)
        archive_writer->finalize();

    archive_reader.reset();
    archive_writer.reset();
}

std::shared_ptr<const IBackup> BackupImpl::getBaseBackup() const
{
    std::lock_guard lock{mutex};
    return getBaseBackupUnlocked();
}

std::shared_ptr<const IBackup> BackupImpl::getBaseBackupUnlocked() const
{
    if (!base_backup && base_backup_info)
    {
        if (use_same_s3_credentials_for_base_backup)
            backup_info.copyS3CredentialsTo(*base_backup_info);

        BackupFactory::CreateParams params;
        params.backup_info = *base_backup_info;
        params.open_mode = OpenMode::READ;
        params.context = context;
        params.is_internal_backup = is_internal_backup;
        /// use_same_s3_credentials_for_base_backup should be inherited for base backups
        params.use_same_s3_credentials_for_base_backup = use_same_s3_credentials_for_base_backup;
        /// use_same_password_for_base_backup should be inherited for base backups
        params.use_same_password_for_base_backup = use_same_password_for_base_backup;

        if (params.use_same_password_for_base_backup)
            params.password = archive_params.password;

        base_backup = BackupFactory::instance().createBackup(params);

        if ((open_mode == OpenMode::READ) && (base_backup_uuid != base_backup->getUUID()))
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
    return base_backup;
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
    LOG_TRACE(log, "Backup {}: Writing metadata", backup_name_for_logging);
    auto timer = DB::CurrentThread::getProfileEvents().timer(ProfileEvents::BackupWriteMetadataMicroseconds);

    assert(!is_internal_backup);
    checkLockFile(true);

    std::unique_ptr<WriteBuffer> out;
    if (use_archive)
        out = archive_writer->writeFile(".backup");
    else
        out = writer->writeFile(".backup");

    *out << "<config>";
    *out << "<version>" << CURRENT_BACKUP_VERSION << "</version>";
    *out << "<deduplicate_files>" << deduplicate_files << "</deduplicate_files>";
    *out << "<timestamp>" << toString(LocalDateTime{timestamp}) << "</timestamp>";
    *out << "<uuid>" << toString(*uuid) << "</uuid>";

    auto all_file_infos = coordination->getFileInfosForAllHosts();

    if (all_file_infos.empty())
        throw Exception(ErrorCodes::BACKUP_IS_EMPTY, "Backup must not be empty");

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
            *out << "<base_backup>" << xml << base_backup_info->toString() << "</base_backup>";
            *out << "<base_backup_uuid>" << toString(*base_backup_uuid) << "</base_backup_uuid>";
        }
    }

    num_files = all_file_infos.size();
    total_size = 0;
    num_entries = 0;
    size_of_entries = 0;

    *out << "<contents>";
    for (const auto & info : all_file_infos)
    {
        *out << "<file>";

        *out << "<name>" << xml << info.file_name << "</name>";
        *out << "<size>" << info.size << "</size>";

        if (info.size)
        {
            *out << "<checksum>" << hexChecksum(info.checksum) << "</checksum>";
            if (info.base_size)
            {
                *out << "<use_base>true</use_base>";
                if (info.base_size != info.size)
                {
                    *out << "<base_size>" << info.base_size << "</base_size>";
                    *out << "<base_checksum>" << hexChecksum(info.base_checksum) << "</base_checksum>";
                }
            }
            if (!info.data_file_name.empty() && (info.data_file_name != info.file_name))
                *out << "<data_file>" << xml << info.data_file_name << "</data_file>";
            if (info.encrypted_by_disk)
                *out << "<encrypted_by_disk>true</encrypted_by_disk>";
        }

        total_size += info.size;
        bool has_entry = !deduplicate_files || (info.size && (info.size != info.base_size) && (info.data_file_name.empty() || (info.data_file_name == info.file_name)));
        if (has_entry)
        {
            ++num_entries;
            size_of_entries += info.size - info.base_size;
        }

        *out << "</file>";
    }
    *out << "</contents>";

    *out << "</config>";

    out->finalize();

    uncompressed_size = size_of_entries + out->count();

    LOG_TRACE(log, "Backup {}: Metadata was written", backup_name_for_logging);
}


void BackupImpl::readBackupMetadata()
{
    LOG_TRACE(log, "Backup {}: Reading metadata", backup_name_for_logging);
    auto timer = DB::CurrentThread::getProfileEvents().timer(ProfileEvents::BackupReadMetadataMicroseconds);

    using namespace XMLUtils;

    std::unique_ptr<ReadBuffer> in;
    if (use_archive)
    {
        if (!archive_reader->fileExists(".backup"))
            throw Exception(ErrorCodes::BACKUP_NOT_FOUND, "Archive {} is not a backup", backup_name_for_logging);
        setCompressedSize();
        in = archive_reader->readFile(".backup", /*throw_on_not_found=*/true);
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
            BackupFileInfo info;
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
                }
                info.encrypted_by_disk = getBool(file_config, "encrypted_by_disk", false);
            }

            file_names.emplace(info.file_name, std::pair{info.size, info.checksum});
            if (info.size)
                file_infos.try_emplace(std::pair{info.size, info.checksum}, info);

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
    if (!use_archive)
        setCompressedSize();

    LOG_TRACE(log, "Backup {}: Metadata was read", backup_name_for_logging);
}

void BackupImpl::checkBackupDoesntExist() const
{
    String file_name_to_check_existence;
    if (use_archive)
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
    if (open_mode != OpenMode::READ)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Backup is not opened for reading");

    String prefix = removeLeadingSlash(directory);
    if (!prefix.empty() && !prefix.ends_with('/'))
        prefix += '/';
    String terminator = recursive ? "" : "/";
    Strings elements;

    std::lock_guard lock{mutex};
    for (auto it = file_names.lower_bound(prefix); it != file_names.end(); ++it)
    {
        const String & name = it->first;
        if (!name.starts_with(prefix))
            break;
        size_t start_pos = prefix.length();
        size_t end_pos = String::npos;
        if (!terminator.empty())
            end_pos = name.find(terminator, start_pos);
        std::string_view new_element = std::string_view{name}.substr(start_pos, end_pos - start_pos);
        if (!elements.empty() && (elements.back() == new_element))
            continue;
        elements.push_back(String{new_element});
    }

    return elements;
}

bool BackupImpl::hasFiles(const String & directory) const
{
    if (open_mode != OpenMode::READ)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Backup is not opened for reading");

    String prefix = removeLeadingSlash(directory);
    if (!prefix.empty() && !prefix.ends_with('/'))
        prefix += '/';

    std::lock_guard lock{mutex};
    auto it = file_names.lower_bound(prefix);
    if (it == file_names.end())
        return false;

    const String & name = it->first;
    return name.starts_with(prefix);
}

bool BackupImpl::fileExists(const String & file_name) const
{
    if (open_mode != OpenMode::READ)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Backup is not opened for reading");

    auto adjusted_path = removeLeadingSlash(file_name);
    std::lock_guard lock{mutex};
    return file_names.contains(adjusted_path);
}

bool BackupImpl::fileExists(const SizeAndChecksum & size_and_checksum) const
{
    if (open_mode != OpenMode::READ)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Backup is not opened for reading");

    std::lock_guard lock{mutex};
    return file_infos.contains(size_and_checksum);
}

UInt64 BackupImpl::getFileSize(const String & file_name) const
{
    return getFileSizeAndChecksum(file_name).first;
}

UInt128 BackupImpl::getFileChecksum(const String & file_name) const
{
    return getFileSizeAndChecksum(file_name).second;
}

SizeAndChecksum BackupImpl::getFileSizeAndChecksum(const String & file_name) const
{
    if (open_mode != OpenMode::READ)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Backup is not opened for reading");

    auto adjusted_path = removeLeadingSlash(file_name);

    std::lock_guard lock{mutex};
    auto it = file_names.find(adjusted_path);
    if (it == file_names.end())
    {
        throw Exception(
            ErrorCodes::BACKUP_ENTRY_NOT_FOUND,
            "Backup {}: Entry {} not found in the backup",
            backup_name_for_logging,
            quoteString(file_name));
    }

    return it->second;
}

std::unique_ptr<SeekableReadBuffer> BackupImpl::readFile(const String & file_name) const
{
    return readFile(getFileSizeAndChecksum(file_name));
}

std::unique_ptr<SeekableReadBuffer> BackupImpl::readFile(const SizeAndChecksum & size_and_checksum) const
{
    return readFileImpl(size_and_checksum, /* read_encrypted= */ false);
}

std::unique_ptr<SeekableReadBuffer> BackupImpl::readFileImpl(const SizeAndChecksum & size_and_checksum, bool read_encrypted) const
{
    if (open_mode != OpenMode::READ)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Backup is not opened for reading");

    if (size_and_checksum.first == 0)
    {
        /// Entry's data is empty.
        std::lock_guard lock{mutex};
        ++num_read_files;
        return std::make_unique<ReadBufferFromMemory>(static_cast<char *>(nullptr), 0);
    }

    BackupFileInfo info;
    {
        std::lock_guard lock{mutex};
        auto it = file_infos.find(size_and_checksum);
        if (it == file_infos.end())
        {
            throw Exception(
                ErrorCodes::BACKUP_ENTRY_NOT_FOUND,
                "Backup {}: Entry {} not found in the backup",
                backup_name_for_logging,
                formatSizeAndChecksum(size_and_checksum));
        }
        info = it->second;
    }

    if (info.encrypted_by_disk != read_encrypted)
    {
        throw Exception(
            ErrorCodes::CANNOT_RESTORE_TO_NONENCRYPTED_DISK,
            "File {} is encrypted in the backup, it can be restored only to an encrypted disk",
            info.data_file_name);
    }

    std::unique_ptr<SeekableReadBuffer> read_buffer;
    std::unique_ptr<SeekableReadBuffer> base_read_buffer;

    if (info.size > info.base_size)
    {
        /// Make `read_buffer` if there is data for this backup entry in this backup.
        if (use_archive)
            read_buffer = archive_reader->readFile(info.data_file_name, /*throw_on_not_found=*/true);
        else
            read_buffer = reader->readFile(info.data_file_name);
    }

    if (info.base_size)
    {
        /// Make `base_read_buffer` if there is data for this backup entry in the base backup.
        auto base = getBaseBackup();
        if (!base)
        {
            throw Exception(
                ErrorCodes::NO_BASE_BACKUP,
                "Backup {}: Entry {} is marked to be read from a base backup, but there is no base backup specified",
                backup_name_for_logging, formatSizeAndChecksum(size_and_checksum));
        }

        if (!base->fileExists(std::pair(info.base_size, info.base_checksum)))
        {
            throw Exception(
                ErrorCodes::WRONG_BASE_BACKUP,
                "Backup {}: Entry {} is marked to be read from a base backup, but doesn't exist there",
                backup_name_for_logging, formatSizeAndChecksum(size_and_checksum));
        }

        base_read_buffer = base->readFile(std::pair{info.base_size, info.base_checksum});
    }

    {
        /// Update number of read files.
        std::lock_guard lock{mutex};
        ++num_read_files;
        num_read_bytes += info.size;
    }

    if (!info.base_size)
    {
        /// Data comes completely from this backup, the base backup isn't used.
        return read_buffer;
    }
    if (info.size == info.base_size)
    {
        /// Data comes completely from the base backup (nothing comes from this backup).
        return base_read_buffer;
    }

    /// The beginning of the data comes from the base backup,
    /// and the ending comes from this backup.
    return std::make_unique<ConcatSeekableReadBuffer>(
        std::move(base_read_buffer), info.base_size, std::move(read_buffer), info.size - info.base_size);
}

size_t BackupImpl::copyFileToDisk(const String & file_name,
                                  DiskPtr destination_disk, const String & destination_path, WriteMode write_mode) const
{
    return copyFileToDisk(getFileSizeAndChecksum(file_name), destination_disk, destination_path, write_mode);
}

size_t BackupImpl::copyFileToDisk(const SizeAndChecksum & size_and_checksum,
                                  DiskPtr destination_disk, const String & destination_path, WriteMode write_mode) const
{
    if (open_mode != OpenMode::READ)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Backup is not opened for reading");

    if (size_and_checksum.first == 0)
    {
        /// Entry's data is empty.
        if (write_mode == WriteMode::Rewrite)
        {
            /// Just create an empty file.
            destination_disk->createFile(destination_path);
        }
        std::lock_guard lock{mutex};
        ++num_read_files;
        return 0;
    }

    BackupFileInfo info;
    {
        std::lock_guard lock{mutex};
        auto it = file_infos.find(size_and_checksum);
        if (it == file_infos.end())
        {
            throw Exception(
                ErrorCodes::BACKUP_ENTRY_NOT_FOUND,
                "Backup {}: Entry {} not found in the backup",
                backup_name_for_logging,
                formatSizeAndChecksum(size_and_checksum));
        }
        info = it->second;
    }

    if (info.encrypted_by_disk && !destination_disk->getDataSourceDescription().is_encrypted)
    {
        throw Exception(
            ErrorCodes::CANNOT_RESTORE_TO_NONENCRYPTED_DISK,
            "File {} is encrypted in the backup, it can be restored only to an encrypted disk",
            info.data_file_name);
    }

    bool file_copied = false;

    if (info.size && !info.base_size && !use_archive)
    {
        /// Data comes completely from this backup.
        reader->copyFileToDisk(info.data_file_name, info.size, info.encrypted_by_disk, destination_disk, destination_path, write_mode);
        file_copied = true;
    }
    else if (info.size && (info.size == info.base_size))
    {
        /// Data comes completely from the base backup (nothing comes from this backup).
        getBaseBackup()->copyFileToDisk(std::pair{info.base_size, info.base_checksum}, destination_disk, destination_path, write_mode);
        file_copied = true;
    }

    if (file_copied)
    {
        /// The file is already copied, but `num_read_files` is not updated yet.
        std::lock_guard lock{mutex};
        ++num_read_files;
        num_read_bytes += info.size;
    }
    else
    {
        /// Use the generic way to copy data. `readFile()` will update `num_read_files`.
        auto read_buffer = readFileImpl(size_and_checksum, /* read_encrypted= */ info.encrypted_by_disk);
        std::unique_ptr<WriteBuffer> write_buffer;
        size_t buf_size = std::min<size_t>(info.size, reader->getWriteBufferSize());
        if (info.encrypted_by_disk)
            write_buffer = destination_disk->writeEncryptedFile(destination_path, buf_size, write_mode, reader->getWriteSettings());
        else
            write_buffer = destination_disk->writeFile(destination_path, buf_size, write_mode, reader->getWriteSettings());
        copyData(*read_buffer, *write_buffer, info.size);
        write_buffer->finalize();
    }

    return info.size;
}


void BackupImpl::writeFile(const BackupFileInfo & info, BackupEntryPtr entry)
{
    /// we don't write anything for reference files
    if (entry->isReference())
        return;

    if (open_mode != OpenMode::WRITE)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Backup is not opened for writing");

    if (writing_finalized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Backup is already finalized");

    {
        std::lock_guard lock{mutex};
        ++num_files;
        total_size += info.size;
    }

    auto src_disk = entry->getDisk();
    auto src_file_path = entry->getFilePath();
    bool from_immutable_file = entry->isFromImmutableFile();
    String src_file_desc = src_file_path.empty() ? "memory buffer" : ("file " + src_file_path);

    if (info.data_file_name.empty())
    {
        LOG_TRACE(log, "Writing backup for file {} from {}: skipped, {}", info.data_file_name, src_file_desc, !info.size ? "empty" : "base backup has it");
        return;
    }

    if (!coordination->startWritingFile(info.data_file_index))
    {
        LOG_TRACE(log, "Writing backup for file {} from {}: skipped, data file #{} is already being written", info.data_file_name, src_file_desc, info.data_file_index);
        return;
    }

    if (!lock_file_before_first_file_checked.exchange(true))
        checkLockFile(true);

    /// NOTE: `mutex` must be unlocked during copying otherwise writing will be in one thread maximum and hence slow.

    const auto write_info_to_archive = [&](const auto & file_name)
    {
        auto out = archive_writer->writeFile(file_name, info.size);
        auto read_buffer = entry->getReadBuffer(writer->getReadSettings());
        if (info.base_size != 0)
            read_buffer->seek(info.base_size, SEEK_SET);
        copyData(*read_buffer, *out);
        out->finalize();
    };

    if (use_archive)
    {
        LOG_TRACE(log, "Writing backup for file {} from {}: data file #{}, adding to archive", info.data_file_name, src_file_desc, info.data_file_index);
        write_info_to_archive(info.data_file_name);
    }
    else if (src_disk && from_immutable_file)
    {
        LOG_INFO(log, "Writing backup for file {} from {} (disk {}): data file #{}", info.data_file_name, src_file_desc, src_disk->getName(), info.data_file_index);
        writer->copyFileFromDisk(info.data_file_name, src_disk, src_file_path, info.encrypted_by_disk, info.base_size, info.size - info.base_size);
    }
    else
    {
        LOG_INFO(log, "Writing backup for file {} from {}: data file #{}", info.data_file_name, src_file_desc, info.data_file_index);
        auto create_read_buffer = [entry, read_settings = writer->getReadSettings()] { return entry->getReadBuffer(read_settings); };
        writer->copyDataToFile(info.data_file_name, create_read_buffer, info.base_size, info.size - info.base_size);
    }

    std::function<void(const String &)> copy_file_inside_backup;
    if (use_archive)
    {
        copy_file_inside_backup = write_info_to_archive;
    }
    else
    {
        copy_file_inside_backup = [&](const auto & data_file_copy)
        {
            writer->copyFile(data_file_copy, info.data_file_name, info.size - info.base_size);
        };
    }

    std::ranges::for_each(info.data_file_copies, copy_file_inside_backup);

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

    if (!is_internal_backup)
    {
        LOG_TRACE(log, "Finalizing backup {}", backup_name_for_logging);
        writeBackupMetadata();
        closeArchive(/* finalize= */ true);
        setCompressedSize();
        removeLockFile();
        LOG_TRACE(log, "Finalized backup {}", backup_name_for_logging);
    }

    writing_finalized = true;
}


void BackupImpl::setCompressedSize()
{
    if (use_archive)
        compressed_size = writer ? writer->getFileSize(archive_params.archive_name) : reader->getFileSize(archive_params.archive_name);
    else
        compressed_size = uncompressed_size;
}


void BackupImpl::tryRemoveAllFiles()
{
    if (open_mode != OpenMode::WRITE)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Backup is not opened for writing");

    if (is_internal_backup)
        return;

    try
    {
        LOG_INFO(log, "Removing all files of backup {}", backup_name_for_logging);
        closeArchive(/* finalize= */ false);

        Strings files_to_remove;
        if (use_archive)
        {
            files_to_remove.push_back(archive_params.archive_name);
        }
        else
        {
            files_to_remove.push_back(".backup");
            for (const auto & file_info : coordination->getFileInfosForAllHosts())
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
