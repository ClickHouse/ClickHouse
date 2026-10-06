#include <Backups/BackupImpl.h>
#include <Backups/BackupFactory.h>
#include <Backups/BackupFileInfo.h>
#include <Backups/BackupMetadataHandler.h>
#include <Backups/BackupIO.h>
#include <Backups/IBackupEntry.h>
#include <Backups/BackupIO_S3.h>
#include <Backups/getBackupDataFileName.h>
#include <Common/CurrentThread.h>
#include <Common/ProfileEvents.h>
#include <Common/FailPoint.h>
#include <Common/StackTrace.h>
#include <Common/StringUtils.h>
#include <base/hex.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Core/UUID.h>
#include <IO/Archives/IArchiveReader.h>
#include <IO/Archives/IArchiveWriter.h>
#include <IO/Archives/createArchiveReader.h>
#include <IO/Archives/createArchiveWriter.h>
#include <IO/ConcatReadBufferFromFile.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <IO/copyData.h>
#include <Poco/Util/XMLConfiguration.h>
#if CLICKHOUSE_CLOUD && USE_SSL
#include <Backups/BackupEncryptionSidecar.h>
#endif
#include <Poco/SAX/SAXParser.h>
#include <Poco/SAX/XMLReader.h>

#include <charconv>
#include <filesystem>


namespace ProfileEvents
{
    extern const Event BackupsOpenedForRead;
    extern const Event BackupsOpenedForWrite;
    extern const Event BackupsOpenedForUnlock;
    extern const Event BackupReadMetadataMicroseconds;
    extern const Event BackupWriteMetadataMicroseconds;
    extern const Event BackupLockFileReads;
}

namespace DB
{

namespace FailPoints
{
    extern const char backup_fail_before_writing_metadata[];
    extern const char backup_fail_lock_file_removal[];
    extern const char backup_pause_before_lock_file_creation[];
}

namespace ErrorCodes
{
    extern const int BACKUP_NOT_FOUND;
    extern const int BACKUP_ALREADY_EXISTS;
    extern const int BACKUP_VERSION_NOT_SUPPORTED;
    extern const int BACKUP_DAMAGED;
    extern const int BAD_ARGUMENTS;
    extern const int NO_BASE_BACKUP;
    extern const int WRONG_BASE_BACKUP;
    extern const int BACKUP_ENTRY_NOT_FOUND;
    extern const int BACKUP_IS_EMPTY;
    extern const int CANNOT_RESTORE_TO_NONENCRYPTED_DISK;
    extern const int FAILED_TO_SYNC_BACKUP_OR_RESTORE;
    extern const int LOGICAL_ERROR;
    extern const int INSECURE_PATH;
    extern const int FAULT_INJECTED;
}

namespace fs = std::filesystem;

namespace
{
    const int INITIAL_BACKUP_VERSION = 1;
    /// We may use lightweight backup in version 2.
    const int CURRENT_BACKUP_VERSION = 2;
    constexpr auto BASE_BACKUP_COPY_S3_CREDENTIALS_FROM_BACKUP = "base_backup_copy_s3_credentials_from_backup";

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

    /// Validate that a file name from a backup does not contain path traversal sequences.
    /// This prevents a corrupted or tampered backup from accessing files outside the intended directories during restore.
    void validateFileNameFromBackup(const String & file_name, const String & field_name, const String & backup_name_for_logging)
    {
        fs::path path(file_name);

        /// Reject absolute or rooted paths.
        if (path.is_absolute() || path.has_root_name() || path.has_root_directory())
            throw Exception(
                ErrorCodes::INSECURE_PATH,
                "Backup {}: <{}> {} is an absolute path, which is not allowed",
                backup_name_for_logging,
                field_name,
                quoteString(file_name));

        /// Normalize the path and check that it does not escape the backup root.
        auto normalized = path.lexically_normal();

        /// Reject empty or degenerate paths.
        if (normalized.empty() || normalized == fs::path("."))
            throw Exception(
                ErrorCodes::BACKUP_DAMAGED,
                "Backup {}: <{}> {} is empty or invalid",
                backup_name_for_logging,
                field_name,
                quoteString(file_name));

        /// After normalization, a path that escapes the root starts with "..".
        if (*normalized.begin() == "..")
            throw Exception(
                ErrorCodes::INSECURE_PATH,
                "Backup {}: <{}> {} resolves to a path outside the backup, which is not allowed",
                backup_name_for_logging,
                field_name,
                quoteString(file_name));

        /// The name is kept verbatim, and `listFiles` cuts a directory prefix off it by byte offset, so a
        /// name that is not already normalized yields a remainder that is rooted or escapes its directory.
        /// Compare the strings: two `fs::path` objects compare element-wise, so "a//b" equals "a/b".
        if (normalized.string() != file_name)
            throw Exception(
                ErrorCodes::INSECURE_PATH,
                "Backup {}: <{}> {} is not a normalized path, which is not allowed",
                backup_name_for_logging,
                field_name,
                quoteString(file_name));
    }
}


BackupImpl::BackupImpl(
    BackupFactory::CreateParams params_,
    const ArchiveParams & archive_params_,
    std::shared_ptr<IBackupReader> reader_,
    SnapshotReaderCreator lightweight_snapshot_reader_creator_)
    : params(std::move(params_))
    , backup_info(params.backup_info)
    , backup_name_for_logging(backup_info.toStringForLogging())
    , use_archive(!archive_params_.archive_name.empty())
    , archive_params(archive_params_)
    , open_mode(OpenMode::READ)
    , reader(std::move(reader_))
    , lightweight_snapshot_reader_creator(lightweight_snapshot_reader_creator_)
    , version(INITIAL_BACKUP_VERSION)
    , base_backup_info(params.base_backup_info)
    , log(getLogger("BackupImpl"))
{
    open();
}


BackupImpl::BackupImpl(
    BackupFactory::CreateParams params_,
    const ArchiveParams & archive_params_,
    std::shared_ptr<IBackupWriter> writer_)
    : params(std::move(params_))
    , backup_info(params.backup_info)
    , backup_name_for_logging(backup_info.toStringForLogging())
    , use_archive(!archive_params_.archive_name.empty())
    , archive_params(archive_params_)
    , open_mode(OpenMode::WRITE)
    , writer(std::move(writer_))
    , data_file_name_generator(params.data_file_name_generator)
    , data_file_name_prefix_length(params.data_file_name_prefix_length)
    , coordination(params.backup_coordination)
    , uuid(params.backup_uuid)
    , backup_id(params.backup_id)
    , version(CURRENT_BACKUP_VERSION)
    , base_backup_info(params.base_backup_info)
    , log(getLogger("BackupImpl"))
{
    open();
}

BackupImpl::BackupImpl(
    const BackupInfo & backup_info_,
    const ArchiveParams & archive_params_,
    std::shared_ptr<IBackupReader> reader_)
    : backup_info(backup_info_)
    , backup_name_for_logging(backup_info.toStringForLogging())
    , use_archive(!archive_params_.archive_name.empty())
    , archive_params(archive_params_)
    , open_mode(OpenMode::UNLOCK)
    , reader(reader_)
    , log(getLogger("BackupImpl"))
{
    open();
}

BackupImpl::~BackupImpl()
{
    if ((open_mode == OpenMode::WRITE) && !writing_finalized && !corrupted)
    {
        /// It is suspicious to destroy BackupImpl without finalization while writing a backup when there is no exception.
        LOG_ERROR(log, "BackupImpl is not finalized or marked as corrupted when destructor is called. Stack trace: {}", StackTrace().toString());
        chassert(false, "BackupImpl is not finalized or marked as corrupted when destructor is called.");
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

#if CLICKHOUSE_CLOUD && USE_SSL
    encryption_sidecar = std::make_unique<BackupEncryptionSidecar>(*this);
#endif

    if (open_mode == OpenMode::UNLOCK)
    {
        ProfileEvents::increment(ProfileEvents::BackupsOpenedForUnlock);
        LOG_INFO(log, "Unlocking backup: {}", backup_name_for_logging);
    }
    else if (open_mode == OpenMode::READ)
    {
        ProfileEvents::increment(ProfileEvents::BackupsOpenedForRead);
        LOG_INFO(log, "Reading backup: {}", backup_name_for_logging);
    }
    else
    {
        ProfileEvents::increment(ProfileEvents::BackupsOpenedForWrite);
        timestamp = std::time(nullptr);
        lock_file_name = use_archive ? (archive_params.archive_name + ".lock") : ".lock";
        lock_file_before_first_file_checked = false;
        writing_finalized = false;

        /// `open` runs from the constructor, so a throw anywhere below leaves no backup behind for
        /// anything else to clean up. The lock must not outlive the attempt that created it: it fences
        /// the destination against every later one, which cannot match it either, because a retry picks
        /// a fresh backup UUID. That covers opening the archive as much as taking the lock.
        try
        {
#if CLICKHOUSE_CLOUD
            if (params.resume)
                BackupResumer(*this, *params.resume).openDestination();
            else
#endif
            {
                LOG_INFO(log, "Writing backup: {}", backup_name_for_logging);
                if (!uuid)
                    uuid = UUIDHelpers::generateV4();

                /// Check that we can write a backup there and create the lock file to own this destination.
                checkBackupDoesntExist();
                if (!params.is_internal_backup)
                    createLockFile();
                checkLockFile(true);
            }

            if (use_archive)
                openArchive();
        }
        catch (...)
        {
            if (!params.is_internal_backup)
                tryRemoveOwnLockFile();
            throw;
        }
    }

    /// A write opens the archive inside the guard above, where a failure still removes the lock.
    if (use_archive && open_mode != OpenMode::WRITE)
        openArchive();

    if (open_mode == OpenMode::READ || open_mode == OpenMode::UNLOCK)
        readBackupMetadata();
}

void BackupImpl::close()
{
    std::lock_guard lock{mutex};
    closeArchive(/* finalize= */ false);
    writer.reset();
    reader.reset();
    lightweight_snapshot_reader.reset();
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
        archive_writer = createArchiveWriter(
            archive_name, writer->writeFile(archive_name), DBMS_DEFAULT_BUFFER_SIZE, archive_params.adaptive_buffer_max_size);
        archive_writer->setPassword(archive_params.password);
        archive_writer->setCompression(archive_params.compression_method, archive_params.compression_level);
    }
}

void BackupImpl::closeArchive(bool finalize)
{
    if (archive_writer)
    {
        if (finalize)
            archive_writer->finalize();
        else
            archive_writer->cancel();
    }

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
        /// Copy the credentials into a local copy only used for opening the base backup.
        /// The stored `base_backup_info` must stay unchanged because `writeBackupMetadata`
        /// serializes it into the `.backup` file, and the copied credentials must not be persisted there.
        BackupInfo effective_base_backup_info = *base_backup_info;
        if (params.use_same_s3_credentials_for_base_backup)
        {
            backup_info.copyS3CredentialsTo(effective_base_backup_info, params.context);
        }
        else if (base_backup_copy_s3_credentials_from_backup && backup_info.canCopyS3CredentialsTo(effective_base_backup_info, params.context))
        {
            /// Metadata marker asks to copy credentials from this backup locator at restore time.
            backup_info.copyS3CredentialsTo(effective_base_backup_info, params.context);
        }

        BackupFactory::CreateParams base_params = params.getCreateParamsForBaseBackup(std::move(effective_base_backup_info), archive_params.password);
        base_backup = BackupFactory::instance().createBackup(base_params);

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

        base_backup_uuid = base_backup->getUUID();
    }
    return base_backup;
}

std::map<String, String> BackupImpl::getEngineSettings() const
{
    std::lock_guard lock{mutex};

    /// Both a BACKUP and a RESTORE can involve more than one engine with different endpoint settings, which
    /// a flat map cannot represent: an incremental BACKUP writes through `writer` but also reads from the
    /// base backup, and a RESTORE reads from the base backup (incremental restores) and/or the lightweight
    /// snapshot reader in addition to the top-level backup. Report the engine settings only when a single
    /// engine is involved; otherwise omit them.
    if (base_backup_info || lightweight_snapshot_reader)
        return {};

    if (writer)
        return writer->getSerializedSettings();

    if (reader)
        return reader->getSerializedSettings();

    return {};
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

    chassert(!params.is_internal_backup);
    checkLockFile(true);

#if CLICKHOUSE_CLOUD
    /// A Keeper session can expire while this upload is in flight. The progress fingerprint covers every
    /// input written to the manifest, so a new owner can only publish the same metadata bytes.
    if (params.resume)
        params.resume->check_owner();
#endif

    std::unique_ptr<WriteBuffer> out;
    if (use_archive)
        out = archive_writer->writeFile(".backup");
    else
        out = writer->writeFile(".backup");

    *out << "<config>";
    *out << "<version>" << (params.is_lightweight_snapshot ? CURRENT_BACKUP_VERSION : INITIAL_BACKUP_VERSION) << "</version>";
    *out << "<deduplicate_files>" << params.deduplicate_files << "</deduplicate_files>";
    *out << "<timestamp>"
#if CLICKHOUSE_CLOUD
         /// A continued attempt republishes the timestamp of the one it continues, byte for byte.
         << (params.resume ? params.resume->timestamp_text : toString(LocalDateTime{timestamp}))
#else
         << toString(LocalDateTime{timestamp})
#endif
         << "</timestamp>";
    *out << "<uuid>" << toString(*uuid) << "</uuid>";
    if (!backup_id.empty())
        *out << "<backup_id>" << xml << backup_id << "</backup_id>";
    if (data_file_name_generator != BackupDataFileNameGeneratorType::FirstFileName)
        *out << "<data_file_name_generator>" << SettingFieldBackupDataFileNameGeneratorTypeTraits::toString(data_file_name_generator)
             << "</data_file_name_generator>";

    /// Iterate in place instead of copying all file infos (a backup can contain millions).
    size_t num_all_file_infos = 0;
    bool base_backup_in_use = false;
    coordination->forEachFileInfoForAllHosts([&](const BackupFileInfo & info)
    {
        ++num_all_file_infos;
        if (info.base_size)
            base_backup_in_use = true;
    });

    if (num_all_file_infos == 0)
        throw Exception(ErrorCodes::BACKUP_IS_EMPTY, "Backup must not be empty");

    if (base_backup_info)
    {
        if (base_backup_in_use)
        {
            /// Persist base backup locators without inline `S3` credentials.
            BackupInfo effective_base_backup_info = *base_backup_info;
            if (params.use_same_s3_credentials_for_base_backup)
                backup_info.copyS3CredentialsTo(effective_base_backup_info, params.context);

            const BackupInfo base_backup_info_for_metadata = effective_base_backup_info.withoutS3Credentials(params.context);
            const bool base_backup_credentials_were_stripped = base_backup_info_for_metadata.toString() != effective_base_backup_info.toString();
            bool base_backup_can_use_this_backup_credentials = false;

            if (base_backup_credentials_were_stripped && backup_info.canCopyS3CredentialsTo(base_backup_info_for_metadata, params.context))
            {
                BackupInfo base_backup_info_with_this_backup_credentials = base_backup_info_for_metadata;
                backup_info.copyS3CredentialsTo(base_backup_info_with_this_backup_credentials, params.context);
                base_backup_can_use_this_backup_credentials = base_backup_info_with_this_backup_credentials.toString() == effective_base_backup_info.toString();
            }

            *out << "<base_backup>" << xml << base_backup_info_for_metadata.toString() << "</base_backup>";
            *out << "<base_backup_uuid>" << getBaseBackupUnlocked()->getUUID() << "</base_backup_uuid>";
            if (base_backup_can_use_this_backup_credentials)
                *out << "<" << BASE_BACKUP_COPY_S3_CREDENTIALS_FROM_BACKUP << ">true</"
                     << BASE_BACKUP_COPY_S3_CREDENTIALS_FROM_BACKUP << ">";
        }
    }

    if (params.is_lightweight_snapshot)
    {
        *out << "<original_endpoint>" << original_endpoint << "</original_endpoint>";
        *out << "<original_namespace>" << original_namespace << "</original_namespace>";
    }

    num_files = num_all_file_infos;
    total_size = 0;
    num_entries = 0;
    size_of_entries = 0;

    *out << "<contents>";
    coordination->forEachFileInfoForAllHosts([&](const BackupFileInfo & info)
    {
        *out << "<file>";

        *out << "<name>" << xml << info.file_name << "</name>";
        *out << "<size>" << info.size << "</size>";

        if (!info.object_key.empty())
        {
            *out << "<object_key>" << info.object_key << "</object_key>";
            if (original_endpoint.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "In lightweight snapshot backup, the endpoint should not be empty. Do not run this command with `ON CLUSTER`");
        }

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
        bool has_entry = !params.deduplicate_files
            || (info.size && (info.size != info.base_size)
                && (info.data_file_name.empty()
                    || info.data_file_name == getBackupDataFileName(info, data_file_name_generator, data_file_name_prefix_length)));
        if (has_entry)
        {
            ++num_entries;
            size_of_entries += info.size - info.base_size;
        }

        *out << "</file>";
    });
    *out << "</contents>";

    *out << "</config>";

    out->finalize();

    uncompressed_size = size_of_entries + out->count();
#if CLICKHOUSE_CLOUD && USE_SSL
    uncompressed_size += encryption_sidecar->getFileSize();
#endif

    LOG_TRACE(log, "Backup {}: Metadata was written", backup_name_for_logging);
}


#if CLICKHOUSE_CLOUD
void BackupImpl::recalculateMetadataCounters()
{
    num_files = 0;
    total_size = 0;
    num_entries = 0;
    size_of_entries = 0;

    coordination->forEachFileInfoForAllHosts([&](const BackupFileInfo & info)
    {
        ++num_files;
        total_size += info.size;
        const bool has_entry = !params.deduplicate_files
            || (info.size && info.size != info.base_size
                && (info.data_file_name.empty()
                    || info.data_file_name == getBackupDataFileName(info, data_file_name_generator, data_file_name_prefix_length)));
        if (has_entry)
        {
            ++num_entries;
            size_of_entries += info.size - info.base_size;
        }
    });

    uncompressed_size = size_of_entries + writer->getFileSize(".backup");
#if USE_SSL
    uncompressed_size += encryption_sidecar->getFileSize();
#endif
}
#endif


void BackupImpl::readBackupMetadata()
{
    LOG_TRACE(log, "Backup {}: Reading metadata", backup_name_for_logging);
    auto timer = DB::CurrentThread::getProfileEvents().timer(ProfileEvents::BackupReadMetadataMicroseconds);

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

    num_files = 0;
    total_size = 0;
    num_entries = 0;
    size_of_entries = 0;

    bool contents_seen = false;

    /// Strict parsers: reject trailing garbage / unknown boolean text (fail closed with BACKUP_DAMAGED)
    /// instead of DB::parse's lenient truncation (e.g. <size>12x34</size> read as 12).
    auto to_uint64 = [&](const String & value, const String & key) -> UInt64
    {
        UInt64 result = 0;
        const char * begin = value.data();
        const char * end = begin + value.size();
        auto [ptr, ec] = std::from_chars(begin, end, result);
        if (ec != std::errc{} || ptr != end)
            throw Exception(
                ErrorCodes::BACKUP_DAMAGED, "Backup {}: Cannot parse <{}> value {}", backup_name_for_logging, key, quoteString(value));
        return result;
    };
    auto to_bool = [&](const String & value, const String & key) -> bool
    {
        if (value == "true" || value == "1")
            return true;
        if (value == "false" || value == "0")
            return false;
        throw Exception(
            ErrorCodes::BACKUP_DAMAGED, "Backup {}: Cannot parse <{}> boolean value {}", backup_name_for_logging, key, quoteString(value));
    };

    BackupMetadataHandler handler;

    handler.on_header = [&](const BackupMetadataHandler::Fields & h)
    {
        auto req = [&](const String & key) -> const String &
        {
            auto it = h.find(key);
            if (it == h.end())
                throw Exception(
                    ErrorCodes::BACKUP_DAMAGED, "Backup {}: Cannot read <{}> from metadata", backup_name_for_logging, key);
            return it->second;
        };

        /// Range-check the parsed UInt64 before narrowing to int: a value that fits in UInt64 but not in
        /// int would otherwise wrap (e.g. 4294967298 -> 2) and pass the supported-range check.
        const auto version_value = to_uint64(req("version"), "version");
        if ((version_value < INITIAL_BACKUP_VERSION) || (version_value > CURRENT_BACKUP_VERSION))
            throw Exception(
                ErrorCodes::BACKUP_VERSION_NOT_SUPPORTED, "Backup {}: Version {} is not supported", backup_name_for_logging, version_value);
        version = static_cast<int>(version_value);

        timestamp = parse<::LocalDateTime>(req("timestamp")).to_time_t();
        uuid = parse<UUID>(req("uuid"));

        if (h.contains("backup_id"))
            backup_id = req("backup_id");

        if (h.contains("base_backup") && !base_backup_info)
        {
            base_backup_info = BackupInfo::fromString(req("base_backup"));

            /// The marker is honored only when the base backup locator itself comes from the metadata:
            /// if the locator was overridden with the `base_backup` setting, the override is used as is.
            auto it = h.find(BASE_BACKUP_COPY_S3_CREDENTIALS_FROM_BACKUP);
            base_backup_copy_s3_credentials_from_backup
                = (it != h.end()) && to_bool(it->second, BASE_BACKUP_COPY_S3_CREDENTIALS_FROM_BACKUP);
        }

        if (h.contains("base_backup_uuid"))
            base_backup_uuid = parse<UUID>(req("base_backup_uuid"));

        if (h.contains("original_endpoint"))
            original_endpoint = req("original_endpoint");
        if (h.contains("original_namespace"))
            original_namespace = req("original_namespace");

        contents_seen = true;
    };

    /// `readBackupMetadata` runs under `mutex` (TSA_REQUIRES), and `on_file` is invoked synchronously from
    /// `parseMemoryNP` below while that lock is held, so the guarded members are safe to touch here. TSA cannot
    /// see through the lambda boundary, hence the explicit suppression.
    handler.on_file = [&](const BackupMetadataHandler::Fields & f) TSA_NO_THREAD_SAFETY_ANALYSIS
    {
        auto req = [&](const String & key) -> const String &
        {
            auto it = f.find(key);
            if (it == f.end())
                throw Exception(
                    ErrorCodes::BACKUP_DAMAGED, "Backup {}: Cannot read <{}> of a file from metadata", backup_name_for_logging, key);
            return it->second;
        };
        auto opt = [&](const String & key, const String & def) -> String
        {
            auto it = f.find(key);
            return it == f.end() ? def : it->second;
        };
        auto get_bool = [&](const String & key, bool def)
        {
            auto it = f.find(key);
            return it == f.end() ? def : to_bool(it->second, key);
        };

        BackupFileInfo info;
        info.file_name = req("name");
        validateFileNameFromBackup(info.file_name, "name", backup_name_for_logging);
        info.object_key = opt("object_key", "");
        info.size = to_uint64(req("size"), "size");
        if (info.size)
        {
            info.checksum = unhexChecksum(req("checksum"));

            bool use_base = get_bool("use_base", false);
            auto base_size_it = f.find("base_size");
            info.base_size = (base_size_it != f.end()) ? to_uint64(base_size_it->second, "base_size") : (use_base ? info.size : 0);
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
                    info.base_checksum = unhexChecksum(req("base_checksum"));
            }

            if (info.size > info.base_size)
            {
                info.data_file_name = opt("data_file", info.file_name);
                if (info.data_file_name != info.file_name)
                    validateFileNameFromBackup(info.data_file_name, "data_file", backup_name_for_logging);
            }
            info.encrypted_by_disk = get_bool("encrypted_by_disk", false);
        }

        const auto size_and_checksum = std::pair{info.size, info.checksum};

        /// Update counters before `info` is moved below.
        ++num_files;
        total_size += info.size;
        bool has_entry = !params.deduplicate_files || (info.size && (info.size != info.base_size) && (info.data_file_name.empty() || info.data_file_name == info.file_name));
        if (has_entry)
        {
            ++num_entries;
            size_of_entries += info.size - info.base_size;
        }

        if (!info.object_key.empty())
        {
            if (original_endpoint.empty() || original_namespace.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "In lightweight snapshot backup, the endpoint or namespace should be not empty. We cannot restore this file.");

            /// UNLOCK only reads table metadata to remove the snapshot's locks, never the data parts
            /// (`object_key` entries). Skipping their bookkeeping avoids holding one `BackupFileInfo` per
            /// part, which OOMs the server for snapshots with millions of parts.
            if (open_mode != OpenMode::UNLOCK)
            {
                if (open_mode == OpenMode::READ)
                    lightweight_snapshot_reader = lightweight_snapshot_reader_creator(original_endpoint, original_namespace);

                file_names.emplace(info.file_name, size_and_checksum);
                file_object_keys.emplace(info.file_name, info.object_key);
                /// The key is copied from `info.object_key` before `info` is moved into the value.
                lightweight_snapshot_file_infos.try_emplace(info.object_key, std::move(info));
            }
        }
        else
        {
            file_names.emplace(info.file_name, size_and_checksum);
            if (info.size)
                file_infos.try_emplace(size_and_checksum, std::move(info));
        }
    };

    Poco::XML::SAXParser xml_parser;
    xml_parser.setContentHandler(&handler);
    /// Keep the namespace prefix in the element name (the old DOM parser enabled this too). Without it a
    /// prefixed element like <x:contents> arrives as local name "contents" and would be accepted as an
    /// ordinary element; with it the handler sees "x:contents" and ignores it (writeBackupMetadata never
    /// emits namespaces, so this only rejects hand-crafted manifests).
    xml_parser.setFeature(Poco::XML::XMLReader::FEATURE_NAMESPACE_PREFIXES, true);
    try
    {
        xml_parser.parseMemoryNP(str.data(), str.size());
    }
    catch (...)
    {
        /// A callback exception captured earlier is the root cause; prefer it over a secondary XML parse
        /// error that a callback failure may have led to.
        if (handler.saved_exception)
            std::rethrow_exception(handler.saved_exception);
        throw;
    }

    /// Callbacks must not throw through expat; a captured exception is rethrown here.
    if (handler.saved_exception)
        std::rethrow_exception(handler.saved_exception);

    /// A well-formed but incomplete manifest (no <contents>) leaves the header unapplied - version/uuid
    /// unset - and must be rejected instead of being treated as an empty backup.
    if (!contents_seen)
        throw Exception(ErrorCodes::BACKUP_DAMAGED, "Backup {}: Metadata has no <contents>", backup_name_for_logging);

    uncompressed_size = size_of_entries + str.size();

#if CLICKHOUSE_CLOUD && USE_SSL
    /// A backup written with an encryption config file next to it carries the TDE key information of that
    /// file (a backup created from this one may carry it further, see `BACKUP FROM SNAPSHOT`), and counts
    /// the file in its sizes the same way as when it was written.
    if (open_mode == OpenMode::READ)
    {
        encryption_sidecar->read();
        uncompressed_size += encryption_sidecar->getFileSize();
    }
#endif
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
#if CLICKHOUSE_CLOUD && USE_SSL
    if (encryption_sidecar->existsInDestination())
        throw Exception(ErrorCodes::BACKUP_ALREADY_EXISTS, "Backup {} already exists", backup_name_for_logging);
#endif

    /// Check that no other backup (excluding internal backups) is writing to the same destination.
    if (!params.is_internal_backup)
    {
        chassert(!lock_file_name.empty());
        if (writer->fileExists(lock_file_name))
            throw Exception(ErrorCodes::BACKUP_ALREADY_EXISTS, "Backup {} is being written already", backup_name_for_logging);
    }
}

void BackupImpl::createLockFile()
{
    /// Internal backup must not create the lock file (it should be created by the initiator).
    chassert(!params.is_internal_backup);

    chassert(uuid);
    if (lock_file_contents.empty())
        lock_file_contents = toString(*uuid);
    const String completed_file = use_archive ? archive_params.archive_name : ".backup";
    FailPointInjection::pauseFailPoint(FailPoints::backup_pause_before_lock_file_creation);
    try
    {
        auto out = writer->writeFileIfNotExists(lock_file_name);
        *out << lock_file_contents;
        out->finalize();
        created_own_lock_file = true;
    }
    catch (...)
    {
        auto exception = std::current_exception();
        String actual_file_contents;
        bool lock_contents_match = false;
        /// The write may have committed the lock, and no check below is guaranteed to observe it: each
        /// issues its own request and can fail on its own. So the lock is this `open`'s to take back
        /// unless it continues an earlier attempt; `removeLockFile` re-reads it and has the final say.
#if CLICKHOUSE_CLOUD
        if (!params.resume || !params.resume->continuing_existing_progress)
#endif
            created_own_lock_file = true;
        try
        {
            lock_contents_match = writer->fileContentsEqual(lock_file_name, lock_file_contents, actual_file_contents);
        }
        catch (...)
        {
            /// The lock can be removed while we read it, by the backup that created it. The existence
            /// checks below decide who owns the destination, and rethrow if they find nothing.
            tryLogCurrentException(__PRETTY_FUNCTION__, fmt::format("Could not read lock file {}", lock_file_name));
        }
#if CLICKHOUSE_CLOUD
        /// A resumable attempt whose own contents are already there falls through, so a later failure
        /// lands inside `BackupResumer`'s inner try, which reports the lock and keeps its progress.
        if (lock_contents_match && !params.resume)
#else
        if (lock_contents_match)
#endif
            throw Exception(
                ErrorCodes::BACKUP_ALREADY_EXISTS,
                "A concurrent backup writing to the same destination {} detected",
                backup_name_for_logging);
        if (!lock_contents_match)
        {
            if (writer->fileExists(lock_file_name))
                throw Exception(
                    ErrorCodes::BACKUP_ALREADY_EXISTS,
                    "A concurrent backup writing to the same destination {} detected",
                    backup_name_for_logging);
            if (writer->fileExists(completed_file))
                throw Exception(ErrorCodes::BACKUP_ALREADY_EXISTS, "Backup {} already exists", backup_name_for_logging);
            std::rethrow_exception(exception);
        }
    }

    if (writer->fileExists(completed_file))
    {
        tryRemoveOwnLockFile();
        throw Exception(ErrorCodes::BACKUP_ALREADY_EXISTS, "Backup {} already exists", backup_name_for_logging);
    }
}

bool BackupImpl::checkLockFile(bool throw_if_failed) const
{
    try
    {
        if (!lock_file_name.empty() && uuid)
        {
            LOG_TRACE(log, "Checking lock file {}", lock_file_name);
            ProfileEvents::increment(ProfileEvents::BackupLockFileReads);
            String actual_file_contents;
            const String expected_file_contents = lock_file_contents.empty() ? toString(*uuid) : lock_file_contents;
            if (writer->fileContentsEqual(lock_file_name, expected_file_contents, actual_file_contents))
                return true;
            LOG_TRACE(log, "Lock file {} contents do not match, expected: {}, actual: {}", lock_file_name, expected_file_contents, actual_file_contents);
        }
    }
    catch (...)
    {
        if (throw_if_failed)
        {
            throw;
        }

        tryLogCurrentException(__PRETTY_FUNCTION__, fmt::format("Could not verify lock file {} for backup {}",
            lock_file_name, backup_name_for_logging));
        return false;
    }

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

bool BackupImpl::removeLockFile()
{
    /// `checkLockFile(false)` returns false both for a foreign lock and for one it could not read, so a
    /// caller cannot tell "the lock is not ours" from "we do not know" -- and in the second case the lock
    /// is still there. Report that as a failure to remove: everything upstream that decides whether the
    /// destination is clean has to treat an unverifiable lock as one that survived.
    fiu_do_on(FailPoints::backup_fail_lock_file_removal, { return false; });
    if (!checkLockFile(false))
        return false;
    writer->removeFile(lock_file_name);
    return true;
}

bool BackupImpl::tryRemoveOwnLockFile() noexcept
{
    /// A failed `open` must leave the destination as it found it. Otherwise the lock it just created
    /// outlives it and fences the destination against every later attempt, which cannot match it either:
    /// a retry that finds no progress picks a fresh backup UUID, so the orphaned lock's UUID belongs to
    /// nobody. `removeLockFile` only removes a lock this backup still owns, so a foreign lock -- which may
    /// belong to a concurrent attempt that won the race -- is deliberately left alone. Never throws: it
    /// runs from an exception handler, where throwing would hide the original error.
    ///
    /// At most one attempt per `open`, and a repeat call answers with what that attempt found. A second
    /// removal could delete a lock the first attempt reported as left behind, leaving the record of that
    /// report describing a destination it no longer matches.
    if (own_lock_cleanup_result.has_value())
        return *own_lock_cleanup_result;
    /// Only a lock this `open` wrote is ours to take back: a continued attempt holds the contents of the
    /// lock the attempt it continues wrote, which `removeLockFile` cannot tell from its own, so removing it
    /// would leave the progress naming a lock that is gone and fail every later attempt.
    if (!created_own_lock_file)
        return false;
    try
    {
        own_lock_cleanup_result = removeLockFile();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        own_lock_cleanup_result = false;
    }
    return *own_lock_cleanup_result;
}

bool BackupImpl::directoryExists(const String & directory) const
{
    return !listFiles(directory, true /*recursive*/).empty();
}

Strings BackupImpl::listFiles(const String & directory, bool recursive) const
{
    if (open_mode == OpenMode::WRITE)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The backup file should not be opened for writing. Something is wrong internally");

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
    if (open_mode == OpenMode::WRITE)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The backup file should not be opened for writing. Something is wrong internally");

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
    if (open_mode == OpenMode::WRITE)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The backup file should not be opened for writing. Something is wrong internally");

    auto adjusted_path = removeLeadingSlash(file_name);
    std::lock_guard lock{mutex};
    return file_names.contains(adjusted_path);
}

bool BackupImpl::fileExists(const SizeAndChecksum & size_and_checksum) const
{
    if (open_mode == OpenMode::WRITE)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The backup file should not be opened for writing. Something is wrong internally");

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
    if (open_mode == OpenMode::WRITE)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The backup file should not be opened for writing. Something is wrong internally");

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

std::unique_ptr<ReadBufferFromFileBase> BackupImpl::readFile(const String & file_name) const
{
    return readFile(file_name, getFileSizeAndChecksum(file_name));
}

std::unique_ptr<ReadBufferFromFileBase> BackupImpl::readFile(const String & file_name, const SizeAndChecksum & size_and_checksum) const
{
    return readFileImpl(file_name, size_and_checksum, /* read_encrypted= */ false);
}

std::unique_ptr<ReadBufferFromFileBase> BackupImpl::readFileByObjectKey(const BackupFileInfo & info) const
{
    if (open_mode == OpenMode::WRITE)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The backup file should not be opened for writing. Something is wrong internally");

    if (info.object_key.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Object key of {} is empty string", info.data_file_name);

    return lightweight_snapshot_reader->readFile(info.object_key);
}

std::unique_ptr<ReadBufferFromFileBase>
BackupImpl::readFileImpl(const String & file_name, const SizeAndChecksum & size_and_checksum, bool read_encrypted) const

{
    if (open_mode == OpenMode::WRITE)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The backup file should not be opened for writing. Something is wrong internally");

    // Zero-sized files are not inserted into `file_infos` during metadata load,
    // but they are present in `file_names`. Short-circuit them here and return
    // an empty buffer without consulting `file_infos`.
    if (size_and_checksum.first == 0)
    {
        std::lock_guard lock{mutex};
        ++num_read_files;
        return std::make_unique<ReadBufferFromOutsideMemoryFile>(file_name, std::string_view{});
    }

    BackupFileInfo info;
    {
        std::lock_guard lock{mutex};
        auto it = file_infos.find(size_and_checksum);
        if (it == file_infos.end())
        {
            throw Exception(
                ErrorCodes::BACKUP_ENTRY_NOT_FOUND,
                "Backup {}: Entry {} for file '{}' not found in the backup",
                backup_name_for_logging,
                formatSizeAndChecksum(size_and_checksum),
                file_name);
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

    std::unique_ptr<ReadBufferFromFileBase> read_buffer;
    std::unique_ptr<ReadBufferFromFileBase> base_read_buffer;

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

        base_read_buffer = base->readFile(info.file_name, std::pair{info.base_size, info.base_checksum});
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
    return std::make_unique<ConcatReadBufferFromFile>(
        info.data_file_name, std::move(base_read_buffer), info.base_size, std::move(read_buffer), info.size - info.base_size);
}

String BackupImpl::getObjectKey(const String & file_name) const
{
    if (open_mode == OpenMode::WRITE)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The backup file should not be opened for writing. Something is wrong internally");

    auto adjusted_path = removeLeadingSlash(file_name);

    std::lock_guard lock{mutex};
    auto it = file_object_keys.find(adjusted_path);
    if (it != file_object_keys.end())
        return it->second;
    return "";
}

size_t BackupImpl::copyFileToDisk(const String & file_name,
                                  DiskPtr destination_disk, const String & destination_path, WriteMode write_mode, bool sync) const
{
    String object_key = getObjectKey(file_name);
    if (!object_key.empty())
    {
        /// The optimized object-key copy exposes no buffer to fsync, so the sync case needs a buffered path.
        if (sync)
            return copyObjectKeyEntryToDiskSynced(object_key, destination_disk, destination_path, write_mode);
#if CLICKHOUSE_CLOUD
        return copyFileToDiskByObjectKey(object_key, destination_disk, destination_path, write_mode);
#endif
    }
    return copyFileToDisk(getFileSizeAndChecksum(file_name), destination_disk, destination_path, write_mode, sync);
}

size_t BackupImpl::copyObjectKeyEntryToDiskSynced(
    const String & object_key, DiskPtr destination_disk, const String & destination_path, WriteMode write_mode) const
{
    if (open_mode == OpenMode::WRITE)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The backup file should not be opened for writing. Something is wrong internally");

    BackupFileInfo info;
    {
        std::lock_guard lock{mutex};
        auto it = lightweight_snapshot_file_infos.find(object_key);
        if (it == lightweight_snapshot_file_infos.end())
            throw Exception(
                ErrorCodes::BACKUP_ENTRY_NOT_FOUND,
                "Backup {}: Entry with object key {} not found in the backup",
                backup_name_for_logging, object_key);
        info = it->second;
    }

    if (info.encrypted_by_disk && !destination_disk->getDataSourceDescription().is_encrypted)
    {
        throw Exception(
            ErrorCodes::CANNOT_RESTORE_TO_NONENCRYPTED_DISK,
            "File {} is encrypted in the backup, it can be restored only to an encrypted disk",
            info.data_file_name);
    }

    auto read_buffer = readFileByObjectKey(info);
    size_t buf_size = std::min<size_t>(info.size ? info.size : DBMS_DEFAULT_BUFFER_SIZE, reader->getWriteBufferSize());
    std::unique_ptr<WriteBufferFromFileBase> write_buffer;
    /// readFileByObjectKey returns the bytes as stored (still encrypted for encrypted-by-disk entries),
    /// so write them through writeEncryptedFile to avoid re-encrypting, mirroring the generic copy path.
    if (info.encrypted_by_disk)
        write_buffer = destination_disk->writeEncryptedFile(destination_path, buf_size, write_mode, reader->getWriteSettings());
    else
        write_buffer = destination_disk->writeFile(destination_path, buf_size, write_mode, reader->getWriteSettings());
    copyData(*read_buffer, *write_buffer, info.size);
    write_buffer->finalize();
    /// fdatasync the contents so a restored part survives power loss (see copyFileToDisk above).
    write_buffer->sync();

    {
        std::lock_guard lock{mutex};
        ++num_read_files;
        num_read_bytes += info.size;
    }
    return info.size;
}

size_t BackupImpl::copyFileToDisk(const SizeAndChecksum & size_and_checksum,
                                  DiskPtr destination_disk, const String & destination_path, WriteMode write_mode, bool sync) const
{
    if (open_mode == OpenMode::WRITE)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The backup file should not be opened for writing. Something is wrong internally");

    if (size_and_checksum.first == 0)
    {
        /// Entry's data is empty.
        if (write_mode == WriteMode::Rewrite)
        {
            if (sync)
            {
                /// createFile() leaves the empty contents unsynced; a live buffer lets us fsync it.
                auto write_buffer = destination_disk->writeFile(destination_path, DBMS_DEFAULT_BUFFER_SIZE, write_mode, reader->getWriteSettings());
                write_buffer->finalize();
                write_buffer->sync();
            }
            else
            {
                /// Just create an empty file.
                destination_disk->createFile(destination_path);
            }
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

    /// When `sync` is requested we must copy through a live destination buffer so we can fsync its
    /// contents below. The optimized delegate paths (reader->copyFileToDisk / base backup) may use
    /// fs::copy or an object-storage copy and expose no buffer, so skip them and take the buffered
    /// branch, which is already correct for every source (this backup, base backup, archive).
    if (!sync && info.size && !info.base_size && !use_archive)
    {
        /// Data comes completely from this backup. The reader copies without exposing a write
        /// buffer we could fsync, so this fast path is used only when `sync` isn't requested.
        reader->copyFileToDisk(info.data_file_name, info.size, info.encrypted_by_disk, destination_disk, destination_path, write_mode);
        file_copied = true;
    }
    else if (info.size && (info.size == info.base_size))
    {
        /// Data comes completely from the base backup (nothing comes from this backup). The base
        /// backup is itself a BackupImpl that honours `sync` and can read its own encrypted-by-disk
        /// entries, so forward the copy (and the `sync` request) there. Going through the generic
        /// branch below instead would read the base via the public readFile(), which always requests
        /// unencrypted data and would fail on an encrypted entry (CANNOT_RESTORE_TO_NONENCRYPTED_DISK).
        getBaseBackup()->copyFileToDisk(std::pair{info.base_size, info.base_checksum}, destination_disk, destination_path, write_mode, sync);
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
        auto read_buffer = readFileImpl(info.file_name, size_and_checksum, /* read_encrypted= */ info.encrypted_by_disk);
        std::unique_ptr<WriteBufferFromFileBase> write_buffer;
        size_t buf_size = std::min<size_t>(info.size, reader->getWriteBufferSize());
        if (info.encrypted_by_disk)
            write_buffer = destination_disk->writeEncryptedFile(destination_path, buf_size, write_mode, reader->getWriteSettings());
        else
            write_buffer = destination_disk->writeFile(destination_path, buf_size, write_mode, reader->getWriteSettings());
        copyData(*read_buffer, *write_buffer, info.size);
        write_buffer->finalize();
        /// fdatasync the contents so a restored part survives power loss, matching the durability
        /// an inserted part gets from fsync_after_insert (the caller passes `sync` accordingly).
        if (sync)
            write_buffer->sync();
    }

    return info.size;
}


void BackupImpl::writeFile(const BackupFileInfo & info, BackupEntryPtr entry)
{
    /// we don't write anything for reference files
    if (entry->isReference())
        return;

    if (open_mode == OpenMode::READ)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The backup file should not be opened for reading. Something is wrong internally");

    if (writing_finalized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Backup is already finalized");

    {
        std::lock_guard lock{mutex};
#if CLICKHOUSE_CLOUD
        /// Only a continued attempt can find the manifest already published.
        if (params.resume && params.resume->metadata_published())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Backup metadata is already published");
#endif
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
        LOG_TRACE(log, "Writing backup for file {} from {} (disk {}): data file #{}", info.data_file_name, src_file_desc, src_disk->getName(), info.data_file_index);
        writer->copyFileFromDisk(info.data_file_name, src_disk, src_file_path, info.encrypted_by_disk, info.base_size, info.size - info.base_size);
    }
    else
    {
        LOG_TRACE(log, "Writing backup for file {} from {}: data file #{}", info.data_file_name, src_file_desc, info.data_file_index);
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
    if (open_mode == OpenMode::READ)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The backup file should not be opened for reading. Something is wrong internally");

    if (corrupted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Backup can't be finalized after an error happened");

    if (writing_finalized)
        return;

    if (!params.is_internal_backup)
    {
        LOG_TRACE(log, "Finalizing backup {}", backup_name_for_logging);
        fiu_do_on(FailPoints::backup_fail_before_writing_metadata,
        {
            throw Exception(ErrorCodes::FAULT_INJECTED, "Failpoint backup_fail_before_writing_metadata is triggered");
        });
#if CLICKHOUSE_CLOUD && USE_SSL
        if (!use_archive)
            uncompressed_size += encryption_sidecar->write();
#endif
#if CLICKHOUSE_CLOUD
        /// A continued attempt whose manifest is already in the destination republishes nothing; it only
        /// recomputes the counters it reports.
        if (params.resume && params.resume->metadata_published())
            recalculateMetadataCounters();
        else
#endif
            writeBackupMetadata();
#if CLICKHOUSE_CLOUD && USE_SSL
        if (use_archive)
            uncompressed_size += encryption_sidecar->write();
#endif
        closeArchive(/* finalize= */ true);
        setCompressedSize();
#if CLICKHOUSE_CLOUD
        if (params.resume)
            params.resume->check_owner();
#endif
        removeLockFile();
        LOG_TRACE(log, "Finalized backup {}", backup_name_for_logging);
    }

    writing_finalized = true;
}


void BackupImpl::setCompressedSize()
{
    if (use_archive)
        compressed_size = writer ? writer->getFileSize(archive_params.archive_name) : reader->getFileSize(archive_params.archive_name);
#if CLICKHOUSE_CLOUD && USE_SSL
        /// The encryption config file is written outside of the archive, so its size must be added
        /// to the size of the archive to get the physical footprint of the backup.
        compressed_size += encryption_sidecar->getFileSize();
#endif
    else
        compressed_size = uncompressed_size;
}


void BackupImpl::setOriginalEndpointAndNamespaceIfEmpty(const String & endpoint_, const String & namespace_) noexcept
{
    if (original_endpoint.empty())
    {
        original_endpoint = endpoint_;
        original_namespace = namespace_;
    }
}


bool BackupImpl::setIsCorrupted() noexcept
{
    try
    {
        std::lock_guard lock{mutex};
        if (open_mode != OpenMode::WRITE)
        {
            LOG_ERROR(log, "Backup is not opened for writing. Stack trace: {}", StackTrace().toString());
            chassert(false, "Backup is not opened for writing when setIsCorrupted() is called");
            return false;
        }

        if (writing_finalized)
        {
            LOG_WARNING(log, "An error happened after the backup was completed successfully, the backup must be correct!");
            return false;
        }

        if (corrupted)
            return true;

        LOG_WARNING(log, "An error happened, the backup won't be completed");

        closeArchive(/* finalize= */ false);

        corrupted = true;
        return true;
    }
    catch (...)
    {
        DB::tryLogCurrentException(log, "Caught exception while setting that the backup was corrupted");
        return false;
    }
}


bool BackupImpl::tryRemoveAllFiles() noexcept
{
    try
    {
        std::lock_guard lock{mutex};
        if (!corrupted)
        {
            LOG_ERROR(log, "Backup is not set as corrupted. Stack trace: {}", StackTrace().toString());
            chassert(false, "Backup is not set as corrupted when tryRemoveAllFiles() is called");
            return false;
        }

        LOG_INFO(log, "Removing all files of backup {}", backup_name_for_logging);

        Strings files_to_remove;

        if (use_archive)
        {
            files_to_remove.push_back(archive_params.archive_name);
        }
        else
        {
            files_to_remove.push_back(".backup");
            coordination->forEachFileInfoForAllHosts([&](const BackupFileInfo & file_info)
            {
                /// Skip entries with no data file — an empty file, or one wholly covered by the base backup.
                /// Their `data_file_name` is empty, which would otherwise resolve to the backup root.
                if (!file_info.data_file_name.empty())
                    files_to_remove.push_back(file_info.data_file_name);
            });
        }

#if CLICKHOUSE_CLOUD && USE_SSL
        /// The encryption config file is written outside of the archive, so it must be removed in both cases.
        if (!encryption_sidecar->getKeyInfos().empty())
            files_to_remove.push_back(encryption_sidecar->fileName());
#endif

        if (!checkLockFile(false))
            return false;

        writer->removeFiles(files_to_remove);
        /// The lock is the last thing to go, and it can survive its removal: `removeLockFile` gives up
        /// when it cannot verify the lock is still ours. Returning true then would tell the caller the
        /// destination is empty while it is still fenced by a lock no later attempt can match.
        const bool removed_lock_file = removeLockFile();
        writer->removeEmptyDirectories();
        return removed_lock_file;
    }
    catch (...)
    {
        DB::tryLogCurrentException(log, "Caught exception while removing files of a corrupted backup");
        return false;
    }
}

}
