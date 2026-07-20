#include <Backups/BackupImpl.h>
#include <Backups/BackupFactory.h>
#include <Backups/BackupFileInfo.h>
#include <Backups/BackupMetadataHandler.h>
#include <Backups/BackupIO.h>
#include <Backups/IBackupEntry.h>
#include <Backups/BackupIO_S3.h>
#include <Backups/BackupPacker.h>
#include <Backups/getBackupDataFileName.h>
#include <IO/PackedFilesReader.h>
#include <IO/PackedFilesWriter.h>
#include <IO/PackedFilesIO.h>
#include <fmt/format.h>
#include <Common/CurrentThread.h>
#include <Common/ProfileEvents.h>
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
#include <Poco/SAX/SAXParser.h>
#include <Poco/SAX/XMLReader.h>

#include <charconv>
#include <filesystem>
#include <optional>
#include <set>


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
}

namespace fs = std::filesystem;

namespace
{
    const int INITIAL_BACKUP_VERSION = 1;
    /// Lightweight snapshot backups are written as version 2.
    const int LIGHTWEIGHT_SNAPSHOT_VERSION = 2;
    /// Object-packed backups are written as version 3. This is the read-side gate: a member is not a
    /// standalone blob, so an older server (which only knows up to version 2) must refuse a packed backup
    /// instead of reading a whole pack as one file; a new server still reads old whole-object backups.
    const int PACKED_FORMAT_VERSION = 3;
    const int CURRENT_BACKUP_VERSION = 3;
    constexpr auto BASE_BACKUP_COPY_S3_CREDENTIALS_FROM_BACKUP = "base_backup_copy_s3_credentials_from_backup";

    using SizeAndChecksum = IBackup::SizeAndChecksum;

    /// Object name of a pack blob. Packs are few, so names are flat (packs_NNNN) and derived from the pack
    /// index; the count lives in the manifest header so restore can reconstruct them.
    String getBackupPackObjectName(size_t pack_id)
    {
        return fmt::format("packs_{:04}", pack_id);
    }

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
    , layout(!archive_params_.archive_name.empty() ? BackupLayout::Archive : BackupLayout::Plain)
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
    , layout(!archive_params_.archive_name.empty()
            ? BackupLayout::Archive
            : (params.experimental_backup_pack_format ? BackupLayout::Packed : BackupLayout::Plain))
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
    , layout(!archive_params_.archive_name.empty() ? BackupLayout::Archive : BackupLayout::Plain)
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
        LOG_INFO(log, "Writing backup: {}", backup_name_for_logging);
        timestamp = std::time(nullptr);
        if (!uuid)
            uuid = UUIDHelpers::generateV4();
        lock_file_name = isArchive() ? (archive_params.archive_name + ".lock") : ".lock";
        lock_file_before_first_file_checked = false;
        writing_finalized = false;

        /// Check that we can write a backup there and create the lock file to own this destination.
        checkBackupDoesntExist();
        if (!params.is_internal_backup)
            createLockFile();
        checkLockFile(true);
    }

    if (isArchive())
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
    if (!isArchive())
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
            backup_info.copyS3CredentialsTo(effective_base_backup_info);
        }
        else if (base_backup_copy_s3_credentials_from_backup && backup_info.canCopyS3CredentialsTo(effective_base_backup_info))
        {
            /// Metadata marker asks to copy credentials from this backup locator at restore time.
            backup_info.copyS3CredentialsTo(effective_base_backup_info);
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

    std::unique_ptr<WriteBuffer> out;
    if (isArchive())
        out = archive_writer->writeFile(".backup");
    else
        out = writer->writeFile(".backup");

    /// Iterate in place instead of copying all file infos (a backup can contain millions).
    size_t num_all_file_infos = 0;
    bool base_backup_in_use = false;
    Int64 max_pack_id = -1;
    coordination->forEachFileInfoForAllHosts([&](const BackupFileInfo & info)
    {
        ++num_all_file_infos;
        if (info.base_size)
            base_backup_in_use = true;
        max_pack_id = std::max(max_pack_id, info.pack_id);
    });

    if (num_all_file_infos == 0)
        throw Exception(ErrorCodes::BACKUP_IS_EMPTY, "Backup must not be empty");

    /// The version bump (and num_packs header) apply only if at least one pack was actually written -- a
    /// packed backup whose blobs were all large enough to stay their own objects is a plain whole-object
    /// backup and stays readable by older servers.
    const bool has_packs = isPacked() && (max_pack_id >= 0);
    num_packs = has_packs ? static_cast<size_t>(max_pack_id + 1) : 0;

    int version_to_write = INITIAL_BACKUP_VERSION;
    if (has_packs)
        version_to_write = PACKED_FORMAT_VERSION;
    else if (params.is_lightweight_snapshot)
        version_to_write = LIGHTWEIGHT_SNAPSHOT_VERSION;

    *out << "<config>";
    *out << "<version>" << version_to_write << "</version>";
    *out << "<deduplicate_files>" << params.deduplicate_files << "</deduplicate_files>";
    *out << "<timestamp>" << toString(LocalDateTime{timestamp}) << "</timestamp>";
    *out << "<uuid>" << toString(*uuid) << "</uuid>";
    if (!backup_id.empty())
        *out << "<backup_id>" << xml << backup_id << "</backup_id>";
    if (data_file_name_generator != BackupDataFileNameGeneratorType::FirstFileName)
        *out << "<data_file_name_generator>" << SettingFieldBackupDataFileNameGeneratorTypeTraits::toString(data_file_name_generator)
             << "</data_file_name_generator>";

    /// Record the number of pack objects so restore can reconstruct their names (packs_0000..) and load
    /// their front indexes. No per-file pack pointer is written -- a member is located via those indexes.
    if (has_packs)
        *out << "<num_packs>" << num_packs << "</num_packs>";

    if (base_backup_info)
    {
        if (base_backup_in_use)
        {
            /// Persist base backup locators without inline `S3` credentials.
            BackupInfo effective_base_backup_info = *base_backup_info;
            if (params.use_same_s3_credentials_for_base_backup)
                backup_info.copyS3CredentialsTo(effective_base_backup_info);

            const BackupInfo base_backup_info_for_metadata = effective_base_backup_info.withoutS3Credentials(params.context);
            const bool base_backup_credentials_were_stripped = base_backup_info_for_metadata.toString() != effective_base_backup_info.toString();
            bool base_backup_can_use_this_backup_credentials = false;

            if (base_backup_credentials_were_stripped && backup_info.canCopyS3CredentialsTo(base_backup_info_for_metadata))
            {
                BackupInfo base_backup_info_with_this_backup_credentials = base_backup_info_for_metadata;
                backup_info.copyS3CredentialsTo(base_backup_info_with_this_backup_credentials);
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
            /// A packed member is not its own stored object -- the pack is. Count packs once, below, using the
            /// serialized pack sizes recorded at write time (they include the front-index bytes).
            if (info.pack_id < 0)
            {
                ++num_entries;
                size_of_entries += info.size - info.base_size;
            }
        }

        *out << "</file>";
    });
    *out << "</contents>";

    /// Each written pack object is one entry whose size (index + member bodies) was recorded by writeFilePack.
    for (const auto & [pack_id, pack_size] : pack_object_sizes)
    {
        ++num_entries;
        size_of_entries += pack_size;
    }

    *out << "</config>";

    out->finalize();

    uncompressed_size = size_of_entries + out->count();

    LOG_TRACE(log, "Backup {}: Metadata was written", backup_name_for_logging);
}


void BackupImpl::readBackupMetadata()
{
    LOG_TRACE(log, "Backup {}: Reading metadata", backup_name_for_logging);
    auto timer = DB::CurrentThread::getProfileEvents().timer(ProfileEvents::BackupReadMetadataMicroseconds);

    std::unique_ptr<ReadBuffer> in;
    if (isArchive())
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

        if (h.contains("num_packs"))
        {
            num_packs = to_uint64(req("num_packs"), "num_packs");
            /// Packing is detected on read from the manifest, not from a setting -- promote the layout so the
            /// read path routes packed members. Archive and packing are mutually exclusive (rejected earlier).
            if (num_packs > 0)
                layout = BackupLayout::Packed;
        }

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

        file_names.emplace(info.file_name, std::pair{info.size, info.checksum});
        if (!info.object_key.empty())
        {
            if (original_endpoint.empty() || original_namespace.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "In lightweight snapshot backup, the endpoint or namespace should be not empty. We cannot restore this file.");

            if (open_mode == OpenMode::READ)
                lightweight_snapshot_reader = lightweight_snapshot_reader_creator(original_endpoint, original_namespace);

            file_object_keys.emplace(info.file_name, info.object_key);
            lightweight_snapshot_file_infos.try_emplace(info.object_key, info);
        }
        else if (info.size)
            file_infos.try_emplace(std::pair{info.size, info.checksum}, info);

        ++num_files;
        total_size += info.size;
        bool has_entry = !params.deduplicate_files || (info.size && (info.size != info.base_size) && (info.data_file_name.empty() || info.data_file_name == info.file_name));
        if (has_entry)
        {
            ++num_entries;
            size_of_entries += info.size - info.base_size;
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

    /// Packed backup: load each pack's front index once so packed members can be located at read time.
    if ((open_mode == OpenMode::READ) && (num_packs > 0))
    {
        loadPackIndexes();

        /// The per-file SAX pass above counted each packed member as its own entry with only its body bytes.
        /// But a pack is a single stored object whose size includes the serialized front index, and the write
        /// side accounts it that way -- so recompute pack-aware here to make the RESTORE row match the BACKUP
        /// row. Own-object data files (a data_file_name absent from packed_members, e.g. a large blob that kept
        /// its own object) stay individual entries; each pack object is one entry sized (front index + member
        /// bodies). Pack geometry comes from packed_members (pack_id is not persisted in the manifest); the
        /// front-index size of a pack is its smallest member offset, since bodies follow the index.
        ///
        /// The reset-and-rebuild is complete only because a packed backup has no object_key files -- those live
        /// in lightweight_snapshot_file_infos (not file_infos) and would be dropped by the reset. Packing is
        /// rejected together with experimental_lightweight_snapshot (see BackupsWorker), so this holds; assert
        /// it so a future relaxation of that guard can't silently reintroduce the under-count.
        chassert(lightweight_snapshot_file_infos.empty());

        std::unordered_map<String, UInt64> pack_index_size;
        std::unordered_map<String, UInt64> pack_body_size;
        for (const auto & [member_name, location] : packed_members)
        {
            auto [it, inserted] = pack_index_size.try_emplace(location.pack_object, location.offset);
            if (!inserted)
                it->second = std::min(it->second, location.offset);
            pack_body_size[location.pack_object] += location.size;
        }

        num_entries = 0;
        size_of_entries = 0;
        for (const auto & [size_and_checksum, info] : file_infos)
        {
            if (info.size <= info.base_size)
                continue; /// Fully from the base backup: no body stored in this backup.
            if (packed_members.contains(info.data_file_name))
                continue; /// A packed member; accounted per pack object below.
            ++num_entries;
            size_of_entries += info.size - info.base_size;
        }
        for (const auto & [pack_object, body_size] : pack_body_size)
        {
            ++num_entries;
            size_of_entries += pack_index_size[pack_object] + body_size;
        }
    }

    uncompressed_size = size_of_entries + str.size();
    compressed_size = uncompressed_size;
    if (!isArchive())
        setCompressedSize();

    LOG_TRACE(log, "Backup {}: Metadata was read", backup_name_for_logging);
}

void BackupImpl::checkBackupDoesntExist() const
{
    String file_name_to_check_existence;
    if (isArchive())
        file_name_to_check_existence = archive_params.archive_name;
    else
        file_name_to_check_existence = ".backup";

    if (writer->fileExists(file_name_to_check_existence))
        throw Exception(ErrorCodes::BACKUP_ALREADY_EXISTS, "Backup {} already exists", backup_name_for_logging);

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
    auto out = writer->writeFile(lock_file_name);
    writeUUIDText(*uuid, *out);
    out->finalize();
}

bool BackupImpl::checkLockFile(bool throw_if_failed) const
{
    if (!lock_file_name.empty() && uuid)
    {
        LOG_TRACE(log, "Checking lock file {}", lock_file_name);
        ProfileEvents::increment(ProfileEvents::BackupLockFileReads);
        String actual_file_contents;
        if (writer->fileContentsEqual(lock_file_name, toString(*uuid), actual_file_contents))
            return true;
        LOG_TRACE(log, "Lock file {} contents do not match, expected: {}, actual: {}", lock_file_name, toString(*uuid), actual_file_contents);
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

void BackupImpl::removeLockFile()
{
    if (checkLockFile(false))
        writer->removeFile(lock_file_name);
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

void BackupImpl::loadPackIndexes()
{
    for (size_t pack_id = 0; pack_id < num_packs; ++pack_id)
    {
        const String pack_object = getBackupPackObjectName(pack_id);
        auto in = reader->readFile(pack_object);
        const auto index = PackedFilesReader::readIndex(*in);
        for (const auto & [member_name, file_offset] : index)
        {
            if (!packed_members.emplace(member_name, MemberLocation{pack_object, file_offset.offset, file_offset.size}).second)
                throw Exception(
                    ErrorCodes::BACKUP_DAMAGED,
                    "Backup {}: Member {} appears in more than one pack",
                    backup_name_for_logging,
                    quoteString(member_name));
        }
    }
}

std::unique_ptr<ReadBufferFromFileBase> BackupImpl::readPackedMember(const MemberLocation & member) const
{
    /// PackedFilesReader::readFile can't be reused wholesale: it opens the archive through a DiskPtr, but a
    /// pack is read through IBackupReader (S3/Disk/File), whose readFile takes no ReadSettings. Only the view
    /// construction is shared (viewMember). readFileForView opens the pack with view-safe settings -- local
    /// Disk/File readers strip mmap/direct-io (which ReadBufferFromFileView can't wrap); S3/Azure never produce
    /// such buffers so their default is plain readFile.
    ///
    /// TODO(backup-packing): this opens the pack object once per member (one ranged GET per member on S3).
    /// It reads only the member's [offset, size) range -- no whole-pack re-read -- but when several members
    /// of the same pack are restored via the buffered path, a batch-restore API could read the pack once and
    /// slice out all its members (~num_packs reads instead of one per member). Deferred: the native
    /// server-side-copy path (S3->S3) can't be range-batched anyway, and coalescing needs pack-grouping
    /// plumbed into the restore driver + reconciled with restore parallelism. See status doc v2 items.
    return PackedFilesReader::viewMember(reader->readFileForView(member.pack_object), member.pack_object, member.offset, member.size);
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
    std::optional<MemberLocation> packed_location;
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
        if (auto pit = packed_members.find(info.data_file_name); pit != packed_members.end())
            packed_location = pit->second;
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
        if (isArchive())
            read_buffer = archive_reader->readFile(info.data_file_name, /*throw_on_not_found=*/true);
        else if (packed_location)
            read_buffer = readPackedMember(*packed_location);
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
                                  DiskPtr destination_disk, const String & destination_path, WriteMode write_mode) const
{
#if CLICKHOUSE_CLOUD
    String object_key = getObjectKey(file_name);
    if (!object_key.empty())
        return copyFileToDiskByObjectKey(object_key, destination_disk, destination_path, write_mode);
#endif
    return copyFileToDisk(getFileSizeAndChecksum(file_name), destination_disk, destination_path, write_mode);
}

size_t BackupImpl::copyFileToDisk(const SizeAndChecksum & size_and_checksum,
                                  DiskPtr destination_disk, const String & destination_path, WriteMode write_mode) const
{
    if (open_mode == OpenMode::WRITE)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The backup file should not be opened for writing. Something is wrong internally");

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
    std::optional<MemberLocation> packed_location;
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
        if (auto pit = packed_members.find(info.data_file_name); pit != packed_members.end())
            packed_location = pit->second;
    }

    if (info.encrypted_by_disk && !destination_disk->getDataSourceDescription().is_encrypted)
    {
        throw Exception(
            ErrorCodes::CANNOT_RESTORE_TO_NONENCRYPTED_DISK,
            "File {} is encrypted in the backup, it can be restored only to an encrypted disk",
            info.data_file_name);
    }

    bool file_copied = false;

    if (info.size && !info.base_size && !isArchive() && !packed_location)
    {
        /// Data comes completely from this backup as its own whole object.
        reader->copyFileToDisk(info.data_file_name, 0, info.size, info.encrypted_by_disk, destination_disk, destination_path, write_mode);
        file_copied = true;
    }
    else if (info.size && !info.base_size && !isArchive() && packed_location)
    {
        /// A packed member with no base is a byte range inside a pack object; copy only that range. On S3 the
        /// reader forces a ranged UploadPartCopy (never a whole-object CopyObject, which would copy the entire
        /// pack); Disk/File readers do a buffered ranged read. Packed members that also have a base take the
        /// generic path below, which concatenates the base backup's data with the member's range.
        reader->copyFileToDisk(packed_location->pack_object, packed_location->offset, packed_location->size,
                               info.encrypted_by_disk, destination_disk, destination_path, write_mode);
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
        auto read_buffer = readFileImpl(info.file_name, size_and_checksum, /* read_encrypted= */ info.encrypted_by_disk);
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

    if (entry->isFromRemoteFile())
    {
        LOG_TRACE(log, "Writing backup for file {} : skipped because of lightweight snapshot", info.data_file_name);
        std::lock_guard lock{mutex};
        original_endpoint = entry->getEndpointURI();
        original_namespace = entry->getNamespace();
        return;
    }

    if (open_mode == OpenMode::READ)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The backup file should not be opened for reading. Something is wrong internally");

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

    if (isArchive())
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
    if (isArchive())
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


void BackupImpl::writeFilePack(size_t pack_id, const PackMembers & members)
{
    if (open_mode == OpenMode::READ)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The backup file should not be opened for reading. Something is wrong internally");

    if (writing_finalized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Backup is already finalized");

    if (members.empty())
        return;

    if (!lock_file_before_first_file_checked.exchange(true))
        checkLockFile(true);

    /// Build the ordered member manifest. Each member streams its physical suffix (size - base_size),
    /// seeking past the base bytes for incrementals -- the same suffix that own-object writes store.
    /// Invariant: each member is the read representative (BackupPacker::selectPackMembers), so info.base_size
    /// here equals the base_size restore reconstructs the whole (size, checksum) class with -- the stored
    /// [base_size, size) blob and the restore-side concat are consistent by construction.
    std::vector<BackupPacker::MemberSource> pack_members;
    pack_members.reserve(members.size());
    UInt64 total_physical_size = 0;
    UInt64 total_logical_size = 0;
    for (const auto & [info, entry] : members)
    {
        const UInt64 physical_size = info.size - info.base_size;
        total_physical_size += physical_size;
        total_logical_size += info.size;
        pack_members.push_back(BackupPacker::MemberSource{
            info.data_file_name,
            physical_size,
            [entry, base_size = info.base_size, read_settings = writer->getReadSettings()]() -> std::unique_ptr<ReadBuffer>
            {
                auto read_buffer = entry->getReadBuffer(read_settings);
                if (base_size != 0)
                    read_buffer->seek(base_size, SEEK_SET);
                return read_buffer;
            }});
    }

    const String pack_object = getBackupPackObjectName(pack_id);
    LOG_TRACE(log, "Writing backup pack {} with {} members", pack_object, members.size());
    constexpr UInt8 pack_version = PackedFilesIO::VERSION_WITHOUT_UNCOMPRESSED_SIZE;
    BackupPacker::writePack(writer->writeFile(pack_object), pack_members, pack_version);

    /// Accounting mirrors the non-packed writeFile, but per PACK OBJECT rather than per member. num_files /
    /// total_size track logical files, so a pack still counts as members.size() files of total_logical_size
    /// bytes. But the pack is a single stored object -- one entry -- whose serialized byte size is the front
    /// index plus every member's physical bytes; the index bytes would be lost if we summed member sizes alone.
    Strings member_names;
    member_names.reserve(pack_members.size());
    for (const auto & pack_member : pack_members)
        member_names.push_back(pack_member.name);
    const UInt64 pack_object_size
        = PackedFilesWriter::getSerializedIndexSize(member_names, pack_version) + total_physical_size;

    std::lock_guard lock{mutex};
    num_files += members.size();
    total_size += total_logical_size;
    ++num_entries;
    size_of_entries += pack_object_size;
    uncompressed_size += pack_object_size;
    /// Recorded for the authoritative recompute in writeBackupMetadata (which resets these counters).
    pack_object_sizes[pack_id] = pack_object_size;
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
    if (isArchive())
        compressed_size = writer ? writer->getFileSize(archive_params.archive_name) : reader->getFileSize(archive_params.archive_name);
    else
        compressed_size = uncompressed_size;
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

        if (isArchive())
        {
            files_to_remove.push_back(archive_params.archive_name);
        }
        else
        {
            files_to_remove.push_back(".backup");
            std::set<size_t> pack_ids;
            coordination->forEachFileInfoForAllHosts([&](const BackupFileInfo & file_info)
            {
                /// A packed member's data_file_name is a member key inside a pack, not an object; remove
                /// the pack objects instead (each once).
                if (file_info.pack_id >= 0)
                    pack_ids.insert(static_cast<size_t>(file_info.pack_id));
                else
                    files_to_remove.push_back(file_info.data_file_name);
            });
            for (size_t pack_id : pack_ids)
                files_to_remove.push_back(getBackupPackObjectName(pack_id));
        }

        if (!checkLockFile(false))
            return false;

        writer->removeFiles(files_to_remove);
        removeLockFile();
        writer->removeEmptyDirectories();
        return true;
    }
    catch (...)
    {
        DB::tryLogCurrentException(log, "Caught exception while removing files of a corrupted backup");
        return false;
    }
}

}
