#include <Backups/BackupImpl.h>
#include <Backups/BackupFactory.h>
#include <Backups/BackupEntryFromMemory.h>
#include <Backups/BackupIO.h>
#include <Backups/IBackupEntry.h>
#include <Backups/BackupCoordinationLocal.h>
#include <Backups/BackupCoordinationDistributed.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/hex.h>
#include <Common/quoteString.h>
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
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{
    const UInt64 INITIAL_BACKUP_VERSION = 1;
    const UInt64 CURRENT_BACKUP_VERSION = 1;

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

private:
    const std::shared_ptr<const BackupImpl> backup;
    const String archive_suffix;
    const String data_file_name;
    const UInt64 size;
    const UInt128 checksum;
    BackupEntryPtr base_backup_entry;
};


BackupImpl::BackupImpl(
    const String & backup_name_,
    const ArchiveParams & archive_params_,
    const std::optional<BackupInfo> & base_backup_info_,
    std::shared_ptr<IBackupReader> reader_,
    const ContextPtr & context_)
    : backup_name(backup_name_)
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
    const String & backup_name_,
    const ArchiveParams & archive_params_,
    const std::optional<BackupInfo> & base_backup_info_,
    std::shared_ptr<IBackupWriter> writer_,
    const ContextPtr & context_,
    const std::optional<UUID> & backup_uuid_,
    bool is_internal_backup_,
    const std::shared_ptr<IBackupCoordination> & coordination_)
    : backup_name(backup_name_)
    , archive_params(archive_params_)
    , use_archives(!archive_params.archive_name.empty())
    , open_mode(OpenMode::WRITE)
    , writer(std::move(writer_))
    , is_internal_backup(is_internal_backup_)
    , coordination(coordination_ ? coordination_ : std::make_shared<BackupCoordinationLocal>())
    , uuid(backup_uuid_)
    , version(CURRENT_BACKUP_VERSION)
    , base_backup_info(base_backup_info_)
    , log(&Poco::Logger::get("Backup"))
{
    open(context_);
}


BackupImpl::~BackupImpl()
{
    close();
}


void BackupImpl::open(const ContextPtr & context)
{
    std::lock_guard lock{mutex};

    String file_name_to_check_existence;
    if (use_archives)
        file_name_to_check_existence = archive_params.archive_name;
    else
        file_name_to_check_existence = ".backup";
    bool backup_exists = (open_mode == OpenMode::WRITE) ? writer->fileExists(file_name_to_check_existence) : reader->fileExists(file_name_to_check_existence);

    if (open_mode == OpenMode::WRITE)
    {
        if (backup_exists)
            throw Exception(ErrorCodes::BACKUP_ALREADY_EXISTS, "Backup {} already exists", backup_name);
    }
    else
    {
        if (!backup_exists)
            throw Exception(ErrorCodes::BACKUP_NOT_FOUND, "Backup {} not found", backup_name);
    }

    if (open_mode == OpenMode::WRITE)
    {
        timestamp = std::time(nullptr);
        if (!uuid)
            uuid = UUIDHelpers::generateV4();
        writing_finalized = false;
    }

    if (open_mode == OpenMode::READ)
        readBackupMetadata();

    assert(uuid); /// Backup's UUID must be loaded or generated at this point.

    if (base_backup_info)
    {
        BackupFactory::CreateParams params;
        params.backup_info = *base_backup_info;
        params.open_mode = OpenMode::READ;
        params.context = context;
        base_backup = BackupFactory::instance().createBackup(params);

        if (open_mode == OpenMode::WRITE)
            base_backup_uuid = base_backup->getUUID();
        else if (base_backup_uuid != base_backup->getUUID())
            throw Exception(ErrorCodes::WRONG_BASE_BACKUP, "Backup {}: The base backup {} has different UUID ({} != {})",
                            backup_name, base_backup->getName(), toString(base_backup->getUUID()), (base_backup_uuid ? toString(*base_backup_uuid) : ""));
    }
}

void BackupImpl::close()
{
    std::lock_guard lock{mutex};

    if (!is_internal_backup && writing_finalized)
    {
        LOG_TRACE(log, "Finalizing backup {}", backup_name);
        writeBackupMetadata();
        LOG_INFO(log, "Finalized backup {}", backup_name);
    }

    archive_readers.clear();
    for (auto & archive_writer : archive_writers)
        archive_writer = {"", nullptr};

    if (!is_internal_backup && writer && !writing_finalized)
    {
        LOG_INFO(log, "Removing all files of backup {} after failure", backup_name);
        removeAllFilesAfterFailure();
    }
}

time_t BackupImpl::getTimestamp() const
{
    std::lock_guard lock{mutex};
    return timestamp;
}

void BackupImpl::writeBackupMetadata()
{
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config{new Poco::Util::XMLConfiguration()};
    config->setUInt("version", CURRENT_BACKUP_VERSION);
    config->setString("timestamp", toString(LocalDateTime{timestamp}));
    config->setString("uuid", toString(*uuid));

    if (base_backup_info)
    {
        bool base_backup_in_use = false;
        for (const auto & info : coordination->getAllFileInfos())
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

    size_t index = 0;
    for (const auto & info : coordination->getAllFileInfos())
    {
        String prefix = index ? "contents.file[" + std::to_string(index) + "]." : "contents.file.";
        config->setUInt(prefix + "size", info.size);
        if (info.size)
        {
            config->setString(prefix + "name", info.file_name);
            config->setString(prefix + "checksum", hexChecksum(info.checksum));
            if (info.base_size)
            {
                config->setBool(prefix + "use_base", true);
                if (info.base_size != info.size)
                {
                    config->setUInt(prefix + "base_size", info.base_size);
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
        ++index;
    }

    std::ostringstream stream; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    config->save(stream);
    String str = stream.str();

    std::unique_ptr<WriteBuffer> out;
    if (use_archives)
        out = getArchiveWriter("")->writeFile(".backup");
    else
        out = writer->writeFile(".backup");
    out->write(str.data(), str.size());
}

void BackupImpl::readBackupMetadata()
{
    std::unique_ptr<ReadBuffer> in;
    if (use_archives)
        in = getArchiveReader("")->readFile(".backup");
    else
        in = reader->readFile(".backup");

    String str;
    readStringUntilEOF(str, *in);
    std::istringstream stream(str); // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config{new Poco::Util::XMLConfiguration()};
    config->load(stream);

    version = config->getUInt("version");
    if ((version < INITIAL_BACKUP_VERSION) || (version > CURRENT_BACKUP_VERSION))
        throw Exception(ErrorCodes::BACKUP_VERSION_NOT_SUPPORTED, "Backup {}: Version {} is not supported", backup_name, version);

    timestamp = parse<LocalDateTime>(config->getString("timestamp")).to_time_t();
    uuid = parse<UUID>(config->getString("uuid"));

    if (config->has("base_backup") && !base_backup_info)
        base_backup_info = BackupInfo::fromString(config->getString("base_backup"));

    if (config->has("base_backup_uuid"))
        base_backup_uuid = parse<UUID>(config->getString("base_backup_uuid"));

    Poco::Util::AbstractConfiguration::Keys keys;
    config->keys("contents", keys);
    for (const auto & key : keys)
    {
        if ((key == "file") || key.starts_with("file["))
        {
            String prefix = "contents." + key + ".";
            FileInfo info;
            info.file_name = config->getString(prefix + "name");
            info.size = config->getUInt(prefix + "size");
            if (info.size)
            {
                info.checksum = unhexChecksum(config->getString(prefix + "checksum"));

                bool use_base = config->getBool(prefix + "use_base", false);
                info.base_size = config->getUInt(prefix + "base_size", use_base ? info.size : 0);
                if (info.base_size)
                    use_base = true;

                if (info.base_size > info.size)
                    throw Exception(ErrorCodes::BACKUP_DAMAGED, "Backup {}: Base size must not be greater than the size of entry {}", backup_name, quoteString(info.file_name));

                if (use_base)
                {
                    if (info.base_size == info.size)
                        info.base_checksum = info.checksum;
                    else
                        info.base_checksum = unhexChecksum(config->getString(prefix + "base_checksum"));
                }

                if (info.size > info.base_size)
                {
                    info.data_file_name = config->getString(prefix + "data_file", info.file_name);
                    info.archive_suffix = config->getString(prefix + "archive_suffix", "");
                    info.pos_in_archive = config->getUInt64(prefix + "pos_in_archive", static_cast<UInt64>(-1));
                }
            }

            coordination->addFileInfo(info);
        }
    }
}

Strings BackupImpl::listFiles(const String & prefix, const String & terminator) const
{
    std::lock_guard lock{mutex};
    if (!prefix.ends_with('/') && !prefix.empty())
        throw Exception("prefix should end with '/'", ErrorCodes::BAD_ARGUMENTS);
    return coordination->listFiles(prefix, terminator);
}

bool BackupImpl::fileExists(const String & file_name) const
{
    std::lock_guard lock{mutex};
    return coordination->getFileInfo(file_name).has_value();
}

bool BackupImpl::fileExists(const SizeAndChecksum & size_and_checksum) const
{
    std::lock_guard lock{mutex};
    return coordination->getFileInfo(size_and_checksum).has_value();
}

UInt64 BackupImpl::getFileSize(const String & file_name) const
{
    std::lock_guard lock{mutex};
    auto info = coordination->getFileInfo(file_name);
    if (!info)
        throw Exception(
            ErrorCodes::BACKUP_ENTRY_NOT_FOUND, "Backup {}: Entry {} not found in the backup", backup_name, quoteString(file_name));
    return info->size;
}

UInt128 BackupImpl::getFileChecksum(const String & file_name) const
{
    std::lock_guard lock{mutex};
    auto info = coordination->getFileInfo(file_name);
    if (!info)
        throw Exception(
            ErrorCodes::BACKUP_ENTRY_NOT_FOUND, "Backup {}: Entry {} not found in the backup", backup_name, quoteString(file_name));
    return info->checksum;
}

SizeAndChecksum BackupImpl::getFileSizeAndChecksum(const String & file_name) const
{
    std::lock_guard lock{mutex};
    auto info = coordination->getFileInfo(file_name);
    if (!info)
        throw Exception(
            ErrorCodes::BACKUP_ENTRY_NOT_FOUND, "Backup {}: Entry {} not found in the backup", backup_name, quoteString(file_name));
    return std::pair(info->size, info->checksum);
}

BackupEntryPtr BackupImpl::readFile(const String & file_name) const
{
    return readFile(getFileSizeAndChecksum(file_name));
}

BackupEntryPtr BackupImpl::readFile(const SizeAndChecksum & size_and_checksum) const
{
    std::lock_guard lock{mutex};
    if (open_mode != OpenMode::READ)
        throw Exception("Backup is not opened for reading", ErrorCodes::LOGICAL_ERROR);

    auto info_opt = coordination->getFileInfo(size_and_checksum);
    if (!info_opt)
        throw Exception(
            ErrorCodes::BACKUP_ENTRY_NOT_FOUND, "Backup {}: Entry {} not found in the backup", backup_name, formatSizeAndChecksum(size_and_checksum));

    const auto & info = *info_opt;
    if (!info.size)
    {
        /// Entry's data is empty.
        return std::make_unique<BackupEntryFromMemory>(nullptr, 0, UInt128{0, 0});
    }

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
            backup_name, formatSizeAndChecksum(size_and_checksum));
    }

    if (!base_backup->fileExists(std::pair(info.base_size, info.base_checksum)))
    {
        throw Exception(
            ErrorCodes::WRONG_BASE_BACKUP,
            "Backup {}: Entry {} is marked to be read from a base backup, but doesn't exist there",
            backup_name, formatSizeAndChecksum(size_and_checksum));
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


void BackupImpl::writeFile(const String & file_name, BackupEntryPtr entry)
{
    std::lock_guard lock{mutex};
    if (open_mode != OpenMode::WRITE)
        throw Exception("Backup is not opened for writing", ErrorCodes::LOGICAL_ERROR);

    if (coordination->getFileInfo(file_name))
        throw Exception(
            ErrorCodes::BACKUP_ENTRY_ALREADY_EXISTS, "Backup {}: Entry {} already exists", backup_name, quoteString(file_name));

    FileInfo info;
    info.file_name = file_name;
    size_t size = entry->getSize();
    info.size = size;

    /// Check if the entry's data is empty.
    if (!info.size)
    {
        coordination->addFileInfo(info);
        return;
    }

    /// Maybe we have a copy of this file in the backup already.
    std::optional<UInt128> checksum = entry->getChecksum();
    if (checksum && coordination->getFileInfo(std::pair{size, *checksum}))
    {
        info.checksum = *checksum;
        coordination->addFileInfo(info);
        return;
    }

    /// Check if a entry with such name exists in the base backup.
    bool base_exists = (base_backup && base_backup->fileExists(file_name));
    UInt64 base_size = 0;
    UInt128 base_checksum{0, 0};
    if (base_exists)
    {
        base_size = base_backup->getFileSize(file_name);
        base_checksum = base_backup->getFileChecksum(file_name);
    }

    std::unique_ptr<SeekableReadBuffer> read_buffer; /// We'll set that later.
    std::optional<HashingReadBuffer> hashing_read_buffer;
    UInt64 hashing_pos = 0; /// Current position in `hashing_read_buffer`.

    /// Determine whether it's possible to receive this entry's data from the base backup completely or partly.
    bool use_base = false;
    if (base_exists && base_size && (size >= base_size))
    {
        if (checksum && (size == base_size))
        {
            /// The size is the same, we need to compare checksums to find out
            /// if the entry's data has not changed since the base backup.
            use_base = (*checksum == base_checksum);
        }
        else
        {
            /// The size has increased, we need to calculate a partial checksum to find out
            /// if the entry's data has only appended since the base backup.
            read_buffer = entry->getReadBuffer();
            hashing_read_buffer.emplace(*read_buffer);
            hashing_read_buffer->ignore(base_size);
            hashing_pos = base_size;
            UInt128 partial_checksum = hashing_read_buffer->getHash();
            if (size == base_size)
                checksum = partial_checksum;
            if (partial_checksum == base_checksum)
                use_base = true;
        }
    }

    /// Finish calculating the checksum.
    if (!checksum)
    {
        if (!read_buffer)
            read_buffer = entry->getReadBuffer();
        if (!hashing_read_buffer)
            hashing_read_buffer.emplace(*read_buffer);
        hashing_read_buffer->ignore(size - hashing_pos);
        checksum = hashing_read_buffer->getHash();
    }
    hashing_read_buffer.reset();
    info.checksum = *checksum;

    /// Maybe we have a copy of this file in the backup already.
    if (coordination->getFileInfo(std::pair{size, *checksum}))
    {
        coordination->addFileInfo(info);
        return;
    }

    /// Check if a entry with the same checksum exists in the base backup.
    if (base_backup && !use_base && base_backup->fileExists(std::pair{size, *checksum}))
    {
        /// The entry's data has not changed since the base backup,
        /// but the entry itself has been moved or renamed.
        base_size = size;
        base_checksum = *checksum;
        use_base = true;
    }

    if (use_base)
    {
        info.base_size = base_size;
        info.base_checksum = base_checksum;
    }

    if (use_base && (size == base_size))
    {
        /// The entry's data has not been changed since the base backup.
        coordination->addFileInfo(info);
        return;
    }

    bool is_data_file_required;
    info.data_file_name = info.file_name;
    info.archive_suffix = current_archive_suffix;
    coordination->addFileInfo(info, is_data_file_required);
    if (!is_data_file_required)
        return; /// We copy data only if it's a new combination of size & checksum.

    /// Either the entry wasn't exist in the base backup
    /// or the entry has data appended to the end of the data from the base backup.
    /// In both those cases we have to copy data to this backup.

    /// Find out where the start position to copy data is.
    auto copy_pos = use_base ? base_size : 0;

    /// Move the current read position to the start position to copy data.
    if (!read_buffer)
        read_buffer = entry->getReadBuffer();
    read_buffer->seek(copy_pos, SEEK_SET);

    /// Copy the entry's data after `copy_pos`.
    std::unique_ptr<WriteBuffer> out;
    if (use_archives)
    {
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
        out = getArchiveWriter(current_archive_suffix)->writeFile(info.data_file_name);
    }
    else
    {
        out = writer->writeFile(info.data_file_name);
    }

    copyData(*read_buffer, *out);
}


void BackupImpl::finalizeWriting()
{
    std::lock_guard lock{mutex};
    if (open_mode != OpenMode::WRITE)
        throw Exception("Backup is not opened for writing", ErrorCodes::LOGICAL_ERROR);

    writing_finalized = true;
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

    size_t pos = suffix.empty() ? 0 : 1;
    archive_writers[pos] = {suffix, new_archive_writer};

    return new_archive_writer;
}

void BackupImpl::removeAllFilesAfterFailure()
{
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

    writer->removeFilesAfterFailure(files_to_remove);
}

}
