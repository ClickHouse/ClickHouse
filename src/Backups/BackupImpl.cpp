#include <Backups/BackupImpl.h>
#include <Backups/BackupFactory.h>
#include <Backups/BackupEntryConcat.h>
#include <Backups/BackupEntryFromCallback.h>
#include <Backups/BackupEntryFromMemory.h>
#include <Backups/IBackupEntry.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/hex.h>
#include <Common/typeid_cast.h>
#include <Common/quoteString.h>
#include <IO/HashingReadBuffer.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadHelpers.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <Poco/Util/XMLConfiguration.h>
#include <boost/range/adaptor/map.hpp>


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
    const UInt64 BACKUP_VERSION = 1;

    UInt128 unhexChecksum(const String & checksum)
    {
        if (checksum.size() != sizeof(UInt128) * 2)
            throw Exception(ErrorCodes::BACKUP_DAMAGED, "Unexpected size of checksum: {}, must be {}", checksum.size(), sizeof(UInt128) * 2);
        return unhexUInt<UInt128>(checksum.data());
    }
}

BackupImpl::BackupImpl(const String & backup_name_, OpenMode open_mode_, const ContextPtr & context_, const std::optional<BackupInfo> & base_backup_info_)
    : backup_name(backup_name_), open_mode(open_mode_), context(context_), base_backup_info(base_backup_info_)
{
}

BackupImpl::~BackupImpl() = default;

void BackupImpl::open()
{
    if (open_mode == OpenMode::WRITE)
    {
        if (backupExists())
            throw Exception(ErrorCodes::BACKUP_ALREADY_EXISTS, "Backup {} already exists", getName());

        timestamp = std::time(nullptr);
        uuid = UUIDHelpers::generateV4();

        startWriting();
        writing_started = true;
    }

    if (open_mode == OpenMode::READ)
    {
        if (!backupExists())
            throw Exception(ErrorCodes::BACKUP_NOT_FOUND, "Backup {} not found", getName());
        readBackupMetadata();
    }

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
                            getName(), base_backup->getName(), toString(base_backup->getUUID()), (base_backup_uuid ? toString(*base_backup_uuid) : ""));
    }
}

void BackupImpl::close()
{
    if (open_mode == OpenMode::WRITE)
    {
        if (writing_started && !writing_finalized)
        {
            /// Creating of the backup wasn't finished correctly,
            /// so the backup cannot be used and it's better to remove its files.
            removeAllFilesAfterFailure();
        }
    }
}

void BackupImpl::writeBackupMetadata()
{
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config{new Poco::Util::XMLConfiguration()};
    config->setUInt("version", BACKUP_VERSION);
    config->setString("timestamp", toString(LocalDateTime{timestamp}));
    config->setString("uuid", toString(uuid));

    if (base_backup_info)
        config->setString("base_backup", base_backup_info->toString());
    if (base_backup_uuid)
        config->setString("base_backup_uuid", toString(*base_backup_uuid));

    size_t index = 0;
    for (const auto & [name, info] : file_infos)
    {
        String prefix = index ? "contents.file[" + std::to_string(index) + "]." : "contents.file.";
        config->setString(prefix + "name", name);
        config->setUInt(prefix + "size", info.size);
        if (info.size)
        {
            config->setString(prefix + "checksum", getHexUIntLowercase(info.checksum));
            if (info.base_size)
            {
                config->setUInt(prefix + "base_size", info.base_size);
                if (info.base_size != info.size)
                    config->setString(prefix + "base_checksum", getHexUIntLowercase(info.base_checksum));
            }
        }
        ++index;
    }

    std::ostringstream stream; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    config->save(stream);
    String str = stream.str();
    auto out = addFileImpl(".backup");
    out->write(str.data(), str.size());
}

void BackupImpl::readBackupMetadata()
{
    auto in = readFileImpl(".backup");
    String str;
    readStringUntilEOF(str, *in);
    std::istringstream stream(str); // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config{new Poco::Util::XMLConfiguration()};
    config->load(stream);

    UInt64 version = config->getUInt("version");
    if (version != BACKUP_VERSION)
        throw Exception(ErrorCodes::BACKUP_VERSION_NOT_SUPPORTED, "Backup {}: Version {} is not supported", getName(), version);

    timestamp = parse<LocalDateTime>(config->getString("timestamp")).to_time_t();
    uuid = parse<UUID>(config->getString("uuid"));

    if (config->has("base_backup") && !base_backup_info)
        base_backup_info = BackupInfo::fromString(config->getString("base_backup"));

    if (config->has("base_backup_uuid") && !base_backup_uuid)
        base_backup_uuid = parse<UUID>(config->getString("base_backup_uuid"));

    file_infos.clear();
    Poco::Util::AbstractConfiguration::Keys keys;
    config->keys("contents", keys);
    for (const auto & key : keys)
    {
        if ((key == "file") || key.starts_with("file["))
        {
            String prefix = "contents." + key + ".";
            String name = config->getString(prefix + "name");
            FileInfo & info = file_infos.emplace(name, FileInfo{}).first->second;
            info.size = config->getUInt(prefix + "size");
            if (info.size)
            {
                info.checksum = unhexChecksum(config->getString(prefix + "checksum"));
                if (config->has(prefix + "base_size"))
                {
                    info.base_size = config->getUInt(prefix + "base_size");
                    if (info.base_size == info.size)
                        info.base_checksum = info.checksum;
                    else
                        info.base_checksum = unhexChecksum(config->getString(prefix + "base_checksum"));
                }
            }
        }
    }
}

Strings BackupImpl::listFiles(const String & prefix, const String & terminator) const
{
    if (!prefix.ends_with('/') && !prefix.empty())
        throw Exception("prefix should end with '/'", ErrorCodes::BAD_ARGUMENTS);
    std::lock_guard lock{mutex};
    Strings elements;
    for (auto it = file_infos.lower_bound(prefix); it != file_infos.end(); ++it)
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

bool BackupImpl::fileExists(const String & file_name) const
{
    std::lock_guard lock{mutex};
    return file_infos.count(file_name) != 0;
}

size_t BackupImpl::getFileSize(const String & file_name) const
{
    std::lock_guard lock{mutex};
    auto it = file_infos.find(file_name);
    if (it == file_infos.end())
        throw Exception(
            ErrorCodes::BACKUP_ENTRY_NOT_FOUND, "Backup {}: Entry {} not found in the backup", getName(), quoteString(file_name));
    return it->second.size;
}

UInt128 BackupImpl::getFileChecksum(const String & file_name) const
{
    std::lock_guard lock{mutex};
    auto it = file_infos.find(file_name);
    if (it == file_infos.end())
        throw Exception(
            ErrorCodes::BACKUP_ENTRY_NOT_FOUND, "Backup {}: Entry {} not found in the backup", getName(), quoteString(file_name));
    return it->second.checksum;
}


BackupEntryPtr BackupImpl::readFile(const String & file_name) const
{
    std::lock_guard lock{mutex};
    auto it = file_infos.find(file_name);
    if (it == file_infos.end())
        throw Exception(
            ErrorCodes::BACKUP_ENTRY_NOT_FOUND, "Backup {}: Entry {} not found in the backup", getName(), quoteString(file_name));

    const auto & info = it->second;
    if (!info.size)
    {
        /// Entry's data is empty.
        return std::make_unique<BackupEntryFromMemory>(nullptr, 0, UInt128{0, 0});
    }

    auto read_callback = [backup = std::static_pointer_cast<const BackupImpl>(shared_from_this()), file_name]()
    {
        return backup->readFileImpl(file_name);
    };

    if (!info.base_size)
    {
        /// Data goes completely from this backup, the base backup isn't used.
        return std::make_unique<BackupEntryFromCallback>(read_callback, info.size, info.checksum);
    }

    if (info.size < info.base_size)
    {
        throw Exception(
            ErrorCodes::BACKUP_DAMAGED,
            "Backup {}: Entry {} has its data size less than in the base backup {}: {} < {}",
            getName(), quoteString(file_name), base_backup->getName(), info.size, info.base_size);
    }

    if (!base_backup)
    {
        throw Exception(
            ErrorCodes::NO_BASE_BACKUP,
            "Backup {}: Entry {} is marked to be read from a base backup, but there is no base backup specified",
            getName(), quoteString(file_name));
    }

    if (!base_backup->fileExists(file_name))
    {
        throw Exception(
            ErrorCodes::WRONG_BASE_BACKUP,
            "Backup {}: Entry {} is marked to be read from a base backup, but doesn't exist there",
            getName(), quoteString(file_name));
    }

    auto base_entry = base_backup->readFile(file_name);
    auto base_size = base_entry->getSize();
    if (base_size != info.base_size)
    {
        throw Exception(
            ErrorCodes::WRONG_BASE_BACKUP,
            "Backup {}: Entry {} has unexpected size in the base backup {}: {} (expected size: {})",
            getName(), quoteString(file_name), base_backup->getName(), base_size, info.base_size);
    }

    auto base_checksum = base_entry->getChecksum();
    if (base_checksum && (*base_checksum != info.base_checksum))
    {
        throw Exception(
            ErrorCodes::WRONG_BASE_BACKUP,
            "Backup {}: Entry {} has unexpected checksum in the base backup {}",
            getName(), quoteString(file_name), base_backup->getName());
    }

    if (info.size == info.base_size)
    {
        /// Data goes completely from the base backup (nothing goes from this backup).
        return base_entry;
    }

    /// The beginning of the data goes from the base backup,
    /// and the ending goes from this backup.
    return std::make_unique<BackupEntryConcat>(
        std::move(base_entry),
        std::make_unique<BackupEntryFromCallback>(read_callback, info.size - info.base_size),
        info.checksum);
}


void BackupImpl::addFile(const String & file_name, BackupEntryPtr entry)
{
    std::lock_guard lock{mutex};
    if (open_mode != OpenMode::WRITE)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Illegal operation: Cannot write to a backup opened for reading");

    if (file_infos.contains(file_name))
        throw Exception(
            ErrorCodes::BACKUP_ENTRY_ALREADY_EXISTS, "Backup {}: Entry {} already exists", getName(), quoteString(file_name));

    UInt64 size = entry->getSize();
    std::optional<UInt128> checksum = entry->getChecksum();

    /// Check if the entry's data is empty.
    if (!size)
    {
        file_infos.emplace(file_name, FileInfo{});
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

    std::unique_ptr<ReadBuffer> read_buffer; /// We'll set that later.
    UInt64 read_pos = 0; /// Current position in read_buffer.

    /// Determine whether it's possible to receive this entry's data from the base backup completely or partly.
    bool use_base = false;
    if (base_exists && base_size)
    {
        if (size == base_size)
        {
            /// The size is the same, we need to compare checksums to find out
            /// if the entry's data has not been changed since the base backup.
            if (!checksum)
            {
                read_buffer = entry->getReadBuffer();
                HashingReadBuffer hashing_read_buffer{*read_buffer};
                hashing_read_buffer.ignore(size);
                read_pos = size;
                checksum = hashing_read_buffer.getHash();
            }
            if (checksum == base_checksum)
                use_base = true; /// The data has not been changed.
        }
        else if (size > base_size)
        {
            /// The size has been increased, we need to calculate a partial checksum to find out
            /// if the entry's data has been only appended since the base backup.
            read_buffer = entry->getReadBuffer();
            HashingReadBuffer hashing_read_buffer{*read_buffer};
            hashing_read_buffer.ignore(base_size);
            UInt128 partial_checksum = hashing_read_buffer.getHash();
            read_pos = base_size;
            if (!checksum)
            {
                hashing_read_buffer.ignore(size - base_size);
                checksum = hashing_read_buffer.getHash();
                read_pos = size;
            }
            if (partial_checksum == base_checksum)
                use_base = true; /// The data has been appended.
        }
    }

    if (use_base && (size == base_size))
    {
        /// The entry's data has not been changed since the base backup.
        FileInfo info;
        info.size = base_size;
        info.checksum = base_checksum;
        info.base_size = base_size;
        info.base_checksum = base_checksum;
        file_infos.emplace(file_name, info);
        return;
    }

    {
        /// Either the entry wasn't exist in the base backup
        /// or the entry has data appended to the end of the data from the base backup.
        /// In both those cases we have to copy data to this backup.

        /// Find out where the start position to copy data is.
        auto copy_pos = use_base ? base_size : 0;

        /// Move the current read position to the start position to copy data.
        /// If `read_buffer` is seekable it's easier, otherwise we can use ignore().
        if (auto * seekable_buffer = dynamic_cast<SeekableReadBuffer *>(read_buffer.get()))
        {
            if (read_pos != copy_pos)
                seekable_buffer->seek(copy_pos, SEEK_SET);
        }
        else
        {
            if (read_pos > copy_pos)
            {
                read_buffer.reset();
                read_pos = 0;
            }

            if (!read_buffer)
                read_buffer = entry->getReadBuffer();

            if (read_pos < copy_pos)
                read_buffer->ignore(copy_pos - read_pos);
        }

        /// If we haven't received or calculated a checksum yet, calculate it now.
        ReadBuffer * maybe_hashing_read_buffer = read_buffer.get();
        std::optional<HashingReadBuffer> hashing_read_buffer;
        if (!checksum)
            maybe_hashing_read_buffer = &hashing_read_buffer.emplace(*read_buffer);

        /// Copy the entry's data after `copy_pos`.
        auto out = addFileImpl(file_name);
        copyData(*maybe_hashing_read_buffer, *out);

        if (hashing_read_buffer)
            checksum = hashing_read_buffer->getHash();

        /// Done!
        FileInfo info;
        info.size = size;
        info.checksum = *checksum;
        if (use_base)
        {
            info.base_size = base_size;
            info.base_checksum = base_checksum;
        }
        file_infos.emplace(file_name, info);
    }
}

void BackupImpl::finalizeWriting()
{
    if (open_mode != OpenMode::WRITE)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Illegal operation: Cannot write to a backup opened for reading");
    writeBackupMetadata();
    writing_finalized = true;
}

}
