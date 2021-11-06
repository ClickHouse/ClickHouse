#include <Backups/BackupInDirectory.h>
#include <Backups/BackupFactory.h>
#include <Backups/BackupEntryConcat.h>
#include <Backups/BackupEntryFromImmutableFile.h>
#include <Backups/BackupEntryFromMemory.h>
#include <Backups/IBackupEntry.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/hex.h>
#include <Common/typeid_cast.h>
#include <Common/quoteString.h>
#include <Disks/DiskSelector.h>
#include <Disks/IDisk.h>
#include <Disks/DiskLocal.h>
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
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
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

BackupInDirectory::BackupInDirectory(const String & backup_name_, OpenMode open_mode_, const DiskPtr & disk_, const String & path_, const ContextPtr & context_, const std::optional<BackupInfo> & base_backup_info_)
    : backup_name(backup_name_), open_mode(open_mode_), disk(disk_), path(path_), context(context_), base_backup_info(base_backup_info_)
{
    if (!path.empty() && (path.back() != '/'))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Backup {}: Path to backup must end with '/', but {} doesn't.", getName(), path);

    if (!disk)
    {
        if (path.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Backup {}: Path to backup must not be empty.", getName());

        disk = std::make_shared<DiskLocal>("internal", path, 0);
        path = "";
    }

    open();
}

BackupInDirectory::~BackupInDirectory()
{
    close();
}

void BackupInDirectory::open()
{
    if (open_mode == OpenMode::WRITE)
    {
        if (disk->exists(path + ".contents"))
            throw Exception(ErrorCodes::BACKUP_ALREADY_EXISTS, "Backup {} already exists", getName());

        if (!path.empty() && !disk->isDirectory(path))
        {
            disk->createDirectories(path);
            directory_was_created = true;
        }
    }

    if (open_mode == OpenMode::READ)
    {
        if (!path.empty() && !disk->isDirectory(path))
            throw Exception(ErrorCodes::BACKUP_NOT_FOUND, "Backup {} not found", getName());
        readMetadata();
    }

    if (base_backup_info)
    {
        BackupFactory::CreateParams params;
        params.backup_info = *base_backup_info;
        params.open_mode = OpenMode::READ;
        params.context = context;
        base_backup = BackupFactory::instance().createBackup(params);
    }
}

void BackupInDirectory::close()
{
    if (open_mode == OpenMode::WRITE)
    {
        if (!finalized && directory_was_created)
        {
            /// Creating of the backup wasn't finished correctly,
            /// so the backup cannot be used and it's better to remove its files.
            disk->removeRecursive(path);
        }
    }
}

void BackupInDirectory::writeMetadata()
{
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config{new Poco::Util::XMLConfiguration()};
    config->setUInt("version", BACKUP_VERSION);

    if (base_backup_info)
        config->setString("base_backup", base_backup_info->toString());

    size_t index = 0;
    for (const auto & [name, info] : infos)
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

    std::ostringstream stream;
    config->save(stream);
    auto out = disk->writeFile(path + ".backup");
    writeString(std::move(stream).str(), *out);
}

void BackupInDirectory::readMetadata()
{
    auto in = disk->readFile(path + ".backup");
    String str;
    readStringUntilEOF(str, *in);
    std::istringstream stream(std::move(str));
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config{new Poco::Util::XMLConfiguration()};
    config->load(stream);

    UInt64 version = config->getUInt("version", 1);
    if (version != BACKUP_VERSION)
        throw Exception(ErrorCodes::BACKUP_VERSION_NOT_SUPPORTED, "Backup {}: Version {} is not supported", getName(), version);

    if (config->has("base_backup") && !base_backup_info)
        base_backup_info.emplace(BackupInfo::fromString(config->getString("base_backup")));

    infos.clear();
    Poco::Util::AbstractConfiguration::Keys keys;
    config->keys("contents", keys);
    for (const auto & key : keys)
    {
        if ((key == "file") || key.starts_with("file["))
        {
            String prefix = "contents." + key + ".";
            String name = config->getString(prefix + "name");
            EntryInfo & info = infos.emplace(name, EntryInfo{}).first->second;
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

Strings BackupInDirectory::list(const String & prefix, const String & terminator) const
{
    if (!prefix.ends_with('/') && !prefix.empty())
        throw Exception("prefix should end with '/'", ErrorCodes::BAD_ARGUMENTS);
    std::lock_guard lock{mutex};
    Strings elements;
    for (auto it = infos.lower_bound(prefix); it != infos.end(); ++it)
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

bool BackupInDirectory::exists(const String & name) const
{
    std::lock_guard lock{mutex};
    return infos.count(name) != 0;
}

size_t BackupInDirectory::getSize(const String & name) const
{
    std::lock_guard lock{mutex};
    auto it = infos.find(name);
    if (it == infos.end())
        throw Exception(
            ErrorCodes::BACKUP_ENTRY_NOT_FOUND, "Backup {}: Entry {} not found in the backup", getName(), quoteString(name));
    return it->second.size;
}

UInt128 BackupInDirectory::getChecksum(const String & name) const
{
    std::lock_guard lock{mutex};
    auto it = infos.find(name);
    if (it == infos.end())
        throw Exception(
            ErrorCodes::BACKUP_ENTRY_NOT_FOUND, "Backup {}: Entry {} not found in the backup", getName(), quoteString(name));
    return it->second.checksum;
}


BackupEntryPtr BackupInDirectory::read(const String & name) const
{
    std::lock_guard lock{mutex};
    auto it = infos.find(name);
    if (it == infos.end())
        throw Exception(
            ErrorCodes::BACKUP_ENTRY_NOT_FOUND, "Backup {}: Entry {} not found in the backup", getName(), quoteString(name));

    const auto & info = it->second;
    if (!info.size)
    {
        /// Entry's data is empty.
        return std::make_unique<BackupEntryFromMemory>(nullptr, 0, UInt128{0, 0});
    }

    if (!info.base_size)
    {
        /// Data goes completely from this backup, the base backup isn't used.
        return std::make_unique<BackupEntryFromImmutableFile>(disk, path + name, info.size, info.checksum);
    }

    if (info.size < info.base_size)
    {
        throw Exception(
            ErrorCodes::BACKUP_DAMAGED,
            "Backup {}: Entry {} has its data size less than in the base backup {}: {} < {}",
            getName(), quoteString(name), base_backup->getName(), info.size, info.base_size);
    }

    if (!base_backup)
    {
        throw Exception(
            ErrorCodes::NO_BASE_BACKUP,
            "Backup {}: Entry {} is marked to be read from a base backup, but there is no base backup specified",
            getName(), quoteString(name));
    }

    if (!base_backup->exists(name))
    {
        throw Exception(
            ErrorCodes::WRONG_BASE_BACKUP,
            "Backup {}: Entry {} is marked to be read from a base backup, but doesn't exist there",
            getName(), quoteString(name));
    }

    auto base_entry = base_backup->read(name);
    auto base_size = base_entry->getSize();
    if (base_size != info.base_size)
    {
        throw Exception(
            ErrorCodes::WRONG_BASE_BACKUP,
            "Backup {}: Entry {} has unexpected size in the base backup {}: {} (expected size: {})",
            getName(), quoteString(name), base_backup->getName(), base_size, info.base_size);
    }

    auto base_checksum = base_entry->getChecksum();
    if (base_checksum && (*base_checksum != info.base_checksum))
    {
        throw Exception(
            ErrorCodes::WRONG_BASE_BACKUP,
            "Backup {}: Entry {} has unexpected checksum in the base backup {}",
            getName(), quoteString(name), base_backup->getName());
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
        std::make_unique<BackupEntryFromImmutableFile>(disk, path + name, info.size - info.base_size),
        info.checksum);
}


void BackupInDirectory::write(const String & name, BackupEntryPtr entry)
{
    std::lock_guard lock{mutex};
    if (open_mode != OpenMode::WRITE)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Illegal operation: Cannot write to a backup opened for reading");

    if (infos.contains(name))
        throw Exception(
            ErrorCodes::BACKUP_ENTRY_ALREADY_EXISTS, "Backup {}: Entry {} already exists", getName(), quoteString(name));

    UInt64 size = entry->getSize();
    std::optional<UInt128> checksum = entry->getChecksum();

    /// Check if the entry's data is empty.
    if (!size)
    {
        infos.emplace(name, EntryInfo{});
        return;
    }

    /// Check if a entry with such name exists in the base backup.
    bool base_exists = (base_backup && base_backup->exists(name));
    UInt64 base_size = 0;
    UInt128 base_checksum{0, 0};
    if (base_exists)
    {
        base_size = base_backup->getSize(name);
        base_checksum = base_backup->getChecksum(name);
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
        EntryInfo info;
        info.size = base_size;
        info.checksum = base_checksum;
        info.base_size = base_size;
        info.base_checksum = base_checksum;
        infos.emplace(name, info);
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
        if ((read_pos > copy_pos) && !typeid_cast<SeekableReadBuffer *>(read_buffer.get()))
        {
            read_buffer.reset();
            read_pos = 0;
        }

        if (!read_buffer)
            read_buffer = entry->getReadBuffer();

        if (read_pos != copy_pos)
        {
            if (auto * seekable_buffer = typeid_cast<SeekableReadBuffer *>(read_buffer.get()))
                seekable_buffer->seek(copy_pos, SEEK_SET);
            else if (copy_pos)
                read_buffer->ignore(copy_pos - read_pos);
        }

        /// If we haven't received or calculated a checksum yet, calculate it now.
        ReadBuffer * maybe_hashing_read_buffer = read_buffer.get();
        std::optional<HashingReadBuffer> hashing_read_buffer;
        if (!checksum)
            maybe_hashing_read_buffer = &hashing_read_buffer.emplace(*read_buffer);

        /// Copy the entry's data after `copy_pos`.
        String out_file_path = path + name;
        disk->createDirectories(directoryPath(out_file_path));
        auto out = disk->writeFile(out_file_path);

        copyData(*maybe_hashing_read_buffer, *out, size - copy_pos);

        if (hashing_read_buffer)
            checksum = hashing_read_buffer->getHash();

        /// Done!
        EntryInfo info;
        info.size = size;
        info.checksum = *checksum;
        if (use_base)
        {
            info.base_size = base_size;
            info.base_checksum = base_checksum;
        }
        infos.emplace(name, info);
    }
}

void BackupInDirectory::finalizeWriting()
{
    if (open_mode != OpenMode::WRITE)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Illegal operation: Cannot write to a backup opened for reading");
    writeMetadata();
    finalized = true;
}


void registerBackupEngineFile(BackupFactory & factory)
{
    auto creator_fn = [](const BackupFactory::CreateParams & params)
    {
        String backup_name = params.backup_info.toString();
        const String & engine_name = params.backup_info.backup_engine_name;
        const auto & args = params.backup_info.args;

        DiskPtr disk;
        String path;
        if (engine_name == "File")
        {
            if (args.size() != 1)
            {
                throw Exception(
                    "Backup engine 'File' requires 1 argument (path)",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
            }
            path = args[0].safeGet<String>();
        }
        else if (engine_name == "Disk")
        {
            if (args.size() < 1 || args.size() > 2)
            {
                throw Exception(
                    "Backup engine 'Disk' requires 1 or 2 arguments",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
            }
            String disk_name = args[0].safeGet<String>();
            disk = params.context->getDisk(disk_name);
            if (args.size() >= 2)
                path = args[1].safeGet<String>();
        }

        return std::make_shared<BackupInDirectory>(backup_name, params.open_mode, disk, path, params.context, params.base_backup_info);
    };

    factory.registerBackupEngine("File", creator_fn);
    factory.registerBackupEngine("Disk", creator_fn);
}

}
