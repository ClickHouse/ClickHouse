#include <Backups/BackupImpl.h>
#include <Backups/BackupFactory.h>
#include <Backups/BackupEntryFromMemory.h>
#include <Backups/IBackupEntry.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/hex.h>
#include <Common/quoteString.h>
#include <IO/ConcatSeekableReadBuffer.h>
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
    const UInt64 INITIAL_BACKUP_VERSION = 1;
    const UInt64 CURRENT_BACKUP_VERSION = 2;

    const UInt64 BACKUP_VERSION_USE_CHECKSUMS_AS_DATA_FILE_NAMES = 2;

    UInt128 unhexChecksum(const String & checksum)
    {
        if (checksum.size() != sizeof(UInt128) * 2)
            throw Exception(ErrorCodes::BACKUP_DAMAGED, "Unexpected size of checksum: {}, must be {}", checksum.size(), sizeof(UInt128) * 2);
        return unhexUInt<UInt128>(checksum.data());
    }
}


class BackupImpl::BackupEntryFromBackupImpl : public IBackupEntry
{
public:
    BackupEntryFromBackupImpl(
        const std::shared_ptr<const BackupImpl> & backup_,
        const String & data_file_name_,
        UInt64 size_,
        const UInt128 checksum_,
        BackupEntryPtr base_backup_entry_ = {})
        : backup(backup_), data_file_name(data_file_name_), size(size_), checksum(checksum_),
          base_backup_entry(std::move(base_backup_entry_))
    {
    }

    std::unique_ptr<SeekableReadBuffer> getReadBuffer() const override
    {
        auto read_buffer = backup->readFileImpl(data_file_name);
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
    const String data_file_name;
    const UInt64 size;
    const UInt128 checksum;
    BackupEntryPtr base_backup_entry;
};


class BackupImpl::LocalFileInfos : public IFileInfos
{
public:
    LocalFileInfos() = default;
    ~LocalFileInfos() override = default;

    void add(FileInfo && file_info, bool & is_new_checksum) override
    {
        file_names.emplace(file_info.file_name, file_info.checksum);
        is_new_checksum = (file_info.checksum && !file_infos.contains(file_info.checksum));
        if (is_new_checksum)
            file_infos.emplace(file_info.checksum, std::move(file_info));
    }

    std::vector<FileInfo> getAllFileInfos() override
    {
        std::vector<FileInfo> res;
        for (const auto & [file_name, checksum] : file_names)
        {
            FileInfo info = file_infos.at(checksum);
            info.file_name = file_name;
            res.push_back(std::move(info));
        }
        return res;
    }

    Strings listFiles(const String & prefix, const String & terminator) override
    {
        Strings elements;
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

    std::optional<UInt128> getChecksumByFileName(const String & file_name) override
    {
        auto it = file_names.find(file_name);
        if (it == file_names.end())
            return std::nullopt;
        return it->second;
    }

    std::optional<FileInfo> getFileInfoByChecksum(const UInt128 & checksum) override
    {
        auto it = file_infos.find(checksum);
        if (it == file_infos.end())
            return std::nullopt;
        return it->second;
    }

    std::optional<FileInfo> getFileInfoByFileName(const String & file_name) override
    {
        auto it = file_names.find(file_name);
        if (it == file_names.end())
            return std::nullopt;
        FileInfo info = file_infos.at(it->second);
        info.file_name = file_name;
        return info;
    }

private:
    std::map<String /* file_name */, UInt128 /* checksum */> file_names; /// Should be ordered alphabetically, see listFiles(). For empty files we assume checksum = 0.
    std::unordered_map<UInt128 /* checksum */, FileInfo> file_infos; /// Information about files. Without empty files.
};


BackupImpl::BackupImpl(const String & backup_name_, const ContextPtr & context_, const std::optional<BackupInfo> & base_backup_info_)
    : backup_name(backup_name_), context(context_), base_backup_info_param(base_backup_info_), file_infos(std::make_unique<LocalFileInfos>())
{
}

BackupImpl::~BackupImpl() = default;

void BackupImpl::open(OpenMode open_mode_)
{
    std::lock_guard lock{mutex};
    if (open_mode == open_mode_)
        return;

    if (open_mode != OpenMode::NONE)
        throw Exception("Backup is already opened", ErrorCodes::LOGICAL_ERROR);

    if (open_mode_ == OpenMode::WRITE)
    {
        if (backupExists())
            throw Exception(ErrorCodes::BACKUP_ALREADY_EXISTS, "Backup {} already exists", getName());

        timestamp = std::time(nullptr);
        uuid = UUIDHelpers::generateV4();
        writing_finalized = false;
    }

    if (open_mode_ == OpenMode::READ)
    {
        if (!backupExists())
            throw Exception(ErrorCodes::BACKUP_NOT_FOUND, "Backup {} not found", getName());
    }

    openImpl(open_mode_);

    base_backup_info = base_backup_info_param;
    if (open_mode_ == OpenMode::READ)
        readBackupMetadata();

    if (base_backup_info)
    {
        BackupFactory::CreateParams params;
        params.backup_info = *base_backup_info;
        params.open_mode = OpenMode::READ;
        params.context = context;
        base_backup = BackupFactory::instance().createBackup(params);

        if (open_mode_ == OpenMode::WRITE)
            base_backup_uuid = base_backup->getUUID();
        else if (base_backup_uuid != base_backup->getUUID())
            throw Exception(ErrorCodes::WRONG_BASE_BACKUP, "Backup {}: The base backup {} has different UUID ({} != {})",
                            getName(), base_backup->getName(), toString(base_backup->getUUID()), (base_backup_uuid ? toString(*base_backup_uuid) : ""));
    }

    open_mode = open_mode_;
}

void BackupImpl::close()
{
    std::lock_guard lock{mutex};
    if (open_mode == OpenMode::NONE)
        return;

    closeImpl({}/*written_files*/, writing_finalized);

    uuid = UUIDHelpers::Nil;
    timestamp = 0;
    base_backup_info.reset();
    base_backup.reset();
    base_backup_uuid.reset();
    //file_infos.clear();
    open_mode = OpenMode::NONE;
}

IBackup::OpenMode BackupImpl::getOpenMode() const
{
    std::lock_guard lock{mutex};
    return open_mode;
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
    config->setString("uuid", toString(uuid));

    if (base_backup_info)
    {
        bool base_backup_in_use = false;
        for (const auto & info : file_infos->getAllFileInfos())
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
    for (const auto & info : file_infos->getAllFileInfos())
    {
        String prefix = index ? "contents.file[" + std::to_string(index) + "]." : "contents.file.";
        config->setString(prefix + "name", info.file_name);
        config->setUInt(prefix + "size", info.size);
        if (info.size)
        {
            config->setString(prefix + "checksum", getHexUIntLowercase(info.checksum));
            if (info.base_size)
            {
                config->setBool(prefix + "use_base", true);
                if (info.base_size != info.size)
                {
                    config->setUInt(prefix + "base_size", info.base_size);
                    config->setString(prefix + "base_checksum", getHexUIntLowercase(info.base_checksum));
                }
            }
        }
        ++index;
    }

    std::ostringstream stream; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    config->save(stream);
    String str = stream.str();
    //written_files.push_back(".backup");
    auto out = writeFileImpl(".backup");
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

    version = config->getUInt("version");
    if ((version < INITIAL_BACKUP_VERSION) || (version > CURRENT_BACKUP_VERSION))
        throw Exception(ErrorCodes::BACKUP_VERSION_NOT_SUPPORTED, "Backup {}: Version {} is not supported", getName(), version);

    timestamp = parse<LocalDateTime>(config->getString("timestamp")).to_time_t();
    uuid = parse<UUID>(config->getString("uuid"));

    if (config->has("base_backup") && !base_backup_info)
        base_backup_info = BackupInfo::fromString(config->getString("base_backup"));

    if (config->has("base_backup_uuid"))
        base_backup_uuid = parse<UUID>(config->getString("base_backup_uuid"));

    //file_infos.clear();
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
            info.checksum = info.size ? unhexChecksum(config->getString(prefix + "checksum")) : UInt128{0};

            bool use_base = config->getBool(prefix + "use_base", false);
            info.base_size = config->getUInt(prefix + "base_size", use_base ? info.size : 0);
            if (info.base_size)
                use_base = true;

            if (use_base)
            {
                if (info.base_size == info.size)
                    info.base_checksum = info.checksum;
                else
                    info.base_checksum = unhexChecksum(config->getString(prefix + "base_checksum"));
            }

            file_infos->add(std::move(info));
        }
    }
}

Strings BackupImpl::listFiles(const String & prefix, const String & terminator) const
{
    if (!prefix.ends_with('/') && !prefix.empty())
        throw Exception("prefix should end with '/'", ErrorCodes::BAD_ARGUMENTS);
    return file_infos->listFiles(prefix, terminator);
}

bool BackupImpl::fileExists(const String & file_name) const
{
    std::lock_guard lock{mutex};
    return file_infos->getChecksumByFileName(file_name).has_value();
}

bool BackupImpl::fileExistsByChecksum(const UInt128 & checksum) const
{
    return file_infos->getFileInfoByChecksum(checksum).has_value();
}

size_t BackupImpl::getFileSize(const String & file_name) const
{
    auto info = file_infos->getFileInfoByFileName(file_name);
    if (!info)
        throw Exception(
            ErrorCodes::BACKUP_ENTRY_NOT_FOUND, "Backup {}: Entry {} not found in the backup", getName(), quoteString(file_name));
    return info->size;
}

size_t BackupImpl::getFileSizeByChecksum(const UInt128 & checksum) const
{
    auto info = file_infos->getFileInfoByChecksum(checksum);
    if (!info)
        throw Exception(
            ErrorCodes::BACKUP_ENTRY_NOT_FOUND, "Backup {}: Entry {} not found in the backup", getName(), getHexUIntLowercase(checksum));
    return info->size;
}

UInt128 BackupImpl::getFileChecksum(const String & file_name) const
{
    auto info = file_infos->getFileInfoByFileName(file_name);
    if (!info)
        throw Exception(
            ErrorCodes::BACKUP_ENTRY_NOT_FOUND, "Backup {}: Entry {} not found in the backup", getName(), quoteString(file_name));
    return info->checksum;
}

BackupEntryPtr BackupImpl::readFile(const String & file_name) const
{
    return readFileByChecksum(getFileChecksum(file_name));
}

BackupEntryPtr BackupImpl::readFileByChecksum(const UInt128 & checksum) const
{
    std::lock_guard lock{mutex};
    if (open_mode != OpenMode::READ)
        throw Exception("Backup is not opened for reading", ErrorCodes::LOGICAL_ERROR);

    auto info_opt = file_infos->getFileInfoByChecksum(checksum);
    if (!info_opt)
        throw Exception(
            ErrorCodes::BACKUP_ENTRY_NOT_FOUND, "Backup {}: Entry {} not found in the backup", getName(), getHexUIntLowercase(checksum));

    const auto & info = *info_opt;
    if (!info.size)
    {
        /// Entry's data is empty.
        return std::make_unique<BackupEntryFromMemory>(nullptr, 0, UInt128{0, 0});
    }

    if (!info.base_size)
    {
        /// Data goes completely from this backup, the base backup isn't used.
        String data_file_name = (version >= BACKUP_VERSION_USE_CHECKSUMS_AS_DATA_FILE_NAMES) ? getHexUIntLowercase(checksum) : info.file_name;
        return std::make_unique<BackupEntryFromBackupImpl>(
            std::static_pointer_cast<const BackupImpl>(shared_from_this()), data_file_name, info.size, info.checksum);
    }

    if (info.size < info.base_size)
    {
        throw Exception(
            ErrorCodes::BACKUP_DAMAGED,
            "Backup {}: Entry {} has its data size less than in the base backup {}: {} < {}",
            getName(), getHexUIntLowercase(checksum), base_backup->getName(), info.size, info.base_size);
    }

    if (!base_backup)
    {
        throw Exception(
            ErrorCodes::NO_BASE_BACKUP,
            "Backup {}: Entry {} is marked to be read from a base backup, but there is no base backup specified",
            getName(), getHexUIntLowercase(checksum));
    }

    if (!base_backup->fileExistsByChecksum(info.base_checksum))
    {
        throw Exception(
            ErrorCodes::WRONG_BASE_BACKUP,
            "Backup {}: Entry {} is marked to be read from a base backup, but doesn't exist there",
            getName(), getHexUIntLowercase(checksum));
    }

    auto base_entry = base_backup->readFileByChecksum(info.base_checksum);
    auto base_size = base_entry->getSize();
    if (base_size != info.base_size)
    {
        throw Exception(
            ErrorCodes::WRONG_BASE_BACKUP,
            "Backup {}: Entry {} has unexpected size in the base backup {}: {} (expected size: {})",
            getName(), getHexUIntLowercase(checksum), base_backup->getName(), base_size, info.base_size);
    }

    if (info.size == info.base_size)
    {
        /// Data goes completely from the base backup (nothing goes from this backup).
        return base_entry;
    }

    {
        /// The beginning of the data goes from the base backup,
        /// and the ending goes from this backup.
        String data_file_name = (version >= BACKUP_VERSION_USE_CHECKSUMS_AS_DATA_FILE_NAMES) ? getHexUIntLowercase(checksum) : info.file_name;
        return std::make_unique<BackupEntryFromBackupImpl>(
            static_pointer_cast<const BackupImpl>(shared_from_this()), data_file_name, info.size, info.checksum, std::move(base_entry));
    }
}


void BackupImpl::writeFile(const String & file_name, BackupEntryPtr entry)
{
    std::lock_guard lock{mutex};
    if (open_mode != OpenMode::WRITE)
        throw Exception("Backup is not opened for writing", ErrorCodes::LOGICAL_ERROR);

    if (file_infos->getChecksumByFileName(file_name))
        throw Exception(
            ErrorCodes::BACKUP_ENTRY_ALREADY_EXISTS, "Backup {}: Entry {} already exists", getName(), quoteString(file_name));

    FileInfo info;
    info.file_name = file_name;
    size_t size = entry->getSize();
    info.size = size;

    /// Check if the entry's data is empty.
    if (!info.size)
    {
        file_infos->add(std::move(info));
        return;
    }

    /// Maybe we have a copy of this file in the backup already.
    std::optional<UInt128> checksum = entry->getChecksum();
    if (checksum && file_infos->getFileInfoByChecksum(*checksum))
    {
        info.checksum = *checksum;
        file_infos->add(std::move(info));
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
    if (file_infos->getFileInfoByChecksum(*checksum))
    {
        file_infos->add(std::move(info));
        return;
    }

    /// Check if a entry with the same checksum exists in the base backup.
    if (base_backup && !use_base && base_backup->fileExistsByChecksum(*checksum))
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
        file_infos->add(std::move(info));
        return;
    }

    bool is_new_checksum;
    file_infos->add(std::move(info), is_new_checksum);
    if (!is_new_checksum)
        return; /// We copy data only if it's a new checksum.

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
    auto out = writeFileImpl(getHexUIntLowercase(*checksum));
    copyData(*read_buffer, *out);
}


void BackupImpl::finalizeWriting()
{
    std::lock_guard lock{mutex};
    if (writing_finalized)
        return;

    if (open_mode != OpenMode::WRITE)
        throw Exception("Backup is not opened for writing", ErrorCodes::LOGICAL_ERROR);

    writeBackupMetadata();
    writing_finalized = true;
}

}
