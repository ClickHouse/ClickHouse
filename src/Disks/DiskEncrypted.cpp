#include <Disks/DiskEncrypted.h>

#if USE_SSL
#include <Disks/DiskFactory.h>
#include <IO/FileEncryptionCommon.h>
#include <IO/ReadBufferFromEncryptedFile.h>
#include <IO/ReadBufferFromFileDecorator.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromEncryptedFile.h>
#include <boost/algorithm/hex.hpp>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DISK_INDEX;
    extern const int DATA_ENCRYPTION_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace
{
    using DiskEncryptedPtr = std::shared_ptr<DiskEncrypted>;
    using namespace FileEncryption;

    constexpr Algorithm DEFAULT_ENCRYPTION_ALGORITHM = Algorithm::AES_128_CTR;

    String unhexKey(const String & hex)
    {
        try
        {
            return boost::algorithm::unhex(hex);
        }
        catch (const std::exception &)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot read key_hex, check for valid characters [0-9a-fA-F] and length");
        }
    }

    std::unique_ptr<const DiskEncryptedSettings> parseDiskEncryptedSettings(
        const String & name, const Poco::Util::AbstractConfiguration & config, const String & config_prefix, const DisksMap & map)
    {
        try
        {
            auto res = std::make_unique<DiskEncryptedSettings>();
            res->current_algorithm = DEFAULT_ENCRYPTION_ALGORITHM;
            if (config.has(config_prefix + ".algorithm"))
                parseFromString(res->current_algorithm, config.getString(config_prefix + ".algorithm"));

            Strings config_keys;
            config.keys(config_prefix, config_keys);
            for (const std::string & config_key : config_keys)
            {
                String key;
                UInt64 key_id;

                if ((config_key == "key") || config_key.starts_with("key["))
                {
                    key = config.getString(config_prefix + "." + config_key, "");
                    key_id = config.getUInt64(config_prefix + "." + config_key + "[@id]", 0);
                }
                else if ((config_key == "key_hex") || config_key.starts_with("key_hex["))
                {
                    key = unhexKey(config.getString(config_prefix + "." + config_key, ""));
                    key_id = config.getUInt64(config_prefix + "." + config_key + "[@id]", 0);
                }
                else
                    continue;

                if (res->keys.contains(key_id))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Multiple keys have the same ID {}", key_id);
                res->keys[key_id] = key;
            }

            if (res->keys.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "No keys, an encrypted disk needs keys to work");

            if (!config.has(config_prefix + ".current_key_id"))
            {
                /// In case of multiple keys, current_key_id is mandatory
                if (res->keys.size() > 1)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "There are multiple keys in config. current_key_id is required");

                /// If there is only one key with non zero ID, curren_key_id should be defined.
                if (res->keys.size() == 1 && !res->keys.contains(0))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Config has one key with non zero id. сurrent_key_id is required");
            }

            res->current_key_id = config.getUInt64(config_prefix + ".current_key_id", 0);
            if (!res->keys.contains(res->current_key_id))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not found a key with the current ID {}", res->current_key_id);
            FileEncryption::checkKeySize(res->current_algorithm, res->keys[res->current_key_id].size());

            String wrapped_disk_name = config.getString(config_prefix + ".disk", "");
            if (wrapped_disk_name.empty())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Name of the wrapped disk must not be empty. Encrypted disk is a wrapper over another disk");

            auto wrapped_disk_it = map.find(wrapped_disk_name);
            if (wrapped_disk_it == map.end())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "The wrapped disk must have been announced earlier. No disk with name {}",
                    wrapped_disk_name);
            res->wrapped_disk = wrapped_disk_it->second;

            res->disk_path = config.getString(config_prefix + ".path", "");
            if (!res->disk_path.empty() && (res->disk_path.back() != '/'))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Disk path must ends with '/', but '{}' doesn't.", quoteString(res->disk_path));

            return res;
        }
        catch (Exception & e)
        {
            e.addMessage("Disk " + name);
            throw;
        }
    }

    FileEncryption::Header readHeader(ReadBufferFromFileBase & read_buffer)
    {
        try
        {
            FileEncryption::Header header;
            header.read(read_buffer);
            return header;
        }
        catch (Exception & e)
        {
            e.addMessage("While reading the header of encrypted file " + quoteString(read_buffer.getFileName()));
            throw;
        }
    }

    String getCurrentKey(const String & path, const DiskEncryptedSettings & settings)
    {
        auto it = settings.keys.find(settings.current_key_id);
        if (it == settings.keys.end())
            throw Exception(
                ErrorCodes::DATA_ENCRYPTION_ERROR,
                "Not found a key with the current ID {} required to cipher file {}",
                settings.current_key_id,
                quoteString(path));

        return it->second;
    }

    String getKey(const String & path, const FileEncryption::Header & header, const DiskEncryptedSettings & settings)
    {
        auto it = settings.keys.find(header.key_id);
        if (it == settings.keys.end())
            throw Exception(
                ErrorCodes::DATA_ENCRYPTION_ERROR,
                "Not found a key with ID {} required to decipher file {}",
                header.key_id,
                quoteString(path));

        String key = it->second;
        if (calculateKeyHash(key) != header.key_hash)
            throw Exception(
                ErrorCodes::DATA_ENCRYPTION_ERROR, "Wrong key with ID {}, could not decipher file {}", header.key_id, quoteString(path));

        return key;
    }

    bool inline isSameDiskType(const IDisk & one, const IDisk & another)
    {
        return typeid(one) == typeid(another);
    }
}

class DiskEncryptedReservation : public IReservation
{
public:
    DiskEncryptedReservation(DiskEncryptedPtr disk_, std::unique_ptr<IReservation> reservation_)
        : disk(std::move(disk_)), reservation(std::move(reservation_))
    {
    }

    UInt64 getSize() const override { return reservation->getSize(); }
    UInt64 getUnreservedSpace() const override { return reservation->getUnreservedSpace(); }

    DiskPtr getDisk(size_t i) const override
    {
        if (i != 0)
            throw Exception("Can't use i != 0 with single disk reservation", ErrorCodes::INCORRECT_DISK_INDEX);
        return disk;
    }

    Disks getDisks() const override { return {disk}; }

    void update(UInt64 new_size) override { reservation->update(new_size); }

private:
    DiskEncryptedPtr disk;
    std::unique_ptr<IReservation> reservation;
};

DiskEncrypted::DiskEncrypted(
    const String & name_, const Poco::Util::AbstractConfiguration & config_, const String & config_prefix_, const DisksMap & map_)
    : DiskEncrypted(name_, parseDiskEncryptedSettings(name_, config_, config_prefix_, map_))
{
}

DiskEncrypted::DiskEncrypted(const String & name_, std::unique_ptr<const DiskEncryptedSettings> settings_)
    : DiskDecorator(settings_->wrapped_disk)
    , name(name_)
    , disk_path(settings_->disk_path)
    , disk_absolute_path(settings_->wrapped_disk->getPath() + settings_->disk_path)
    , current_settings(std::move(settings_))
{
    delegate->createDirectories(disk_path);
}

ReservationPtr DiskEncrypted::reserve(UInt64 bytes)
{
    auto reservation = delegate->reserve(bytes);
    if (!reservation)
        return {};
    return std::make_unique<DiskEncryptedReservation>(std::static_pointer_cast<DiskEncrypted>(shared_from_this()), std::move(reservation));
}

void DiskEncrypted::copy(const String & from_path, const std::shared_ptr<IDisk> & to_disk, const String & to_path)
{
    /// Check if we can copy the file without deciphering.
    if (isSameDiskType(*this, *to_disk))
    {
        /// Disk type is the same, check if the key is the same too.
        if (auto * to_disk_enc = typeid_cast<DiskEncrypted *>(to_disk.get()))
        {
            auto from_settings = current_settings.get();
            auto to_settings = to_disk_enc->current_settings.get();
            if (from_settings->keys == to_settings->keys)
            {
                /// Keys are the same so we can simply copy the encrypted file.
                auto wrapped_from_path = wrappedPath(from_path);
                auto to_delegate = to_disk_enc->delegate;
                auto wrapped_to_path = to_disk_enc->wrappedPath(to_path);
                delegate->copy(wrapped_from_path, to_delegate, wrapped_to_path);
                return;
            }
        }
    }

    /// Copy the file through buffers with deciphering.
    copyThroughBuffers(from_path, to_disk, to_path);
}


void DiskEncrypted::copyDirectoryContent(const String & from_dir, const std::shared_ptr<IDisk> & to_disk, const String & to_dir)
{
    /// Check if we can copy the file without deciphering.
    if (isSameDiskType(*this, *to_disk))
    {
        /// Disk type is the same, check if the key is the same too.
        if (auto * to_disk_enc = typeid_cast<DiskEncrypted *>(to_disk.get()))
        {
            auto from_settings = current_settings.get();
            auto to_settings = to_disk_enc->current_settings.get();
            if (from_settings->keys == to_settings->keys)
            {
                /// Keys are the same so we can simply copy the encrypted file.
                auto wrapped_from_path = wrappedPath(from_dir);
                auto to_delegate = to_disk_enc->delegate;
                auto wrapped_to_path = to_disk_enc->wrappedPath(to_dir);
                delegate->copyDirectoryContent(wrapped_from_path, to_delegate, wrapped_to_path);
                return;
            }
        }
    }

    if (!to_disk->exists(to_dir))
        to_disk->createDirectories(to_dir);

    /// Copy the file through buffers with deciphering.
    copyThroughBuffers(from_dir, to_disk, to_dir);
}

std::unique_ptr<ReadBufferFromFileBase> DiskEncrypted::readFile(
    const String & path,
    const ReadSettings & settings,
    std::optional<size_t> read_hint,
    std::optional<size_t> file_size) const
{
    auto wrapped_path = wrappedPath(path);
    auto buffer = delegate->readFile(wrapped_path, settings, read_hint, file_size);
    if (buffer->eof())
    {
        /// File is empty, that's a normal case, see DiskEncrypted::truncateFile().
        /// There is no header so we just return `ReadBufferFromString("")`.
        return std::make_unique<ReadBufferFromFileDecorator>(std::make_unique<ReadBufferFromString>(std::string_view{}), wrapped_path);
    }
    auto encryption_settings = current_settings.get();
    FileEncryption::Header header = readHeader(*buffer);
    String key = getKey(path, header, *encryption_settings);
    return std::make_unique<ReadBufferFromEncryptedFile>(settings.local_fs_buffer_size, std::move(buffer), key, header);
}

std::unique_ptr<WriteBufferFromFileBase> DiskEncrypted::writeFile(const String & path, size_t buf_size, WriteMode mode, const WriteSettings &)
{
    auto wrapped_path = wrappedPath(path);
    FileEncryption::Header header;
    String key;
    UInt64 old_file_size = 0;
    auto settings = current_settings.get();
    if (mode == WriteMode::Append && exists(path))
    {
        old_file_size = getFileSize(path);
        if (old_file_size)
        {
            /// Append mode: we continue to use the same header.
            auto read_buffer = delegate->readFile(wrapped_path, ReadSettings().adjustBufferSize(FileEncryption::Header::kSize));
            header = readHeader(*read_buffer);
            key = getKey(path, header, *settings);
        }
    }
    if (!old_file_size)
    {
        /// Rewrite mode: we generate a new header.
        key = getCurrentKey(path, *settings);
        header.algorithm = settings->current_algorithm;
        header.key_id = settings->current_key_id;
        header.key_hash = calculateKeyHash(key);
        header.init_vector = InitVector::random();
    }
    auto buffer = delegate->writeFile(wrapped_path, buf_size, mode);
    return std::make_unique<WriteBufferFromEncryptedFile>(buf_size, std::move(buffer), key, header, old_file_size);
}


size_t DiskEncrypted::getFileSize(const String & path) const
{
    auto wrapped_path = wrappedPath(path);
    size_t size = delegate->getFileSize(wrapped_path);
    return size > FileEncryption::Header::kSize ? (size - FileEncryption::Header::kSize) : 0;
}

void DiskEncrypted::truncateFile(const String & path, size_t size)
{
    auto wrapped_path = wrappedPath(path);
    delegate->truncateFile(wrapped_path, size ? (size + FileEncryption::Header::kSize) : 0);
}

SyncGuardPtr DiskEncrypted::getDirectorySyncGuard(const String & path) const
{
    auto wrapped_path = wrappedPath(path);
    return delegate->getDirectorySyncGuard(wrapped_path);
}

void DiskEncrypted::applyNewSettings(
    const Poco::Util::AbstractConfiguration & config,
    ContextPtr /*context*/,
    const String & config_prefix,
    const DisksMap & disk_map)
{
    auto new_settings = parseDiskEncryptedSettings(name, config, config_prefix, disk_map);
    if (new_settings->wrapped_disk != delegate)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Сhanging wrapped disk on the fly is not supported. Disk {}", name);

    if (new_settings->disk_path != disk_path)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Сhanging disk path on the fly is not supported. Disk {}", name);

    current_settings.set(std::move(new_settings));
}

void registerDiskEncrypted(DiskFactory & factory)
{
    auto creator = [](const String & name,
                      const Poco::Util::AbstractConfiguration & config,
                      const String & config_prefix,
                      ContextPtr /*context*/,
                      const DisksMap & map) -> DiskPtr
    {
        return std::make_shared<DiskEncrypted>(name, config, config_prefix, map);
    };
    factory.registerDiskType("encrypted", creator);
}

}


#endif
