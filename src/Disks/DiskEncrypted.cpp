#include <Disks/DiskEncrypted.h>

#if USE_SSL
#include <Disks/DiskFactory.h>
#include <IO/FileEncryptionCommon.h>
#include <IO/ReadBufferFromEncryptedFile.h>
#include <IO/WriteBufferFromEncryptedFile.h>
#include <boost/algorithm/hex.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DISK_INDEX;
    extern const int DATA_ENCRYPTION_ERROR;
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

    struct DiskEncryptedSettings
    {
        DiskPtr wrapped_disk;
        String path_on_wrapped_disk;
        std::unordered_map<UInt64, String> keys;
        UInt64 current_key_id;
        Algorithm current_algorithm;

        DiskEncryptedSettings(
            const String & disk_name, const Poco::Util::AbstractConfiguration & config, const String & config_prefix, const DisksMap & map)
        {
            try
            {
                current_algorithm = DEFAULT_ENCRYPTION_ALGORITHM;
                if (config.has(config_prefix + ".algorithm"))
                    parseFromString(current_algorithm, config.getString(config_prefix + ".algorithm"));

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

                    auto it = keys.find(key_id);
                    if (it != keys.end())
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Multiple keys have the same ID {}", key_id);
                    keys[key_id] = key;
                }

                if (keys.empty())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "No keys, an encrypted disk needs keys to work", current_key_id);

                current_key_id = config.getUInt64(config_prefix + ".current_key_id", 0);
                if (!keys.contains(current_key_id))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Key with ID {} not found", current_key_id);
                FileEncryption::checkKeySize(current_algorithm, keys[current_key_id].size());

                String wrapped_disk_name = config.getString(config_prefix + ".disk", "");
                if (wrapped_disk_name.empty())
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Name of the wrapped disk must not be empty. An encrypted disk is a wrapper over another disk");

                auto wrapped_disk_it = map.find(wrapped_disk_name);
                if (wrapped_disk_it == map.end())
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "The wrapped disk must have been announced earlier. No disk with name {}",
                        wrapped_disk_name);
                wrapped_disk = wrapped_disk_it->second;

                path_on_wrapped_disk = config.getString(config_prefix + ".path", "");
            }
            catch (Exception & e)
            {
                e.addMessage("Disk " + disk_name);
                throw;
            }
        }
    };

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

ReservationPtr DiskEncrypted::reserve(UInt64 bytes)
{
    auto reservation = delegate->reserve(bytes);
    if (!reservation)
        return {};
    return std::make_unique<DiskEncryptedReservation>(std::static_pointer_cast<DiskEncrypted>(shared_from_this()), std::move(reservation));
}

DiskEncrypted::DiskEncrypted(
    const String & name_,
    DiskPtr wrapped_disk_,
    const String & path_on_wrapped_disk_,
    const std::unordered_map<UInt64, String> & keys_,
    UInt64 current_key_id_,
    FileEncryption::Algorithm current_algorithm_)
    : DiskDecorator(wrapped_disk_)
    , name(name_)
    , disk_path(path_on_wrapped_disk_)
    , keys(keys_)
    , current_key_id(current_key_id_)
    , current_algorithm(current_algorithm_)
{
    initialize();
}

void DiskEncrypted::initialize()
{
    disk_absolute_path = delegate->getPath() + disk_path;

    // use wrapped_disk as an EncryptedDisk store
    if (disk_path.empty())
        return;

    if (disk_path.back() != '/')
        throw Exception("Disk path must ends with '/', but '" + disk_path + "' doesn't.", ErrorCodes::BAD_ARGUMENTS);

    delegate->createDirectories(disk_path);
}


String DiskEncrypted::getKey(UInt64 key_id) const
{
    auto it = keys.find(key_id);
    if (it == keys.end())
        throw Exception(ErrorCodes::DATA_ENCRYPTION_ERROR, "Key with ID {} not found", key_id);
    return it->second;
}

void DiskEncrypted::copy(const String & from_path, const std::shared_ptr<IDisk> & to_disk, const String & to_path)
{
    /// Check if we can copy the file without deciphering.
    if (isSameDiskType(*this, *to_disk))
    {
        /// Disk type is the same, check if the key is the same too.
        if (auto * to_encrypted_disk = typeid_cast<DiskEncrypted *>(to_disk.get()))
        {
            if (keys == to_encrypted_disk->keys)
            {
                /// Keys are the same so we can simply copy the encrypted file.
                delegate->copy(wrappedPath(from_path), to_encrypted_disk->delegate, to_encrypted_disk->wrappedPath(to_path));
                return;
            }
        }
    }

    /// Copy the file through buffers with deciphering.
    copyThroughBuffers(from_path, to_disk, to_path);
}

std::unique_ptr<ReadBufferFromFileBase> DiskEncrypted::readFile(
    const String & path,
    size_t buf_size,
    size_t estimated_size,
    size_t aio_threshold,
    size_t mmap_threshold,
    MMappedFileCache * mmap_cache) const
{
    try
    {
        auto wrapped_path = wrappedPath(path);
        auto buffer = delegate->readFile(wrapped_path, buf_size, estimated_size, aio_threshold, mmap_threshold, mmap_cache);
        FileEncryption::Header header;
        header.read(*buffer);
        String key = getKey(header.key_id);
        if (calculateKeyHash(key) != header.key_hash)
            throw Exception(ErrorCodes::DATA_ENCRYPTION_ERROR, "Wrong key, could not read file");
        return std::make_unique<ReadBufferFromEncryptedFile>(buf_size, std::move(buffer), key, header);
    }
    catch (Exception & e)
    {
        e.addMessage("File " + quoteString(path));
        throw;
    }
}

std::unique_ptr<WriteBufferFromFileBase> DiskEncrypted::writeFile(const String & path, size_t buf_size, WriteMode mode)
{
    try
    {
        auto wrapped_path = wrappedPath(path);
        FileEncryption::Header header;
        String key;
        UInt64 old_file_size = 0;
        if (mode == WriteMode::Append && exists(path))
        {
            old_file_size = getFileSize(path);
            if (old_file_size)
            {
                /// Append mode: we continue to use the same header.
                auto read_buffer = delegate->readFile(wrapped_path, FileEncryption::Header::kSize);
                header.read(*read_buffer);
                key = getKey(header.key_id);
                if (calculateKeyHash(key) != header.key_hash)
                    throw Exception(ErrorCodes::DATA_ENCRYPTION_ERROR, "Wrong key, could not append file");
            }
        }
        if (!old_file_size)
        {
            /// Rewrite mode: we generate a new header.
            key = getKey(current_key_id);
            header.algorithm = current_algorithm;
            header.key_id = current_key_id;
            header.key_hash = calculateKeyHash(key);
            header.init_vector = InitVector::random();
        }
        auto buffer = delegate->writeFile(wrapped_path, buf_size, mode);
        return std::make_unique<WriteBufferFromEncryptedFile>(buf_size, std::move(buffer), key, header, old_file_size);
    }
    catch (Exception & e)
    {
        e.addMessage("File " + quoteString(path));
        throw;
    }
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
    const DisksMap & map)
{
    DiskEncryptedSettings settings{name, config, config_prefix, map};
    delegate = settings.wrapped_disk;
    disk_path = settings.path_on_wrapped_disk;
    keys = settings.keys;
    current_key_id = settings.current_key_id;
    current_algorithm = settings.current_algorithm;
    initialize();
}

void registerDiskEncrypted(DiskFactory & factory)
{
    auto creator = [](const String & name,
                      const Poco::Util::AbstractConfiguration & config,
                      const String & config_prefix,
                      ContextPtr /*context*/,
                      const DisksMap & map) -> DiskPtr
    {
        DiskEncryptedSettings settings{name, config, config_prefix, map};
        return std::make_shared<DiskEncrypted>(
            name,
            settings.wrapped_disk,
            settings.path_on_wrapped_disk,
            settings.keys,
            settings.current_key_id,
            settings.current_algorithm);
    };
    factory.registerDiskType("encrypted", creator);
}

}


#endif
