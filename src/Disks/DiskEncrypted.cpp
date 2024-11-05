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

    /// Reads encryption keys from the configuration.
    void getKeysFromConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix,
                           std::map<UInt64, String> & out_keys_by_id, Strings & out_keys_without_id)
    {
        Strings config_keys;
        config.keys(config_prefix, config_keys);

        for (const std::string & config_key : config_keys)
        {
            String key;
            std::optional<UInt64> key_id;

            if ((config_key == "key") || config_key.starts_with("key["))
            {
                String key_path = config_prefix + "." + config_key;
                key = config.getString(key_path);
                String key_id_path = key_path + "[@id]";
                if (config.has(key_id_path))
                    key_id = config.getUInt64(key_id_path);
            }
            else if ((config_key == "key_hex") || config_key.starts_with("key_hex["))
            {
                String key_path = config_prefix + "." + config_key;
                key = unhexKey(config.getString(key_path));
                String key_id_path = key_path + "[@id]";
                if (config.has(key_id_path))
                    key_id = config.getUInt64(key_id_path);
            }
            else
                continue;

            if (key_id)
            {
                if (!out_keys_by_id.contains(*key_id))
                    out_keys_by_id[*key_id] = key;
                else
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Multiple keys specified for same ID {}", *key_id);
            }
            else
                out_keys_without_id.push_back(key);
        }

        if (out_keys_by_id.empty() && out_keys_without_id.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "No encryption keys found");

        if (out_keys_by_id.empty() && (out_keys_without_id.size() == 1))
        {
            out_keys_by_id[0] = out_keys_without_id.front();
            out_keys_without_id.clear();
        }
    }

    /// Reads the current encryption key from the configuration.
    String getCurrentKeyFromConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix,
                                   const std::map<UInt64, String> & keys_by_id, const Strings & keys_without_id)
    {
        String key_path = config_prefix + ".current_key";
        String key_hex_path = config_prefix + ".current_key_hex";
        String key_id_path = config_prefix + ".current_key_id";

        if (config.has(key_path) + config.has(key_hex_path) + config.has(key_id_path) > 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The current key is specified multiple times");

        auto check_current_key_found = [&](const String & current_key_)
        {
            for (const auto & [_, key] : keys_by_id)
            {
                if (key == current_key_)
                    return;
            }
            for (const auto & key : keys_without_id)
            {
                if (key == current_key_)
                    return;
            }
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The current key is not found in keys");
        };

        if (config.has(key_path))
        {
            String current_key = config.getString(key_path);
            check_current_key_found(current_key);
            return current_key;
        }
        if (config.has(key_hex_path))
        {
            String current_key = unhexKey(config.getString(key_hex_path));
            check_current_key_found(current_key);
            return current_key;
        }
        if (config.has(key_id_path))
        {
            UInt64 current_key_id = config.getUInt64(key_id_path);
            auto it = keys_by_id.find(current_key_id);
            if (it == keys_by_id.end())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not found a key with the current ID {}", current_key_id);
            return it->second;
        }
        if (keys_by_id.size() == 1 && keys_without_id.empty() && keys_by_id.begin()->first == 0)
        {
            /// There is only a single key defined with id=0, so we can choose it as current.
            return keys_by_id.begin()->second;
        }

        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The current key is not specified");
    }

    /// Reads the current encryption algorithm from the configuration.
    Algorithm getCurrentAlgorithmFromConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
    {
        String path = config_prefix + ".algorithm";
        if (!config.has(path))
            return DEFAULT_ENCRYPTION_ALGORITHM;
        return parseAlgorithmFromString(config.getString(path));
    }

    /// Reads the name of a wrapped disk & the path on the wrapped disk and then finds that disk in a disk map.
    void getDiskAndPathFromConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, const DisksMap & map,
                                  DiskPtr & out_disk, String & out_path)
    {
        String disk_name = config.getString(config_prefix + ".disk", "");
        if (disk_name.empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Name of the wrapped disk must not be empty. Encrypted disk is a wrapper over another disk");

        auto disk_it = map.find(disk_name);
        if (disk_it == map.end())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "The wrapped disk must have been announced earlier. No disk with name {}", disk_name);

        out_disk = disk_it->second;

        out_path = config.getString(config_prefix + ".path", "");
        if (!out_path.empty() && (out_path.back() != '/'))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Disk path must ends with '/', but '{}' doesn't.", quoteString(out_path));
    }

    /// Parses the settings of an ecnrypted disk from the configuration.
    std::unique_ptr<const DiskEncryptedSettings> parseDiskEncryptedSettings(
        const String & disk_name,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        const DisksMap & disk_map)
    {
        try
        {
            auto res = std::make_unique<DiskEncryptedSettings>();

            std::map<UInt64, String> keys_by_id;
            Strings keys_without_id;
            getKeysFromConfig(config, config_prefix, keys_by_id, keys_without_id);

            for (const auto & [key_id, key] : keys_by_id)
            {
                auto fingerprint = calculateKeyFingerprint(key);
                res->all_keys[fingerprint] = key;

                /// Version 1 used key fingerprints based on the key id.
                /// We have to add such fingerprints to the map too to support reading files encrypted by version 1.
                auto v1_fingerprint = calculateV1KeyFingerprint(key, key_id);
                res->all_keys[v1_fingerprint] = key;
            }

            for (const auto & key : keys_without_id)
            {
                auto fingerprint = calculateKeyFingerprint(key);
                res->all_keys[fingerprint] = key;
            }

            String current_key = getCurrentKeyFromConfig(config, config_prefix, keys_by_id, keys_without_id);
            res->current_key = current_key;
            res->current_key_fingerprint = calculateKeyFingerprint(current_key);

            res->current_algorithm = getCurrentAlgorithmFromConfig(config, config_prefix);

            FileEncryption::checkKeySize(res->current_key.size(), res->current_algorithm);

            DiskPtr wrapped_disk;
            String disk_path;
            getDiskAndPathFromConfig(config, config_prefix, disk_map, wrapped_disk, disk_path);
            res->wrapped_disk = wrapped_disk;
            res->disk_path = disk_path;

            return res;
        }
        catch (Exception & e)
        {
            e.addMessage("Disk " + disk_name);
            throw;
        }
    }

    /// Reads the header of an encrypted file.
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
    std::optional<UInt64> getUnreservedSpace() const override { return reservation->getUnreservedSpace(); }

    DiskPtr getDisk(size_t i) const override
    {
        if (i != 0)
            throw Exception(ErrorCodes::INCORRECT_DISK_INDEX, "Can't use i != 0 with single disk reservation");
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
    : DiskEncrypted(name_, parseDiskEncryptedSettings(name_, config_, config_prefix_, map_), config_, config_prefix_)
{
}

DiskEncrypted::DiskEncrypted(const String & name_, std::unique_ptr<const DiskEncryptedSettings> settings_,
                             const Poco::Util::AbstractConfiguration & config_, const String & config_prefix_)
    : IDisk(name_, config_, config_prefix_)
    , delegate(settings_->wrapped_disk)
    , encrypted_name(name_)
    , disk_path(settings_->disk_path)
    , disk_absolute_path(settings_->wrapped_disk->getPath() + settings_->disk_path)
    , current_settings(std::move(settings_))
    , use_fake_transaction(config_.getBool(config_prefix_ + ".use_fake_transaction", true))
{
    delegate->createDirectories(disk_path);
}

DiskEncrypted::DiskEncrypted(const String & name_, std::unique_ptr<const DiskEncryptedSettings> settings_)
    : IDisk(name_)
    , delegate(settings_->wrapped_disk)
    , encrypted_name(name_)
    , disk_path(settings_->disk_path)
    , disk_absolute_path(settings_->wrapped_disk->getPath() + settings_->disk_path)
    , current_settings(std::move(settings_))
    , use_fake_transaction(true)
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


void DiskEncrypted::copyDirectoryContent(
    const String & from_dir,
    const std::shared_ptr<IDisk> & to_disk,
    const String & to_dir,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    const std::function<void()> & cancellation_hook)
{
    /// Check if we can copy the file without deciphering.
    if (isSameDiskType(*this, *to_disk))
    {
        /// Disk type is the same, check if the key is the same too.
        if (auto * to_disk_enc = typeid_cast<DiskEncrypted *>(to_disk.get()))
        {
            auto from_settings = current_settings.get();
            auto to_settings = to_disk_enc->current_settings.get();
            if (from_settings->all_keys == to_settings->all_keys)
            {
                /// Keys are the same so we can simply copy the encrypted file.
                auto wrapped_from_path = wrappedPath(from_dir);
                auto to_delegate = to_disk_enc->delegate;
                auto wrapped_to_path = to_disk_enc->wrappedPath(to_dir);
                delegate->copyDirectoryContent(wrapped_from_path, to_delegate, wrapped_to_path, read_settings, write_settings, cancellation_hook);
                return;
            }
        }
    }

    /// Copy the file through buffers with deciphering.
    IDisk::copyDirectoryContent(from_dir, to_disk, to_dir, read_settings, write_settings, cancellation_hook);
}

std::unique_ptr<ReadBufferFromFileBase> DiskEncrypted::readFile(
    const String & path,
    const ReadSettings & settings,
    std::optional<size_t> read_hint,
    std::optional<size_t> file_size) const
{
    if (read_hint && *read_hint > 0)
        read_hint = *read_hint + FileEncryption::Header::kSize;

    if (file_size && *file_size > 0)
        file_size = *file_size + FileEncryption::Header::kSize;

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
    String key = encryption_settings->findKeyByFingerprint(header.key_fingerprint, path);
    return std::make_unique<ReadBufferFromEncryptedFile>(settings.local_fs_buffer_size, std::move(buffer), key, header);
}

size_t DiskEncrypted::getFileSize(const String & path) const
{
    auto wrapped_path = wrappedPath(path);
    size_t size = delegate->getFileSize(wrapped_path);
    return size > FileEncryption::Header::kSize ? (size - FileEncryption::Header::kSize) : 0;
}

UInt128 DiskEncrypted::getEncryptedFileIV(const String & path) const
{
    auto wrapped_path = wrappedPath(path);
    auto read_buffer = delegate->readFile(wrapped_path, getReadSettings().adjustBufferSize(FileEncryption::Header::kSize));
    if (read_buffer->eof())
        return 0;
    auto header = readHeader(*read_buffer);
    return header.init_vector.get();
}

size_t DiskEncrypted::getEncryptedFileSize(size_t unencrypted_size) const
{
    if (unencrypted_size)
        return unencrypted_size + FileEncryption::Header::kSize;
    return 0;
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

std::unordered_map<String, String> DiskEncrypted::getSerializedMetadata(const std::vector<String> & paths) const
{
    std::vector<String> wrapped_paths;
    wrapped_paths.reserve(paths.size());
    for (const auto & path : paths)
        wrapped_paths.emplace_back(wrappedPath(path));
    auto metadata = delegate->getSerializedMetadata(wrapped_paths);
    std::unordered_map<String, String> res;
    for (size_t i = 0; i != paths.size(); ++i)
        res.emplace(paths[i], metadata.at(wrapped_paths.at(i)));
    return res;
}

void DiskEncrypted::applyNewSettings(
    const Poco::Util::AbstractConfiguration & config,
    ContextPtr context,
    const String & config_prefix,
    const DisksMap & disk_map)
{
    auto new_settings = parseDiskEncryptedSettings(name, config, config_prefix, disk_map);
    if (new_settings->wrapped_disk != delegate)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Changing wrapped disk on the fly is not supported. Disk {}", name);

    if (new_settings->disk_path != disk_path)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Changing disk path on the fly is not supported. Disk {}", name);

    current_settings.set(std::move(new_settings));
    IDisk::applyNewSettings(config, context, config_prefix, disk_map);
}

void registerDiskEncrypted(DiskFactory & factory, bool global_skip_access_check)
{
    auto creator = [global_skip_access_check](
        const String & name,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        ContextPtr context,
        const DisksMap & map,
        bool, bool) -> DiskPtr
    {
        bool skip_access_check = global_skip_access_check || config.getBool(config_prefix + ".skip_access_check", false);
        DiskPtr disk = std::make_shared<DiskEncrypted>(name, config, config_prefix, map);
        disk->startup(context, skip_access_check);
        return disk;
    };
    factory.registerDiskType("encrypted", creator);
}

}


#endif
