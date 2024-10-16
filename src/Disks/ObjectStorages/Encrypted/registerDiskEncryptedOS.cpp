#include "config.h"

#if USE_SSL

#    include <Disks/DiskFactory.h>
#    include <Disks/ObjectStorages/Cached/CachedObjectStorage.h>
#    include <Disks/ObjectStorages/DiskObjectStorage.h>
#    include <Interpreters/Cache/FileCache.h>
#    include <Interpreters/Cache/FileCacheFactory.h>
#    include <Interpreters/Cache/FileCacheSettings.h>
#    include <Interpreters/Context.h>
#    include <Common/assert_cast.h>
#    include <Common/logger_useful.h>

#    include "EncryptedObjectStorage.h"
#    include <IO/FileEncryptionCommon.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

static EncryptedObjectStorageSettingsPtr parseDiskEncryptedOSSettings(
    const String & disk_name, const Poco::Util::AbstractConfiguration & config, const String & config_prefix, const DisksMap & disk_map)
{
    try
    {
        auto ret = std::make_shared<EncryptedObjectStorageSettings>();

        std::map<UInt64, String> keys_by_id;
        Strings keys_without_id;
        FileEncryption::getKeysFromConfig(config, config_prefix, keys_by_id, keys_without_id);

        for (const auto & [key_id, key] : keys_by_id)
        {
            auto fingerprint = FileEncryption::calculateKeyFingerprint(key);
            ret->all_keys[fingerprint] = key;

            /// Version 1 used key fingerprints based on the key id.
            /// We have to add such fingerprints to the map too to support reading files encrypted by version 1.
            auto v1_fingerprint = FileEncryption::calculateV1KeyFingerprint(key, key_id);
            ret->all_keys[v1_fingerprint] = key;
        }

        for (const auto & key : keys_without_id)
        {
            auto fingerprint = FileEncryption::calculateKeyFingerprint(key);
            ret->all_keys[fingerprint] = key;
        }

        String current_key = FileEncryption::getCurrentKeyFromConfig(config, config_prefix, keys_by_id, keys_without_id);
        ret->current_key = current_key;
        ret->current_key_fingerprint = FileEncryption::calculateKeyFingerprint(current_key);
        ret->current_algorithm = FileEncryption::getCurrentAlgorithmFromConfig(config, config_prefix);

        FileEncryption::checkKeySize(ret->current_key.size(), ret->current_algorithm);

        auto wrapped_disk_name = config.getString(config_prefix + ".disk", "");
        if (wrapped_disk_name.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Disk EncryptedOS requires `disk` field in config");

        auto disk_it = disk_map.find(wrapped_disk_name);
        if (disk_it == disk_map.end())
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Cannot wrap disk `{}` with encryption layer `{}`: there is no such disk (it should be initialized before encryption disk)",
                wrapped_disk_name,
                disk_name);
        }
        ret->wrapped_disk = disk_it->second;
        if (!dynamic_cast<const DiskObjectStorage *>(ret->wrapped_disk.get()))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Cannot wrap disk `{}` with encryption layer `{}`: encrypted_os disk is allowed only on top of object storage",
                wrapped_disk_name,
                disk_name);
        ret->cache_header_on_write = config.getBool(config_prefix + ".cache_header_on_write", false);

        auto header_cache = config.getString(config_prefix + ".header_cache", "");
        if (!header_cache.empty())
        {
            auto cache = FileCacheFactory::instance().getByName(header_cache)->cache;
            cache->initialize();
            ret->header_cache = cache;
        }
        return ret;
    }
    catch (Exception & e)
    {
        e.addMessage("Disk " + disk_name);
        throw;
    }
}

void registerDiskEncryptedOS(DiskFactory & factory, bool /* global_skip_access_check */)
{
    auto creator = [](const String & name,
                      const Poco::Util::AbstractConfiguration & config,
                      const String & config_prefix,
                      ContextPtr /*context*/,
                      const DisksMap & map, bool, bool) -> DiskPtr
    {
        auto enc_settings = parseDiskEncryptedOSSettings(name, config, config_prefix, map);
        auto disk_object_storage = enc_settings->wrapped_disk->createDiskObjectStorage();
        disk_object_storage->wrapWithEncryption(enc_settings, name);

        LOG_INFO(
            &Poco::Logger::get("DiskEncryptedOS"),
            "Registered encrypted_os disk (`{}`) with structure: {}",
            name,
            assert_cast<DiskObjectStorage *>(disk_object_storage.get())->getStructure());

        return disk_object_storage;
    };

    factory.registerDiskType("encrypted_os", creator);
}

}

#endif
