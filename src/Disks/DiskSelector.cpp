#include <Disks/DiskObjectStorage/DiskObjectStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/Cache/MetadataStorageFromCacheObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageFactory.h>
#include <Disks/DiskLocal.h>
#include <Disks/DiskSelector.h>

#include <IO/WriteHelpers.h>
#include <Common/escapeForFileName.h>
#include <Common/quoteString.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>

#include <algorithm>
#include <unordered_set>


namespace DB
{

namespace ErrorCodes
{
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int UNKNOWN_DISK;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

MetadataStoragePtr unwrapEncryptedAndCacheLayers(DiskPtr disk)
{
    if (disk->getDataSourceDescription().is_encrypted)
        return unwrapEncryptedAndCacheLayers(disk->getDelegateDiskIfExists());

    DiskObjectStoragePtr object_storage_disk = std::dynamic_pointer_cast<DiskObjectStorage>(disk);
    if (!object_storage_disk)
        return nullptr;

    MetadataStoragePtr metadata_storage = object_storage_disk->getMetadataStorage();

    while (auto * cache_metadata = dynamic_cast<MetadataStorageFromCacheObjectStorage *>(metadata_storage.get()))
        metadata_storage = cache_metadata->getUnderlying();

    chassert(metadata_storage);
    return metadata_storage;
}

void DiskSelector::assertInitialized() const
{
    if (!is_initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DiskSelector not initialized");
}

namespace
{

bool isWritablePlainDisk(const DiskPtr & disk)
{
    return disk->isPlain() && !disk->isReadOnly() && !disk->isWriteOnce();
}

/// Same endpoint, same bucket, and nested common key prefixes.
bool areObjectStoragePrefixesOverlapping(const ObjectStoragePtr & object_storage, const ObjectStoragePtr & other_object_storage)
{
    if (object_storage->getDescription() != other_object_storage->getDescription())
        return false;

    if (object_storage->getObjectsNamespace() != other_object_storage->getObjectsNamespace())
        return false;

    const auto prefix = object_storage->getCommonKeyPrefix();
    const auto other_prefix = other_object_storage->getCommonKeyPrefix();
    return prefix.starts_with(other_prefix) || other_prefix.starts_with(prefix);
}

}

bool DiskSelector::arePlainRewritableDisksOverlapping(const DiskPtr & disk, const DiskPtr & other_disk)
{
    if (!isWritablePlainDisk(disk) || !isWritablePlainDisk(other_disk))
        return false;

    LOG_TEST(getLogger("recordDisk"), "Validating plain disk: {}-{}-{}-{}-{} vs {}-{}-{}-{}-{}",
        disk->getName(), disk->isReadOnly(), disk->isWriteOnce(), disk->getObjectStorage()->getDescription(), disk->getObjectStorage()->getObjectsNamespace(),
        other_disk->getName(), other_disk->isReadOnly(), other_disk->isWriteOnce(), other_disk->getObjectStorage()->getDescription(), other_disk->getObjectStorage()->getObjectsNamespace());

    if (!areObjectStoragePrefixesOverlapping(disk->getObjectStorage(), other_disk->getObjectStorage()))
        return false;

    /// The same metadata storage under the `cache` and `encrypted` layers is the same disk.
    return unwrapEncryptedAndCacheLayers(other_disk).get() != unwrapEncryptedAndCacheLayers(disk).get();
}

std::vector<String> DiskSelector::findPlainRewritableDisksOverlappingNewDisk(
    const String & disk_name,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    ContextPtr context,
    bool run_local_paths_check,
    RemoteHostFilterCheck remote_host_filter_check,
    const std::vector<std::pair<String, DiskPtr>> & disks)
{
    std::vector<String> result;

    if (!std::ranges::any_of(disks, [](const auto & name_and_disk) { return isWritablePlainDisk(name_and_disk.second); }))
        return result;

    /// The disk types registered in `registerDiskObjectStorage`, and the rule it uses to choose `plain_rewritable` metadata.
    static const std::unordered_set<std::string_view> object_storage_disk_types
        = {"object_storage", "s3", "s3_plain", "s3_with_keeper", "s3_plain_rewritable", "hdfs", "azure_blob_storage", "local_blob_storage", "web"};

    const auto disk_type = config.getString(config_prefix + ".type", "local");
    if (!object_storage_disk_types.contains(disk_type))
        return result;

    if (config.has(config_prefix + ".metadata_type"))
    {
        if (config.getString(config_prefix + ".metadata_type") != "plain_rewritable")
            return result;
    }
    else if (!disk_type.contains("plain") || !disk_type.contains("rewritable"))
    {
        return result;
    }

    /// A read-only disk cannot overwrite the metadata of another disk, as in `arePlainRewritableDisksOverlapping`.
    if (config.getBool(config_prefix + ".read_only", false))
        return result;

    Strings object_storage_config_prefixes;
    if (config.has(config_prefix + ".locations"))
    {
        Poco::Util::AbstractConfiguration::Keys locations;
        config.keys(config_prefix + ".locations", locations);
        for (const auto & location : locations)
            object_storage_config_prefixes.push_back(config_prefix + ".locations." + location);
    }
    else
    {
        object_storage_config_prefixes.push_back(config_prefix);
    }

    for (const auto & object_storage_config_prefix : object_storage_config_prefixes)
    {
        const auto object_storage = ObjectStorageFactory::instance().create(
            disk_name, config, object_storage_config_prefix, context, /* run_access_check = */ false, run_local_paths_check,
            remote_host_filter_check);

        if (object_storage->isReadOnly())
            continue;

        for (const auto & [other_disk_name, other_disk] : disks)
        {
            if (isWritablePlainDisk(other_disk)
                && areObjectStoragePrefixesOverlapping(object_storage, other_disk->getObjectStorage())
                && std::ranges::find(result, other_disk_name) == result.end())
                result.push_back(other_disk_name);
        }
    }

    return result;
}

void DiskSelector::recordDisk(const std::string & disk_name, DiskPtr disk)
{
    for (const auto & [saved_disk_name, saved_disk] : disks)
    {
        if (arePlainRewritableDisksOverlapping(disk, saved_disk))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "It is not possible to register multiple plain-rewritable disks with the same object storage prefix. Disks '{}' and '{}'",
                disk_name, saved_disk_name);
    }

    const auto [_, inserted] = disks.emplace(disk_name, std::move(disk));
    if (!inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Disk with name `{}` is already in disks map", disk_name);
}

void DiskSelector::initialize(
    const Poco::Util::AbstractConfiguration & config, const String & config_prefix, ContextPtr context, DiskValidator disk_validator)
try
{
    auto component_guard = Coordination::setCurrentComponent("DiskSelector::initialize");
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);

    auto & factory = DiskFactory::instance();

    bool has_default_disk = false;
    bool has_local_disk = false;
    for (const auto & disk_name : keys)
    {
        if (!std::all_of(disk_name.begin(), disk_name.end(), isWordCharASCII))
            throw Exception(ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG, "Disk name can contain only alphanumeric and '_' ({})", disk_name);

        if (disk_name == DEFAULT_DISK_NAME)
            has_default_disk = true;

        if (disk_name == LOCAL_DISK_NAME)
            has_local_disk = true;

        const auto disk_config_prefix = config_prefix + "." + disk_name;

        if (disk_validator && !disk_validator(config, disk_config_prefix, disk_name))
            continue;
        auto created_disk
            = factory.create(disk_name, config, disk_config_prefix, context, disks, /*attach*/ false, /*custom_disk*/ false, skip_types);
        if (created_disk.get())
        {
            recordDisk(disk_name, std::move(created_disk));
        }
    }
    if (!has_default_disk)
    {
        auto tracked_config = DiskFactory::trackImplicitLocalDisk(DEFAULT_DISK_NAME, config, config_prefix + "." + DEFAULT_DISK_NAME, context);
        auto default_disk = std::make_shared<DiskLocal>(DEFAULT_DISK_NAME, context->getPath(), 0, context, config, config_prefix);
        default_disk->keepConfigurationAlive(tracked_config);
        recordDisk(DEFAULT_DISK_NAME, std::move(default_disk));
    }

    if (!has_local_disk && (context->getApplicationType() == Context::ApplicationType::DISKS))
    {
        throw_away_local_on_update = true;
        recordDisk(LOCAL_DISK_NAME, std::make_shared<DiskLocal>(LOCAL_DISK_NAME, "/", 0, context, config, config_prefix));
    }
    is_initialized = true;
}
catch (...)
{
    for (const auto & [name, disk] : disks)
        disk->shutdown();
    throw;
}

DiskSelectorPtr DiskSelector::updateFromConfig(
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    ContextPtr context,
    const std::vector<std::pair<String, DiskPtr>> & retiring_disks) const
{
    assertInitialized();

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);

    auto & factory = DiskFactory::instance();

    std::shared_ptr<DiskSelector> result = std::make_shared<DiskSelector>(*this);

    constexpr auto default_disk_name = "default";
    constexpr auto local_disk_name = "local";
    DisksMap old_disks_minus_new_disks(result->getDisksMap());

    for (const auto & disk_name : keys)
    {
        if (!std::all_of(disk_name.begin(), disk_name.end(), isWordCharASCII))
            throw Exception(ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG, "Disk name can contain only alphanumeric and '_' ({})", disk_name);

        auto disk_config_prefix = config_prefix + "." + disk_name;
        if (!result->getDisksMap().contains(disk_name))
        {
            /// `addToDiskMap` checks the new disk only against the disks of the selector, after it is created.
            if (!skip_types.contains(config.getString(disk_config_prefix + ".type", "local")))
            {
                const auto overlapping_disk_names = findPlainRewritableDisksOverlappingNewDisk(
                    disk_name, config, disk_config_prefix, context, /* run_local_paths_check = */ false, RemoteHostFilterCheck::None,
                    retiring_disks);
                if (!overlapping_disk_names.empty())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Cannot create the disk '{}' from the configuration yet: the plain-rewritable disk '{}' works with the same "
                        "object storage prefix, it is not used by any table or database anymore, but it is not shut down yet. "
                        "Reload the configuration again later",
                        disk_name, overlapping_disk_names.front());
            }

            auto created_disk = factory.create(
                disk_name, config, disk_config_prefix, context, result->getDisksMap(), /*attach*/ false, /*custom_disk*/ false, skip_types);
            if (created_disk)
            {
                result->addToDiskMap(disk_name, created_disk);
            }
        }
        else
        {
            auto disk = old_disks_minus_new_disks[disk_name];

            factory.applyNewSettings(disk, disk_name, config, disk_config_prefix, context, result->getDisksMap());

            old_disks_minus_new_disks.erase(disk_name);
        }
    }

    old_disks_minus_new_disks.erase(default_disk_name);
    if (throw_away_local_on_update)
    {
        old_disks_minus_new_disks.erase(local_disk_name);
    }

    if (!old_disks_minus_new_disks.empty())
    {
        WriteBufferFromOwnString warning;
        if (old_disks_minus_new_disks.size() == 1)
            writeString("Disk ", warning);
        else
            writeString("Disks ", warning);

        int num_disks_removed_from_config = 0;
        for (const auto & [name, disk] : old_disks_minus_new_disks)
        {
            /// Custom disks are not present in config.
            if (disk->isCustomDisk())
                continue;

            if (num_disks_removed_from_config++ > 0)
                writeString(", ", warning);

            writeBackQuotedString(name, warning);
        }

        if (num_disks_removed_from_config > 0)
        {
            LOG_WARNING(
                getLogger("DiskSelector"),
                "{} disappeared from configuration, this change will be applied after restart of ClickHouse",
                warning.str());
        }
    }

    return result;
}

DiskPtr DiskSelector::tryGet(const String & name) const
{
    assertInitialized();
    auto it = disks.find(name);
    if (it == disks.end())
        return nullptr;
    return it->second;
}

DiskPtr DiskSelector::get(const String & name) const
{
    auto disk = tryGet(name);
    if (!disk)
        throw Exception(ErrorCodes::UNKNOWN_DISK, "Unknown disk {}", name);
    return disk;
}

const DisksMap & DiskSelector::getDisksMap() const
{
    assertInitialized();
    return disks;
}

void DiskSelector::addToDiskMap(const String & name, DiskPtr disk)
{
    assertInitialized();
    recordDisk(name, disk);
}

void DiskSelector::removeFromDiskMap(const String & name)
{
    assertInitialized();
    disks.erase(name);
}

void DiskSelector::shutdown()
{
    assertInitialized();
    for (auto & e : disks)
        e.second->shutdown();
}

}
