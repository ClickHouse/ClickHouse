#include <Disks/DiskObjectStorage/DiskObjectStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/Cache/MetadataStorageFromCacheObjectStorage.h>
#include <Disks/DiskLocal.h>
#include <Disks/DiskSelector.h>

#include <IO/WriteHelpers.h>
#include <Common/escapeForFileName.h>
#include <Common/quoteString.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>

#include <filesystem>
#include <optional>


namespace DB
{

namespace ErrorCodes
{
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int UNKNOWN_DISK;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

namespace
{

MetadataStoragePtr unwrapEncryptedAndCacheLayers(DiskPtr disk)
{
    if (disk->getDataSourceDescription().is_encrypted)
        return unwrapEncryptedAndCacheLayers(disk->getDelegateDiskIfExists());

    DiskObjectStoragePtr object_storage_disk = std::dynamic_pointer_cast<DiskObjectStorage>(disk);
    MetadataStoragePtr metadata_storage = object_storage_disk->getMetadataStorage();

    while (auto * cache_metadata = dynamic_cast<MetadataStorageFromCacheObjectStorage *>(metadata_storage.get()))
        metadata_storage = cache_metadata->getUnderlying();

    chassert(metadata_storage);
    return metadata_storage;
}

/// Unwraps encryption and cache layers like `unwrapEncryptedAndCacheLayers` does, but returns
/// nothing instead of dereferencing a null pointer when the disk is not backed by object storage.
/// This one runs for every pair of registered disks, local disks included.
MetadataStoragePtr tryUnwrapEncryptedAndCacheLayers(DiskPtr disk)
{
    while (disk && disk->getDataSourceDescription().is_encrypted)
        disk = disk->getDelegateDiskIfExists();

    if (!disk)
        return nullptr;

    DiskObjectStoragePtr object_storage_disk = std::dynamic_pointer_cast<DiskObjectStorage>(disk);
    if (!object_storage_disk)
        return nullptr;

    MetadataStoragePtr metadata_storage = object_storage_disk->getMetadataStorage();
    while (auto * cache_metadata = dynamic_cast<MetadataStorageFromCacheObjectStorage *>(metadata_storage.get()))
        metadata_storage = cache_metadata->getUnderlying();

    return metadata_storage;
}

/// Whether both disks are layers over one and the same storage, as a `cache` or `encrypted` disk and
/// the disk it wraps are. Such a pair reports one storage namespace by design: the layers share the
/// metadata storage, while two separately configured disks have one each.
bool areLayersOverOneStorage(const DiskPtr & lhs, const DiskPtr & rhs)
{
    const auto lhs_metadata_storage = tryUnwrapEncryptedAndCacheLayers(lhs);
    if (!lhs_metadata_storage)
        return false;

    return lhs_metadata_storage.get() == tryUnwrapEncryptedAndCacheLayers(rhs).get();
}

/// Identity of the storage that a disk reads and writes.
struct StorageNamespace
{
    DataSourceType type;
    ObjectStorageType object_storage_type;
    String description;
    String objects_namespace;
    String zookeeper_name;
    String path;

    bool operator==(const StorageNamespace &) const = default;
};

/// Returns the storage namespace of a disk, or nothing when the disk has no persistent one.
std::optional<StorageNamespace> getStorageNamespace(const DiskPtr & disk)
{
    const auto data_source = disk->getDataSourceDescription();

    /// Separate in-memory disks do not share a persistent namespace even though their paths are empty.
    if (data_source.type == DataSourceType::RAM)
        return {};

    /// Resolve symlinks of a local directory so that two aliases of one directory match.
    auto path = std::filesystem::path(disk->getPath()).lexically_normal();
    if (data_source.isPathLocal())
        path = std::filesystem::weakly_canonical(path);

    /// `DiskBackup` reports `ObjectStorage` with `ObjectStorageType::None` and has no object storage behind it,
    /// so its identity is its description and its path.
    String objects_namespace;
    if (data_source.type == DataSourceType::ObjectStorage && data_source.object_storage_type != ObjectStorageType::None)
        objects_namespace = disk->getObjectStorage()->getObjectsNamespace();

    return StorageNamespace{
        .type = data_source.type,
        .object_storage_type = data_source.object_storage_type,
        .description = data_source.type == DataSourceType::ObjectStorage ? data_source.description : String{},
        .objects_namespace = std::move(objects_namespace),
        .zookeeper_name = data_source.type == DataSourceType::ObjectStorage ? data_source.zookeeper_name : String{},
        .path = path.string(),
    };
}

}

bool haveSameStorageNamespace(const DiskPtr & lhs, const DiskPtr & rhs)
{
    const auto lhs_namespace = getStorageNamespace(lhs);
    if (!lhs_namespace)
        return false;

    return lhs_namespace == getStorageNamespace(rhs);
}

void DiskSelector::assertInitialized() const
{
    if (!is_initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DiskSelector not initialized");
}

void DiskSelector::recordDisk(const std::string & disk_name, DiskPtr disk)
{
    if (disk->isPlain() && !disk->isReadOnly() && !disk->isWriteOnce())
    {
        for (const auto & [saved_disk_name, saved_disk] : disks)
        {
            if (!saved_disk->isPlain() || saved_disk->isReadOnly() || saved_disk->isWriteOnce())
                continue;

            /// Same endpoint
            if (disk->getObjectStorage()->getDescription() != saved_disk->getObjectStorage()->getDescription())
                continue;

            /// Same bucket
            if (disk->getObjectStorage()->getObjectsNamespace() != saved_disk->getObjectStorage()->getObjectsNamespace())
                continue;

            LOG_TEST(getLogger("recordDisk"), "Validating plain disk: {}-{}-{}-{}-{} vs {}-{}-{}-{}-{}",
                disk_name, disk->isReadOnly(), disk->isWriteOnce(), disk->getObjectStorage()->getDescription(), disk->getObjectStorage()->getObjectsNamespace(),
                saved_disk_name, saved_disk->isReadOnly(), saved_disk->isWriteOnce(), saved_disk->getObjectStorage()->getDescription(), saved_disk->getObjectStorage()->getObjectsNamespace());

            /// Nested common keys
            const auto new_prefix = disk->getObjectStorage()->getCommonKeyPrefix();
            const auto saved_prefix = saved_disk->getObjectStorage()->getCommonKeyPrefix();
            if (new_prefix.starts_with(saved_prefix) || saved_prefix.starts_with(new_prefix))
            {
                const auto unwrapped_metadata_storage_saved = unwrapEncryptedAndCacheLayers(saved_disk);
                const auto unwrapped_metadata_storage_new = unwrapEncryptedAndCacheLayers(disk);

                if (unwrapped_metadata_storage_saved.get() != unwrapped_metadata_storage_new.get())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "It is not possible to register multiple plain-rewritable disks with the same object storage prefix. Disks '{}' and '{}'",
                        disk_name, saved_disk_name);
            }
        }
    }

    /// Two disks that resolve to the same storage namespace read and write the same files. A storage policy built
    /// from both of them makes `MergeTree` enumerate every part twice, which it reports as duplicate parts.
    for (const auto & [saved_disk_name, saved_disk] : disks)
    {
        if (saved_disk_name == disk_name)
            continue;

        /// A `cache` or `encrypted` disk sits on top of another disk and reports the namespace of the
        /// disk it wraps. The two are one storage behind one metadata storage, not a duplicate
        /// registration, so they are not a clash.
        if (areLayersOverOneStorage(disk, saved_disk))
            continue;

        if (haveSameStorageNamespace(disk, saved_disk))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Disks {} and {} resolve to the same storage namespace ({})",
                backQuote(saved_disk_name),
                backQuote(disk_name),
                quoteString(disk->getPath()));
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
    const Poco::Util::AbstractConfiguration & config, const String & config_prefix, ContextPtr context) const
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

void DiskSelector::shutdown()
{
    assertInitialized();
    for (auto & e : disks)
        e.second->shutdown();
}

}
