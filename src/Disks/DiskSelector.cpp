#include <Disks/DiskObjectStorage/DiskObjectStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/Cache/MetadataStorageFromCacheObjectStorage.h>
#include <Disks/DiskLocal.h>
#include <Disks/DiskSelector.h>
#include <Disks/DiskEncrypted.h>

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

namespace
{

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

/// Normalizes a disk path. A local directory is canonicalized before its `..` components are collapsed,
/// because a `..` after a symlink leaves the directory the symlink points to, not the one that holds it.
/// Any other path is a prefix of object keys, where repeated separators and `..` are significant bytes
/// that address different objects, so it is kept as is.
/// A trailing separator is dropped in both cases, so that a root with and without one compare equal.
///
/// The directory of a broken disk is not canonicalized: it may be inaccessible, and canonicalizing it would throw
/// and stop the server from starting, while a broken disk is meant to be skipped (see `IDisk::startup` and
/// `MergeTreeData::loadDataParts`). Its path is only collapsed lexically, which still finds two disks with one
/// directory. A symlink to it is checked at the next start, as no parts are loaded from the disk until then.
String normalizeDiskPath(const String & path, bool is_local, bool is_broken)
{
    if (!is_local)
    {
        if (path.size() > 1 && path.ends_with('/'))
            return path.substr(0, path.size() - 1);
        return path;
    }

    auto normalized = is_broken
        ? std::filesystem::path(path).lexically_normal()
        : std::filesystem::weakly_canonical(std::filesystem::path(path)).lexically_normal();
    if (!normalized.has_filename() && normalized.has_relative_path())
        normalized = normalized.parent_path();

    return normalized.string();
}

/// The path of `relative_path` on `disk`: the root of the disk followed by `relative_path`.
String joinDiskPath(const DiskPtr & disk, const String & relative_path)
{
    if (relative_path.empty())
        return disk->getPath();
    return (std::filesystem::path(disk->getPath()) / relative_path).string();
}

/// Returns the storage namespace of `relative_path` on a disk, or nothing when the disk has no persistent one.
std::optional<StorageNamespace> getStorageNamespace(const DiskPtr & disk, const String & relative_path = {})
{
    const auto data_source = disk->getDataSourceDescription();

    /// Separate in-memory disks do not share a persistent namespace even though their paths are empty.
    if (data_source.type == DataSourceType::RAM)
        return {};

    const auto path = normalizeDiskPath(joinDiskPath(disk, relative_path), data_source.isPathLocal(), disk->isBroken());

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
        .path = path,
    };
}

/// Returns the local directory that a disk lists its files from, or nothing when the listing does not come
/// from the local filesystem.
///
/// For a `local` disk it is the disk's directory. For an object storage disk with `metadata_type = local`,
/// `DiskObjectStorage::iterateDirectory` lists the local metadata directory, whatever the object storage
/// behind it is. For a `local` object storage with plain metadata, it is the directory of the objects.
std::optional<String> getLocalListingDirectory(const DiskPtr & disk, const String & relative_path = {})
{
    const auto data_source = disk->getDataSourceDescription();
    if (data_source.type == DataSourceType::RAM || !data_source.isPathLocal())
        return {};

    return normalizeDiskPath(joinDiskPath(disk, relative_path), /*is_local=*/ true, disk->isBroken());
}

/// Returns the disk below the `encrypted` layers of `disk` and the path in it that `relative_path` on `disk` maps to.
///
/// An `encrypted` disk does not always prefix a path with its own `path`: a path that already starts with it is
/// taken as already prefixed and passed to the wrapped disk as is (see `DiskEncryptedTransaction::wrappedPath`).
/// So the root reported by `getPath` does not tell where the files of a table are: an `encrypted` disk with
/// `path = 'store/'` keeps the files of a table at `store/<prefix>/<uuid>/` directly in the wrapped disk, where
/// an `encrypted` disk without a `path` over the same disk, or the wrapped disk itself, keeps them too.
std::pair<DiskPtr, String> resolveEncryptedLayers(DiskPtr disk, String relative_path)
{
#if USE_SSL
    while (const auto * encrypted = dynamic_cast<const DiskEncrypted *>(disk.get()))
    {
        relative_path = DiskEncryptedTransaction::wrappedPath(encrypted->getPathInWrappedDisk(), relative_path);
        disk = encrypted->getDelegateDiskIfExists();
    }
#endif
    return {std::move(disk), std::move(relative_path)};
}

bool haveSameStorageNamespaceForPath(const DiskPtr & lhs, const DiskPtr & rhs, const String & relative_path)
{
    const auto [lhs_disk, lhs_path] = resolveEncryptedLayers(lhs, relative_path);
    const auto [rhs_disk, rhs_path] = resolveEncryptedLayers(rhs, relative_path);

    if (const auto lhs_directory = getLocalListingDirectory(lhs_disk, lhs_path))
    {
        if (lhs_directory == getLocalListingDirectory(rhs_disk, rhs_path))
            return true;
    }

    const auto lhs_namespace = getStorageNamespace(lhs_disk, lhs_path);
    if (!lhs_namespace)
        return false;

    return lhs_namespace == getStorageNamespace(rhs_disk, rhs_path);
}

}

void checkDisksHaveDistinctStorageForPath(const Disks & disks, const String & relative_path, const String & storage_policy_name)
{
    if (relative_path.empty())
        return;

    for (size_t i = 0; i < disks.size(); ++i)
    {
        for (size_t j = i + 1; j < disks.size(); ++j)
        {
            if (disks[i] == disks[j])
                continue;

            if (haveSameStorageNamespaceForPath(disks[i], disks[j], relative_path))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Disks {} and {} in storage policy {} keep the path {} in the same storage. "
                    "A table that reads through both of them loads every part twice",
                    backQuote(disks[i]->getName()),
                    backQuote(disks[j]->getName()),
                    backQuote(storage_policy_name),
                    quoteString(relative_path));
        }
    }
}

bool haveSameStorageNamespace(const DiskPtr & lhs, const DiskPtr & rhs)
{
    /// Two disks that list one local directory see the same parts, even when the objects behind them are in
    /// different buckets.
    if (const auto lhs_directory = getLocalListingDirectory(lhs))
    {
        if (lhs_directory == getLocalListingDirectory(rhs))
            return true;
    }

    const auto lhs_namespace = getStorageNamespace(lhs);
    if (!lhs_namespace)
        return false;

    return lhs_namespace == getStorageNamespace(rhs);
}

bool areDisksDuplicateStorage(const DiskPtr & lhs, const DiskPtr & rhs)
{
    /// Every pair of disks over one storage in one policy makes the table enumerate the same parts twice. This
    /// holds for a read-only disk beside a writable one too: if the reader's copy of a part wins, the writer's
    /// copy is loaded as outdated and removed through the writable disk, which deletes the files the reader
    /// uses. And it holds for a `cache` or `encrypted` disk beside the disk it wraps, which list the same files.
    return haveSameStorageNamespace(lhs, rhs);
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
