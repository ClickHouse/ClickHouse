#pragma once

#include <Disks/DiskFactory.h>
#include <Disks/IDisk.h>

#include <map>
#include <vector>

namespace Poco::Util
{
    class AbstractConfiguration;
};

namespace DB
{

class DiskSelector;
enum class RemoteHostFilterCheck : uint8_t;
using DiskSelectorPtr = std::shared_ptr<const DiskSelector>;

/// The metadata storage below the `encrypted` and `cache` layers of `disk`, or nullptr if `disk` is not over an object storage disk.
MetadataStoragePtr unwrapEncryptedAndCacheLayers(DiskPtr disk);

/// Parse .xml configuration and store information about disks
/// Mostly used for introspection.
class DiskSelector
{
public:
    static constexpr auto TMP_INTERNAL_DISK_PREFIX = "__tmp_internal_";

    explicit DiskSelector(std::unordered_set<String> skip_types_ = {}) : skip_types(skip_types_) { }
    DiskSelector(const DiskSelector & from) = default;

    using DiskValidator = std::function<bool(const Poco::Util::AbstractConfiguration & config, const String & disk_config_prefix, const String & disk_name)>;
    void initialize(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, ContextPtr context, DiskValidator disk_validator = {});

    /// `retiring_disks` are the custom disks that are not in the selector anymore but are not shut down yet, see
    /// `findPlainRewritableDisksOverlappingNewDisk`. A new disk overlapping one of them is not created.
    DiskSelectorPtr updateFromConfig(
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        ContextPtr context,
        const std::vector<std::pair<String, DiskPtr>> & retiring_disks = {}) const;

    /// Get disk by name
    DiskPtr get(const String & name) const;

    DiskPtr tryGet(const String & name) const;

    /// Get all disks with names
    const DisksMap & getDisksMap() const;

    void addToDiskMap(const String & name, DiskPtr disk);

    /// Only for disks defined inline with `disk(...)`: they are removed as soon as the last table
    /// or database using them is gone. Disks from the configuration stay until the server stops.
    void removeFromDiskMap(const String & name);

    void shutdown();

    inline static const String DEFAULT_DISK_NAME = "default";
    inline static const String LOCAL_DISK_NAME = "local";

    /// Whether `disk` and `other_disk` are different writable `plain_rewritable` disks over the same object storage
    /// prefix (or nested prefixes). Two such disks would overwrite the metadata of each other.
    static bool arePlainRewritableDisksOverlapping(const DiskPtr & disk, const DiskPtr & other_disk);

    /// The names of the disks in `disks` that a disk not created yet, described by the configuration at `config_prefix`,
    /// would overlap with, as in `arePlainRewritableDisksOverlapping`. The check has to be done before the disk is
    /// created: a `plain_rewritable` metadata storage loads its metadata in the constructor, which may already write to
    /// the object storage. So only the object storages of the new disk are created here (they do not write anything),
    /// and only if its configuration may give it `plain_rewritable` metadata and some disk in `disks` is a writable
    /// plain disk. A read-only new disk (the `read_only` setting) or object storage overlaps nothing, because it cannot
    /// overwrite the metadata of another disk. `run_local_paths_check` and `remote_host_filter_check` are as for the
    /// creation of the disk itself.
    static std::vector<String> findPlainRewritableDisksOverlappingNewDisk(
        const String & disk_name,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        ContextPtr context,
        bool run_local_paths_check,
        RemoteHostFilterCheck remote_host_filter_check,
        const std::vector<std::pair<String, DiskPtr>> & disks);

private:
    DisksMap disks;
    bool is_initialized = false;

    void assertInitialized() const;
    void recordDisk(const std::string & disk_name, DiskPtr disk);

    const std::unordered_set<String> skip_types;

    bool throw_away_local_on_update = false;
};

}
