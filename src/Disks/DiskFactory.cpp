#include <Disks/DiskFactory.h>
#include <Disks/loadLocalDiskConfig.h>

#include <Common/Config/ConfigurationWithUsageTracking.h>
#include <Interpreters/Context.h>

#include <fmt/ranges.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
}

DiskFactory & DiskFactory::instance()
{
    static DiskFactory factory;
    return factory;
}

void DiskFactory::registerDiskType(const String & disk_type, Creator creator, Documentation documentation)
{
    if (!registry.emplace(disk_type, creator).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DiskFactory: the disk type '{}' is not unique", disk_type);
    documentations.emplace(disk_type, std::move(documentation));
}

std::vector<String> DiskFactory::getAllRegisteredNames() const // STYLE_CHECK_ALLOW_STD_CONTAINERS
{
    std::vector<String> result; // STYLE_CHECK_ALLOW_STD_CONTAINERS
    result.reserve(registry.size());
    for (const auto & pair : registry)
        result.push_back(pair.first);
    return result;
}

Documentation DiskFactory::getDocumentation(const String & disk_type) const
{
    if (auto it = documentations.find(disk_type); it != documentations.end())
        return it->second;
    return {};
}

DiskPtr DiskFactory::create(
    const String & name,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    ContextPtr context,
    const DisksMap & map,
    bool attach,
    bool custom_disk,
    const std::unordered_set<String> & skip_types) const
{
    /// The disk is created from the configuration through a proxy remembering which keys are read.
    /// Everything that is left unread in its section is not an option of this disk type: it does
    /// nothing, and reporting it is much better than surprising the user later, see below.
    auto tracked_config = std::make_shared<ConfigurationWithUsageTracking>(config);

    /// A disk defined in a query (`disk(type = ..., name = ...)`) has its own configuration,
    /// in which these elements are read by the caller and not by the disk itself:
    /// `name` in `getOrCreateCustomDisk` and `_server_credentials_allowed` in `getDiskConfigurationFromAST`.
    if (custom_disk)
    {
        tracked_config->markAsUsed(config_prefix + ".name");
        tracked_config->markAsUsed(config_prefix + "._server_credentials_allowed");
    }

    const auto disk_type = tracked_config->getString(config_prefix + ".type", "local");

    const auto found = registry.find(disk_type);
    if (found == registry.end())
    {
        throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG,
                        "DiskFactory: the disk '{}' has unknown disk type: {}", name, disk_type);
    }

    if (skip_types.contains(found->first))
    {
        return nullptr;
    }

    const auto & disk_creator = found->second;
    DiskPtr disk = disk_creator(name, *tracked_config, config_prefix, context, map, attach, custom_disk);
    if (!disk)
        return disk;

    disk->keepConfigurationAlive(tracked_config);

    /// A disk of a table that is being attached has been accepted by an older version of the server
    /// already, and its data has to be read even if the definition contains something we do not know.
    if (!attach)
        checkForUnknownKeys(*tracked_config, name, disk_type, config_prefix, context, /* skip_used_sections = */ false);

    return disk;
}

void DiskFactory::applyNewSettings(
    const DiskPtr & disk,
    const String & name,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    ContextPtr context,
    const DisksMap & map) const
{
    /// The keys that were read while this disk was created. A key is remembered even when it is
    /// absent from the configuration, so this is the set of the elements this disk type supports,
    /// not only the set of the elements that were present. A disk of an unknown origin - one that
    /// was not created by this factory nor by `trackImplicitLocalDisk` - cannot be checked.
    const auto creation_config = disk->getCreationConfiguration();

    /// Only the names of the keys are taken from it: the configuration it was created from is
    /// already replaced by the new one at this point.
    auto tracked_config = std::make_shared<ConfigurationWithUsageTracking>(config);
    if (creation_config)
    {
        for (const auto & key : creation_config->getUsedKeys())
            tracked_config->markAsUsed(key);
    }

    /// `type` is read by the factory, not by the disk itself.
    const auto disk_type = tracked_config->getString(config_prefix + ".type", "local");

    /// `applyNewSettings` changes the live disk, so a definition with an unknown element has to be
    /// rejected before it, otherwise a failed reload leaves a part of the rejected definition applied.
    /// Only the inside of a section that the creation of the disk has looked at (such as `proxy`)
    /// cannot be judged yet: a section that was not there before may be read by `applyNewSettings`
    /// only, and it is checked after it.
    if (creation_config)
        checkForUnknownKeys(*tracked_config, name, disk_type, config_prefix, context, /* skip_used_sections = */ true);

    /// Unlike the creation of a disk, `applyNewSettings` does not keep a reference to the
    /// configuration anywhere: it reads the settings it supports and returns, so this proxy is not
    /// kept alive after the call. The proxy of the creation must not be replaced by it either -
    /// the parts of the disk that read the configuration later still refer to that one.
    disk->applyNewSettings(*tracked_config, context, config_prefix, map);

    if (!creation_config)
        return;

    /// An element inside a section that neither the creation of this disk nor `applyNewSettings`
    /// reads does nothing, exactly as it does nothing at the start of the server, where it is
    /// reported as well.
    checkForUnknownKeys(*tracked_config, name, disk_type, config_prefix, context, /* skip_used_sections = */ false);
}

std::shared_ptr<const ConfigurationWithUsageTracking> DiskFactory::trackImplicitLocalDisk(
    const String & name,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    ContextPtr context)
{
    auto tracked_config = std::make_shared<ConfigurationWithUsageTracking>(config);

    /// The same elements as the creation of a `local` disk reads, see `registerDiskLocal`.
    /// The section does not exist (otherwise the disk would not be implicit), so nothing is taken
    /// from it: this only records which elements a `local` disk supports.
    String path;
    UInt64 keep_free_space_bytes = 0;
    loadDiskLocalConfig(name, *tracked_config, config_prefix, context, path, keep_free_space_bytes);
    tracked_config->markAsUsed(config_prefix + ".type");
    tracked_config->markAsUsed(config_prefix + ".skip_access_check");
    tracked_config->markAsUsed(config_prefix + ".thread_pool_size");

    return tracked_config;
}

void DiskFactory::checkForUnknownKeys(
    const ConfigurationWithUsageTracking & tracked_config,
    const String & name,
    const String & disk_type,
    const String & config_prefix,
    const ContextPtr & context,
    bool skip_used_sections)
{
    const Strings unknown_keys = tracked_config.getUnusedKeys(config_prefix, skip_used_sections);
    if (unknown_keys.empty())
        return;

    if (context->getConfigRef().getBool("skip_check_for_incorrect_settings", false))
        return;

    throw Exception(
        ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG,
        "Unknown element{} in the definition of the disk `{}` of type `{}`: {}. "
        "Nothing reads {}, so {} no effect - most likely it is a typo or an option of another disk type. "
        "You can disable this check with <skip_check_for_incorrect_settings>1</skip_check_for_incorrect_settings>.",
        unknown_keys.size() == 1 ? "" : "s",
        name,
        disk_type,
        fmt::join(unknown_keys, ", "),
        unknown_keys.size() == 1 ? "it" : "them",
        unknown_keys.size() == 1 ? "it has" : "they have");
}

void DiskFactory::clearRegistry()
{
    registry.clear();
    documentations.clear();
}
}
