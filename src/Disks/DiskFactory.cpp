#include <Disks/DiskFactory.h>

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
    tracked_config->markAsUsed("name");
    tracked_config->markAsUsed("_server_credentials_allowed");

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
        checkForUnknownKeys(*tracked_config, name, disk_type, config_prefix, context);

    return disk;
}

void DiskFactory::checkForUnknownKeys(
    const ConfigurationWithUsageTracking & tracked_config,
    const String & name,
    const String & disk_type,
    const String & config_prefix,
    const ContextPtr & context)
{
    const Strings unknown_keys = tracked_config.getUnusedKeys(config_prefix);
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
