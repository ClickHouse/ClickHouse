#include <Disks/RemoteDisksCommon.h>
#include <Common/getRandomASCIIString.h>
#include <Disks/DiskLocal.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

static String getDiskMetadataPath(
    const String & name,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    ContextPtr context)
{
    return config.getString(config_prefix + ".metadata_path", context->getPath() + "disks/" + name + "/");
}

std::pair<String, DiskPtr> prepareForLocalMetadata(
    const String & name,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    ContextPtr context)
{
    /// where the metadata files are stored locally
    auto metadata_path = getDiskMetadataPath(name, config, config_prefix, context);
    fs::create_directories(metadata_path);
    auto metadata_disk = std::make_shared<DiskLocal>(name + "-metadata", metadata_path, 0);
    return std::make_pair(metadata_path, metadata_disk);
}

}
