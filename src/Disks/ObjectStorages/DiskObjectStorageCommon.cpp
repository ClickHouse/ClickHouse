#include <Disks/ObjectStorages/DiskObjectStorageCommon.h>
#include <Common/getRandomASCIIString.h>
#include <Disks/DiskLocal.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Interpreters/Context.h>

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

bool isFileWithPersistentCache(const String & path)
{
    auto path_extension = std::filesystem::path(path).extension();
    return path_extension == ".idx" // index files.
            || path_extension == ".mrk" || path_extension == ".mrk2" || path_extension == ".mrk3" /// mark files.
            || path_extension == ".txt" || path_extension == ".dat";
}

}
