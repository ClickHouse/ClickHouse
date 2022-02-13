#include <Disks/RemoteDisksCommon.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

std::shared_ptr<DiskCacheWrapper> wrapWithCache(
    std::shared_ptr<IDisk> disk, String cache_name, String cache_path, String metadata_path)
{
    if (metadata_path == cache_path)
        throw Exception("Metadata and cache paths should be different: " + metadata_path, ErrorCodes::BAD_ARGUMENTS);

    auto cache_disk = std::make_shared<DiskLocal>(cache_name, cache_path, 0);
    auto cache_file_predicate = [] (const String & path)
    {
        return path.ends_with("idx") // index files.
                || path.ends_with("mrk") || path.ends_with("mrk2") || path.ends_with("mrk3") /// mark files.
                || path.ends_with("txt") || path.ends_with("dat");
    };

    return std::make_shared<DiskCacheWrapper>(disk, cache_disk, cache_file_predicate);
}


std::pair<String, DiskPtr> prepareForLocalMetadata(
    const String & name,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    ContextPtr context)
{
    /// where the metadata files are stored locally
    auto metadata_path = config.getString(config_prefix + ".metadata_path", context->getPath() + "disks/" + name + "/");
    fs::create_directories(metadata_path);
    auto metadata_disk = std::make_shared<DiskLocal>(name + "-metadata", metadata_path, 0);
    return std::make_pair(metadata_path, metadata_disk);
}

}
