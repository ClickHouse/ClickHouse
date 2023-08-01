#include <Poco/Exception.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Disks/DiskFactory.h>
#include <Disks/ObjectStorages/Web/WebObjectStorage.h>
#include <Disks/ObjectStorages/Web/MetadataStorageFromStaticFilesWebServer.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>
#include <Common/assert_cast.h>
#include <Common/Macros.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void registerDiskWebServer(DiskFactory & factory, bool global_skip_access_check)
{
    auto creator = [global_skip_access_check](
        const String & disk_name,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        ContextPtr context,
        const DisksMap & /*map*/) -> DiskPtr
    {
        String uri = context->getMacros()->expand(config.getString(config_prefix + ".endpoint"));
        bool skip_access_check = global_skip_access_check || config.getBool(config_prefix + ".skip_access_check", false);

        if (!uri.ends_with('/'))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "URI must end with '/', but '{}' doesn't.", uri);
        try
        {
            Poco::URI poco_uri(uri);
        }
        catch (const Poco::Exception & e)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Bad URI: `{}`. Error: {}", uri, e.what());
        }

        auto object_storage = std::make_shared<WebObjectStorage>(uri, context);
        auto metadata_storage = std::make_shared<MetadataStorageFromStaticFilesWebServer>(assert_cast<const WebObjectStorage &>(*object_storage));
        std::string root_path;

        DiskPtr disk = std::make_shared<DiskObjectStorage>(
            disk_name,
            root_path,
            "DiskWebServer",
            metadata_storage,
            object_storage,
            config,
            config_prefix);
        disk->startup(context, skip_access_check);
        return disk;
    };

    factory.registerDiskType("web", creator);
}

}
