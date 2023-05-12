#include <Poco/Exception.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Disks/DiskFactory.h>
#include <Disks/ObjectStorages/Web/WebObjectStorage.h>
#include <Disks/ObjectStorages/Web/MetadataStorageFromStaticFilesWebServer.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void registerDiskWebServer(DiskFactory & factory)
{
    auto creator = [](const String & disk_name,
                      const Poco::Util::AbstractConfiguration & config,
                      const String & config_prefix,
                      ContextPtr context,
                      const DisksMap & /*map*/) -> DiskPtr
    {
        String uri{config.getString(config_prefix + ".endpoint")};

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

        return std::make_shared<DiskObjectStorage>(
            disk_name,
            root_path,
            "DiskWebServer",
            metadata_storage,
            object_storage,
            DiskType::WebServer,
            /* send_metadata */false,
            /* threadpool_size */16);
    };

    factory.registerDiskType("web", creator);
}

}
