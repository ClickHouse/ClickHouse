#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#include <Disks/DiskFactory.h>

#if USE_AZURE_BLOB_STORAGE

#include <Disks/BlobStorage/DiskBlobStorage.h>
#include <Disks/DiskRestartProxy.h>
#include <azure/identity/managed_identity_credential.hpp>


namespace DB
{

void registerDiskBlobStorage(DiskFactory & factory)
{
    auto creator = [](
        const String & name,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        ContextPtr context,
        const DisksMap &)
    {
        auto endpoint_url = config.getString(config_prefix + ".endpoint", "https://sadttmpstgeus.blob.core.windows.net/data");

        auto managed_identity_credential = std::make_shared<Azure::Identity::ManagedIdentityCredential>();

        auto blob_container_client = Azure::Storage::Blobs::BlobContainerClient(endpoint_url, managed_identity_credential);

        auto metadata_path = config.getString(config_prefix + ".metadata_path", context->getPath() + "disks/" + name + "/");
        fs::create_directories(metadata_path);

        std::shared_ptr<IDisk> diskBlobStorage = std::make_shared<DiskBlobStorage>(
            name,
            metadata_path,
            endpoint_url,
            managed_identity_credential,
            blob_container_client
        );

        return std::make_shared<DiskRestartProxy>(diskBlobStorage);
    };
    factory.registerDiskType("blob_storage", creator);
}

}

#else

namespace DB
{

void registerDiskBlobStorage(DiskFactory &) {}

}

#endif
