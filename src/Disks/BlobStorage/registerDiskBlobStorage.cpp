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

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int PATH_ACCESS_DENIED;
}


void checkWriteAccess(IDisk & disk)
{
    auto file = disk.writeFile("test_acl", DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite);
    file->write("test", 4);
}


void checkReadAccess(IDisk & disk)
{
    auto file = disk.readFile("test_acl", DBMS_DEFAULT_BUFFER_SIZE);
    String buf(4, '0');
    file->readStrict(buf.data(), 4);
    std::cout << "buf: ";
    for (size_t i = 0; i < 4; i++)
    {
        std::cout << static_cast<uint8_t>(buf[i]) << " ";
    }
    std::cout << "\n";

    if (buf != "test")
        throw Exception("No read access to disk", ErrorCodes::PATH_ACCESS_DENIED);
}


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

        std::shared_ptr<IDisk> blob_storage_disk = std::make_shared<DiskBlobStorage>(
            name,
            metadata_path,
            endpoint_url,
            managed_identity_credential,
            blob_container_client
        );

        bool initial_check = true;

        if (initial_check)
        {
            checkWriteAccess(*blob_storage_disk);
            checkReadAccess(*blob_storage_disk);
        }

        return std::make_shared<DiskRestartProxy>(blob_storage_disk);
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
