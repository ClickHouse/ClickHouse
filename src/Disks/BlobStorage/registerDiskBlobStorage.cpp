#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#include <Disks/DiskFactory.h>

#if USE_AZURE_BLOB_STORAGE

#include <Disks/BlobStorage/DiskBlobStorage.h>
#include <Disks/DiskRestartProxy.h>


namespace DB
{

void registerDiskBlobStorage(DiskFactory & factory)
{
    auto creator = [](
        const String &,
        const Poco::Util::AbstractConfiguration &,
        const String &,
        ContextPtr,
        const DisksMap &)
    {
        std::shared_ptr<IDisk> diskBlobStorage = std::make_shared<DiskBlobStorage>();

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
