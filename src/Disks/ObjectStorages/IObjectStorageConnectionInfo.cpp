#include <Disks/ObjectStorages/IObjectStorageConnectionInfo.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Disks/ObjectStorages/S3/S3ObjectStorageConnectionInfo.h>
#include <Disks/ObjectStorages/Web/WebObjectStorageConnectionInfo.h>
#include <Disks/ObjectStorages/AzureBlobStorage/AzureObjectStorageConnectionInfo.h>
#include "config.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int SUPPORT_IS_DISABLED;
}

void IObjectStorageConnectionInfo::writeBinary(size_t protocol_version, WriteBuffer & out)
{
    DB::writeBinary(UInt8(getType()), out);
    writeBinaryImpl(protocol_version, out);
}

ObjectStorageConnectionInfoPtr IObjectStorageConnectionInfo::readBinary(ReadBuffer & in, const std::string & user_id [[maybe_unused]])
{
    UInt8 type = 0;
    DB::readBinary(type, in);

    if (type >= static_cast<UInt8>(ObjectStorageType::Max))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Expected value smaller than {}, got {}",
                        toString(static_cast<UInt8>(ObjectStorageType::Max)), toString(type));
    }

    switch (ObjectStorageType(type))
    {
        case ObjectStorageType::S3:
        {
#if USE_AWS_S3
            auto info = std::make_unique<S3ObjectStorageConnectionInfo>(user_id);
            info->readBinaryImpl(in);
            return info;
#else
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Server is built without S3 support");
#endif
        }
        case ObjectStorageType::Azure:
        {
#if USE_AZURE_BLOB_STORAGE
            auto info = std::make_unique<AzureObjectStorageConnectionInfo>(user_id);
            info->readBinaryImpl(in);
            return info;
#else
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Server is built without Azure support");
#endif
        }
        case ObjectStorageType::Web:
        {
            return getWebObjectStorageConnectionInfo(in);
        }
        case ObjectStorageType::None:
        {
            /// Data is not stored in object storage, but written by client directly to cache.
            /// It's used for temporary data.
            return std::make_shared<NoneStorageConnectionInfo>();
        }
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                            "Storage {} is not supported / implemented", ObjectStorageType(type));
    }
    UNREACHABLE();
}

}
