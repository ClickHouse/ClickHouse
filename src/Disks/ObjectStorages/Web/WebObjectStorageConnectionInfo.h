#pragma once
#include <Disks/ObjectStorages/IObjectStorageConnectionInfo.h>

namespace DB
{
class ReadBuffer;

ObjectStorageConnectionInfoPtr getWebObjectStorageConnectionInfo(const std::string & url);
ObjectStorageConnectionInfoPtr getWebObjectStorageConnectionInfo(ReadBuffer & in);

}
