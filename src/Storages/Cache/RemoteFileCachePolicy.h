#pragma once

#include <Storages/Cache/RemoteCacheController.h>

namespace DB
{

struct RemoteFileCacheWeightFunction
{
    size_t operator()(const RemoteCacheController & cache) const { return cache.getFileSize(); }
};

struct RemoteFileCacheReleaseFunction
{
    void operator()(std::shared_ptr<RemoteCacheController> controller)
    {
        if (controller)
            controller->close();
    }
};

}
