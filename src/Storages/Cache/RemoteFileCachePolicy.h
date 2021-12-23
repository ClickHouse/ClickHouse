#pragma once
namespace DB
{
struct RemoteFileCacheWeightFunction
{
    size_t operator()(const RemoteCacheController & cache) const
    {
        return cache.getFileSize();
    }
};

struct RemoteFileCacheEvictPolicy
{
    bool canRelease(std::shared_ptr<RemoteCacheController> cache) const
    {
        return (!cache || cache->closable());
    }
    void release(std::shared_ptr<RemoteCacheController>  cache)
    {
        if (cache)
            cache->close();
    }
};

}
