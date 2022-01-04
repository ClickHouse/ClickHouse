#pragma once
namespace DB
{
struct RemoteFileCacheWeightFunction
{
    size_t operator()(const RemoteCacheController & cache) const { return cache.getFileSize(); }
};

}
