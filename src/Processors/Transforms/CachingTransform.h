#pragma once
#include <unordered_map>
#include <Processors/ISimpleTransform.h>
#include <Parsers/IAST.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Storages/QueryCache.h>

namespace DB
{

class CachingTransform : public ISimpleTransform
{
public:
    CachingTransform(const Block & header_, QueryCachePtr cache, CacheKey cache_key)
        : ISimpleTransform(header_, header_, false)
        , holder(cache->tryPutInCache(cache_key))
    {
    }

    String getName() const override { return "CachingTransform"; }

protected:
    void transform(Chunk & chunk) override;
private:
    CachePutHolder holder;
};

}
