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
    CachingTransform(const Block & header_, QueryCachePtr cache_, ASTPtr query_ptr_, Settings settings_, std::optional<String> username_)
        : ISimpleTransform(header_, header_, false)
        , cache(cache_)
        , cache_key(query_ptr_, header_, settings_, username_)
        , data(std::move(cache->getOrSet(cache_key, [&]
                                         {
                                             return std::make_shared<Data>(header_, Chunks{});
                                         }).first))
    {}
    String getName() const override { return "CachingTransform"; }
    ~CachingTransform() override
    {
        cache->getPutInCacheMutex(cache_key).unlock();
        cache->scheduleRemoval(cache_key);
    }

protected:
    void transform(Chunk & chunk) override;
private:
    QueryCachePtr cache;
    CacheKey cache_key;
    std::shared_ptr<Data> data;

};

}
