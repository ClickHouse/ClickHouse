#pragma once
#include <unordered_map>
#include <Processors/ISimpleTransform.h>
#include <Parsers/IAST.h>
#include <Interpreters/InterpreterSelectQuery.h>

namespace DB
{

class CachingTransform : public ISimpleTransform
{
public:
    CachingTransform(const Block & header, LRUCache<CacheKey, Data, CacheKeyHasher> & cache, const CacheKey & cache_key)
        : ISimpleTransform(header, header, false)
        , data(std::move(cache.getOrSet(cache_key, [&]
                            {
                                return std::make_shared<Data>(header, Chunks{});
                            }).first))
    {}
    String getName() const override { return "CachingTransform"; }

protected:
    void transform(Chunk & chunk) override;
private:
    std::shared_ptr<Data> data;

};

}
