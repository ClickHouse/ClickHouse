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
    CachingTransform(const Block & header_, LRUCache<CacheKey, Data, CacheKeyHasher> & cache_, ASTPtr query_ptr_)
        : ISimpleTransform(header_, header_, false)
        , cache(cache_)
        , query_ptr(query_ptr_)
        , header(header_)
    {}
    String getName() const override { return "CachingTransform"; }

protected:
    void transform(Chunk & chunk) override;
private:
    LRUCache<CacheKey, Data, CacheKeyHasher> & cache;
    ASTPtr query_ptr;
    Block header;
};

}
