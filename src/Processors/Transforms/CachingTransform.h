#pragma once
#include <Processors/ISimpleTransform.h>
#include <Parsers/IAST.h>
#include <Interpreters/InterpreterSelectQuery.h>

namespace DB
{

class CachingTransform : public ISimpleTransform
{
public:
    CachingTransform(const Block & header_, std::unordered_map<IAST::Hash, Data> & cache_, ASTPtr query_ptr_)
        : ISimpleTransform(header_, header_, false)
        , cache(cache_)
        , query_ptr(query_ptr_)
    {}
    String getName() const override { return "CachingTransform"; }

protected:
    void transform(Chunk & chunk) override;
    std::unordered_map<IAST::Hash, Data> & cache;
    ASTPtr query_ptr;
};

}
