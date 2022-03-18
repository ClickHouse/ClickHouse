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
    CachingTransform(const Block & header_, std::unordered_map<IAST::Hash, Data, ASTHash> & cache_, ASTPtr query_ptr_)
        : ISimpleTransform(header_, header_, false)
        , cache(cache_)
        , query_ptr(query_ptr_)
        , header(header_)
    {}
    String getName() const override { return "CachingTransform"; }

protected:
    void transform(Chunk & chunk) override;
private:
    std::unordered_map<IAST::Hash, Data, ASTHash> & cache;
    ASTPtr query_ptr;
    Block header;
};

}
