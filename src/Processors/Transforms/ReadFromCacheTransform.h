#pragma once
#include <Processors/ISource.h>
#include <Parsers/IAST.h>
#include <Interpreters/InterpreterSelectQuery.h>

namespace DB
{

class ReadFromCacheTransform : public ISource
{
public:
    ReadFromCacheTransform(const Block & header_, std::unordered_map<IAST::Hash, Data, ASTHash> & cache_, ASTPtr query_ptr_);
    String getName() const override { return "ReadFromCacheTransform"; }

protected:
    Chunk generate() override;
private:
    std::unordered_map<IAST::Hash, Data, ASTHash> & cache;
    ASTPtr query_ptr;
    size_t chunks_read_count;
};

}
