#pragma once
#include <Processors/ISource.h>
#include <Parsers/IAST.h>
#include <Interpreters/InterpreterSelectQuery.h>

namespace DB
{

class ReadFromCacheTransform : public ISource
{
public:
    ReadFromCacheTransform(const Block & header, const Chunks& chunks);
    String getName() const override { return "ReadFromCacheTransform"; }

protected:
    Chunk generate() override;
private:
    const Chunks & chunks;
    size_t chunks_read_count;
};

}

