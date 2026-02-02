#pragma once
#include <Processors/ISimpleTransform.h>
#include <Processors/IInflatingTransform.h>

namespace DB
{

class ArrayJoinAction;
using ArrayJoinActionPtr = std::shared_ptr<ArrayJoinAction>;

class ArrayJoinResultIterator;
using ArrayJoinResultIteratorPtr = std::unique_ptr<ArrayJoinResultIterator>;

/// Execute ARRAY JOIN
class ArrayJoinTransform : public IInflatingTransform
{
public:
    ArrayJoinTransform(
            const Block & header_,
            ArrayJoinActionPtr array_join_,
            bool on_totals_ = false);

    String getName() const override { return "ArrayJoinTransform"; }

    static Block transformHeader(Block header, const Names & array_join_columns);

protected:
    void consume(Chunk chunk) override;
    bool canGenerate() override;
    Chunk generate() override;

private:
    ArrayJoinActionPtr array_join;
    ArrayJoinResultIteratorPtr result_iterator;
};

}
