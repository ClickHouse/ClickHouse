#pragma once
#include <Processors/ISimpleTransform.h>

namespace DB
{

class ArrayJoinAction;
using ArrayJoinActionPtr = std::shared_ptr<ArrayJoinAction>;

/// Execute ARRAY JOIN
class ArrayJoinTransform : public ISimpleTransform
{
public:
    ArrayJoinTransform(
            const Block & header_,
            ArrayJoinActionPtr array_join_,
            bool on_totals_ = false);

    String getName() const override { return "ArrayJoinTransform"; }

    static Block transformHeader(Block header, const ArrayJoinActionPtr & array_join);

protected:
    void transform(Chunk & chunk) override;

private:
    ArrayJoinActionPtr array_join;
};

}
