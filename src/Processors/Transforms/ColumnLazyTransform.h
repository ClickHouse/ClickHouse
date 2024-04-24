#pragma once
#include <Processors/ISimpleTransform.h>

namespace DB
{

class ColumnLazyTransform : public ISimpleTransform
{
public:
    explicit ColumnLazyTransform(const Block & header_);

    static Block transformHeader(Block header);

    String getName() const override { return "ColumnLazyTransform"; }

protected:
    void transform(Chunk & chunk) override;
};

}
