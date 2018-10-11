#pragma once

#include <Core/Block.h>
#include <Processors/ISimpleTransform.h>

namespace DB
{

class CreateConstantColumnTransform : public ISimpleTransform
{
public:
    CreateConstantColumnTransform(Block header, size_t position, Field value)
        : ISimpleTransform(header, createConstColumn(Block(header), position, value))
        , position(position)
        , value(value)
    {
    }

    String getName() const override { return "CreateConstantColumnTransform"; }

protected:
    void transform(Block & block) override
    {
        block = createConstColumn(std::move(block), position, value);
    }

private:
    size_t position;
    Field value;

    static Block createConstColumn(Block && block, size_t position, const Field & value)
    {
        auto & col = block.getByPosition(position);
        col.column = col.type->createColumnConst(block.getNumRows(), value);
        return block;
    }
};

}
