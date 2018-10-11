#pragma once

#include <Processors/ITransform.h>
#include <Core/ColumnNumbers.h>

namespace DB
{

class RemoveNullableTransform : public ITransform
{
public:
    RemoveNullableTransform(Block input_header, const ColumnNumbers & column_numbers, size_t result);

    String getName() const override { return "RemoveNullableTransform"; }

protected:
    Blocks transform(Blocks && blocks) override;

private:
    ColumnNumbers column_numbers;
    size_t result;
};

}
