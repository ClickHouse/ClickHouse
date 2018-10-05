#pragma once;

#include <Processors/ITransform.h>
#include <Core/ColumnNumbers.h>

namespace DB
{

class RemoveNullableTransform : public ITransform
{
public:
    RemoveNullableTransform(Block input_header, const ColumnNumbers & column_numbers, size_t result);

protected:
    Blocks transform(Blocks && blocks) override;

private:
    const ColumnNumbers & column_numbers;
    size_t result;
};

}
