#pragma once

#include <Processors/ITransform.h>
#include <Core/ColumnNumbers.h>

namespace DB
{

class WrapNullableTransform : public ITransform
{
public:
    WrapNullableTransform(Blocks input_headers, const ColumnNumbers & column_numbers, size_t result);

    String getName() const override { return "WrapNullableTransform"; }

protected:
    Blocks transform(Blocks && blocks) override;

private:
    ColumnNumbers column_numbers;
    size_t result;
};

}
