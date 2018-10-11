#pragma once

#include <Processors/ISimpleTransform.h>
#include <Core/ColumnNumbers.h>

namespace DB
{

class WrapConstantsTransform : public ISimpleTransform
{
public:
    WrapConstantsTransform(Block input_header, const ColumnNumbers & column_numbers, size_t result);

    String getName() const override { return "WrapConstantsTransform"; }

protected:
    void transform(Block & block) override;

private:
    ColumnNumbers column_numbers;
    size_t result;
};

}
