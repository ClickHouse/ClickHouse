#pragma once

#include <Processors/ISimpleTransform.h>
#include <Core/ColumnNumbers.h>

namespace DB
{

class RemoveConstantsTransform : public ISimpleTransform
{
public:
    RemoveConstantsTransform(Block input_header, const ColumnNumbers & arguments_to_remain_constants,
                             const ColumnNumbers & column_numbers, size_t result);

    String getName() const override { return "RemoveConstantsTransform"; }

    static Block removeConstants(
        Block && block,
        const ColumnNumbers & remain_constants,
        const ColumnNumbers & column_numbers,
        size_t result);

protected:
    void transform(Block & block) override;

private:
    ColumnNumbers arguments_to_remain_constants;
    ColumnNumbers column_numbers;
    size_t result;
};

}
