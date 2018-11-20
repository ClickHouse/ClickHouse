#pragma once

#include <Processors/ISimpleTransform.h>
#include <Core/ColumnNumbers.h>

namespace DB
{

class IPreparedFunction;
using PreparedFunctionPtr = std::shared_ptr<IPreparedFunction>;

class ExecuteFunctionTransform : public ISimpleTransform
{
public:
    ExecuteFunctionTransform(
        const PreparedFunctionPtr & function,
        Block input_header,
        const ColumnNumbers & column_numbers,
        size_t result,
        bool use_default_implementation_for_constants,
        const ColumnNumbers & remain_constants);

    String getName() const override { return "ExecuteFunctionTransform"; }

protected:
    void transform(Block & block) override;

private:
    PreparedFunctionPtr prepared_function;
    ColumnNumbers column_numbers;
    size_t result;
    bool use_default_implementation_for_constants;
    ColumnNumbers remain_constants;
};

}
