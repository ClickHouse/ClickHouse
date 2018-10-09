#pragma once;

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
        size_t result);

protected:
    void transform(Block & block) override;

private:
    PreparedFunctionPtr prepared_function;
    const ColumnNumbers & column_numbers;
    size_t result;
};

}
