#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionAdaptors.h>

namespace DB
{
ColumnPtr FunctionToExecutableFunctionAdaptor::executeImpl(const ColumnsWithTypeAndName& arguments,
        const DataTypePtr& result_type, size_t input_rows_count) const
{
    checkFunctionArgumentSizes(arguments, input_rows_count);
    return function->executeImpl(arguments, result_type, input_rows_count);
}

ColumnPtr FunctionToExecutableFunctionAdaptor::executeDryRunImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
{
    checkFunctionArgumentSizes(arguments, input_rows_count);
    return function->executeImplDryRun(arguments, result_type, input_rows_count);
}

}
