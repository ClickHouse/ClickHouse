#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
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

bool FunctionToFunctionBaseAdaptor::isInjective(const ColumnsWithTypeAndName & sample_columns) const
{
    /// A result type admitting at most one value maps the whole argument domain onto that value, so
    /// injectivity could only hold over a single-valued domain. The overload resolver produces such a
    /// type by itself (a NULL constant argument, or a `Nothing` one), bypassing the implementation.
    if (isNothing(removeNullable(getResultType())))
        return false;

    return function->isInjective(sample_columns);
}

}
