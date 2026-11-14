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
    /// A `Nothing` or `Nullable(Nothing)` result holds at most one value, so it maps the whole argument
    /// domain onto that value. Injectivity then survives only over a domain that is itself at most one
    /// tuple wide, and the overload resolver forms this result type without consulting `function`.
    if (isNothing(removeNullable(getResultType())))
    {
        /// The argument types decide how wide the domain is; an empty sample decides nothing.
        if (sample_columns.empty())
            return false;

        for (const auto & argument : sample_columns)
            if (!isNothing(removeNullable(argument.type)))
                return false;
    }

    return function->isInjective(sample_columns);
}

}
