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

ColumnPtr FunctionToExecutableFunctionAdaptor::executeImplWithProfile(
    const ColumnsWithTypeAndName& arguments,
    const DataTypePtr& result_type,
    size_t input_rows_count,
    FunctionExecutionProfile * profile) const
{
    checkFunctionArgumentSizes(arguments, input_rows_count);
    return function->executeImplWithProfile(arguments, result_type, input_rows_count, profile);
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

    /// The default implementation for NULLs collapses rows in two ways.
    /// With a `Nullable` result, a row where one argument is NULL maps to NULL whatever the other
    /// arguments hold, so an argument that can be NULL must not sit next to one that varies.
    /// With a result that cannot be `Nullable` (e.g. `Array`), a NULL row is evaluated as `f(default)`,
    /// so NULL and the default value of a varying `Nullable` argument share one result. A constant
    /// NULL is the same `f(default)` on every row there and merges nothing.
    if (function->useDefaultImplementationForNulls())
    {
        const bool result_is_nullable = isNullableOrLowCardinalityNullable(getResultType());

        size_t non_constant_arguments = 0;
        for (const auto & argument : sample_columns)
            if (!argument.column || !isColumnConst(*argument.column))
                ++non_constant_arguments;

        for (const auto & argument : sample_columns)
        {
            const bool is_constant = argument.column && isColumnConst(*argument.column);
            if (is_constant)
            {
                if (result_is_nullable && argument.column->onlyNull() && non_constant_arguments > 0)
                    return false;
            }
            else if (isNullableOrLowCardinalityNullable(argument.type) && (!result_is_nullable || non_constant_arguments > 1))
            {
                return false;
            }
        }
    }

    return function->isInjective(sample_columns);
}

}
