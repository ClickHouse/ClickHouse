#include <Functions/FunctionFixedArrayAdaptor.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedArray.h>
#include <Columns/ColumnReplicated.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedArray.h>


namespace DB
{

namespace
{

class ExecutableFunctionFixedArrayAdaptor final : public IExecutableFunction
{
public:
    explicit ExecutableFunctionFixedArrayAdaptor(ExecutableFunctionPtr function_) : function(std::move(function_)) {}

    String getName() const override { return function->getName(); }

protected:
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return execute(arguments, result_type, input_rows_count, false);
    }

    ColumnPtr executeDryRunImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return execute(arguments, result_type, input_rows_count, true);
    }

    /// The default implementations are applied by the wrapped function to the converted arguments.
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForNothing() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    bool useDefaultImplementationForSparseColumns() const override { return false; }
    bool useDefaultImplementationForReplicatedColumns() const override { return false; }

private:
    ColumnPtr execute(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count, bool dry_run) const
    {
        auto converted = FunctionBaseFixedArrayAdaptor::convertArguments(arguments);
        return function->execute(converted ? *converted : arguments, result_type, input_rows_count, dry_run);
    }

    ExecutableFunctionPtr function;
};

}

std::optional<ColumnsWithTypeAndName> FunctionBaseFixedArrayAdaptor::convertArguments(const ColumnsWithTypeAndName & arguments)
{
    std::optional<ColumnsWithTypeAndName> result;
    for (size_t i = 0; i < arguments.size(); ++i)
    {
        const auto & argument = arguments[i];
        if (!argument.type)
            continue;

        auto converted_type = recursiveRemoveFixedArray(argument.type);
        if (converted_type.get() == argument.type.get())
            continue;

        if (!result)
            result = arguments;

        auto & converted = (*result)[i];
        converted.type = std::move(converted_type);
        if (converted.column)
            converted.column = recursiveRemoveFixedArray(converted.column);
    }
    return result;
}

bool FunctionBaseFixedArrayAdaptor::isEqual(const IFunctionBase & rhs) const
{
    const auto * rhs_adaptor = typeid_cast<const FunctionBaseFixedArrayAdaptor *>(&rhs);
    return rhs_adaptor && function->isEqual(*rhs_adaptor->function);
}

ExecutableFunctionPtr FunctionBaseFixedArrayAdaptor::prepare(const ColumnsWithTypeAndName & arguments) const
{
    auto converted = convertArguments(arguments);
    return std::make_shared<ExecutableFunctionFixedArrayAdaptor>(function->prepare(converted ? *converted : arguments));
}

bool FunctionBaseFixedArrayAdaptor::isInjective(const ColumnsWithTypeAndName & sample_columns) const
{
    auto converted = convertArguments(sample_columns);
    return function->isInjective(converted ? *converted : sample_columns);
}

ColumnPtr FunctionBaseFixedArrayAdaptor::getConstantResultForNonConstArguments(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const
{
    auto converted = convertArguments(arguments);
    return function->getConstantResultForNonConstArguments(converted ? *converted : arguments, result_type);
}

DataTypePtr removeOuterFixedArray(const DataTypePtr & type)
{
    if (const auto * fixed_array_type = typeid_cast<const DataTypeFixedArray *>(type.get()))
        return std::make_shared<DataTypeArray>(fixed_array_type->getNestedType());
    return type;
}

ColumnWithTypeAndName removeOuterFixedArray(const ColumnWithTypeAndName & argument)
{
    if (!isFixedArray(argument.type))
        return argument;

    ColumnWithTypeAndName res = argument;
    res.type = removeOuterFixedArray(argument.type);
    if (const auto * column_const = typeid_cast<const ColumnConst *>(argument.column.get()))
        res.column = ColumnConst::create(
            assert_cast<const ColumnFixedArray &>(column_const->getDataColumn()).convertToArray(), column_const->size());
    else if (const auto * column_replicated = typeid_cast<const ColumnReplicated *>(argument.column.get()))
        res.column = ColumnReplicated::create(
            assert_cast<const ColumnFixedArray &>(*column_replicated->getNestedColumn()).convertToArray(),
            column_replicated->getIndexesColumn());
    else if (argument.column)
        res.column = assert_cast<const ColumnFixedArray &>(*argument.column).convertToArray();
    return res;
}

}
