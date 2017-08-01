#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeNull.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnConst.h>
#include <Interpreters/ExpressionActions.h>
#include <Common/typeid_cast.h>
#include <ext/range.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/// Suppose a function which has no special support for nullable arguments
/// has been called with arguments, one or more of them being nullable.
/// Then the method below endows the result, which is nullable, with a null
/// byte map that is determined by OR-ing the null byte maps of the nullable
/// arguments.
void createNullMap(Block & block, const ColumnNumbers & args, size_t result)
{
    ColumnNullable & res_col = static_cast<ColumnNullable &>(*block.getByPosition(result).column);

    for (const auto & arg : args)
    {
        const ColumnWithTypeAndName & elem = block.getByPosition(arg);
        if (elem.column->isNullable())
        {
            const ColumnNullable & nullable_col = static_cast<const ColumnNullable &>(*elem.column);
            res_col.applyNullMap(nullable_col);    /// TODO excessive copy in case of single nullable argument.
        }
    }
}


struct NullPresense
{
    bool has_nullable = false;
    bool has_null_constant = false;
};

NullPresense getNullPresense(const Block & block, const ColumnNumbers & args)
{
    NullPresense res;

    for (const auto & arg : args)
    {
        const auto & elem = block.getByPosition(arg);

        if (!res.has_nullable)
            res.has_nullable = elem.type->isNullable();
        if (!res.has_null_constant)
            res.has_null_constant = elem.type->isNull();
    }

    return res;
}

NullPresense getNullPresense(const ColumnsWithTypeAndName & args)
{
    NullPresense res;

    for (const auto & elem : args)
    {
        if (!res.has_nullable)
            res.has_nullable = elem.type->isNullable();
        if (!res.has_null_constant)
            res.has_null_constant = elem.type->isNull();
    }

    return res;
}

NullPresense getNullPresense(const DataTypes & types)
{
    NullPresense res;

    for (const auto & type : types)
    {
        if (!res.has_nullable)
            res.has_nullable = type->isNullable();
        if (!res.has_null_constant)
            res.has_null_constant = type->isNull();
    }

    return res;
}

/// Turn the specified set of columns into their respective nested columns.
ColumnsWithTypeAndName toNestedColumns(const ColumnsWithTypeAndName & args)
{
    ColumnsWithTypeAndName new_args;
    new_args.reserve(args.size());

    for (const auto & arg : args)
    {
        if (arg.type->isNullable())
        {
            auto nullable_col = static_cast<const ColumnNullable *>(arg.column.get());
            ColumnPtr nested_col = (nullable_col) ? nullable_col->getNestedColumn() : nullptr;
            auto nullable_type = static_cast<const DataTypeNullable *>(arg.type.get());
            DataTypePtr nested_type = nullable_type->getNestedType();

            new_args.emplace_back(nested_col, nested_type, arg.name);
        }
        else
            new_args.emplace_back(arg.column, arg.type, arg.name);
    }

    return new_args;
}

/// Turn the specified set of data types into their respective nested data types.
DataTypes toNestedDataTypes(const DataTypes & args)
{
    DataTypes new_args;
    new_args.reserve(args.size());

    for (const auto & arg : args)
    {
        if (arg->isNullable())
        {
            auto nullable_type = static_cast<const DataTypeNullable *>(arg.get());
            DataTypePtr nested_type = nullable_type->getNestedType();
            new_args.push_back(nested_type);
        }
        else
            new_args.push_back(arg);
    }

    return new_args;
}


bool allArgumentsAreConstants(const Block & block, const ColumnNumbers & args)
{
    for (auto arg : args)
        if (!typeid_cast<const ColumnConst *>(block.getByPosition(arg).column.get()))
            return false;
    return true;
}

bool defaultImplementationForConstantArguments(
    IFunction & func, Block & block, const ColumnNumbers & args, size_t result)
{
    if (args.empty() || !func.useDefaultImplementationForConstants() || !allArgumentsAreConstants(block, args))
        return false;

    ColumnNumbers arguments_to_remain_constants = func.getArgumentsThatAreAlwaysConstant();

    Block temporary_block;

    size_t arguments_size = args.size();
    for (size_t arg_num = 0; arg_num < arguments_size; ++arg_num)
    {
        const ColumnWithTypeAndName & column = block.getByPosition(args[arg_num]);

        if (arguments_to_remain_constants.end() != std::find(arguments_to_remain_constants.begin(), arguments_to_remain_constants.end(), arg_num))
            temporary_block.insert(column);
        else
            temporary_block.insert({ static_cast<const ColumnConst *>(column.column.get())->getDataColumnPtr(), column.type, column.name });
    }

    temporary_block.insert(block.getByPosition(result));

    ColumnNumbers temporary_argument_numbers(arguments_size);
    for (size_t i = 0; i < arguments_size; ++i)
        temporary_argument_numbers[i] = i;

    func.execute(temporary_block, temporary_argument_numbers, arguments_size);

    block.getByPosition(result).column = std::make_shared<ColumnConst>(temporary_block.getByPosition(arguments_size).column, block.rows());
    return true;
}


bool defaultImplementationForNulls(
    IFunction & func, Block & block, const ColumnNumbers & args, size_t result)
{
    if (args.empty() || !func.useDefaultImplementationForNulls())
        return false;

    NullPresense null_presense = getNullPresense(block, args);

    if (null_presense.has_null_constant)
    {
        block.getByPosition(result).column = block.getByPosition(result).type->createConstColumn(block.rows(), Null());
        return true;
    }

    if (null_presense.has_nullable)
    {
        Block temporary_block = createBlockWithNestedColumns(block, args, result);
        func.execute(temporary_block, args, result);

        const ColumnWithTypeAndName & source_col = temporary_block.getByPosition(result);
        ColumnWithTypeAndName & dest_col = block.getByPosition(result);

        /// Initialize the result column.
        ColumnPtr null_map = std::make_shared<ColumnUInt8>(block.rows(), 0);
        dest_col.column = std::make_shared<ColumnNullable>(source_col.column, null_map);

        /// Deduce the null map of the result from the null maps of the nullable columns.
        createNullMap(block, args, result);

        return true;
    }

    return false;
}

}


void IFunction::checkNumberOfArguments(size_t number_of_arguments) const
{
    if (isVariadic())
        return;

    size_t expected_number_of_arguments = getNumberOfArguments();

    if (number_of_arguments != expected_number_of_arguments)
        throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
            + toString(number_of_arguments) + ", should be " + toString(expected_number_of_arguments),
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
}


DataTypePtr IFunction::getReturnType(const DataTypes & arguments) const
{
    checkNumberOfArguments(arguments.size());

    if (!arguments.empty() && useDefaultImplementationForNulls())
    {
        NullPresense null_presense = getNullPresense(arguments);
        if (null_presense.has_null_constant)
            return std::make_shared<DataTypeNull>();
        if (null_presense.has_nullable)
            return std::make_shared<DataTypeNullable>(getReturnTypeImpl(toNestedDataTypes(arguments)));
    }

    return getReturnTypeImpl(arguments);
}


void IFunction::getReturnTypeAndPrerequisites(
    const ColumnsWithTypeAndName & arguments,
    DataTypePtr & out_return_type,
    std::vector<ExpressionAction> & out_prerequisites)
{
    checkNumberOfArguments(arguments.size());

    if (!arguments.empty() && useDefaultImplementationForNulls())
    {
        NullPresense null_presense = getNullPresense(arguments);

        if (null_presense.has_null_constant)
        {
            out_return_type = std::make_shared<DataTypeNull>();
            return;
        }
        if (null_presense.has_nullable)
        {
            getReturnTypeAndPrerequisitesImpl(toNestedColumns(arguments), out_return_type, out_prerequisites);
            out_return_type = std::make_shared<DataTypeNullable>(out_return_type);
            return;
        }
    }

    getReturnTypeAndPrerequisitesImpl(arguments, out_return_type, out_prerequisites);
}


void IFunction::getLambdaArgumentTypes(DataTypes & arguments) const
{
    checkNumberOfArguments(arguments.size());
    getLambdaArgumentTypesImpl(arguments);
}


void IFunction::execute(Block & block, const ColumnNumbers & args, size_t result)
{
    if (defaultImplementationForConstantArguments(*this, block, args, result))
        return;

    if (defaultImplementationForNulls(*this, block, args, result))
        return;

    executeImpl(block, args, result);
}


void IFunction::execute(Block & block, const ColumnNumbers & args, const ColumnNumbers & prerequisites, size_t result)
{
    if (!prerequisites.empty())
    {
        executeImpl(block, args, prerequisites, result);
        return;
    }

    execute(block, args, result);
}

}

