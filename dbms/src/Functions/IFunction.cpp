#include <Functions/IFunction.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeNull.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/ExpressionActions.h>

#include <numeric>


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


struct NullPresence
{
    bool has_nullable = false;
    bool has_null = false;
};

void getNullPresence(const IDataType & type, NullPresence & res)
{
    res.has_null |= type.isNull();
    res.has_nullable |= type.isNullable();
}

/// Check if a block contains at least one special column, in the sense
/// defined above, among the specified columns.
NullPresence getNullPresence(const Block & block, const ColumnNumbers & args)
{
    NullPresence res;
    for (const auto & arg : args)
        getNullPresence(*block.getByPosition(arg).type, res);
    return res;
}

NullPresence getNullPresence(const Block & block)
{
    NullPresence res;
    for (size_t arg = 0, size = block.columns(); arg < size; ++arg)
        getNullPresence(*block.getByPosition(arg).type, res);
    return res;
}

NullPresence getNullPresence(const DataTypes & args)
{
    NullPresence res;
    for (const auto & arg : args)
        getNullPresence(*arg, res);
    return res;
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


/// If required by the specified strategy, process the given block, then
/// return the processed block. Otherwise return an empty block.
Block preProcessBlock(NullPresence null_presence, const Block & block, const ColumnNumbers & args, size_t result)
{
    if (null_presence.has_null)
        return {};
    else if (null_presence.has_nullable)
    {
        /// Run the function on a block whose nullable columns have been replaced
        /// with their respective nested columns.
        return unwrapNullable(block, args, result);
    }
    else
        return {};
}

/// If required by the specified strategy, post-process the result column.
void postProcessResult(NullPresence null_presence, Block & block, const Block & processed_block,
    const ColumnNumbers & args, size_t result)
{
    if (null_presence.has_null)
    {
        /// We have found at least one NULL argument. Therefore we return NULL.
        ColumnWithTypeAndName & dest_col = block.safeGetByPosition(result);
        dest_col.column = std::make_shared<ColumnNull>(block.rows(), Null());
    }
    else if (null_presence.has_nullable)
    {
        const ColumnWithTypeAndName & source_col = processed_block.safeGetByPosition(result);
        ColumnWithTypeAndName & dest_col = block.safeGetByPosition(result);

        /// Initialize the result column.
        ColumnPtr null_map = std::make_shared<ColumnUInt8>(block.rows(), 0);
        dest_col.column = std::make_shared<ColumnNullable>(source_col.column, null_map);

        /// Deduce the null map of the result from the null maps of the
        /// nullable columns.
        createNullMap(block, args, result);
    }
}

}


Block unwrapNullable(const Block & block, const ColumnNumbers & args, size_t result)
{
    ColumnNumbers args_sorted = args;
    std::sort(args_sorted.begin(), args_sorted.end());

    Block res;

    size_t j = 0;
    for (size_t i = 0; i < block.columns(); ++i)
    {
        const auto & col = block.getByPosition(i);
        bool is_inserted = false;

        if ((j < args_sorted.size()) && (i == args_sorted[j]))
        {
            ++j;

            if (col.column->isNullable())
            {
                auto nullable_col = static_cast<const ColumnNullable *>(col.column.get());
                const ColumnPtr & nested_col = nullable_col->getNestedColumn();

                auto nullable_type = static_cast<const DataTypeNullable *>(col.type.get());
                const DataTypePtr & nested_type = nullable_type->getNestedType();

                res.insert(i, {nested_col, nested_type, col.name});

                is_inserted = true;
            }
        }
        else if (i == result)
        {
            if (col.type->isNullable())
            {
                auto nullable_type = static_cast<const DataTypeNullable *>(col.type.get());
                const DataTypePtr & nested_type = nullable_type->getNestedType();

                res.insert(i, {nullptr, nested_type, col.name});
                is_inserted = true;
            }
        }

        if (!is_inserted)
            res.insert(i, col);
    }

    return res;
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

    if (!hasSpecialSupportForNulls())
        return getReturnTypeImpl(arguments);

    NullPresence null_presence = getNullPresence(arguments);

    if (null_presence.has_null)
        return std::make_shared<DataTypeNull>();

    if (null_presence.has_nullable)
        return getReturnTypeImpl(toNestedDataTypes(arguments));

    return getReturnTypeImpl(arguments);
}


DataTypePtr IFunction::getReturnTypeDependingOnConstantArguments(const Block & arguments)
{
    checkNumberOfArguments(arguments.columns());

    if (!hasSpecialSupportForNulls())
        return getReturnTypeDependingOnConstantArgumentsImpl(arguments);

    NullPresence null_presence = getNullPresence(arguments);

    if (null_presence.has_null)
        return std::make_shared<DataTypeNull>();

    if (null_presence.has_nullable)
    {
        ColumnNumbers all_columns(arguments.columns());
        std::iota(all_columns.begin(), all_columns.end(), 0);
        return getReturnTypeDependingOnConstantArgumentsImpl(unwrapNullable(arguments, all_columns, arguments.columns()));
    }

    return getReturnTypeDependingOnConstantArgumentsImpl(arguments);
}


DataTypePtr IFunction::getReturnTypeDependingOnConstantArgumentsImpl(const Block & arguments)
{
    size_t num_columns = arguments.columns();
    DataTypes arg_types(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        arg_types[i] = arguments.getByPosition(i).type;
    return getReturnTypeImpl(arg_types);
}


void IFunction::getLambdaArgumentTypes(DataTypes & arguments) const
{
    checkNumberOfArguments(arguments.size());

    if (!hasSpecialSupportForNulls())
    {
        getLambdaArgumentTypesImpl(arguments);
        return;
    }

    NullPresence null_presence = getNullPresence(arguments);

    if (null_presence.has_null)
        return;

    if (null_presence.has_nullable)
    {
        DataTypes new_args = toNestedDataTypes(arguments);
        getLambdaArgumentTypesImpl(new_args);
        arguments = std::move(new_args);
        return;
    }

    getLambdaArgumentTypesImpl(arguments);
}


void IFunction::execute(Block & block, const ColumnNumbers & args, size_t result)
{
    NullPresence null_presence;
    if (hasSpecialSupportForNulls())
        null_presence = getNullPresence(block, args);

    Block processed_block = preProcessBlock(null_presence, block, args, result);

    if (!null_presence.has_null)
    {
        Block & src = processed_block ? processed_block : block;
        executeImpl(src, args, result);
    }

    postProcessResult(null_presence, block, processed_block, args, result);
}

}

