#include <Functions/IFunction.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeNull.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{

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

/// In a set of objects (columns of a block / set of columns / set of data types),
/// are there any "special" (i.e. nullable or null) objects?
enum class Category
{
    /// No nullable objects. No null objects.
    IS_ORDINARY = 0,
    /// At least one nullable object. No null objects.
    IS_NULLABLE,
    /// At least one null object.
    IS_NULL
};

/// Check if a block contains at least one special column, in the sense
/// defined above, among the specified columns.
Category blockHasSpecialColumns(const Block & block, const ColumnNumbers & args)
{
    bool found_nullable = false;
    bool found_null = false;

    for (const auto & arg : args)
    {
        const auto & elem = block.getByPosition(arg);

        if (!found_null && elem.column->isNull())
        {
            found_null = true;
            break;
        }
        else if (!found_nullable && elem.column->isNullable())
            found_nullable = true;
    }

    if (found_null)
        return Category::IS_NULL;
    else if (found_nullable)
        return Category::IS_NULLABLE;
    else
        return Category::IS_ORDINARY;
}

/// Check if at least one column is special in the sense defined above.
Category hasSpecialColumns(const ColumnsWithTypeAndName & args)
{
    bool found_nullable = false;
    bool found_null = false;

    for (const auto & arg : args)
    {
        if (!found_null && arg.type->isNull())
        {
            found_null = true;
            break;
        }
        else if (!found_nullable && arg.type->isNullable())
            found_nullable = true;
    }

    if (found_null)
        return Category::IS_NULL;
    else if (found_nullable)
        return Category::IS_NULLABLE;
    else
        return Category::IS_ORDINARY;
}

/// Check if at least one data type is special in the sense defined above.
Category hasSpecialDataTypes(const DataTypes & args)
{
    bool found_nullable = false;
    bool found_null = false;

    for (const auto & arg : args)
    {
        if (!found_null && arg->isNull())
        {
            found_null = true;
            break;
        }
        else if (!found_nullable && arg->isNullable())
            found_nullable = true;
    }

    if (found_null)
        return Category::IS_NULL;
    else if (found_nullable)
        return Category::IS_NULLABLE;
    else
        return Category::IS_ORDINARY;
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
            ColumnPtr nested_col = (nullable_col != nullptr) ? nullable_col->getNestedColumn() : nullptr;
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

    auto category = hasSpecialDataTypes(arguments);

    switch (category)
    {
        case Category::IS_ORDINARY:
            break;

        case Category::IS_NULL:
            if (!hasSpecialSupportForNulls())
                return std::make_shared<DataTypeNull>();
            break;

        case Category::IS_NULLABLE:
            if (!hasSpecialSupportForNulls())
                return getReturnTypeImpl(toNestedDataTypes(arguments));
            break;
    }

    return getReturnTypeImpl(arguments);
}


void IFunction::getReturnTypeAndPrerequisites(
    const ColumnsWithTypeAndName & arguments,
    DataTypePtr & out_return_type,
    std::vector<ExpressionAction> & out_prerequisites)
{
    checkNumberOfArguments(arguments.size());

    auto category = hasSpecialColumns(arguments);

    switch (category)
    {
        case Category::IS_ORDINARY:
            break;

        case Category::IS_NULL:
            if (!hasSpecialSupportForNulls())
            {
                out_return_type = std::make_shared<DataTypeNull>();
                return;
            }
            break;

        case Category::IS_NULLABLE:
            if (!hasSpecialSupportForNulls())
            {
                const ColumnsWithTypeAndName new_args = toNestedColumns(arguments);
                getReturnTypeAndPrerequisitesImpl(new_args, out_return_type, out_prerequisites);
                out_return_type = std::make_shared<DataTypeNullable>(out_return_type);
                return;
            }
            break;
    }

    getReturnTypeAndPrerequisitesImpl(arguments, out_return_type, out_prerequisites);
}


void IFunction::getLambdaArgumentTypes(DataTypes & arguments) const
{
    checkNumberOfArguments(arguments.size());

    auto category = hasSpecialDataTypes(arguments);

    switch (category)
    {
        case Category::IS_ORDINARY:
            break;

        case Category::IS_NULL:
            if (!hasSpecialSupportForNulls())
                return;
            break;

        case Category::IS_NULLABLE:
            if (!hasSpecialSupportForNulls())
            {
                DataTypes new_args = toNestedDataTypes(arguments);
                getLambdaArgumentTypesImpl(new_args);
                arguments = std::move(new_args);
                return;
            }
            break;
    }

    getLambdaArgumentTypesImpl(arguments);
}


void IFunction::execute(Block & block, const ColumnNumbers & args, size_t result)
{
    auto strategy = chooseStrategy(block, args);
    Block processed_block = preProcessBlock(strategy, block, args, result);

    if (strategy != RETURN_NULL)
    {
        Block & src = processed_block ? processed_block : block;
        executeImpl(src, args, result);
    }

    postProcessResult(strategy, block, processed_block, args, result);
}


void IFunction::execute(Block & block, const ColumnNumbers & args, const ColumnNumbers & prerequisites, size_t result)
{
    auto strategy = chooseStrategy(block, args);
    Block processed_block = preProcessBlock(strategy, block, args, result);

    if (strategy != RETURN_NULL)
    {
        Block & src = processed_block ? processed_block : block;
        executeImpl(src, args, prerequisites, result);
    }

    postProcessResult(strategy, block, processed_block, args, result);
}


IFunction::Strategy IFunction::chooseStrategy(const Block & block, const ColumnNumbers & args)
{
    auto category = blockHasSpecialColumns(block, args);

    switch (category)
    {
        case Category::IS_ORDINARY:
            break;

        case Category::IS_NULL:
            if (!hasSpecialSupportForNulls())
                return RETURN_NULL;
            break;

        case Category::IS_NULLABLE:
            if (!hasSpecialSupportForNulls())
                return PROCESS_NULLABLE_COLUMNS;
            break;
    }

    return DIRECTLY_EXECUTE;
}


Block IFunction::preProcessBlock(Strategy strategy, const Block & block, const ColumnNumbers & args, size_t result)
{
    if (strategy == DIRECTLY_EXECUTE)
        return {};
    else if (strategy == RETURN_NULL)
        return {};
    else if (strategy == PROCESS_NULLABLE_COLUMNS)
    {
        /// Run the function on a block whose nullable columns have been replaced
        /// with their respective nested columns.
        return createBlockWithNestedColumns(block, args, result);
    }
    else
        throw Exception{"IFunction: logical error, unknown execution strategy.", ErrorCodes::LOGICAL_ERROR};
}


void IFunction::postProcessResult(Strategy strategy, Block & block, const Block & processed_block,
    const ColumnNumbers & args, size_t result)
{
    if (strategy == DIRECTLY_EXECUTE)
    {
    }
    else if (strategy == RETURN_NULL)
    {
        /// We have found at least one NULL argument. Therefore we return NULL.
        ColumnWithTypeAndName & dest_col = block.safeGetByPosition(result);
        dest_col.column =  std::make_shared<ColumnNull>(block.rows(), Null());
    }
    else if (strategy == PROCESS_NULLABLE_COLUMNS)
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
    else
        throw Exception{"IFunction: logical error, unknown execution strategy.", ErrorCodes::LOGICAL_ERROR};
}


Block IFunction::createBlockWithNestedColumns(const Block & block, ColumnNumbers args)
{
    std::sort(args.begin(), args.end());

    Block res;

    size_t j = 0;
    for (size_t i = 0; i < block.columns(); ++i)
    {
        const auto & col = block.getByPosition(i);
        bool is_inserted = false;

        if ((j < args.size()) && (i == args[j]))
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

        if (!is_inserted)
            res.insert(i, col);
    }

    return res;
}


Block IFunction::createBlockWithNestedColumns(const Block & block, ColumnNumbers args, size_t result)
{
    std::sort(args.begin(), args.end());

    Block res;

    size_t j = 0;
    for (size_t i = 0; i < block.columns(); ++i)
    {
        const auto & col = block.getByPosition(i);
        bool is_inserted = false;

        if ((j < args.size()) && (i == args[j]))
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

}

