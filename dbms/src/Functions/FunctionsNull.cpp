#include <Functions/FunctionsNull.h>
#include <Functions/FunctionsLogical.h>
#include <Functions/FunctionsConditional.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeNothing.h>
#include <Columns/ColumnNullable.h>
#include <cstdlib>
#include <string>
#include <memory>


namespace DB
{

void registerFunctionsNull(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIsNull>();
    factory.registerFunction<FunctionIsNotNull>();
    factory.registerFunction<FunctionCoalesce>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionIfNull>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionNullIf>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionAssumeNotNull>();
    factory.registerFunction<FunctionToNullable>();
}

/// Implementation of isNull.

FunctionPtr FunctionIsNull::create(const Context &)
{
    return std::make_shared<FunctionIsNull>();
}

std::string FunctionIsNull::getName() const
{
    return name;
}

DataTypePtr FunctionIsNull::getReturnTypeImpl(const DataTypes &) const
{
    return std::make_shared<DataTypeUInt8>();
}

void FunctionIsNull::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/)
{
    const ColumnWithTypeAndName & elem = block.getByPosition(arguments[0]);
    if (elem.column->isColumnNullable())
    {
        /// Merely return the embedded null map.
        block.getByPosition(result).column = static_cast<const ColumnNullable &>(*elem.column).getNullMapColumnPtr();
    }
    else
    {
        /// Since no element is nullable, return a zero-constant column representing
        /// a zero-filled null map.
        block.getByPosition(result).column = DataTypeUInt8().createColumnConst(elem.column->size(), UInt64(0));
    }
}

/// Implementation of isNotNull.

FunctionPtr FunctionIsNotNull::create(const Context &)
{
    return std::make_shared<FunctionIsNotNull>();
}

std::string FunctionIsNotNull::getName() const
{
    return name;
}

DataTypePtr FunctionIsNotNull::getReturnTypeImpl(const DataTypes &) const
{
    return std::make_shared<DataTypeUInt8>();
}

void FunctionIsNotNull::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count)
{
    Block temp_block
    {
        block.getByPosition(arguments[0]),
        {
            nullptr,
            std::make_shared<DataTypeUInt8>(),
            ""
        },
        {
            nullptr,
            std::make_shared<DataTypeUInt8>(),
            ""
        }
    };

    FunctionIsNull{}.execute(temp_block, {0}, 1, input_rows_count);
    FunctionNot{}.execute(temp_block, {1}, 2, input_rows_count);

    block.getByPosition(result).column = std::move(temp_block.getByPosition(2).column);
}

/// Implementation of coalesce.

FunctionPtr FunctionCoalesce::create(const Context & context)
{
    return std::make_shared<FunctionCoalesce>(context);
}

std::string FunctionCoalesce::getName() const
{
    return name;
}

DataTypePtr FunctionCoalesce::getReturnTypeImpl(const DataTypes & arguments) const
{
    /// Skip all NULL arguments. If any argument is non-Nullable, skip all next arguments.
    DataTypes filtered_args;
    filtered_args.reserve(arguments.size());
    for (const auto & arg : arguments)
    {
        if (arg->onlyNull())
            continue;

        filtered_args.push_back(arg);

        if (!arg->isNullable())
            break;
    }

    DataTypes new_args;
    for (size_t i = 0; i < filtered_args.size(); ++i)
    {
        bool is_last = i + 1 == filtered_args.size();

        if (is_last)
        {
            new_args.push_back(filtered_args[i]);
        }
        else
        {
            new_args.push_back(std::make_shared<DataTypeUInt8>());
            new_args.push_back(removeNullable(filtered_args[i]));
        }
    }

    if (new_args.empty())
        return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>());
    if (new_args.size() == 1)
        return new_args.front();

    auto res = FunctionMultiIf{context}.getReturnTypeImpl(new_args);

    /// if last argument is not nullable, result should be also not nullable
    if (!new_args.back()->isNullable() && res->isNullable())
        res = removeNullable(res);

    return res;
}

void FunctionCoalesce::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count)
{
    /// coalesce(arg0, arg1, ..., argN) is essentially
    /// multiIf(isNotNull(arg0), assumeNotNull(arg0), isNotNull(arg1), assumeNotNull(arg1), ..., argN)
    /// with constant NULL arguments removed.

    ColumnNumbers filtered_args;
    filtered_args.reserve(arguments.size());
    for (const auto & arg : arguments)
    {
        const auto & type = block.getByPosition(arg).type;

        if (type->onlyNull())
            continue;

        filtered_args.push_back(arg);

        if (!type->isNullable())
            break;
    }

    FunctionIsNotNull is_not_null;
    FunctionAssumeNotNull assume_not_null;
    ColumnNumbers multi_if_args;

    Block temp_block = block;

    for (size_t i = 0; i < filtered_args.size(); ++i)
    {
        size_t res_pos = temp_block.columns();
        bool is_last = i + 1 == filtered_args.size();

        if (is_last)
        {
            multi_if_args.push_back(filtered_args[i]);
        }
        else
        {
            temp_block.insert({nullptr, std::make_shared<DataTypeUInt8>(), ""});
            is_not_null.execute(temp_block, {filtered_args[i]}, res_pos, input_rows_count);
            temp_block.insert({nullptr, removeNullable(block.getByPosition(filtered_args[i]).type), ""});
            assume_not_null.execute(temp_block, {filtered_args[i]}, res_pos + 1, input_rows_count);

            multi_if_args.push_back(res_pos);
            multi_if_args.push_back(res_pos + 1);
        }
    }

    /// If all arguments appeared to be NULL.
    if (multi_if_args.empty())
    {
        block.getByPosition(result).column = block.getByPosition(result).type->createColumnConstWithDefaultValue(input_rows_count);
        return;
    }

    if (multi_if_args.size() == 1)
    {
        block.getByPosition(result).column = block.getByPosition(multi_if_args.front()).column;
        return;
    }

    FunctionMultiIf{context}.execute(temp_block, multi_if_args, result, input_rows_count);

    ColumnPtr res = std::move(temp_block.getByPosition(result).column);

    /// if last argument is not nullable, result should be also not nullable
    if (!block.getByPosition(multi_if_args.back()).column->isColumnNullable() && res->isColumnNullable())
        res = static_cast<const ColumnNullable &>(*res).getNestedColumnPtr();

    block.getByPosition(result).column = std::move(res);
}

/// Implementation of ifNull.

FunctionPtr FunctionIfNull::create(const Context &)
{
    return std::make_shared<FunctionIfNull>();
}

std::string FunctionIfNull::getName() const
{
    return name;
}

DataTypePtr FunctionIfNull::getReturnTypeImpl(const DataTypes & arguments) const
{
    if (arguments[0]->onlyNull())
        return arguments[1];

    if (!arguments[0]->isNullable())
        return arguments[0];

    return FunctionIf{}.getReturnTypeImpl({std::make_shared<DataTypeUInt8>(), removeNullable(arguments[0]), arguments[1]});
}

void FunctionIfNull::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count)
{
    /// Always null.
    if (block.getByPosition(arguments[0]).type->onlyNull())
    {
        block.getByPosition(result).column = block.getByPosition(arguments[1]).column;
        return;
    }

    /// Could not contain nulls, so nullIf makes no sense.
    if (!block.getByPosition(arguments[0]).type->isNullable())
    {
        block.getByPosition(result).column = block.getByPosition(arguments[0]).column;
        return;
    }

    /// ifNull(col1, col2) == if(isNotNull(col1), assumeNotNull(col1), col2)

    Block temp_block = block;

    size_t is_not_null_pos = temp_block.columns();
    temp_block.insert({nullptr, std::make_shared<DataTypeUInt8>(), ""});
    size_t assume_not_null_pos = temp_block.columns();
    temp_block.insert({nullptr, removeNullable(block.getByPosition(arguments[0]).type), ""});

    FunctionIsNotNull{}.execute(temp_block, {arguments[0]}, is_not_null_pos, input_rows_count);
    FunctionAssumeNotNull{}.execute(temp_block, {arguments[0]}, assume_not_null_pos, input_rows_count);

    FunctionIf{}.execute(temp_block, {is_not_null_pos, assume_not_null_pos, arguments[1]}, result, input_rows_count);

    block.getByPosition(result).column = std::move(temp_block.getByPosition(result).column);
}

/// Implementation of nullIf.

FunctionPtr FunctionNullIf::create(const Context & context)
{
    return std::make_shared<FunctionNullIf>(context);
}

FunctionNullIf::FunctionNullIf(const Context & context) : context(context) {}

std::string FunctionNullIf::getName() const
{
    return name;
}

DataTypePtr FunctionNullIf::getReturnTypeImpl(const DataTypes & arguments) const
{
    return makeNullable(arguments[0]);
}

void FunctionNullIf::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count)
{
    /// nullIf(col1, col2) == if(col1 == col2, NULL, col1)

    Block temp_block = block;

    size_t res_pos = temp_block.columns();
    temp_block.insert({nullptr, std::make_shared<DataTypeUInt8>(), ""});

    {
        auto equals_func = FunctionFactory::instance().get("equals", context)->build(
            {block.getByPosition(arguments[0]), block.getByPosition(arguments[1])});
        equals_func->execute(temp_block, {arguments[0], arguments[1]}, res_pos, input_rows_count);
    }

    /// Argument corresponding to the NULL value.
    size_t null_pos = temp_block.columns();

    /// Append a NULL column.
    ColumnWithTypeAndName null_elem;
    null_elem.type = block.getByPosition(result).type;
    null_elem.column = null_elem.type->createColumnConstWithDefaultValue(input_rows_count);
    null_elem.name = "NULL";

    temp_block.insert(null_elem);

    FunctionIf{}.execute(temp_block, {res_pos, null_pos, arguments[0]}, result, input_rows_count);

    block.getByPosition(result).column = std::move(temp_block.getByPosition(result).column);
}

/// Implementation of assumeNotNull.

FunctionPtr FunctionAssumeNotNull::create(const Context &)
{
    return std::make_shared<FunctionAssumeNotNull>();
}

std::string FunctionAssumeNotNull::getName() const
{
    return name;
}

DataTypePtr FunctionAssumeNotNull::getReturnTypeImpl(const DataTypes & arguments) const
{
    return removeNullable(arguments[0]);
}

void FunctionAssumeNotNull::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/)
{
    const ColumnPtr & col = block.getByPosition(arguments[0]).column;
    ColumnPtr & res_col = block.getByPosition(result).column;

    if (col->isColumnNullable())
    {
        const ColumnNullable & nullable_col = static_cast<const ColumnNullable &>(*col);
        res_col = nullable_col.getNestedColumnPtr();
    }
    else
        res_col = col;
}

/// Implementation of toNullable.

FunctionPtr FunctionToNullable::create(const Context &)
{
    return std::make_shared<FunctionToNullable>();
}

std::string FunctionToNullable::getName() const
{
    return name;
}

DataTypePtr FunctionToNullable::getReturnTypeImpl(const DataTypes & arguments) const
{
    return makeNullable(arguments[0]);
}

void FunctionToNullable::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/)
{
    block.getByPosition(result).column = makeNullable(block.getByPosition(arguments[0]).column);
}

}
