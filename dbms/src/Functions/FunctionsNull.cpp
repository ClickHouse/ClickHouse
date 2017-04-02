#include <Functions/FunctionsNull.h>
#include <Functions/FunctionsLogical.h>
#include <Functions/FunctionsComparison.h>
#include <Functions/FunctionsConditional.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNull.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>

namespace DB
{

void registerFunctionsNull(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIsNull>();
    factory.registerFunction<FunctionIsNotNull>();
    factory.registerFunction<FunctionCoalesce>();
    factory.registerFunction<FunctionIfNull>();
    factory.registerFunction<FunctionNullIf>();
    factory.registerFunction<FunctionAssumeNotNull>();
    factory.registerFunction<FunctionToNullable>();
}

/// Implementation of isNull.

FunctionPtr FunctionIsNull::create(const Context & context)
{
    return std::make_shared<FunctionIsNull>();
}

std::string FunctionIsNull::getName() const
{
    return name;
}

bool FunctionIsNull::hasSpecialSupportForNulls() const
{
    return true;
}

DataTypePtr FunctionIsNull::getReturnTypeImpl(const DataTypes & arguments) const
{
    return std::make_shared<DataTypeUInt8>();
}

void FunctionIsNull::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
    ColumnWithTypeAndName & elem = block.safeGetByPosition(arguments[0]);
    if (elem.column->isNull())
    {
        /// Trivial case.
        block.safeGetByPosition(result).column = std::make_shared<ColumnConstUInt8>(elem.column->size(), 1);
    }
    else if (elem.column->isNullable())
    {
        /// Merely return the embedded null map.
        ColumnNullable & nullable_col = static_cast<ColumnNullable &>(*elem.column);
        block.safeGetByPosition(result).column = nullable_col.getNullMapColumn();
    }
    else
    {
        /// Since no element is nullable, return a zero-constant column representing
        /// a zero-filled null map.
        block.safeGetByPosition(result).column = std::make_shared<ColumnConstUInt8>(elem.column->size(), 0);
    }
}

/// Implementation of isNotNull.

FunctionPtr FunctionIsNotNull::create(const Context & context)
{
    return std::make_shared<FunctionIsNotNull>();
}

std::string FunctionIsNotNull::getName() const
{
    return name;
}

bool FunctionIsNotNull::hasSpecialSupportForNulls() const
{
    return true;
}

DataTypePtr FunctionIsNotNull::getReturnTypeImpl(const DataTypes & arguments) const
{
    return std::make_shared<DataTypeUInt8>();
}

void FunctionIsNotNull::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
    Block temp_block
    {
        block.safeGetByPosition(arguments[0]),
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

    FunctionIsNull{}.executeImpl(temp_block, {0}, 1);
    FunctionNot{}.executeImpl(temp_block, {1}, 2);

    block.safeGetByPosition(result).column = std::move(temp_block.safeGetByPosition(2).column);
}

/// Implementation of coalesce.

static const DataTypePtr getNestedDataType(const DataTypePtr & type)
{
    if (type->isNullable())
        return static_cast<const DataTypeNullable &>(*type).getNestedType();

    return type;
}

FunctionPtr FunctionCoalesce::create(const Context & context)
{
    return std::make_shared<FunctionCoalesce>();
}

std::string FunctionCoalesce::getName() const
{
    return name;
}

bool FunctionCoalesce::hasSpecialSupportForNulls() const
{
    return true;
}

DataTypePtr FunctionCoalesce::getReturnTypeImpl(const DataTypes & arguments) const
{
    /// Skip all NULL arguments. If any argument is non-Nullable, skip all next arguments.
    DataTypes filtered_args;
    filtered_args.reserve(arguments.size());
    for (const auto & arg : arguments)
    {
        if (arg->isNull())
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
            new_args.push_back(getNestedDataType(filtered_args[i]));
        }
    }

    if (new_args.empty())
        return std::make_shared<DataTypeNull>();
    if (new_args.size() == 1)
        return new_args.front();

    auto res = FunctionMultiIf{}.getReturnTypeImpl(new_args);

    /// if last argument is not nullable, result should be also not nullable
    if (!new_args.back()->isNullable() && res->isNullable())
        res = getNestedDataType(res);

    return res;
}

void FunctionCoalesce::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
    /// coalesce(arg0, arg1, ..., argN) is essentially
    /// multiIf(isNotNull(arg0), assumeNotNull(arg0), isNotNull(arg1), assumeNotNull(arg1), ..., argN)
    /// with constant NULL arguments removed.

    ColumnNumbers filtered_args;
    filtered_args.reserve(arguments.size());
    for (const auto & arg : arguments)
    {
        const auto & column = block.getByPosition(arg).column;

        if (column->isNull())
            continue;

        filtered_args.push_back(arg);

        if (!column->isNullable())
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
            is_not_null.executeImpl(temp_block, {filtered_args[i]}, res_pos);
            temp_block.insert({nullptr, getNestedDataType(block.getByPosition(filtered_args[i]).type), ""});
            assume_not_null.executeImpl(temp_block, {filtered_args[i]}, res_pos + 1);

            multi_if_args.push_back(res_pos);
            multi_if_args.push_back(res_pos + 1);
        }
    }

    /// If all arguments appeared to be NULL.
    if (multi_if_args.empty())
    {
        block.getByPosition(result).column = std::make_shared<ColumnNull>(block.rows(), Null());
        return;
    }

    if (multi_if_args.size() == 1)
    {
        block.getByPosition(result).column = block.getByPosition(multi_if_args.front()).column;
        return;
    }

    FunctionMultiIf{}.executeImpl(temp_block, multi_if_args, result);

    auto res = std::move(temp_block.safeGetByPosition(result).column);

    /// if last argument is not nullable, result should be also not nullable
    if (!block.getByPosition(multi_if_args.back()).column->isNullable() && res->isNullable())
        res = static_cast<ColumnNullable &>(*res).getNestedColumn();

    block.safeGetByPosition(result).column = res;
}

/// Implementation of ifNull.

FunctionPtr FunctionIfNull::create(const Context & context)
{
    return std::make_shared<FunctionIfNull>();
}

std::string FunctionIfNull::getName() const
{
    return name;
}

bool FunctionIfNull::hasSpecialSupportForNulls() const
{
    return true;
}

DataTypePtr FunctionIfNull::getReturnTypeImpl(const DataTypes & arguments) const
{
    if (arguments[0]->isNull())
        return arguments[1];

    if (!arguments[0]->isNullable())
        return arguments[0];

    return FunctionIf{}.getReturnTypeImpl({std::make_shared<DataTypeUInt8>(), getNestedDataType(arguments[0]), arguments[1]});
}

void FunctionIfNull::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
    /// Always null.
    if (block.getByPosition(arguments[0]).column->isNull())
    {
        block.getByPosition(result).column = block.getByPosition(arguments[1]).column;
        return;
    }

    /// Could not contain nulls, so nullIf makes no sense.
    if (!block.getByPosition(arguments[0]).column->isNullable())
    {
        block.getByPosition(result).column = block.getByPosition(arguments[0]).column;
        return;
    }

    /// ifNull(col1, col2) == if(isNotNull(col1), assumeNotNull(col1), col2)

    Block temp_block = block;

    size_t is_not_null_pos = temp_block.columns();
    temp_block.insert({nullptr, std::make_shared<DataTypeUInt8>(), ""});
    size_t assume_not_null_pos = temp_block.columns();
    temp_block.insert({nullptr, getNestedDataType(block.getByPosition(arguments[0]).type), ""});

    FunctionIsNotNull{}.executeImpl(temp_block, {arguments[0]}, is_not_null_pos);
    FunctionAssumeNotNull{}.executeImpl(temp_block, {arguments[0]}, assume_not_null_pos);
    FunctionIf{}.executeImpl(temp_block, {is_not_null_pos, assume_not_null_pos, arguments[1]}, result);

    block.safeGetByPosition(result).column = std::move(temp_block.safeGetByPosition(result).column);
}

/// Implementation of nullIf.

FunctionPtr FunctionNullIf::create(const Context & context)
{
    return std::make_shared<FunctionNullIf>();
}

std::string FunctionNullIf::getName() const
{
    return name;
}

bool FunctionNullIf::hasSpecialSupportForNulls() const
{
    return true;
}

DataTypePtr FunctionNullIf::getReturnTypeImpl(const DataTypes & arguments) const
{
    return FunctionIf{}.getReturnTypeImpl({std::make_shared<DataTypeUInt8>(), std::make_shared<DataTypeNull>(), arguments[0]});
}

void FunctionNullIf::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
    /// nullIf(col1, col2) == multiIf(col1 == col2, NULL, col1)

    Block temp_block = block;

    size_t res_pos = temp_block.columns();
    temp_block.insert({nullptr, std::make_shared<DataTypeUInt8>(), ""});

    FunctionEquals{}.execute(temp_block, {arguments[0], arguments[1]}, res_pos);

    /// Argument corresponding to the NULL value.
    size_t null_pos = temp_block.columns();

    /// Append a NULL column.
    ColumnWithTypeAndName null_elem;
    null_elem.column = std::make_shared<ColumnNull>(temp_block.rows(), Null());
    null_elem.type = std::make_shared<DataTypeNull>();
    null_elem.name = "NULL";

    temp_block.insert(null_elem);

    FunctionIf{}.executeImpl(temp_block, {res_pos, null_pos, arguments[0]}, result);

    block.safeGetByPosition(result).column = std::move(temp_block.safeGetByPosition(result).column);
}

/// Implementation of assumeNotNull.

FunctionPtr FunctionAssumeNotNull::create(const Context & context)
{
    return std::make_shared<FunctionAssumeNotNull>();
}

std::string FunctionAssumeNotNull::getName() const
{
    return name;
}

bool FunctionAssumeNotNull::hasSpecialSupportForNulls() const
{
    return true;
}

DataTypePtr FunctionAssumeNotNull::getReturnTypeImpl(const DataTypes & arguments) const
{
    if (arguments[0]->isNull())
        throw Exception{"NULL is an invalid value for function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
    return getNestedDataType(arguments[0]);
}

void FunctionAssumeNotNull::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
    const ColumnPtr & col = block.safeGetByPosition(arguments[0]).column;
    ColumnPtr & res_col = block.safeGetByPosition(result).column;

    if (col->isNull())
        throw Exception{"NULL is an invalid value for function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
    else if (col->isNullable())
    {
        const ColumnNullable & nullable_col = static_cast<const ColumnNullable &>(*col);
        res_col = nullable_col.getNestedColumn();
    }
    else
        res_col = col;
}

/// Implementation of toNullable.

FunctionPtr FunctionToNullable::create(const Context & context)
{
    return std::make_shared<FunctionToNullable>();
}

std::string FunctionToNullable::getName() const
{
    return name;
}

bool FunctionToNullable::hasSpecialSupportForNulls() const
{
    return true;
}

DataTypePtr FunctionToNullable::getReturnTypeImpl(const DataTypes & arguments) const
{
    if (arguments[0]->isNull() || arguments[0]->isNullable())
        return arguments[0];
    return std::make_shared<DataTypeNullable>(arguments[0]);
}

void FunctionToNullable::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
    const ColumnPtr & col = block.safeGetByPosition(arguments[0]).column;

    if (col->isNull() || col->isNullable())
        block.getByPosition(result).column = col;
    else
        block.getByPosition(result).column = std::make_shared<ColumnNullable>(col,
            std::make_shared<ColumnConstUInt8>(block.rows(), 0)->convertToFullColumn());
}

}
