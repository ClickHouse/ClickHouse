#include <DB/Functions/FunctionsNull.h>
#include <DB/Functions/FunctionsLogical.h>
#include <DB/Functions/FunctionsComparison.h>
#include <DB/Functions/FunctionsConditional.h>
#include <DB/Functions/FunctionFactory.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeNull.h>
#include <DB/DataTypes/DataTypeNullable.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Columns/ColumnNullable.h>

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
	ColumnWithTypeAndName & elem = block.getByPosition(arguments[0]);
	if (elem.column->isNull())
	{
		/// Trivial case.
		block.getByPosition(result).column = std::make_shared<ColumnConstUInt8>(elem.column->size(), 1);
	}
	else if (elem.column->isNullable())
	{
		/// Merely return the embedded null map.
		ColumnNullable & nullable_col = static_cast<ColumnNullable &>(*elem.column);
		block.getByPosition(result).column = nullable_col.getNullMapColumn();
	}
	else
	{
		/// Since no element is nullable, return a zero-constant column representing
		/// a zero-filled null map.
		block.getByPosition(result).column = std::make_shared<ColumnConstUInt8>(elem.column->size(), 0);
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

	FunctionIsNull{}.executeImpl(temp_block, {0}, 1);
	FunctionNot{}.executeImpl(temp_block, {1}, 2);

	block.getByPosition(result).column = std::move(temp_block.getByPosition(2).column);
}

/// Implementation of coalesce.

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
	DataTypes new_args;
	for (size_t i = 0; i < arguments.size(); ++i)
	{
		new_args.push_back(std::make_shared<DataTypeUInt8>());
		new_args.push_back(arguments[i]);
	}
	new_args.push_back(std::make_shared<DataTypeNull>());

	return FunctionMultiIf{}.getReturnTypeImpl(new_args);
}

void FunctionCoalesce::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
	/// coalesce(arg0, arg1, ..., argN) is essentially
	/// multiIf(isNotNull(arg0), arg0, isNotNull(arg1), arg1, ..., isNotNull(argN), argN, NULL)

	FunctionIsNotNull is_not_null;
	ColumnNumbers multi_if_args;

	Block temp_block = block;

	for (size_t i = 0; i < arguments.size(); ++i)
	{
		size_t res_pos = temp_block.columns();
		temp_block.insert({nullptr, std::make_shared<DataTypeUInt8>(), ""});

		is_not_null.executeImpl(temp_block, {arguments[i]}, res_pos);

		multi_if_args.push_back(res_pos);
		multi_if_args.push_back(arguments[i]);
	}

	/// Argument corresponding to the fallback NULL value.
	multi_if_args.push_back(temp_block.columns());

	/// Append a fallback NULL column.
	ColumnWithTypeAndName elem;
	elem.column = std::make_shared<ColumnNull>(temp_block.rowsInFirstColumn(), Null());
	elem.type = std::make_shared<DataTypeNull>();
	elem.name = "NULL";

	temp_block.insert(elem);

	FunctionMultiIf{}.executeImpl(temp_block, multi_if_args, result);

	block.getByPosition(result).column = std::move(temp_block.getByPosition(result).column);
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
	return FunctionMultiIf{}.getReturnTypeImpl({std::make_shared<DataTypeUInt8>(), arguments[0], arguments[1]});
}

void FunctionIfNull::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
	/// ifNull(col1, col2) == multiIf(isNotNull(col1), col1, col2)

	Block temp_block = block;

	size_t res_pos = temp_block.columns();
	temp_block.insert({nullptr, std::make_shared<DataTypeUInt8>(), ""});

	FunctionIsNotNull{}.executeImpl(temp_block, {arguments[0]}, res_pos);
	FunctionMultiIf{}.executeImpl(temp_block, {res_pos, arguments[0], arguments[1]}, result);

	block.getByPosition(result).column = std::move(temp_block.getByPosition(result).column);
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
	return FunctionMultiIf{}.getReturnTypeImpl({std::make_shared<DataTypeUInt8>(), std::make_shared<DataTypeNull>(), arguments[0]});
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
	null_elem.column = std::make_shared<ColumnNull>(temp_block.rowsInFirstColumn(), Null());
	null_elem.type = std::make_shared<DataTypeNull>();
	null_elem.name = "NULL";

	temp_block.insert(null_elem);

	FunctionMultiIf{}.executeImpl(temp_block, {res_pos, null_pos, arguments[0]}, result);

	block.getByPosition(result).column = std::move(temp_block.getByPosition(result).column);
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
	else if (arguments[0]->isNullable())
	{
		const DataTypeNullable & nullable_type = static_cast<const DataTypeNullable &>(*arguments[0]);
		return nullable_type.getNestedType();
	}
	else
		return arguments[0];
}

void FunctionAssumeNotNull::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
	const ColumnPtr & col = block.getByPosition(arguments[0]).column;
	ColumnPtr & res_col = block.getByPosition(result).column;

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

}
