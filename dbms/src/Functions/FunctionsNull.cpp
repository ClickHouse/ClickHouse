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
	if (arguments.size() != 1)
		throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
			+ toString(arguments.size()) + ", should be 1.",
			ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

	return std::make_shared<DataTypeUInt8>();
}

void FunctionIsNull::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
	ColumnWithTypeAndName & elem = block.getByPosition(arguments[0]);
	if (elem.column->isNull())
	{
		/// Trivial case.
		block.getByPosition(result).column = std::make_shared<ColumnConstUInt8>(elem.column.get()->size(), 1);
	}
	else if (elem.column->isNullable())
	{
		/// Merely return the embedded null map.
		ColumnNullable & nullable_col = static_cast<ColumnNullable &>(*elem.column);
		block.getByPosition(result).column = nullable_col.getNullValuesByteMap();
	}
	else
	{
		/// Since no element is nullable, create a zero-filled null map.
		block.getByPosition(result).column = std::make_shared<ColumnConstUInt8>(elem.column.get()->size(), 0);
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
	if (arguments.size() != 1)
		throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
			+ toString(arguments.size()) + ", should be 1.",
			ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

	return std::make_shared<DataTypeUInt8>();
}

void FunctionIsNotNull::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
	ColumnWithTypeAndName temp_res;
	temp_res.type = std::make_shared<DataTypeUInt8>();
	temp_res.name = "isNull(" + block.getByPosition(arguments[0]).name + ")";

	size_t temp_res_num = block.columns();
	block.insert(temp_res);

	FunctionIsNull{}.executeImpl(block, arguments, temp_res_num);
	FunctionNot{}.executeImpl(block, {temp_res_num}, result);

	block.erase(temp_res_num);
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

	for (size_t i = 0; i < arguments.size(); ++i)
	{
		ColumnWithTypeAndName elem;
		elem.type = std::make_shared<DataTypeUInt8>();
		elem.name = "isNotNull(" + block.getByPosition(arguments[i]).name + ")";

		size_t res_pos = block.columns();
		block.insert(elem);

		is_not_null.executeImpl(block, { arguments[i] }, res_pos);

		multi_if_args.push_back(res_pos);
		multi_if_args.push_back(arguments[i]);
	}

	/// Argument corresponding to the fallback NULL value.
	multi_if_args.push_back(block.columns());

	/// Append a fallback NULL column.
	ColumnWithTypeAndName elem;
	elem.column = std::make_shared<ColumnNull>(block.rowsInFirstColumn(), Null());
	elem.type = std::make_shared<DataTypeNull>();
	elem.name = "NULL";

	block.insert(elem);

	FunctionMultiIf{}.executeImpl(block, multi_if_args, result);
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
	if (arguments.size() != 2)
		throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
			+ toString(arguments.size()) + ", should be 2.",
			ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

	return FunctionMultiIf{}.getReturnTypeImpl({std::make_shared<DataTypeUInt8>(), arguments[0], arguments[1]});
}

void FunctionIfNull::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
	/// ifNull(col1, col2) == multiIf(isNotNull(col1), col1, col2)

	ColumnWithTypeAndName elem;
	elem.type = std::make_shared<DataTypeUInt8>();
	elem.name = "isNotNull(" + block.getByPosition(arguments[0]).name + ")";

	size_t res_pos = block.columns();
	block.insert(elem);

	FunctionIsNotNull{}.executeImpl(block, {arguments[0]}, res_pos);
	FunctionMultiIf{}.executeImpl(block, {res_pos, arguments[0], arguments[1]}, result);
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
	if (arguments.size() != 2)
		throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
			+ toString(arguments.size()) + ", should be 2.",
			ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

	return FunctionMultiIf{}.getReturnTypeImpl({std::make_shared<DataTypeUInt8>(), std::make_shared<DataTypeNull>(), arguments[0]});
}

void FunctionNullIf::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
	/// nullIf(col1, col2) == multiIf(col1 == col2, NULL, col1)

	ColumnWithTypeAndName elem;
	elem.type = std::make_shared<DataTypeUInt8>();
	elem.name = "equals(" + block.getByPosition(arguments[0]).name + "," + block.getByPosition(arguments[1]).name + ")";

	size_t res_pos = block.columns();
	block.insert(elem);

	FunctionEquals{}.execute(block, {arguments[0], arguments[1]}, res_pos);

	/// Argument corresponding to the NULL value.
	size_t null_pos = block.columns();

	/// Append a NULL column.
	ColumnWithTypeAndName null_elem;
	null_elem.column = std::make_shared<ColumnNull>(block.rowsInFirstColumn(), Null());
	null_elem.type = std::make_shared<DataTypeNull>();
	null_elem.name = "NULL";

	block.insert(null_elem);

	FunctionMultiIf{}.executeImpl(block, {res_pos, null_pos, arguments[0]}, result);
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
	if (arguments.size() != 1)
		throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
			+ toString(arguments.size()) + ", should be 1.",
			ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

	if (arguments[0].get()->isNull())
		return std::make_shared<DataTypeUInt8>();
	else if (arguments[0].get()->isNullable())
	{
		const DataTypeNullable & nullable_type = static_cast<const DataTypeNullable &>(*(arguments[0].get()));
		return nullable_type.getNestedType();
	}
	else
		return arguments[0];
}

void FunctionAssumeNotNull::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
	const ColumnPtr & col = block.getByPosition(arguments[0]).column;
	ColumnPtr & res_col = block.getByPosition(result).column;

	if (col.get()->isNull())
		res_col = std::make_shared<ColumnConstUInt8>(block.rowsInFirstColumn(), 0);
	else if (col.get()->isNullable())
	{
		const ColumnNullable & nullable_col = static_cast<const ColumnNullable &>(*(col.get()));
		res_col = nullable_col.getNestedColumn();
	}
	else
		res_col = col;
}

}
