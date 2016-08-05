#include <DB/Functions/FunctionsNull.h>
#include <DB/Functions/FunctionsLogical.h>
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
	FunctionIsNull is_null_func;

	ColumnWithTypeAndName temp_res;
	temp_res.type = std::make_shared<DataTypeUInt8>();

	block.insert(temp_res);

	size_t temp_res_num = block.columns() - 1;

	is_null_func.executeImpl(block, arguments, temp_res_num);

	FunctionNot not_func;

	ColumnNumbers new_args;
	new_args.push_back(temp_res_num);
	not_func.executeImpl(block, new_args, result);

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

	return FunctionMultiIf{}.getReturnType(new_args);
}

void FunctionCoalesce::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
	/// coalesce(arg0, arg1, ..., argN) is essentially
	/// multiIf(isNotNull(arg0), arg0, isNotNull(arg1), arg1, ..., isNotNull(argN), argN, NULL)

	for (size_t i = 0; i < arguments.size(); ++i)
	{
		ColumnWithTypeAndName elem;
		elem.type = std::make_shared<DataTypeUInt8>();
		elem.name = "isNotNull(" + block.getByPosition(arguments[i]).name + ")";

		size_t res_pos = block.columns();
		block.insert(elem);

		FunctionIsNotNull{}.executeImpl(block, { arguments[i] }, res_pos);
	}

	ColumnNumbers new_args;
	for (size_t i = 0; i < arguments.size(); ++i)
	{
		new_args.push_back(result + i);
		new_args.push_back(arguments[i]);
	}

	/// For the NULL value.
	new_args.push_back(block.columns());

	/// Append a NULL column.
	ColumnWithTypeAndName elem;
	elem.column = std::make_shared<ColumnNull>(block.rowsInFirstColumn(), Null());
	elem.type = std::make_shared<DataTypeNull>();
	elem.name = "NULL";

	block.insert(elem);

	FunctionMultiIf{}.execute(block, new_args, result);
}

}
