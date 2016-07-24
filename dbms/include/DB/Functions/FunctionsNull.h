#pragma once

#include <DB/Functions/IFunction.h>
#include <DB/Functions/FunctionsLogical.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/Columns/ColumnNullable.h>

namespace DB
{

class FunctionIsNull : public IFunction
{
public:
	static constexpr auto name = "isNull";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionIsNull>(); }

	std::string getName() const override
	{
		return name;
	}

	bool hasSpecialSupportForNulls() const override
	{
		return true;
	}

	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 1.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		return std::make_shared<DataTypeUInt8>();
	}

	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		ColumnWithTypeAndName & elem = block.getByPosition(arguments[0]);
		if (elem.column->isNullable())
		{
			// Merely return the embedded null map.
			ColumnNullable & nullable_col = static_cast<ColumnNullable &>(*elem.column);
			block.getByPosition(result).column = nullable_col.getNullValuesByteMap();
		}
		else
		{
			/// Since no element is nullable, create a zero-filled null map.
			block.getByPosition(result).column = std::make_shared<ColumnConstUInt8>(elem.column.get()->size(), 0);
		}
	}
};

class FunctionIsNotNull : public IFunction
{
public:
	static constexpr auto name = "isNotNull";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionIsNotNull>(); }

	std::string getName() const override
	{
		return name;
	}

	bool hasSpecialSupportForNulls() const override
	{
		return true;
	}

	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 1.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		return std::make_shared<DataTypeUInt8>();
	}

	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
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
	}
};

}
