#pragma once

#include <DB/Functions/IFunction.h>
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
			ColumnPtr holder = std::make_shared<ColumnUInt8>();
			ColumnUInt8 & content = static_cast<ColumnUInt8 &>(*(holder.get()));
			NullValuesByteMap & null_map = content.getData();

			null_map.resize_fill(elem.column.get()->size());
			block.getByPosition(result).column = holder;
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
		ColumnWithTypeAndName & elem = block.getByPosition(arguments[0]);
		if (elem.column->isNullable())
		{
			/// Create a complemented null map.
			const ColumnNullable & src_nullable_col = static_cast<const ColumnNullable &>(*elem.column);
			const ColumnUInt8 & src_content = static_cast<const ColumnUInt8 &>(*(src_nullable_col.getNullValuesByteMap().get()));

			ColumnPtr holder = src_content.clone();
			ColumnUInt8 & dest_content = static_cast<ColumnUInt8 &>(*(holder.get()));
			NullValuesByteMap & dest_null_map = dest_content.getData();

			for (auto & byte : dest_null_map)
				byte = ((byte == 0) ? 1 : 0);

			block.getByPosition(result).column = holder;
		}
		else
		{
			/// Since no element is nullable, create a one-filled null map.
			ColumnPtr holder = std::make_shared<ColumnUInt8>();
			ColumnUInt8 & content = static_cast<ColumnUInt8 &>(*(holder.get()));
			NullValuesByteMap & null_map = content.getData();

			null_map.resize_fill(elem.column.get()->size(), 1);
			block.getByPosition(result).column = holder;
		}
	}
};

}
