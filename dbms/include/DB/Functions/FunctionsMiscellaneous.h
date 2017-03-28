#pragma once

#include <DB/Columns/ColumnAggregateFunction.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnTuple.h>
#include <DB/Functions/IFunction.h>
#include <DB/Functions/NumberTraits.h>
#include <DB/Interpreters/ExpressionActions.h>


namespace DB
{
class FunctionTuple : public IFunction
{
public:
	static constexpr auto name = "tuple";
	static FunctionPtr create(const Context & context);

	String getName() const override
	{
		return name;
	}

	bool isVariadic() const override
	{
		return true;
	}

	size_t getNumberOfArguments() const override
	{
		return 0;
	}

	bool isInjective(const Block &) override
	{
		return true;
	}

	bool hasSpecialSupportForNulls() const override
	{
		return true;
	}

	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
};


/** Создаёт массив, размножая столбец (первый аргумент) по количеству элементов в массиве (втором аргументе).
  * Используется только в качестве prerequisites для функций высшего порядка.
  */
class FunctionReplicate : public IFunction
{
public:
	static constexpr auto name = "replicate";
	static FunctionPtr create(const Context & context);

	String getName() const override
	{
		return name;
	}

	size_t getNumberOfArguments() const override
	{
		return 2;
	}

	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
};
}
