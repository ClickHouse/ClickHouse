#pragma once

#include <DB/Functions/IFunction.h>
#include <DB/DataTypes/IDataType.h>
#include <DB/Core/ColumnNumbers.h>

namespace DB
{

class Block;
class Context;

class FunctionIsNull : public IFunction
{
public:
	static constexpr auto name = "isNull";
	static FunctionPtr create(const Context & context);

	std::string getName() const override;
	bool hasSpecialSupportForNulls() const override;
	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;
	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
};

class FunctionIsNotNull : public IFunction
{
public:
	static constexpr auto name = "isNotNull";
	static FunctionPtr create(const Context & context);

	std::string getName() const override;
	bool hasSpecialSupportForNulls() const override;
	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;
	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
};

class FunctionCoalesce : public IFunction
{
public:
	static constexpr auto name = "coalesce";
	static FunctionPtr create(const Context & context);

	std::string getName() const override;
	bool hasSpecialSupportForNulls() const override;
	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;
	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
};

}
