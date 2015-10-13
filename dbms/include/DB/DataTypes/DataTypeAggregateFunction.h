#pragma once

#include <DB/AggregateFunctions/IAggregateFunction.h>

#include <DB/DataTypes/IDataType.h>


namespace DB
{

using Poco::SharedPtr;

/** Тип - состояние агрегатной функции.
  * Параметры типа - это агрегатная функция, типы её аргументов и её параметры (для параметрических агрегатных функций).
  */
class DataTypeAggregateFunction final : public IDataType
{
private:
	AggregateFunctionPtr function;
	DataTypes argument_types;
	Array parameters;

public:
	DataTypeAggregateFunction(const AggregateFunctionPtr & function_, const DataTypes & argument_types_, const Array & parameters_)
		: function(function_), argument_types(argument_types_), parameters(parameters_)
	{
	}

	std::string getFunctionName() const { return function->getName(); }

	std::string getName() const override;

	DataTypePtr getReturnType() const { return function->getReturnType(); };
	DataTypes getArgumentsDataTypes() const { return argument_types; }

	DataTypePtr clone() const override { return new DataTypeAggregateFunction(function, argument_types, parameters); }

	void serializeBinary(const Field & field, WriteBuffer & ostr) const override;
	void deserializeBinary(Field & field, ReadBuffer & istr) const override;
	void serializeBinary(const IColumn & column, WriteBuffer & ostr, size_t offset = 0, size_t limit = 0) const override;
	void deserializeBinary(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const override;
	void serializeText(const Field & field, WriteBuffer & ostr) const override;
	void deserializeText(Field & field, ReadBuffer & istr) const override;
	void serializeTextEscaped(const Field & field, WriteBuffer & ostr) const override;
	void deserializeTextEscaped(Field & field, ReadBuffer & istr) const override;
	void serializeTextQuoted(const Field & field, WriteBuffer & ostr) const override;
	void deserializeTextQuoted(Field & field, ReadBuffer & istr) const override;
	void serializeTextJSON(const Field & field, WriteBuffer & ostr) const override;

	ColumnPtr createColumn() const override;
	ColumnPtr createConstColumn(size_t size, const Field & field) const override;

	Field getDefault() const override
	{
		throw Exception("There is no default value for AggregateFunction data type", ErrorCodes::THERE_IS_NO_DEFAULT_VALUE);
	}
};


}

