#pragma once

#include <DB/AggregateFunctions/IAggregateFunction.h>

#include <DB/DataTypes/IDataType.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int THERE_IS_NO_DEFAULT_VALUE;
	extern const int NOT_IMPLEMENTED;
}


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

	DataTypePtr clone() const override { return std::make_shared<DataTypeAggregateFunction>(function, argument_types, parameters); }

	/// NOTE Эти две функции сериализации одиночных значений несовместимы с функциями ниже.
	void serializeBinary(const Field & field, WriteBuffer & ostr) const override;
	void deserializeBinary(Field & field, ReadBuffer & istr) const override;

	void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
	void deserializeBinary(IColumn & column, ReadBuffer & istr) const override;
	void serializeBinary(const IColumn & column, WriteBuffer & ostr, size_t offset = 0, size_t limit = 0) const override;
	void deserializeBinary(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const override;
	void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
	void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
	void deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const override;
	void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
	void deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const override;
	void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, bool) const override;
	void deserializeTextJSON(IColumn & column, ReadBuffer & istr) const override;
	void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
	void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
	void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const char delimiter) const override;

	ColumnPtr createColumn() const override;
	ColumnPtr createConstColumn(size_t size, const Field & field) const override;

	Field getDefault() const override
	{
		throw Exception("There is no default value for AggregateFunction data type", ErrorCodes::THERE_IS_NO_DEFAULT_VALUE);
	}
};


}

