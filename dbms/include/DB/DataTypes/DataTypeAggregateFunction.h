#pragma once

#include <DB/AggregateFunctions/IAggregateFunction.h>

#include <DB/DataTypes/IDataType.h>


namespace DB
{

using Poco::SharedPtr;

/** Тип - состояние агрегатной функции.
  * Параметры типа - это агрегатная функция и типы её аргументов.
  */
class DataTypeAggregateFunction : public IDataType
{
private:
	AggregateFunctionPtr function;
	DataTypes argument_types;
	
public:
	DataTypeAggregateFunction(const AggregateFunctionPtr & function_, const DataTypes & argument_types_)
		: function(function_), argument_types(argument_types_)
	{
		function->setArguments(argument_types);
	}
	
	std::string getName() const
	{
		std::stringstream stream;
		stream << "AggregateFunction(" << function->getName();

		for (DataTypes::const_iterator it = argument_types.begin(); it != argument_types.end(); ++it)
			stream << ", " << (*it)->getName();
		
		stream << ")";
		return stream.str();
	}
	
	DataTypePtr clone() const { return new DataTypeAggregateFunction(function, argument_types); }

	void serializeBinary(const Field & field, WriteBuffer & ostr) const;
	void deserializeBinary(Field & field, ReadBuffer & istr) const;
	void serializeBinary(const IColumn & column, WriteBuffer & ostr, size_t offset = 0, size_t limit = 0) const;
	void deserializeBinary(IColumn & column, ReadBuffer & istr, size_t limit) const;
	void serializeText(const Field & field, WriteBuffer & ostr) const;
	void deserializeText(Field & field, ReadBuffer & istr) const;
	void serializeTextEscaped(const Field & field, WriteBuffer & ostr) const;
	void deserializeTextEscaped(Field & field, ReadBuffer & istr) const;
	void serializeTextQuoted(const Field & field, WriteBuffer & ostr) const;
	void deserializeTextQuoted(Field & field, ReadBuffer & istr) const;

	ColumnPtr createColumn() const;
	ColumnPtr createConstColumn(size_t size, const Field & field) const;

	Field getDefault() const
	{
		throw Exception("There is no default value for AggregateFunction data type", ErrorCodes::THERE_IS_NO_DEFAULT_VALUE);
	}
};


}

