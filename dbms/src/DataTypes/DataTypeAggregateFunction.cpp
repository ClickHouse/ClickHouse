#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>

#include <DB/Columns/ColumnConst.h>
#include <DB/Columns/ColumnAggregateFunction.h>

#include <DB/DataTypes/DataTypeAggregateFunction.h>


namespace DB
{

using Poco::SharedPtr;


void DataTypeAggregateFunction::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
	const AggregateFunctionPtr & value = boost::get<const AggregateFunctionPtr &>(field);
	value->serialize(ostr);
}

void DataTypeAggregateFunction::deserializeBinary(Field & field, ReadBuffer & istr) const
{
	AggregateFunctionPtr value = function->cloneEmpty();
	value->deserializeMerge(istr);
	field = value;
}

void DataTypeAggregateFunction::serializeBinary(const IColumn & column, WriteBuffer & ostr, WriteCallback callback) const
{
	const ColumnAggregateFunction & real_column = dynamic_cast<const ColumnAggregateFunction &>(column);
	const ColumnAggregateFunction::Container_t & vec = real_column.getData();

	size_t next_callback_point = callback ? callback() : 0;
	size_t i = 0;
	for (ColumnAggregateFunction::Container_t::const_iterator it = vec.begin(); it != vec.end(); ++it, ++i)
	{
		if (next_callback_point && i == next_callback_point)
			next_callback_point = callback();
		
		(*it)->serialize(ostr);
	}
}

void DataTypeAggregateFunction::deserializeBinary(IColumn & column, ReadBuffer & istr, size_t limit) const
{
	ColumnAggregateFunction & real_column = dynamic_cast<ColumnAggregateFunction &>(column);
	ColumnAggregateFunction::Container_t & vec = real_column.getData();

	vec.reserve(limit);

	for (size_t i = 0; i < limit; ++i)
	{
		if (istr.eof())
			break;

		vec.push_back(function->cloneEmpty());
		vec.back()->deserializeMerge(istr);
	}
}

void DataTypeAggregateFunction::serializeText(const Field & field, WriteBuffer & ostr) const
{
	throw Exception("Cannot write aggregate function as text.", ErrorCodes::CANNOT_WRITE_AGGREGATE_FUNCTION_AS_TEXT);
}

void DataTypeAggregateFunction::deserializeText(Field & field, ReadBuffer & istr) const
{
	throw Exception("Cannot read aggregate function from text.", ErrorCodes::CANNOT_READ_AGGREGATE_FUNCTION_FROM_TEXT);
}

void DataTypeAggregateFunction::serializeTextEscaped(const Field & field, WriteBuffer & ostr) const
{
	throw Exception("Cannot write aggregate function as text.", ErrorCodes::CANNOT_WRITE_AGGREGATE_FUNCTION_AS_TEXT);
}

void DataTypeAggregateFunction::deserializeTextEscaped(Field & field, ReadBuffer & istr) const
{
	throw Exception("Cannot read aggregate function from text.", ErrorCodes::CANNOT_READ_AGGREGATE_FUNCTION_FROM_TEXT);
}

void DataTypeAggregateFunction::serializeTextQuoted(const Field & field, WriteBuffer & ostr) const
{
	throw Exception("Cannot write aggregate function as text.", ErrorCodes::CANNOT_WRITE_AGGREGATE_FUNCTION_AS_TEXT);
}

void DataTypeAggregateFunction::deserializeTextQuoted(Field & field, ReadBuffer & istr) const
{
	throw Exception("Cannot read aggregate function from text.", ErrorCodes::CANNOT_READ_AGGREGATE_FUNCTION_FROM_TEXT);
}

ColumnPtr DataTypeAggregateFunction::createColumn() const
{
	return new ColumnAggregateFunction;
}

ColumnPtr DataTypeAggregateFunction::createConstColumn(size_t size, const Field & field) const
{
	return new ColumnConst<AggregateFunctionPtr>(size, boost::get<AggregateFunctionPtr>(field));
}


}

