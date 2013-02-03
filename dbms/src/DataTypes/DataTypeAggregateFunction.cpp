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
	get<AggregateFunctionPlainPtr>(field)->serialize(ostr);
}

void DataTypeAggregateFunction::deserializeBinary(Field & field, ReadBuffer & istr) const
{
	throw Exception("Deserialization of individual aggregate functions is not supported", ErrorCodes::NOT_IMPLEMENTED);
}

void DataTypeAggregateFunction::serializeBinary(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
	const ColumnAggregateFunction & real_column = dynamic_cast<const ColumnAggregateFunction &>(column);
	const ColumnAggregateFunction::Container_t & vec = real_column.getData();

	ColumnAggregateFunction::Container_t::const_iterator it = vec.begin() + offset;
	ColumnAggregateFunction::Container_t::const_iterator end = limit ? it + limit : vec.end();

	if (end > vec.end())
		end = vec.end();

	for (; it != end; ++it)
		(*it)->serialize(ostr);
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
	throw Exception("Const column with aggregate function is not supported", ErrorCodes::NOT_IMPLEMENTED);
}


}

