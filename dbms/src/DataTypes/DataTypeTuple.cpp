#include <DB/Columns/ColumnTuple.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/DataStreams/NativeBlockInputStream.h>
#include <DB/DataStreams/NativeBlockOutputStream.h>
#include <DB/DataTypes/DataTypeTuple.h>

#include <ext/map.hpp>
#include <ext/enumerate.hpp>
#include <ext/range.hpp>


namespace DB
{

std::string DataTypeTuple::getName() const
{
	std::stringstream s;

	s << "Tuple(";
	for (DataTypes::const_iterator it = elems.begin(); it != elems.end(); ++it)
		s << (it == elems.begin() ? "" : ", ") << (*it)->getName();
	s << ")";

	return s.str();
}

void DataTypeTuple::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
	const auto & tuple = get<const Tuple &>(field).t;
	for (const auto & idx_elem : ext::enumerate(elems))
		idx_elem.second->serializeBinary(tuple[idx_elem.first], ostr);
}

void DataTypeTuple::deserializeBinary(Field & field, ReadBuffer & istr) const
{
	const size_t size = elems.size();
	field = Tuple(TupleBackend(size));
	TupleBackend & tuple = get<Tuple &>(field).t;
	for (const auto i : ext::range(0, size))
		elems[i]->deserializeBinary(tuple[i], istr);
}

void DataTypeTuple::serializeText(const Field & field, WriteBuffer & ostr) const
{
	const TupleBackend & tuple = get<const Tuple &>(field).t;
	writeChar('(', ostr);
	for (const auto i : ext::range(0, ext::size(elems)))
	{
		if (i != 0)
			writeChar(',', ostr);
		elems[i]->serializeTextQuoted(tuple[i], ostr);
	}
	writeChar(')', ostr);
}

void DataTypeTuple::deserializeText(Field & field, ReadBuffer & istr) const
{
	const size_t size = elems.size();
	field = Tuple(TupleBackend(size));
	TupleBackend & tuple = get<Tuple &>(field).t;
	assertChar('(', istr);
	for (const auto i : ext::range(0, size))
	{
		skipWhitespaceIfAny(istr);
		if (i != 0)
			assertChar(',', istr);
		elems[i]->deserializeTextQuoted(tuple[i], istr);
	}
	skipWhitespaceIfAny(istr);
	assertChar(')', istr);
}

void DataTypeTuple::serializeTextEscaped(const Field & field, WriteBuffer & ostr) const
{
	serializeText(field, ostr);
}

void DataTypeTuple::deserializeTextEscaped(Field & field, ReadBuffer & istr) const
{
	deserializeText(field, istr);
}

void DataTypeTuple::serializeTextQuoted(const Field & field, WriteBuffer & ostr) const
{
	serializeText(field, ostr);
}

void DataTypeTuple::deserializeTextQuoted(Field & field, ReadBuffer & istr) const
{
	deserializeText(field, istr);
}

void DataTypeTuple::serializeTextJSON(const Field & field, WriteBuffer & ostr) const
{
	const TupleBackend & tuple = get<const Tuple &>(field).t;
	writeChar('[', ostr);
	for (const auto i : ext::range(0, ext::size(elems)))
	{
		if (i != 0)
			writeChar(',', ostr);
		elems[i]->serializeTextJSON(tuple[i], ostr);
	}
	writeChar(']', ostr);
}

void DataTypeTuple::serializeTextXML(const Field & field, WriteBuffer & ostr) const
{
	const TupleBackend & tuple = get<const Tuple &>(field).t;
	writeCString("<tuple>", ostr);
	for (const auto i : ext::range(0, ext::size(elems)))
	{
		writeCString("<elem>", ostr);
		elems[i]->serializeTextXML(tuple[i], ostr);
		writeCString("</elem>", ostr);
	}
	writeCString("</tuple>", ostr);
}

void DataTypeTuple::serializeTextCSV(const Field & field, WriteBuffer & ostr) const
{
	const TupleBackend & tuple = get<const Tuple &>(field).t;
	for (const auto i : ext::range(0, ext::size(elems)))
	{
		if (i != 0)
			writeChar(',', ostr);
		elems[i]->serializeTextCSV(tuple[i], ostr);
	}
}

void DataTypeTuple::deserializeTextCSV(Field & field, ReadBuffer & istr, const char delimiter) const
{
	const size_t size = elems.size();
	field = Tuple(TupleBackend(size));
	TupleBackend & tuple = get<Tuple &>(field).t;
	for (const auto i : ext::range(0, size))
	{
		if (i != 0)
		{
			skipWhitespaceIfAny(istr);
			assertChar(delimiter, istr);
			skipWhitespaceIfAny(istr);
		}
		elems[i]->deserializeTextCSV(tuple[i], istr, delimiter);
	}
}

void DataTypeTuple::serializeBinary(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
	const ColumnTuple & real_column = static_cast<const ColumnTuple &>(column);
	for (size_t i = 0, size = elems.size(); i < size; ++i)
		NativeBlockOutputStream::writeData(*elems[i], real_column.getData().getByPosition(i).column, ostr, offset, limit);
}

void DataTypeTuple::deserializeBinary(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const
{
	ColumnTuple & real_column = static_cast<ColumnTuple &>(column);
	for (size_t i = 0, size = elems.size(); i < size; ++i)
		NativeBlockInputStream::readData(*elems[i], *real_column.getData().getByPosition(i).column, istr, limit);
}

ColumnPtr DataTypeTuple::createColumn() const
{
	Block tuple_block;
	for (size_t i = 0, size = elems.size(); i < size; ++i)
	{
		ColumnWithTypeAndName col;
		col.column = elems[i]->createColumn();
		col.type = elems[i]->clone();
		tuple_block.insert(col);
	}
	return new ColumnTuple(tuple_block);
}

ColumnPtr DataTypeTuple::createConstColumn(size_t size, const Field & field) const
{
	return new ColumnConstTuple(size, get<const Tuple &>(field), new DataTypeTuple(elems));
}

Field DataTypeTuple::getDefault() const
{
	return Tuple(ext::map<TupleBackend>(elems, [] (const DataTypePtr & elem) { return elem->getDefault(); }));
}

}
