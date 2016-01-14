#pragma once

#include <DB/DataTypes/IDataType.h>
#include <DB/Columns/ColumnTuple.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/DataStreams/NativeBlockInputStream.h>
#include <DB/DataStreams/NativeBlockOutputStream.h>
#include <ext/map.hpp>
#include <ext/enumerate.hpp>
#include <ext/range.hpp>


namespace DB
{

/** Тип данных - кортеж.
  * Используется как промежуточный результат при вычислении выражений.
  * Также может быть использовать в качестве столбца - результата выполнения запроса.
  * Не может быть сохранён в таблицы.
  */
class DataTypeTuple final : public IDataType
{
private:
	DataTypes elems;
public:
	DataTypeTuple(DataTypes elems_) : elems(elems_) {}

	std::string getName() const override
	{
		std::stringstream s;

		s << "Tuple(";
		for (DataTypes::const_iterator it = elems.begin(); it != elems.end(); ++it)
			s << (it == elems.begin() ? "" : ", ") << (*it)->getName();
		s << ")";

		return s.str();
	}

	SharedPtr<IDataType> clone() const override { return new DataTypeTuple(elems); }

	void serializeBinary(const Field & field, WriteBuffer & ostr) const override
	{
		const auto & tuple = get<const Tuple &>(field).t;
		for (const auto & idx_elem : ext::enumerate(elems))
			idx_elem.second->serializeBinary(tuple[idx_elem.first], ostr);
	}

	void deserializeBinary(Field & field, ReadBuffer & istr) const override
	{
		const size_t size = elems.size();
		field = Tuple(TupleBackend(size));
		TupleBackend & tuple = get<Tuple &>(field).t;
		for (const auto i : ext::range(0, size))
			elems[i]->deserializeBinary(tuple[i], istr);
	}

	void serializeText(const Field & field, WriteBuffer & ostr) const override
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

	void deserializeText(Field & field, ReadBuffer & istr) const override
	{
		const size_t size = elems.size();
		field = Tuple(TupleBackend(size));
		TupleBackend & tuple = get<Tuple &>(field).t;
		assertString("(", istr);
		for (const auto i : ext::range(0, size))
		{
			if (i != 0)
				assertString(",", istr);
			elems[i]->deserializeTextQuoted(tuple[i], istr);
		}
		assertString(")", istr);
	}

	void serializeTextEscaped(const Field & field, WriteBuffer & ostr) const override
	{
		serializeText(field, ostr);
	}

	void deserializeTextEscaped(Field & field, ReadBuffer & istr) const override
	{
		deserializeText(field, istr);
	}

	void serializeTextQuoted(const Field & field, WriteBuffer & ostr) const override
	{
		serializeText(field, ostr);
	}

	void deserializeTextQuoted(Field & field, ReadBuffer & istr) const override
	{
		deserializeText(field, istr);
	}

	void serializeTextJSON(const Field & field, WriteBuffer & ostr) const override
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

	void serializeBinary(const IColumn & column, WriteBuffer & ostr, size_t offset = 0, size_t limit = 0) const override
	{
		const ColumnTuple & real_column = static_cast<const ColumnTuple &>(column);
		for (size_t i = 0, size = elems.size(); i < size; ++i)
			NativeBlockOutputStream::writeData(*elems[i], real_column.getData().getByPosition(i).column, ostr, offset, limit);
	}

	/** limit обязательно должен быть в точности равен количеству сериализованных значений.
	  * Именно из-за этого (невозможности читать меньший кусок записанных данных), Tuple не могут быть использованы для хранения данных в таблицах.
	  * (Хотя могут быть использованы для передачи данных по сети в Native формате.)
	  */
	void deserializeBinary(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const override
	{
		ColumnTuple & real_column = static_cast<ColumnTuple &>(column);
		for (size_t i = 0, size = elems.size(); i < size; ++i)
			NativeBlockInputStream::readData(*elems[i], *real_column.getData().getByPosition(i).column, istr, limit);
	}

	ColumnPtr createColumn() const override
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

	ColumnPtr createConstColumn(size_t size, const Field & field) const override
	{
		return new ColumnConstTuple(size, get<const Tuple &>(field), new DataTypeTuple(elems));
	}

	Field getDefault() const override
	{
		return Tuple(ext::map<TupleBackend>(elems, [] (const DataTypePtr & elem) { return elem->getDefault(); }));
	}

	const DataTypes & getElements() const { return elems; }
};

}

