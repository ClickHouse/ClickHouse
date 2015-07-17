#pragma once

#include <DB/DataTypes/IDataType.h>
#include <DB/Columns/ColumnTuple.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/DataStreams/NativeBlockInputStream.h>
#include <DB/DataStreams/NativeBlockOutputStream.h>


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

	std::string getName() const
	{
		std::stringstream s;

		s << "Tuple(";
		for (DataTypes::const_iterator it = elems.begin(); it != elems.end(); ++it)
			s << (it == elems.begin() ? "" : ", ") << (*it)->getName();
		s << ")";

		return s.str();
	}

	SharedPtr<IDataType> clone() const { return new DataTypeTuple(elems); }

	void serializeBinary(const Field & field, WriteBuffer & ostr) const
	{
		const Array & arr = get<const Array &>(field);
		for (size_t i = 0, size = elems.size(); i < size; ++i)
			elems[i]->serializeBinary(arr[i], ostr);
	}

	void deserializeBinary(Field & field, ReadBuffer & istr) const
	{
		size_t size = elems.size();
		field = Array(size);
		Array & arr = get<Array &>(field);
		for (size_t i = 0; i < size; ++i)
			elems[i]->deserializeBinary(arr[i], istr);
	}

	void serializeText(const Field & field, WriteBuffer & ostr) const
	{
		const Array & arr = get<const Array &>(field);
		writeChar('(', ostr);
		for (size_t i = 0, size = elems.size(); i < size; ++i)
		{
			if (i != 0)
				writeChar(',', ostr);
			elems[i]->serializeTextQuoted(arr[i], ostr);
		}
		writeChar(')', ostr);
	}

	void deserializeText(Field & field, ReadBuffer & istr) const
	{
		size_t size = elems.size();
		field = Array(size);
		Array & arr = get<Array &>(field);
		assertString("(", istr);
		for (size_t i = 0; i < size; ++i)
		{
			if (i != 0)
				assertString(",", istr);
			elems[i]->deserializeTextQuoted(arr[i], istr);
		}
		assertString(")", istr);
	}

	void serializeTextEscaped(const Field & field, WriteBuffer & ostr) const
	{
		serializeText(field, ostr);
	}

	void deserializeTextEscaped(Field & field, ReadBuffer & istr) const
	{
		deserializeText(field, istr);
	}

	void serializeTextQuoted(const Field & field, WriteBuffer & ostr) const
	{
		serializeText(field, ostr);
	}

	void deserializeTextQuoted(Field & field, ReadBuffer & istr) const
	{
		deserializeText(field, istr);
	}

	void serializeTextJSON(const Field & field, WriteBuffer & ostr) const
	{
		const Array & arr = get<const Array &>(field);
		writeChar('[', ostr);
		for (size_t i = 0, size = elems.size(); i < size; ++i)
		{
			if (i != 0)
				writeChar(',', ostr);
			elems[i]->serializeTextJSON(arr[i], ostr);
		}
		writeChar(']', ostr);
	}

	void serializeBinary(const IColumn & column, WriteBuffer & ostr, size_t offset = 0, size_t limit = 0) const
	{
		const ColumnTuple & real_column = static_cast<const ColumnTuple &>(column);
		for (size_t i = 0, size = elems.size(); i < size; ++i)
			NativeBlockOutputStream::writeData(*elems[i], real_column.getData().getByPosition(i).column, ostr, offset, limit);
	}

	/** limit обязательно должен быть в точности равен количеству сериализованных значений.
	  * Именно из-за этого (невозможности читать меньший кусок записанных данных), Tuple не могут быть использованы для хранения данных в таблицах.
	  * (Хотя могут быть использованы для передачи данных по сети в Native формате.)
	  */
	void deserializeBinary(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const
	{
		ColumnTuple & real_column = static_cast<ColumnTuple &>(column);
		for (size_t i = 0, size = elems.size(); i < size; ++i)
			NativeBlockInputStream::readData(*elems[i], *real_column.getData().getByPosition(i).column, istr, limit);
	}

	ColumnPtr createColumn() const
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

	ColumnPtr createConstColumn(size_t size, const Field & field) const
	{
		return new ColumnConstArray(size, get<const Array &>(field), new DataTypeTuple(elems));
	}

	Field getDefault() const
	{
		size_t size = elems.size();
		Array res(size);
		for (size_t i = 0; i < size; ++i)
			res[i] = elems[i]->getDefault();
		return res;
	}

	const DataTypes & getElements() const { return elems; }
};

}

