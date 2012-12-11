#pragma once

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/IO/VarInt.h>

#include <DB/Columns/ColumnConst.h>

#include <DB/DataTypes/IDataTypeNumber.h>


namespace DB
{


/** Реализует часть интерфейса IDataType, общую для знаковых и беззнаковых чисел переменной длины
  * - ввод и вывод в текстовом и бинарном виде.
  * Остаётся лишь чисто виртуальный метод getName().
  *
  * Параметры: FieldType - тип единичного значения, ColumnType - тип столбца со значениями.
  * (см. Core/Field.h, Columns/IColumn.h)
  */
template <typename FieldType, typename ColumnType>
class IDataTypeNumberVariable : public IDataTypeNumber<FieldType>
{
public:
	void serializeBinary(const Field & field, WriteBuffer & ostr) const
	{
		writeVarT(static_cast<typename ColumnType::value_type>(boost::get<FieldType>(field)), ostr);
	}
	
	void deserializeBinary(Field & field, ReadBuffer & istr) const
	{
		readVarT(static_cast<typename ColumnType::value_type &>(boost::get<FieldType &>(field)), istr);
	}
	
	void serializeBinary(const IColumn & column, WriteBuffer & ostr, size_t offset = 0, size_t limit = 0) const
	{
		const typename ColumnType::Container_t & x = dynamic_cast<const ColumnType &>(column).getData();
		size_t size = x.size();

		size_t end = limit && offset + limit < size
			? offset + limit
			: size;
		
		for (size_t i = offset; i < end; ++i)
			writeVarT(x[i], ostr);
	}
	
	void deserializeBinary(IColumn & column, ReadBuffer & istr, size_t limit) const
	{
		typename ColumnType::Container_t & x =  dynamic_cast<ColumnType &>(column).getData();
		x.resize(limit);
		size_t i = 0;
		while (i < limit && !istr.eof())
		{
			readVarT(x[i], istr);
			++i;
		}
		x.resize(i);
	}

	ColumnPtr createColumn() const
	{
		return new ColumnType;
	}

	ColumnPtr createConstColumn(size_t size, const Field & field) const
	{
		return new ColumnConst<FieldType>(size, boost::get<FieldType>(field));
	}
};

}
