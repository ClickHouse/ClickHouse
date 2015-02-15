#pragma once

#include <Poco/BinaryWriter.h>
#include <Poco/BinaryReader.h>

#include <DB/DataTypes/IDataTypeNumber.h>

#include <DB/Columns/ColumnConst.h>

namespace DB
{


/** Реализует часть интерфейса IDataType, общую для всяких чисел фиксированной ширины
  * - ввод и вывод в текстовом и бинарном виде.
  * Остаётся лишь чисто виртуальный метод getName() и clone().
  *
  * Параметры: FieldType - тип единичного значения, ColumnType - тип столбца со значениями.
  * (см. Core/Field.h, Columns/IColumn.h)
  */
template <typename FieldType, typename ColumnType>
class IDataTypeNumberFixed : public IDataTypeNumber<FieldType>
{
public:
	/** Формат платформозависимый (зависит от представления данных в памяти).
	  */

	void serializeBinary(const Field & field, WriteBuffer & ostr) const
	{
		/// ColumnType::value_type - более узкий тип. Например, UInt8, когда тип Field - UInt64
		typename ColumnType::value_type x = get<typename NearestFieldType<FieldType>::Type>(field);
		writeBinary(x, ostr);
	}

	void deserializeBinary(Field & field, ReadBuffer & istr) const
	{
		typename ColumnType::value_type x;
		readBinary(x, istr);
		field = typename NearestFieldType<FieldType>::Type(x);
	}

	void serializeBinary(const IColumn & column, WriteBuffer & ostr, size_t offset = 0, size_t limit = 0) const
	{
		const typename ColumnType::Container_t & x = typeid_cast<const ColumnType &>(column).getData();

		size_t size = x.size();

		if (limit == 0 || offset + limit > size)
			limit = size - offset;

		ostr.write(reinterpret_cast<const char *>(&x[offset]), sizeof(typename ColumnType::value_type) * limit);
	}

	void deserializeBinary(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const
	{
		typename ColumnType::Container_t & x = typeid_cast<ColumnType &>(column).getData();
		size_t initial_size = x.size();
		x.resize(initial_size + limit);
		size_t size = istr.readBig(reinterpret_cast<char*>(&x[initial_size]), sizeof(typename ColumnType::value_type) * limit);
		x.resize(initial_size + size / sizeof(typename ColumnType::value_type));
	}

	ColumnPtr createColumn() const
	{
		return new ColumnType;
	}

	ColumnPtr createConstColumn(size_t size, const Field & field) const
	{
		return new ColumnConst<FieldType>(size, get<typename NearestFieldType<FieldType>::Type>(field));
	}
};


}
