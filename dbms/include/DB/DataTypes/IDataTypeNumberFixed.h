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
	typedef IDataType::WriteCallback WriteCallback;
public:
	/** Формат платформозависимый (зависит от представления данных в памяти).
	  */

	void serializeBinary(const Field & field, WriteBuffer & ostr) const
	{
		/// ColumnType::value_type - более узкий тип. Например, UInt8, когда тип Field - UInt64
		typename ColumnType::value_type x = boost::get<typename NearestFieldType<FieldType>::Type>(field);
		writeBinary(x, ostr);
	}
	
	void deserializeBinary(Field & field, ReadBuffer & istr) const
	{
		typename ColumnType::value_type x;
		readBinary(x, istr);
		field = typename NearestFieldType<FieldType>::Type(x);
	}
	
	void serializeBinary(const IColumn & column, WriteBuffer & ostr, WriteCallback callback = WriteCallback()) const
	{
		const typename ColumnType::Container_t & x = dynamic_cast<const ColumnType &>(column).getData();

		size_t prev_callback_point = 0;
		size_t next_callback_point = 0;
		size_t size = x.size();

		while (next_callback_point < size)
		{
			next_callback_point = callback ? callback() : size;
			if (next_callback_point > size)
				next_callback_point = size;
			
			ostr.write(reinterpret_cast<const char *>(&x[prev_callback_point]),
				sizeof(typename ColumnType::value_type) * (next_callback_point - prev_callback_point));

			prev_callback_point = next_callback_point;
		}
	}
	
	void deserializeBinary(IColumn & column, ReadBuffer & istr, size_t limit) const
	{
		typename ColumnType::Container_t & x =  dynamic_cast<ColumnType &>(column).getData();
		x.resize(limit);
		size_t size = istr.read(reinterpret_cast<char*>(&x[0]), sizeof(typename ColumnType::value_type) * limit);
		x.resize(size / sizeof(typename ColumnType::value_type));
	}

	ColumnPtr createColumn() const
	{
		return new ColumnType;
	}

	ColumnPtr createConstColumn(size_t size, const Field & field) const
	{
		return new ColumnConst<FieldType>(size, boost::get<typename NearestFieldType<FieldType>::Type>(field));
	}
};


}
