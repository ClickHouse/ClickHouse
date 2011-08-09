#pragma once

#include <Poco/BinaryWriter.h>
#include <Poco/BinaryReader.h>

#include <DB/DataTypes/IDataTypeNumber.h>

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
		typename ColumnType::value_type x = boost::get<FieldType>(field);
		writeBinary(x, ostr);
	}
	
	void deserializeBinary(Field & field, ReadBuffer & istr) const
	{
		typename ColumnType::value_type x;
		readBinary(x, istr);
		field = typename NearestFieldType<FieldType>::Type(x);
	}
	
	void serializeBinary(const IColumn & column, WriteBuffer & ostr) const
	{
		const typename ColumnType::Container_t & x = dynamic_cast<const ColumnType &>(column).getData();
		ostr.write(reinterpret_cast<const char *>(&x[0]), sizeof(typename ColumnType::value_type) * x.size());
	}
	
	void deserializeBinary(IColumn & column, ReadBuffer & istr, size_t limit) const
	{
		typename ColumnType::Container_t & x =  dynamic_cast<ColumnType &>(column).getData();
		x.resize(limit);
		size_t size = istr.read(reinterpret_cast<char*>(&x[0]), sizeof(typename ColumnType::value_type) * limit);
		x.resize(size / sizeof(typename ColumnType::value_type));
	}

	SharedPtr<IColumn> createColumn() const
	{
		return new ColumnType;
	}
};


}
