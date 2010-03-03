#ifndef DBMS_COLUMN_TYPES_ICOLUMNTYPE_NUMBER_FIXED_H
#define DBMS_COLUMN_TYPES_ICOLUMNTYPE_NUMBER_FIXED_H

#include <Poco/BinaryWriter.h>
#include <Poco/BinaryReader.h>

#include <DB/ColumnTypes/IColumnTypeNumber.h>


namespace DB
{


/** Реализует часть интерфейса IColumnType, общую для всяких чисел фиксированной ширины
  * - ввод и вывод в текстовом и бинарном виде.
  * Остаётся лишь чисто виртуальный метод getName().
  *
  * Параметры: FieldType - тип единичного значения, ColumnType - тип столбца со значениями.
  * (см. Field.h, Column.h)
  */
template <typename FieldType, typename ColumnType>
class IColumnTypeNumberFixed : public IColumnTypeNumber<FieldType>
{
public:
	/** Формат платформозависимый (зависит от представления данных в памяти).
	  */

	void serializeBinary(const Field & field, std::ostream & ostr) const
	{
		/// ColumnType::value_type - более узкий тип. Например, UInt8, когда тип Field - UInt64
		typename ColumnType::value_type x = boost::get<FieldType>(field);
		ostr.write(reinterpret_cast<const char *>(&x), sizeof(x));
	}
	
	void deserializeBinary(Field & field, std::istream & istr) const
	{
		typename ColumnType::value_type x;
		istr.read(reinterpret_cast<char *>(&x), sizeof(x));
		field = x;
	}
	
	void serializeBinary(const Column & column, std::ostream & ostr) const
	{
		const ColumnType & x = boost::get<ColumnType>(column);
		ostr.write(reinterpret_cast<const char *>(&x[0]), sizeof(typename ColumnType::value_type) * x.size());
	}
	
	void deserializeBinary(Column & column, std::istream & istr, size_t limit) const
	{
		ColumnType & x = boost::get<ColumnType>(column);
		x.resize(limit);
		istr.read(reinterpret_cast<char*>(&x[0]), sizeof(typename ColumnType::value_type) * limit);
		x.resize(istr.gcount());
	}
};

}

#endif
