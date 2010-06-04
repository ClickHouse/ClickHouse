#ifndef DBMS_DATA_TYPES_IDATATYPE_NUMBER_VARIABLE_H
#define DBMS_DATA_TYPES_IDATATYPE_NUMBER_VARIABLE_H

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/IO/VarInt.h>

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
	
	void serializeBinary(const IColumn & column, WriteBuffer & ostr) const
	{
		const typename ColumnType::Container_t & x = dynamic_cast<const ColumnType &>(column).getData();
		size_t size = x.size();
		for (size_t i = 0; i < size; ++i)
			writeVarT(x[i], ostr);
	}
	
	void deserializeBinary(IColumn & column, ReadBuffer & istr, size_t limit) const
	{
		typename ColumnType::Container_t & x =  dynamic_cast<ColumnType &>(column).getData();
		x.resize(limit);
		for (size_t i = 0; i < limit; ++i)
		{
			readVarT(x[i], istr);

			if (istr.eof())
			{
				x.resize(i);
				break;
			}
		}
	}

	SharedPtr<IColumn> createColumn() const
	{
		return new ColumnType;
	}
};

}

#endif
