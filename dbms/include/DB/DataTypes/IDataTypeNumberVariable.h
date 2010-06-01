#ifndef DBMS_DATA_TYPES_IDATATYPE_NUMBER_VARIABLE_H
#define DBMS_DATA_TYPES_IDATATYPE_NUMBER_VARIABLE_H

#include <DB/Common/VarInt.h>
#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

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
	void serializeBinary(const Field & field, std::ostream & ostr) const
	{
		writeVarT(static_cast<typename ColumnType::value_type>(boost::get<FieldType>(field)), ostr);
	}
	
	void deserializeBinary(Field & field, std::istream & istr) const
	{
		readVarT(static_cast<typename ColumnType::value_type &>(boost::get<FieldType>(field)), istr);
	}
	
	void serializeBinary(const IColumn & column, std::ostream & ostr) const
	{
		const typename ColumnType::Container_t & x = dynamic_cast<const ColumnType &>(column).getData();
		size_t size = x.size();
		for (size_t i = 0; i < size; ++i)
			writeVarT(x[i], ostr);
	}
	
	void deserializeBinary(IColumn & column, std::istream & istr, size_t limit) const
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
			else if (!istr.good())
				throw Exception("Cannot read data from istream", ErrorCodes::CANNOT_READ_DATA_FROM_ISTREAM);
		}
	}

	SharedPtr<IColumn> createColumn() const
	{
		return new ColumnType;
	}
};

}

#endif
