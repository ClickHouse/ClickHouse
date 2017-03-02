#pragma once

#include <Poco/BinaryWriter.h>
#include <Poco/BinaryReader.h>

#include <DB/DataTypes/IDataTypeNumber.h>

#include <DB/Columns/ColumnConst.h>

namespace DB
{

namespace ErrorCodes
{

extern const int NOT_IMPLEMENTED;

}

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

	void serializeBinary(const Field & field, WriteBuffer & ostr) const override
	{
		/// ColumnType::value_type - более узкий тип. Например, UInt8, когда тип Field - UInt64
		typename ColumnType::value_type x = get<typename NearestFieldType<FieldType>::Type>(field);
		writeBinary(x, ostr);
	}

	void deserializeBinary(Field & field, ReadBuffer & istr) const override
	{
		typename ColumnType::value_type x;
		readBinary(x, istr);
		field = typename NearestFieldType<FieldType>::Type(x);
	}

	void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override
	{
		writeBinary(static_cast<const ColumnType &>(column).getData()[row_num], ostr);
	}

	void deserializeBinary(IColumn & column, ReadBuffer & istr) const override
	{
		typename ColumnType::value_type x;
		readBinary(x, istr);
		static_cast<ColumnType &>(column).getData().push_back(x);
	}

	void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override
	{
		const typename ColumnType::Container_t & x = typeid_cast<const ColumnType &>(column).getData();

		size_t size = x.size();

		if (limit == 0 || offset + limit > size)
			limit = size - offset;

		ostr.write(reinterpret_cast<const char *>(&x[offset]), sizeof(typename ColumnType::value_type) * limit);
	}

	void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const override
	{
		typename ColumnType::Container_t & x = typeid_cast<ColumnType &>(column).getData();
		size_t initial_size = x.size();
		x.resize(initial_size + limit);
		size_t size = istr.readBig(reinterpret_cast<char*>(&x[initial_size]), sizeof(typename ColumnType::value_type) * limit);
		x.resize(initial_size + size / sizeof(typename ColumnType::value_type));
	}

	ColumnPtr createColumn() const override
	{
		return std::make_shared<ColumnType>();
	}

	ColumnPtr createConstColumn(size_t size, const Field & field) const override
	{
		return std::make_shared<ColumnConst<FieldType>>(size, get<typename NearestFieldType<FieldType>::Type>(field));
	}
};

/// Dummy data type which serves as the base of the DataTypeVoid implementation.
template <>
class IDataTypeNumberFixed<void, void> : public IDataTypeNumber<void>
{
public:
	void serializeBinary(const Field & field, WriteBuffer & ostr) const override
	{
		throw Exception{"Method serializeBinary is not supported for DataTypeVoid", ErrorCodes::NOT_IMPLEMENTED};
	}

	void deserializeBinary(Field & field, ReadBuffer & istr) const override
	{
		throw Exception{"Method deserializeBinary is not supported for DataTypeVoid", ErrorCodes::NOT_IMPLEMENTED};
	}

	void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override
	{
		throw Exception{"Method serializeBinary is not supported for DataTypeVoid", ErrorCodes::NOT_IMPLEMENTED};
	}

	void deserializeBinary(IColumn & column, ReadBuffer & istr) const override
	{
		throw Exception{"Method deserializeBinary is not supported for DataTypeVoid", ErrorCodes::NOT_IMPLEMENTED};
	}

	void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override
	{
		throw Exception{"Method serializeBinary is not supported for DataTypeVoid", ErrorCodes::NOT_IMPLEMENTED};
	}

	void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const override
	{
		throw Exception{"Method deserializeBinary is not supported for DataTypeVoid", ErrorCodes::NOT_IMPLEMENTED};
	}

	ColumnPtr createColumn() const override
	{
		throw Exception{"Method createColumn is not supported for DataTypeVoid", ErrorCodes::NOT_IMPLEMENTED};
	}

	ColumnPtr createConstColumn(size_t size, const Field & field) const override
	{
		throw Exception{"Method createConstColumn is not supported for DataTypeVoid", ErrorCodes::NOT_IMPLEMENTED};
	}
};

}
