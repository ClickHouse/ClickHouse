#pragma once

#include <cmath> /// std::isfinite

#include <DB/DataTypes/IDataType.h>

#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/Columns/ColumnVector.h>


namespace DB
{


/** Реализует часть интерфейса IDataType, общую для всяких чисел
  * - ввод и вывод в текстовом виде.
  */
template <typename FType>
class IDataTypeNumber : public IDataType
{
public:
	using FieldType = FType;
	using ColumnType = ColumnVector<FieldType>;

	bool isNumeric() const override { return true; }
	bool behavesAsNumber() const override { return true; }

	void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override
	{
		writeText(static_cast<const ColumnType &>(column).getData()[row_num], ostr);
	}

	static inline void deserializeText(IColumn & column, ReadBuffer & istr);

	void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override
	{
		serializeText(column, row_num, ostr);
	}

	void deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const override
	{
		deserializeText(column, istr);
	}

	void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override
	{
		serializeText(column, row_num, ostr);
	}

	void deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const override
	{
		deserializeText(column, istr);
	}

	inline void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;

	void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override
	{
		serializeText(column, row_num, ostr);
	}

	void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const char delimiter) const override
	{
		FieldType x;
		readCSV(x, istr);
		static_cast<ColumnType &>(column).getData().push_back(x);
	}

	size_t getSizeOfField() const override { return sizeof(FieldType); }

	Field getDefault() const override
	{
		return typename NearestFieldType<FieldType>::Type();
	}
};

template <typename FType> inline void IDataTypeNumber<FType>::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	serializeText(column, row_num, ostr);
}

template <> inline void IDataTypeNumber<Int64>::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	writeChar('"', ostr);
	serializeText(column, row_num, ostr);
	writeChar('"', ostr);
}

template <> inline void IDataTypeNumber<UInt64>::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	writeChar('"', ostr);
	serializeText(column, row_num, ostr);
	writeChar('"', ostr);
}

template <> inline void IDataTypeNumber<Float32>::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	auto x = static_cast<const ColumnType &>(column).getData()[row_num];
	if (likely(std::isfinite(x)))
		writeText(x, ostr);
	else
		writeCString("null", ostr);
}

template <> inline void IDataTypeNumber<Float64>::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	auto x = static_cast<const ColumnType &>(column).getData()[row_num];
	if (likely(std::isfinite(x)))
		writeText(x, ostr);
	else
		writeCString("null", ostr);
}

template <typename FType> inline void IDataTypeNumber<FType>::deserializeText(IColumn & column, ReadBuffer & istr)
{
	FieldType x;
	readIntTextUnsafe(x, istr);
	static_cast<ColumnType &>(column).getData().push_back(x);
}

template <> inline void IDataTypeNumber<Float64>::deserializeText(IColumn & column, ReadBuffer & istr)
{
	Float64 x;
	readText(x, istr);
	static_cast<ColumnType &>(column).getData().push_back(x);
}

template <> inline void IDataTypeNumber<Float32>::deserializeText(IColumn & column, ReadBuffer & istr)
{
	Float64 x;
	readText(x, istr);
	static_cast<ColumnType &>(column).getData().push_back(x);
}


}
