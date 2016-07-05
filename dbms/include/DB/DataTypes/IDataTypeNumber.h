#pragma once

#include <cmath> /// std::isfinite

#include <DB/DataTypes/IDataType.h>
#include <DB/DataTypes/NullSymbol.h>

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
private:
	static FType valueForJSONNull();

public:
	using FieldType = FType;
	using ColumnType = ColumnVector<FieldType>;

	bool isNumeric() const override { return true; }
	bool behavesAsNumber() const override { return true; }

	size_t getSizeOfField() const override { return sizeof(FieldType); }

	Field getDefault() const override
	{
		return typename NearestFieldType<FieldType>::Type();
	}

private:
	template <typename Null> inline void deserializeText(IColumn & column, ReadBuffer & istr, PaddedPODArray<UInt8> * null_map) const;

protected:
	void serializeTextImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr,
		const PaddedPODArray<UInt8> * null_map) const override
	{
		if (isNullValue(null_map, row_num))
			writeCString(NullSymbol::Plain::name, ostr);
		else
			writeText(static_cast<const ColumnType &>(column).getData()[row_num], ostr);
	}

	void serializeTextEscapedImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr,
		const PaddedPODArray<UInt8> * null_map) const override
	{
		if (isNullValue(null_map, row_num))
			writeCString(NullSymbol::Escaped::name, ostr);
		else
			writeText(static_cast<const ColumnType &>(column).getData()[row_num], ostr);
	}

	void deserializeTextEscapedImpl(IColumn & column, ReadBuffer & istr,
		PaddedPODArray<UInt8> * null_map) const override
	{
		deserializeText<NullSymbol::Escaped>(column, istr, null_map);
	}

	void serializeTextQuotedImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr,
		const PaddedPODArray<UInt8> * null_map) const override
	{
		if (isNullValue(null_map, row_num))
			writeCString(NullSymbol::Quoted::name, ostr);
		else
			writeText(static_cast<const ColumnType &>(column).getData()[row_num], ostr);
	}

	void deserializeTextQuotedImpl(IColumn & column, ReadBuffer & istr,
		PaddedPODArray<UInt8> * null_map) const override
	{
		deserializeText<NullSymbol::Quoted>(column, istr, null_map);
	}

	inline void serializeTextJSONImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr,
		const PaddedPODArray<UInt8> * null_map) const override;

	void deserializeTextJSONImpl(IColumn & column, ReadBuffer & istr,
		PaddedPODArray<UInt8> * null_map) const override
	{
		bool has_quote = false;
		if (!istr.eof() && *istr.position() == '"')		/// Понимаем число как в кавычках, так и без.
		{
			has_quote = true;
			++istr.position();
		}

		FieldType x;

		/// null
		if (!has_quote && !istr.eof() && *istr.position() == NullSymbol::JSON::prefix)
		{
			++istr.position();
			assertString(NullSymbol::JSON::suffix, istr);

			x = valueForJSONNull();

			if (null_map != nullptr)
				null_map->push_back(1);
		}
		else
		{
			readText(x, istr);

			if (has_quote)
				assertChar('"', istr);
		}

		static_cast<ColumnType &>(column).getData().push_back(x);
	}

	void serializeTextCSVImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr,
		const PaddedPODArray<UInt8> * null_map) const override
	{
		if (isNullValue(null_map, row_num))
			writeCString(NullSymbol::CSV::name, ostr);
		else
			writeText(static_cast<const ColumnType &>(column).getData()[row_num], ostr);
	}

	void deserializeTextCSVImpl(IColumn & column, ReadBuffer & istr, const char delimiter,
		PaddedPODArray<UInt8> * null_map) const override
	{
		if (NullSymbol::Deserializer<NullSymbol::CSV>::execute(column, istr, null_map))
		{
			FieldType default_val = get<FieldType>(getDefault());
			static_cast<ColumnType &>(column).getData().push_back(default_val);
		}
		else
		{
			FieldType x;
			readCSV(x, istr);
			static_cast<ColumnType &>(column).getData().push_back(x);
		}
	}
};

template <>
class IDataTypeNumber<void> : public IDataType
{
public:
	using FieldType = void;

	bool isNumeric() const override { return true; }
	bool behavesAsNumber() const override { return true; }
	size_t getSizeOfField() const override { return 0; }
	Field getDefault() const override { return {}; }

protected:
	void serializeTextImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr, const PaddedPODArray<UInt8> * null_map) const override {}
	void serializeTextEscapedImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr, const PaddedPODArray<UInt8> * null_map) const override {}
	void deserializeTextEscapedImpl(IColumn & column, ReadBuffer & istr, PaddedPODArray<UInt8> * null_map) const override {}
	void serializeTextQuotedImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr, const PaddedPODArray<UInt8> * null_map) const override {}
	void deserializeTextQuotedImpl(IColumn & column, ReadBuffer & istr, PaddedPODArray<UInt8> * null_map) const override {}
	void serializeTextJSONImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr, const PaddedPODArray<UInt8> * null_map) const override {}
	void deserializeTextJSONImpl(IColumn & column, ReadBuffer & istr, PaddedPODArray<UInt8> * null_map) const override {}
	void serializeTextCSVImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr, const PaddedPODArray<UInt8> * null_map) const override {}
	void deserializeTextCSVImpl(IColumn & column, ReadBuffer & istr, const char delimiter, PaddedPODArray<UInt8> * null_map) const override {}
};

template <typename FType>
inline void IDataTypeNumber<FType>::serializeTextJSONImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr,
	const PaddedPODArray<UInt8> * null_map) const
{
	if (isNullValue(null_map, row_num))
		writeCString(NullSymbol::JSON::name, ostr);
	else
		writeText(static_cast<const ColumnType &>(column).getData()[row_num], ostr);
}

template <>
inline void IDataTypeNumber<Int64>::serializeTextJSONImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr,
	const PaddedPODArray<UInt8> * null_map) const
{
	if (isNullValue(null_map, row_num))
		writeCString(NullSymbol::JSON::name, ostr);
	else
	{
		writeChar('"', ostr);
		writeText(static_cast<const ColumnType &>(column).getData()[row_num], ostr);
		writeChar('"', ostr);
	}
}

template <>
inline void IDataTypeNumber<UInt64>::serializeTextJSONImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr,
	const PaddedPODArray<UInt8> * null_map) const
{
	if (isNullValue(null_map, row_num))
		writeCString(NullSymbol::JSON::name, ostr);
	else
	{
		writeChar('"', ostr);
		writeText(static_cast<const ColumnType &>(column).getData()[row_num], ostr);
		writeChar('"', ostr);
	}
}

template <>
inline void IDataTypeNumber<Float32>::serializeTextJSONImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr,
	const PaddedPODArray<UInt8> * null_map) const
{
	if (isNullValue(null_map, row_num))
		writeCString(NullSymbol::JSON::name, ostr);
	else
	{
		auto x = static_cast<const ColumnType &>(column).getData()[row_num];
		if (likely(std::isfinite(x)))
			writeText(x, ostr);
		else
			writeCString(NullSymbol::JSON::name, ostr);
	}
}

template <>
inline void IDataTypeNumber<Float64>::serializeTextJSONImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr,
	const PaddedPODArray<UInt8> * null_map) const
{
	if (isNullValue(null_map, row_num))
		writeCString(NullSymbol::JSON::name, ostr);
	else
	{
		auto x = static_cast<const ColumnType &>(column).getData()[row_num];
		if (likely(std::isfinite(x)))
			writeText(x, ostr);
		else
			writeCString(NullSymbol::JSON::name, ostr);
	}
}

template <typename FType>
template <typename Null>
inline void IDataTypeNumber<FType>::deserializeText(IColumn & column, ReadBuffer & istr,
	PaddedPODArray<UInt8> * null_map) const
{
	if (NullSymbol::Deserializer<Null>::execute(column, istr, null_map))
	{
		FieldType default_val = get<FieldType>(getDefault());
		static_cast<ColumnType &>(column).getData().push_back(default_val);
	}
	else
	{
		FieldType x;
		readIntTextUnsafe(x, istr);
		static_cast<ColumnType &>(column).getData().push_back(x);
	}
}

template <>
template <typename Null>
inline void IDataTypeNumber<Float64>::deserializeText(IColumn & column, ReadBuffer & istr,
	PaddedPODArray<UInt8> * null_map) const
{
	if (NullSymbol::Deserializer<Null>::execute(column, istr, null_map))
	{
		FieldType default_val = get<FieldType>(getDefault());
		static_cast<ColumnType &>(column).getData().push_back(default_val);
	}
	else
	{
		FieldType x;
		readText(x, istr);
		static_cast<ColumnType &>(column).getData().push_back(x);
	}
}

template <>
template <typename Null>
inline void IDataTypeNumber<Float32>::deserializeText(IColumn & column, ReadBuffer & istr,
	PaddedPODArray<UInt8> * null_map) const
{
	if (NullSymbol::Deserializer<Null>::execute(column, istr, null_map))
	{
		FieldType default_val = get<FieldType>(getDefault());
		static_cast<ColumnType &>(column).getData().push_back(default_val);
	}
	else
	{
		FieldType x;
		readText(x, istr);
		static_cast<ColumnType &>(column).getData().push_back(x);
	}
}

template <typename FType> inline FType IDataTypeNumber<FType>::valueForJSONNull() { return 0; }
template <> inline Float64 IDataTypeNumber<Float64>::valueForJSONNull() { return std::numeric_limits<Float64>::quiet_NaN(); }
template <> inline Float32 IDataTypeNumber<Float32>::valueForJSONNull() { return std::numeric_limits<Float32>::quiet_NaN(); }

}
