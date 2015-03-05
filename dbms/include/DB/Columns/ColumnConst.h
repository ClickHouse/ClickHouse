#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Core/Field.h>
#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>
#include <DB/Columns/ColumnVector.h>
#include <DB/Columns/IColumn.h>
#include <DB/DataTypes/IDataType.h>


namespace DB
{

using Poco::SharedPtr;


class IColumnConst : public IColumn
{
public:
	bool isConst() const override { return true; }
	virtual ColumnPtr convertToFullColumn() const = 0;
};


/** шаблон для столбцов-констант (столбцов одинаковых значений).
  */
template <typename T>
class ColumnConst final : public IColumnConst
{
public:
	typedef T Type;
	typedef typename NearestFieldType<T>::Type FieldType;

	/// Для ColumnConst<Array> data_type_ должен быть ненулевым.
	/// Для ColumnConst<String> data_type_ должен быть ненулевым, если тип данных FixedString.
	ColumnConst(size_t s_, const T & data_, DataTypePtr data_type_ = DataTypePtr()) : s(s_), data(data_), data_type(data_type_) {}

	std::string getName() const override { return "ColumnConst<" + TypeName<T>::get() + ">"; }
	bool isNumeric() const override { return IsNumber<T>::value; }
	bool isFixed() const override { return IsNumber<T>::value; }
	size_t sizeOfField() const override { return sizeof(T); }
	ColumnPtr cloneResized(size_t s_) const override { return new ColumnConst(s_, data); }
	size_t size() const override { return s; }
	Field operator[](size_t n) const override { return FieldType(data); }
	void get(size_t n, Field & res) const override { res = FieldType(data); }

	ColumnPtr cut(size_t start, size_t length) const override
	{
		return new ColumnConst<T>(length, data, data_type);
	}

	void insert(const Field & x) override
	{
		if (x.get<FieldType>() != FieldType(data))
			throw Exception("Cannot insert different element into constant column " + getName(),
				ErrorCodes::CANNOT_INSERT_ELEMENT_INTO_CONSTANT_COLUMN);
		++s;
	}

	void insertData(const char * pos, size_t length) override
	{
		throw Exception("Cannot insert element into constant column " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	void insertFrom(const IColumn & src, size_t n) override
	{
		if (data != static_cast<const ColumnConst<T> &>(src).data)
			throw Exception("Cannot insert different element into constant column " + getName(),
				ErrorCodes::CANNOT_INSERT_ELEMENT_INTO_CONSTANT_COLUMN);
		++s;
	}

	void insertDefault() override { ++s; }

	ColumnPtr filter(const Filter & filt) const override
	{
		if (s != filt.size())
			throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		return new ColumnConst<T>(countBytesInFilter(filt), data, data_type);
	}

	ColumnPtr replicate(const Offsets_t & offsets) const override
	{
		if (s != offsets.size())
			throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		size_t replicated_size = 0 == s ? 0 : offsets.back();
		return new ColumnConst<T>(replicated_size, data, data_type);
	}

	size_t byteSize() const override { return sizeof(data) + sizeof(s); }

	ColumnPtr permute(const Permutation & perm, size_t limit) const override
	{
		if (limit == 0)
			limit = s;
		else
			limit = std::min(s, limit);

		if (perm.size() < limit)
			throw Exception("Size of permutation is less than required.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		return new ColumnConst<T>(limit, data, data_type);
	}

	int compareAt(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const override
	{
		const ColumnConst<T> & rhs = static_cast<const ColumnConst<T> &>(rhs_);
		return data < rhs.data	/// TODO: правильное сравнение NaN-ов в константных столбцах.
			? -1
			: (data == rhs.data
				? 0
				: 1);
	}

	void getPermutation(bool reverse, size_t limit, Permutation & res) const override
	{
		res.resize(s);
		for (size_t i = 0; i < s; ++i)
			res[i] = i;
	}

	StringRef getDataAt(size_t n) const override;
	StringRef getDataAtWithTerminatingZero(size_t n) const override;
	UInt64 get64(size_t n) const override;

	/** Более эффективные методы манипуляции */
	T & getData() { return data; }
	const T & getData() const { return data; }

	/** Преобразование из константы в полноценный столбец */
	ColumnPtr convertToFullColumn() const override;

	void getExtremes(Field & min, Field & max) const override
	{
		min = FieldType(data);
		max = FieldType(data);
	}

	DataTypePtr & getDataType() { return data_type; }
	const DataTypePtr & getDataType() const { return data_type; }

private:
	size_t s;
	T data;
	DataTypePtr data_type;
};


typedef ColumnConst<String> ColumnConstString;
typedef ColumnConst<Array> ColumnConstArray;


template <typename T> ColumnPtr ColumnConst<T>::convertToFullColumn() const
{
	ColumnVector<T> * res_ = new ColumnVector<T>;
	ColumnPtr res = res_;
	res_->getData().assign(s, data);
	return res;
}


template <> ColumnPtr ColumnConst<String>::convertToFullColumn() const;

template <> ColumnPtr ColumnConst<Array>::convertToFullColumn() const;


template <typename T> StringRef ColumnConst<T>::getDataAt(size_t n) const
{
	throw Exception("Method getDataAt is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

template <> inline StringRef ColumnConst<String>::getDataAt(size_t n) const
{
	return StringRef(data);
}

template <typename T> UInt64 ColumnConst<T>::get64(size_t n) const
{
	throw Exception("Method get64 is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

/// Для элементарных типов.
template <typename T> StringRef getDataAtImpl(const T & data)
{
	return StringRef(reinterpret_cast<const char *>(&data), sizeof(data));
}


template <typename T> UInt64 get64IntImpl(const T & data)
{
	return data;
}

template <typename T> UInt64 get64FloatImpl(const T & data)
{
	union
	{
		T src;
		UInt64 res;
	};

	res = 0;
	src = data;
	return res;
}

template <> inline StringRef ColumnConst<UInt8		>::getDataAt(size_t n) const { return getDataAtImpl(data); }
template <> inline StringRef ColumnConst<UInt16		>::getDataAt(size_t n) const { return getDataAtImpl(data); }
template <> inline StringRef ColumnConst<UInt32		>::getDataAt(size_t n) const { return getDataAtImpl(data); }
template <> inline StringRef ColumnConst<UInt64		>::getDataAt(size_t n) const { return getDataAtImpl(data); }
template <> inline StringRef ColumnConst<Int8		>::getDataAt(size_t n) const { return getDataAtImpl(data); }
template <> inline StringRef ColumnConst<Int16		>::getDataAt(size_t n) const { return getDataAtImpl(data); }
template <> inline StringRef ColumnConst<Int32		>::getDataAt(size_t n) const { return getDataAtImpl(data); }
template <> inline StringRef ColumnConst<Int64		>::getDataAt(size_t n) const { return getDataAtImpl(data); }
template <> inline StringRef ColumnConst<Float32	>::getDataAt(size_t n) const { return getDataAtImpl(data); }
template <> inline StringRef ColumnConst<Float64	>::getDataAt(size_t n) const { return getDataAtImpl(data); }

template <> inline UInt64 ColumnConst<UInt8		>::get64(size_t n) const { return get64IntImpl(data); }
template <> inline UInt64 ColumnConst<UInt16	>::get64(size_t n) const { return get64IntImpl(data); }
template <> inline UInt64 ColumnConst<UInt32	>::get64(size_t n) const { return get64IntImpl(data); }
template <> inline UInt64 ColumnConst<UInt64	>::get64(size_t n) const { return get64IntImpl(data); }
template <> inline UInt64 ColumnConst<Int8		>::get64(size_t n) const { return get64IntImpl(data); }
template <> inline UInt64 ColumnConst<Int16		>::get64(size_t n) const { return get64IntImpl(data); }
template <> inline UInt64 ColumnConst<Int32		>::get64(size_t n) const { return get64IntImpl(data); }
template <> inline UInt64 ColumnConst<Int64		>::get64(size_t n) const { return get64IntImpl(data); }
template <> inline UInt64 ColumnConst<Float32	>::get64(size_t n) const { return get64FloatImpl(data); }
template <> inline UInt64 ColumnConst<Float64	>::get64(size_t n) const { return get64FloatImpl(data); }


template <typename T> StringRef ColumnConst<T>::getDataAtWithTerminatingZero(size_t n) const
{
	return getDataAt(n);
}

template <> inline StringRef ColumnConst<String>::getDataAtWithTerminatingZero(size_t n) const
{
	return StringRef(data.data(), data.size() + 1);
}


}
