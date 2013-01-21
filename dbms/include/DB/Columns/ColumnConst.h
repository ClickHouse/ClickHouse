#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Core/Field.h>
#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>
#include <DB/Columns/ColumnVector.h>
#include <DB/Columns/IColumn.h>


namespace DB
{

using Poco::SharedPtr;


class IColumnConst : public IColumn
{
public:
	bool isConst() const { return true; }
	virtual ColumnPtr convertToFullColumn() const = 0;
};


/** шаблон для столбцов-констант (столбцов одинаковых значений).
  */
template <typename T>
class ColumnConst : public IColumnConst
{
public:
	typedef T Type;
	
	ColumnConst(size_t s_, const T & data_) : s(s_), data(data_) {}

	std::string getName() const { return "ColumnConst<" + TypeName<T>::get() + ">"; }
	bool isNumeric() const { return IsNumber<T>::value; }
	size_t sizeOfField() const { return sizeof(T); }
	ColumnPtr cloneEmpty() const { return new ColumnConst(0, data); }
	size_t size() const { return s; }
	Field operator[](size_t n) const { return typename NearestFieldType<T>::Type(data); }
	void get(size_t n, Field & res) const { res = typename NearestFieldType<T>::Type(data); }
	void cut(size_t start, size_t length) { s = length; }
	void clear() { s = 0; }
	
	void insert(const Field & x)
	{
		throw Exception("Cannot insert element into constant column " + getName(), ErrorCodes::CANNOT_INSERT_ELEMENT_INTO_CONSTANT_COLUMN);
	}
	
	void insertDefault() { ++s; }

	void filter(const Filter & filt)
	{
		if (s != filt.size())
			throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);
		
		size_t new_size = 0;
		for (Filter::const_iterator it = filt.begin(); it != filt.end(); ++it)
			if (*it)
				++new_size;
		s = new_size;
	}

	void replicate(const Offsets_t & offsets)
	{
		if (s != offsets.size())
			throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		s = offsets.back();
	}

	size_t byteSize() const { return sizeof(data) + sizeof(s); }

	void permute(const Permutation & perm)
	{
		if (s != perm.size())
			throw Exception("Size of permutation doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);
	}

	int compareAt(size_t n, size_t m, const IColumn & rhs_) const
	{
		const ColumnConst<T> & rhs = static_cast<const ColumnConst<T> &>(rhs_);
		return data < rhs.data
			? -1
			: (data == rhs.data
				? 0
				: 1);
	}

	Permutation getPermutation() const
	{
		Permutation res(s);
		for (size_t i = 0; i < s; ++i)
			res[i] = i;
		return res;
	}

	StringRef getDataAt(size_t n) const;

	/** Более эффективные методы манипуляции */
	T & getData() { return data; }
	const T & getData() const { return data; }

	/** Преобразование из константы в полноценный столбец */
	ColumnPtr convertToFullColumn() const;

private:
	size_t s;
	T data;
};


typedef ColumnConst<String> ColumnConstString;
typedef ColumnConst<Array> ColumnConstArray;


template <typename T> ColumnPtr ColumnConst<T>::convertToFullColumn() const
{
	ColumnVector<T> * res = new ColumnVector<T>;
	res->getData().assign(s, data);
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

/// Для элементарных типов.
template <typename T> StringRef getDataAtImpl(size_t n, const T & data)
{
	return StringRef(reinterpret_cast<const char *>(&data), sizeof(data));
}

template <> inline StringRef ColumnConst<UInt8	>::getDataAt(size_t n) const { return getDataAtImpl(n, data); }
template <> inline StringRef ColumnConst<UInt16	>::getDataAt(size_t n) const { return getDataAtImpl(n, data); }
template <> inline StringRef ColumnConst<UInt32	>::getDataAt(size_t n) const { return getDataAtImpl(n, data); }
template <> inline StringRef ColumnConst<UInt64	>::getDataAt(size_t n) const { return getDataAtImpl(n, data); }
template <> inline StringRef ColumnConst<Int8	>::getDataAt(size_t n) const { return getDataAtImpl(n, data); }
template <> inline StringRef ColumnConst<Int16	>::getDataAt(size_t n) const { return getDataAtImpl(n, data); }
template <> inline StringRef ColumnConst<Int32	>::getDataAt(size_t n) const { return getDataAtImpl(n, data); }
template <> inline StringRef ColumnConst<Int64	>::getDataAt(size_t n) const { return getDataAtImpl(n, data); }
template <> inline StringRef ColumnConst<Float32>::getDataAt(size_t n) const { return getDataAtImpl(n, data); }
template <> inline StringRef ColumnConst<Float64>::getDataAt(size_t n) const { return getDataAtImpl(n, data); }


}
