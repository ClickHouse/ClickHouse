#pragma once

#include <DB/Core/Field.h>
#include <DB/Common/Exception.h>
#include <DB/Columns/ColumnVector.h>
#include <DB/Columns/IColumn.h>
#include <DB/Columns/ColumnsCommon.h>
#include <DB/DataTypes/IDataType.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int CANNOT_INSERT_ELEMENT_INTO_CONSTANT_COLUMN;
	extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
	extern const int NOT_IMPLEMENTED;
	extern const int LOGICAL_ERROR;
}


class IColumnConst : public IColumn
{
public:
	bool isConst() const override { return true; }
	virtual ColumnPtr convertToFullColumn() const = 0;
	ColumnPtr convertToFullColumnIfConst() const override { return convertToFullColumn(); }

	Columns scatter(ColumnIndex num_columns, const Selector & selector) const override
	{
		if (size() != selector.size())
			throw Exception("Size of selector doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		std::vector<size_t> counts(num_columns);
		for (auto idx : selector)
			++counts[idx];

		Columns res(num_columns);
		for (size_t i = 0; i < num_columns; ++i)
			res[i] = cloneResized(counts[i]);

		return res;
	}
};


namespace ColumnConstDetails
{
	template <typename T>
	inline bool equals(const T & x, const T & y)
	{
		return x == y;
	}

	/// Проверяет побитовую идентичность элементов, даже если они являются NaN-ами.
	template <>
	inline bool equals(const Float32 & x, const Float32 & y)
	{
		return 0 == memcmp(&x, &y, sizeof(x));
	}

	template <>
	inline bool equals(const Float64 & x, const Float64 & y)
	{
		return 0 == memcmp(&x, &y, sizeof(x));
	}
}


/** Столбец-константа может содержать внутри себя само значение,
  *  или, в случае массивов, std::shared_ptr от значения-массива,
  *  чтобы избежать проблем производительности при копировании очень больших массивов.
  *
  * T - тип значения,
  * DataHolder - как значение хранится в таблице (либо T, либо std::shared_ptr<T>)
  * Derived должен реализовать методы getDataFromHolderImpl - получить ссылку на значение из holder-а.
  *
  * Для строк и массивов реализации sizeOfField и byteSize могут быть некорректными.
  */
template <typename T, typename DataHolder, typename Derived>
class ColumnConstBase : public IColumnConst
{
protected:
	size_t s;
	DataHolder data;
	DataTypePtr data_type;

	T & getDataFromHolder() { return static_cast<Derived *>(this)->getDataFromHolderImpl(); }
	const T & getDataFromHolder() const { return static_cast<const Derived *>(this)->getDataFromHolderImpl(); }

	ColumnConstBase(size_t s_, const DataHolder & data_, DataTypePtr data_type_)
		: s(s_), data(data_), data_type(data_type_) {}

public:
	using Type = T;
	using FieldType = typename NearestFieldType<T>::Type;

	std::string getName() const override { return "ColumnConst<" + TypeName<T>::get() + ">"; }
	bool isNumeric() const override { return IsNumber<T>::value; }
	bool isFixed() const override { return IsNumber<T>::value; }
	size_t sizeOfField() const override { return sizeof(T); }
	ColumnPtr cloneResized(size_t s_) const override { return std::make_shared<Derived>(s_, data, data_type); }
	size_t size() const override { return s; }
	Field operator[](size_t n) const override { return FieldType(getDataFromHolder()); }
	void get(size_t n, Field & res) const override { res = FieldType(getDataFromHolder()); }

	void insertRangeFrom(const IColumn & src, size_t start, size_t length) override
	{
		if (!ColumnConstDetails::equals(getDataFromHolder(), static_cast<const Derived &>(src).getDataFromHolder()))
			throw Exception("Cannot insert different element into constant column " + getName(),
				ErrorCodes::CANNOT_INSERT_ELEMENT_INTO_CONSTANT_COLUMN);

		s += length;
	}

	void insert(const Field & x) override
	{
		if (!ColumnConstDetails::equals(x.get<FieldType>(), FieldType(getDataFromHolder())))
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
		if (!ColumnConstDetails::equals(getDataFromHolder(), static_cast<const Derived &>(src).getDataFromHolder()))
			throw Exception("Cannot insert different element into constant column " + getName(),
				ErrorCodes::CANNOT_INSERT_ELEMENT_INTO_CONSTANT_COLUMN);
		++s;
	}

	void insertDefault() override { ++s; }

	void popBack(size_t n) override
	{
		s -= n;
	}

	StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override
	{
		throw Exception("Method serializeValueIntoArena is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	const char * deserializeAndInsertFromArena(const char * pos) override
	{
		throw Exception("Method deserializeAndInsertFromArena is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	void updateHashWithValue(size_t n, SipHash & hash) const override
	{
		throw Exception("Method updateHashWithValue is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override
	{
		if (s != filt.size())
			throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		return std::make_shared<Derived>(countBytesInFilter(filt), data, data_type);
	}

	ColumnPtr replicate(const Offsets_t & offsets) const override
	{
		if (s != offsets.size())
			throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		size_t replicated_size = 0 == s ? 0 : offsets.back();
		return std::make_shared<Derived>(replicated_size, data, data_type);
	}

	size_t byteSize() const override { return sizeof(data) + sizeof(s); }
	size_t allocatedSize() const override { return byteSize(); }

	ColumnPtr permute(const Permutation & perm, size_t limit) const override
	{
		if (limit == 0)
			limit = s;
		else
			limit = std::min(s, limit);

		if (perm.size() < limit)
			throw Exception("Size of permutation is less than required.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		return std::make_shared<Derived>(limit, data, data_type);
	}

	int compareAt(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const override
	{
		const Derived & rhs = static_cast<const Derived &>(rhs_);
		return getDataFromHolder() < rhs.getDataFromHolder()	/// TODO: правильное сравнение NaN-ов в константных столбцах.
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

	DataTypePtr & getDataType() { return data_type; }
	const DataTypePtr & getDataType() const { return data_type; }
};


/** шаблон для столбцов-констант (столбцов одинаковых значений).
  */
template <typename T>
class ColumnConst final : public ColumnConstBase<T, T, ColumnConst<T>>
{
private:
	friend class ColumnConstBase<T, T, ColumnConst<T>>;

	T & getDataFromHolderImpl() { return this->data; }
	const T & getDataFromHolderImpl() const { return this->data; }

public:
	/// Для ColumnConst<Array> data_type_ должен быть ненулевым.
	/// Для ColumnConst<Tuple> data_type_ должен быть ненулевым.
	/// Для ColumnConst<String> data_type_ должен быть ненулевым, если тип данных FixedString.
	ColumnConst(size_t s_, const T & data_, DataTypePtr data_type_ = DataTypePtr())
		: ColumnConstBase<T, T, ColumnConst<T>>(s_, data_, data_type_) {}

	bool isNull() const override { return false; };
	StringRef getDataAt(size_t n) const override;
	StringRef getDataAtWithTerminatingZero(size_t n) const override;
	UInt64 get64(size_t n) const override;

	/** Более эффективные методы манипуляции */
	T & getData() { return this->data; }
	const T & getData() const { return this->data; }

	/** Преобразование из константы в полноценный столбец */
	ColumnPtr convertToFullColumn() const override;

	void getExtremes(Field & min, Field & max) const override
	{
		min = typename ColumnConstBase<T, T, ColumnConst<T>>::FieldType(this->data);
		max = typename ColumnConstBase<T, T, ColumnConst<T>>::FieldType(this->data);
	}
};


template <>
class ColumnConst<Array> final : public ColumnConstBase<Array, std::shared_ptr<Array>, ColumnConst<Array>>
{
private:
	friend class ColumnConstBase<Array, std::shared_ptr<Array>, ColumnConst<Array>>;

	Array & getDataFromHolderImpl() { return *data; }
	const Array & getDataFromHolderImpl() const { return *data; }

public:
	/// data_type_ должен быть ненулевым.
	ColumnConst(size_t s_, const Array & data_, DataTypePtr data_type_)
		: ColumnConstBase<Array, std::shared_ptr<Array>, ColumnConst<Array>>(s_, std::make_shared<Array>(data_), data_type_) {}

	ColumnConst(size_t s_, const std::shared_ptr<Array> & data_, DataTypePtr data_type_)
		: ColumnConstBase<Array, std::shared_ptr<Array>, ColumnConst<Array>>(s_, data_, data_type_) {}

	StringRef getDataAt(size_t n) const override;
	StringRef getDataAtWithTerminatingZero(size_t n) const override;
	UInt64 get64(size_t n) const override;

	/** Более эффективные методы манипуляции */
	const Array & getData() const { return *data; }

	/** Преобразование из константы в полноценный столбец */
	ColumnPtr convertToFullColumn() const override;

	void getExtremes(Field & min, Field & max) const override
	{
		min = FieldType();
		max = FieldType();
	}
};


template <>
class ColumnConst<Tuple> final : public ColumnConstBase<Tuple, std::shared_ptr<Tuple>, ColumnConst<Tuple>>
{
private:
	friend class ColumnConstBase<Tuple, std::shared_ptr<Tuple>, ColumnConst<Tuple>>;

	Tuple & getDataFromHolderImpl() { return *data; }
	const Tuple & getDataFromHolderImpl() const { return *data; }

public:
	/// data_type_ должен быть ненулевым.
	ColumnConst(size_t s_, const Tuple & data_, DataTypePtr data_type_)
		: ColumnConstBase<Tuple, std::shared_ptr<Tuple>, ColumnConst<Tuple>>(s_, std::make_shared<Tuple>(data_), data_type_) {}

	ColumnConst(size_t s_, const std::shared_ptr<Tuple> & data_, DataTypePtr data_type_)
		: ColumnConstBase<Tuple, std::shared_ptr<Tuple>, ColumnConst<Tuple>>(s_, data_, data_type_) {}

	StringRef getDataAt(size_t n) const override;
	StringRef getDataAtWithTerminatingZero(size_t n) const override;
	UInt64 get64(size_t n) const override;

	/** Более эффективные методы манипуляции */
	const Tuple & getData() const { return *data; }

	/** Преобразование из константы в полноценный столбец */
	ColumnPtr convertToFullColumn() const override;

	/** Create ColumnTuple of constant columns as elements. */
	ColumnPtr convertToTupleOfConstants() const;

	void getExtremes(Field & min, Field & max) const override;
};


using ColumnNull = ColumnConst<Null>;
using ColumnConstString = ColumnConst<String>;
using ColumnConstArray = ColumnConst<Array>;
using ColumnConstTuple = ColumnConst<Tuple>;

template <>
inline bool ColumnConst<Null>::isNull() const
{
	return true;
}

template <>
inline StringRef ColumnConst<Null>::getDataAt(size_t n) const
{
	return {};
}

template <typename T> ColumnPtr ColumnConst<T>::convertToFullColumn() const
{
	std::shared_ptr<ColumnVector<T>> res = std::make_shared<ColumnVector<T>>();
	res->getData().assign(this->s, this->data);
	return res;
}

template <> ColumnPtr ColumnConst<Null>::convertToFullColumn() const;

template <> ColumnPtr ColumnConst<String>::convertToFullColumn() const;


template <typename T> StringRef ColumnConst<T>::getDataAt(size_t n) const
{
	throw Exception("Method getDataAt is not supported for " + this->getName(), ErrorCodes::NOT_IMPLEMENTED);
}

template <> inline StringRef ColumnConst<String>::getDataAt(size_t n) const
{
	return StringRef(data);
}

template <typename T> UInt64 ColumnConst<T>::get64(size_t n) const
{
	throw Exception("Method get64 is not supported for " + this->getName(), ErrorCodes::NOT_IMPLEMENTED);
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
