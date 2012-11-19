#pragma once

#include <string.h>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/Columns/IColumn.h>


namespace DB
{

/** Эта специализация на самом деле не конвертирует в число, но так удобнее. */
template <>
class FieldVisitorConvertToNumber<SharedPtr<IAggregateFunction> > : public boost::static_visitor<SharedPtr<IAggregateFunction> >
{
public:
	typedef SharedPtr<IAggregateFunction> T;
	T operator() (const Null & x) const
	{
		throw Exception("Cannot convert NULL to Aggregate Function", ErrorCodes::CANNOT_CONVERT_TYPE);
	}
	
	T operator() (const String & x) const
	{
		throw Exception("Cannot convert String to Aggregate Function", ErrorCodes::CANNOT_CONVERT_TYPE);
	}
	
	T operator() (const Array & x) const
	{
		throw Exception("Cannot convert Array to Aggregate Function", ErrorCodes::CANNOT_CONVERT_TYPE);
	}
	
	T operator() (const SharedPtr<IAggregateFunction> & x) const
	{
		return x;
	}
	
	T operator() (const UInt64 	& x) const { throw Exception("Cannot convert UInt64 to Aggregate Function", ErrorCodes::CANNOT_CONVERT_TYPE); }
	T operator() (const Int64 	& x) const { throw Exception("Cannot convert Int64 to Aggregate Function", ErrorCodes::CANNOT_CONVERT_TYPE); }
	T operator() (const Float64 & x) const { throw Exception("Cannot convert Float64 to Aggregate Function", ErrorCodes::CANNOT_CONVERT_TYPE); }
};
	
/** Шаблон столбцов, которые используют для хранения std::vector.
  */
template <typename T>
class ColumnVector : public IColumn
{
public:
	typedef T value_type;
	typedef std::vector<value_type> Container_t;

	ColumnVector() {}
	ColumnVector(size_t n) : data(n) {}

 	std::string getName() const { return "ColumnVector<" + TypeName<T>::get() + ">"; }

	bool isNumeric() const { return IsNumber<T>::value; }

	size_t sizeOfField() const { return sizeof(T); }

	ColumnPtr cloneEmpty() const
	{
		return new ColumnVector<T>;
	}

	size_t size() const
	{
		return data.size();
	}
	
	Field operator[](size_t n) const
	{
		return typename NearestFieldType<T>::Type(data[n]);
	}

	StringRef getDataAt(size_t n) const
	{
		return StringRef(reinterpret_cast<const char *>(&data[n]), sizeof(data[n]));
	}
	
	void cut(size_t start, size_t length)
	{
		if (start + length > data.size())
			throw Exception("Parameters start = "
				+ Poco::NumberFormatter::format(start) + ", length = "
				+ Poco::NumberFormatter::format(length) + " are out of bound in IColumnVector<T>::cut() method"
				" (data.size() = " + Poco::NumberFormatter::format(data.size()) + ").",
				ErrorCodes::PARAMETER_OUT_OF_BOUND);

		if (start == 0)
			data.resize(length);
		else
		{
			Container_t tmp(length);
			memcpy(&tmp[0], &data[start], length * sizeof(data[0]));
			tmp.swap(data);
		}
	}

	void insert(const Field & x)
	{
		data.push_back(boost::apply_visitor(FieldVisitorConvertToNumber<typename NearestFieldType<T>::Type>(), x));
	}

	void insertDefault()
	{
		data.push_back(T());
	}

	void clear()
	{
		data.clear();
	}

	void filter(const Filter & filt)
	{
		size_t size = data.size();
		if (size != filt.size())
			throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);
		
		Container_t tmp;
		tmp.reserve(size);
		
		for (size_t i = 0; i < size; ++i)
			if (filt[i])
				tmp.push_back(data[i]);

		tmp.swap(data);
	}

	size_t byteSize() const
	{
		return data.size() * sizeof(data[0]);
	}

	void permute(const Permutation & perm)
	{
		size_t size = data.size();
		if (size != perm.size())
			throw Exception("Size of permutation doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		Container_t tmp(size);
		for (size_t i = 0; i < size; ++i)
			tmp[i] = data[perm[i]];
		tmp.swap(data);
	}

	int compareAt(size_t n, size_t m, const IColumn & rhs_) const
	{
		const ColumnVector<T> & rhs = static_cast<const ColumnVector<T> &>(rhs_);
		return data[n] < rhs.data[m]
			? -1
			: (data[n] == rhs.data[m]
				? 0
				: 1);
	}

	struct less
	{
		const ColumnVector<T> & parent;
		less(const ColumnVector<T> & parent_) : parent(parent_) {}
		bool operator()(size_t lhs, size_t rhs) const { return parent.data[lhs] < parent.data[rhs]; }
	};

	Permutation getPermutation() const
	{
		size_t s = data.size();
		Permutation res(s);
		for (size_t i = 0; i < s; ++i)
			res[i] = i;

		std::sort(res.begin(), res.end(), less(*this));
		
		return res;
	}

	void replicate(const Offsets_t & offsets)
	{
		size_t size = data.size();
		if (size != offsets.size())
			throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		Container_t tmp;
		tmp.reserve(offsets.back());

		Offset_t prev_offset = 0;
		for (size_t i = 0; i < size; ++i)
		{
			size_t size_to_replicate = offsets[i] - prev_offset;
			prev_offset = offsets[i];

			for (size_t j = 0; j < size_to_replicate; ++j)
				tmp.push_back(data[i]);
		}

		tmp.swap(data);
	}

	/** Более эффективные методы манипуляции */
	Container_t & getData()
	{
		return data;
	}

	const Container_t & getData() const
	{
		return data;
	}

protected:
	Container_t data;
};


}
