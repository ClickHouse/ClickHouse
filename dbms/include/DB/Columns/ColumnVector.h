#pragma once

#include <string.h>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/Columns/IColumn.h>


namespace DB
{


/** Шаблон столбцов, которые используют для хранения std::vector.
  */
template <typename T>
class ColumnVectorBase : public IColumn
{
public:
	typedef T value_type;
	typedef std::vector<value_type> Container_t;

	ColumnVectorBase() {}
	ColumnVectorBase(size_t n) : data(n) {}

	bool isNumeric() const { return IsNumber<T>::value; }

	size_t sizeOfField() const { return sizeof(T); }

	size_t size() const
	{
		return data.size();
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

	void insertFrom(const IColumn & src, size_t n)
	{
		data.push_back(static_cast<const ColumnVectorBase<T> &>(src).getData()[n]);
	}

	void insertData(const char * pos, size_t length)
	{
		data.push_back(*reinterpret_cast<const T *>(pos));
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
		const ColumnVectorBase<T> & rhs = static_cast<const ColumnVectorBase<T> &>(rhs_);
		return data[n] < rhs.data[m]
			? -1
			: (data[n] == rhs.data[m]
				? 0
				: 1);
	}

	struct less
	{
		const ColumnVectorBase<T> & parent;
		less(const ColumnVectorBase<T> & parent_) : parent(parent_) {}
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

	void reserve(size_t n)
	{
		data.reserve(n);
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


/** Реализация для числовых типов.
  * (Есть ещё ColumnAggregateFunction.)
  */
template <typename T>
class ColumnVector : public ColumnVectorBase<T>
{
public:
	ColumnVector() {}
	ColumnVector(size_t n) : ColumnVectorBase<T>(n) {}

 	std::string getName() const { return "ColumnVector<" + TypeName<T>::get() + ">"; }

	ColumnPtr cloneEmpty() const
	{
		return new ColumnVector<T>;
	}

	Field operator[](size_t n) const
	{
		return typename NearestFieldType<T>::Type(this->data[n]);
	}

	void get(size_t n, Field & res) const
	{
		res = typename NearestFieldType<T>::Type(this->data[n]);
	}

	void insert(const Field & x)
	{
		this->data.push_back(DB::get<typename NearestFieldType<T>::Type>(x));
	}
};


}
