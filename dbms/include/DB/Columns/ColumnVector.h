#ifndef DBMS_CORE_COLUMN_VECTOR_H
#define DBMS_CORE_COLUMN_VECTOR_H

#include <string.h>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/Columns/IColumn.h>


namespace DB
{

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

	bool isNumeric() const { return IsNumber<T>::value; }

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
	
	void cut(size_t start, size_t length)
	{
		if (length == 0 || start + length > data.size())
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
		data.push_back(boost::get<typename NearestFieldType<T>::Type>(x));
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
		{
			if (filt[i])
			{
				tmp.push_back(T());
				std::swap(tmp.back(), data[i]);
			}
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

private:
	Container_t data;
};


}

#endif
