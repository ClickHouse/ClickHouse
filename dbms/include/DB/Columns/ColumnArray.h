#ifndef DBMS_CORE_COLUMN_ARRAY_H
#define DBMS_CORE_COLUMN_ARRAY_H

#include <string.h> // memcpy

#include <Poco/SharedPtr.h>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/Columns/IColumn.h>


namespace DB
{

using Poco::SharedPtr;

/** Cтолбeц значений типа массив.
  * В памяти он представлен, как один столбец вложенного типа, размер которого равен сумме размеров всех массивов,
  *  и как массив смещений в нём, который позволяет достать каждый элемент.
  */
class ColumnArray : public IColumn
{
public:
	/** По индексу i находится смещение до начала i + 1 -го элемента. */
	typedef std::vector<size_t> Offsets_t;

	/** Создать пустой столбец массивов, с типом значений, как в столбце nested_column */
	ColumnArray(ColumnPtr nested_column)
		: data(nested_column)
	{
		data->clear();
	}

	ColumnPtr cloneEmpty() const
	{
		return new ColumnArray(data->cloneEmpty());
	}
	
	size_t size() const
	{
		return offsets.size();
	}
	
	Field operator[](size_t n) const
	{
		size_t offset = offsetAt(n);
		size_t size = sizeAt(n);
		Array res(size);
		
		for (size_t i = 0; i < size; ++i)
			res[i] = (*data)[offset + i];
			
		return res;
	}

	void cut(size_t start, size_t length)
	{
		if (length == 0 || start + length > offsets.size())
			throw Exception("Parameter out of bound in IColumnArray::cut() method.",
				ErrorCodes::PARAMETER_OUT_OF_BOUND);

		size_t nested_offset = offsetAt(start);
		size_t nested_length = offsets[start + length - 1] - nested_offset;

		data->cut(nested_offset, nested_length);

		if (start == 0)
			offsets.resize(length);
		else
		{
			Offsets_t tmp(length);

			for (size_t i = 0; i < length; ++i)
				tmp[i] = offsets[start + i] - nested_offset;
			
			tmp.swap(offsets);
		}
	}

	void insert(const Field & x)
	{
		Array & array = boost::get<Array &>(x);
		size_t size = array.size();
		for (size_t i = 0; i < size; ++i)
			data->insert(array[i]);
		offsets.push_back((offsets.size() == 0 ? 0 : offsets.back()) + size);
	}

	void insertDefault()
	{
		data->insertDefault();
		offsets.push_back(offsets.size() == 0 ? 1 : (offsets.back() + 1));
	}

	void clear()
	{
		data->clear();
		offsets.clear();
	}

	void filter(const Filter & filt)
	{
		size_t size = offsets.size();
		if (size != filt.size())
			throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		if (size == 0)
			return;

		/// Не слишком оптимально. Можно сделать специализацию для массивов известных типов.
		Filter nested_filt(offsets.back());
		for (size_t i = 0; i < size; ++i)
			if (filt[i])
				memset(&nested_filt[offsetAt(i)], 1, sizeAt(i));
		data->filter(nested_filt);
				
		Offsets_t tmp;
		tmp.reserve(size);

		size_t current_offset = 0;
		for (size_t i = 0; i < size; ++i)
		{
			if (filt[i])
			{
				current_offset += sizeAt(i);
				tmp.push_back(current_offset);
			}
		}

		tmp.swap(offsets);
	}

	/** Более эффективные методы манипуляции */
	IColumn & getData()
	{
		return *data;
	}

	const IColumn & getData() const
	{
		return *data;
	}

	Offsets_t & getOffsets()
	{
		return offsets;
	}

	const Offsets_t & getOffsets() const
	{
		return offsets;
	}

protected:
	ColumnPtr data;
	Offsets_t offsets;

	inline size_t offsetAt(size_t i) const 	{ return i == 0 ? 0 : offsets[i - 1]; }
	inline size_t sizeAt(size_t i) const	{ return i == 0 ? offsets[0] : (offsets[i] - offsets[i - 1]); }
};


}

#endif
