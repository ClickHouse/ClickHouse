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
		size_t offset = n == 0 ? 0 : offsets[n - 1];
		size_t size = offsets[n] - offset;
		Array res(size);
		
		for (size_t i = 0; i < size; ++i)
			res[i] = (*data)[offset + i];
			
		return res;
	}

	void cut(size_t start, size_t length)
	{
		if (start + length > offsets.size())
			throw Exception("Parameter out of bound in IColumnArray::cut() method.",
				ErrorCodes::PARAMETER_OUT_OF_BOUND);

		if (start == 0)
			offsets.resize(length);
		else
		{
			Offsets_t tmp(length);
			memcpy(&tmp[0], &offsets[start], length * sizeof(offsets[0]));
			tmp.swap(offsets);
		}

		size_t nested_offset = start == 0 ? 0 : offsets[start - 1];
		size_t nested_length = offsets[start + length] - nested_offset;
		
		data->cut(nested_offset, nested_length);
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
};


}

#endif
