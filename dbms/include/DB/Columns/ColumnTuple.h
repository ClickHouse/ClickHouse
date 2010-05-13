#ifndef DBMS_CORE_COLUMN_TUPLE_H
#define DBMS_CORE_COLUMN_TUPLE_H

#include <vector>

#include <Poco/SharedPtr.h>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>
#include <DB/Column/IColumn.h>


namespace DB
{

using Poco::SharedPtr;

/** Столбец со значениями-кортежами.
  */
class ColumnTuple : public IColumn
{
private:
	typedef std::vector<SharedPtr<IColumn> > Container_t;
	Container_t data;

	/// убедиться в том, что размеры столбцов-элементов совпадают.
	void checkSizes() const
	{
		if (data.empty())
			throw Exception("Empty tuple", ErrorCodes::EMPTY_TUPLE);

		size_t size = data[0]->size();
		for (size_t i = 1; i < data.size(); ++i)
			if (data[i]->size() != size)
				throw Exception("Sizes of columns (elements of tuple column) doesn't match",
					ErrorCodes::SIZES_OF_COLUMNS_IN_TUPLE_DOESNT_MATCH);
	}

public:
	ColumnTuple(const Container_t & data_)
		: data(data_)
	{
		checkSizes();
	}

	size_t size() const
	{
		return data[0]->size();
	}

	Field operator[](size_t n) const
	{
		Array res = Array(data.size());
		for (size_t i = 0; i < data.size(); ++i)
			res[i] = (*data[i])[n];
		return res;
	}

	void cut(size_t start, size_t length)
	{
		for (size_t i = 0; i < data.size(); ++i)
			data[i]->cut(start, length);
	}

	void clear()
	{
		data.clear();
	}

	/// манипуляция с Tuple

	void insertColumn(size_t pos, SharedPtr<IColumn> & column)
	{
		if (pos > data.size())
			throw Exception("Position out of bound in ColumnTuple::insertColumn().",
				ErrorCodes::POSITION_OUT_OF_BOUND);
		
		data.insert(data.begin() + pos, column);
		checkSizes();
	}

	void eraseColumn(size_t pos)
	{
		if (data.size() == 1)
			throw Exception("Empty tuple", ErrorCodes::EMPTY_TUPLE);

		if (pos >= data.size())
			throw Exception("Position out of bound in ColumnTuple::eraseColumn().",
				ErrorCodes::POSITION_OUT_OF_BOUND);
		
		data.erase(data.begin() + pos);
	}
};


}

#endif
