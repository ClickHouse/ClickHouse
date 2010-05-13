#ifndef DBMS_CORE_COLUMN_NULLABLE_H
#define DBMS_CORE_COLUMN_NULLABLE_H

#include <Poco/SharedPtr.h>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/Columns/IColumn.h>


namespace DB
{

using Poco::SharedPtr;

/** Cтолбeц значений, которые могут принимать значения некоторого типа или NULL.
  * В памяти он представлен, как столбец вложенного типа,
  *  а также как массив флагов, является ли соответствующий элемент NULL-ом.
  */
class ColumnNullable : public IColumn
{
public:
	typedef char Flag_t;
	typedef std::vector<Flag_t> Nulls_t;

	/** Создать пустой столбец, с типом значений, как в столбце nested_column */
	ColumnVector(SharedPtr<IColumn> nested_column)
		: data(nested_column)
	{
		data.clear();
	}
	
	size_t size() const
	{
		return data->size();
	}
	
	Field operator[](size_t n) const
	{
		return nulls[n] ? boost::none : (*data)[n];
	}

	void cut(size_t start, size_t length)
	{
		if (start + length > nulls.size())
			throw Exception("Parameter out of bound in IColumnNullable::cut() method.",
				ErrorCodes::PARAMETER_OUT_OF_BOUND);

		if (start == 0)
			nulls.resize(length);
		else
		{
			Nulls_t tmp(length);
			memcpy(&tmp[0], &nulls[start], length * sizeof(nulls[0]));
			tmp.swap(nulls);
		}
	
		data->cut(start, length);
	}

	void clear()
	{
		data.clear();
		nulls.clear();
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

	Nulls_t & getNulls()
	{
		return nulls;
	}

	const Nulls_t & getNulls() const
	{
		return nulls;
	}

private:
	SharedPtr<IColumn> data;
	Nulls_t nulls;
};


}

#endif
