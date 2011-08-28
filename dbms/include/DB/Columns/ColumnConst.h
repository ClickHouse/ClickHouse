#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Core/Field.h>
#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>
#include <DB/Columns/IColumn.h>


namespace DB
{

using Poco::SharedPtr;

/** шаблон для столбцов-констант (столбцов одинаковых значений).
  */
template <typename T>
class ColumnConst : public IColumn
{
public:
	typedef T Type;
	
	ColumnConst(size_t s_, const T & data_) : s(s_), data(data_) {}

	std::string getName() const { return "ColumnConst<" + TypeName<T>::get() + ">"; }
	bool isNumeric() const { return IsNumber<T>::value; }
	ColumnPtr cloneEmpty() const { return new ColumnConst(0, data); }
	size_t size() const { return s; }
	Field operator[](size_t n) const { return typename NearestFieldType<T>::Type(data); }
	void cut(size_t start, size_t length) { s = length; }
	void clear() { s = 0; }
	
	void insert(const Field & x)
	{
		throw Exception("Cannot insert element into constant column " + getName(), ErrorCodes::CANNOT_INSERT_ELEMENT_INTO_CONSTANT_COLUMN);
	}
	
	void insertDefault() { ++s; }

	void filter(const Filter & filt)
	{
		size_t new_size = 0;
		for (Filter::const_iterator it = filt.begin(); it != filt.end(); ++it)
			if (*it)
				++new_size;
		s = new_size;
	}

	size_t byteSize() { return sizeof(data) + sizeof(s); }

	/** Более эффективные методы манипуляции */
	T & getData() { return data; }
	const T & getData() const { return data; }

	/** Преобразование из константы в полноценный столбец */
//	virtual ColumnPtr convertToFullColumn() const = 0;

private:
	size_t s;
	T data;
};


typedef ColumnConst<String> ColumnConstString;

}
