#pragma once

#include <string.h> // memcpy

#include <DB/Columns/ColumnFixedArray.h>
#include <DB/Columns/ColumnsNumber.h>


namespace DB
{

/** Cтолбeц значений типа "строка фиксированной длины".
  * Отличается от массива UInt8 фиксированной длины только получением элемента (в виде String, а не Array)
  * Если вставить строку меньшей длины, то она будет дополнена нулевыми байтами.
  */
class ColumnFixedString : public ColumnFixedArray
{
private:
	ColumnUInt8::Container_t & char_data;

public:
	/** Создать пустой столбец строк фиксированной длины n */
	ColumnFixedString(size_t n)
		: ColumnFixedArray(new ColumnUInt8(), n),
		char_data(dynamic_cast<ColumnUInt8 &>(*data).getData())
	{
	}

	ColumnPtr cloneEmpty() const
	{
		return new ColumnFixedString(n);
	}
	
	Field operator[](size_t index) const
	{
		return String(reinterpret_cast<const char *>(&char_data[n * index]), n);
	}

	void insert(const Field & x)
	{
		const String & s = boost::get<const String &>(x);
		size_t old_size = char_data.size();
		char_data.resize(old_size + n);
		memcpy(&char_data[old_size], s.data(), s.size());
	}

	void insertDefault()
	{
		char_data.resize(char_data.size() + n);
	}
};


}
