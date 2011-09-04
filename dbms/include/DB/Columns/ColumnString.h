#pragma once

#include <string.h>

#include <DB/Columns/ColumnArray.h>
#include <DB/Columns/ColumnsNumber.h>


namespace DB
{

/** Cтолбeц значений типа "строка".
  * Отличается от массива UInt8 только получением элемента (в виде String, а не Array)
  */
class ColumnString : public ColumnArray
{
private:
	ColumnUInt8::Container_t & char_data;

public:
	/** Создать пустой столбец строк, с типом значений */
	ColumnString()
		: ColumnArray(new ColumnUInt8()),
		char_data(dynamic_cast<ColumnUInt8 &>(*data).getData())
	{
	}

	std::string getName() const { return "ColumnString"; }

	ColumnPtr cloneEmpty() const
	{
		return new ColumnString;
	}
	
	Field operator[](size_t n) const
	{
		size_t offset = n == 0 ? 0 : offsets[n - 1];
		size_t size = offsets[n] - offset - 1;
		const char * s = reinterpret_cast<const char *>(&dynamic_cast<const ColumnUInt8 &>(*data).getData()[offset]);
		return String(s, size);
	}

	void insert(const Field & x)
	{
		const String & s = boost::get<const String &>(x);
		size_t old_size = char_data.size();
		size_t size_to_append = s.size() + 1;
		char_data.resize(old_size + size_to_append);
		memcpy(&char_data[old_size], s.c_str(), size_to_append);
		offsets.push_back((offsets.size() == 0 ? 0 : offsets.back()) + size_to_append);
	}

	void insertDefault()
	{
		char_data.push_back(0);
		offsets.push_back(offsets.size() == 0 ? 1 : (offsets.back() + 1));
	}

	int compareAt(size_t n, size_t m, const IColumn & rhs_) const
	{
		const ColumnString & rhs = static_cast<const ColumnString &>(rhs_);

		size_t lhs_size = sizeAt(n);
		size_t rhs_size = rhs.sizeAt(m);
		size_t min_size = std::min(lhs_size, rhs_size);
		if (size_t res = memcmp(&char_data[offsetAt(n)], &rhs.char_data[rhs.offsetAt(m)], min_size))
			return res;

		return lhs_size < rhs_size
			? -1
			: (lhs_size == rhs_size
				? 0
				: 1);
		return 0;
	}
};


}
