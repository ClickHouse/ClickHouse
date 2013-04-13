#pragma once

#include <string.h>

#include <DB/Core/Defines.h>

#include <DB/Columns/ColumnArray.h>
#include <DB/Columns/ColumnsNumber.h>


namespace DB
{

/** Cтолбeц значений типа "строка".
  * Отличается от массива UInt8 только получением элемента (в виде String, а не Array),
  *  а также тем, что завершающие нули для строк тоже хранятся.
  */
class ColumnString : public ColumnArray
{
public:
	typedef ColumnUInt8::Container_t DataVector_t;

private:
	DataVector_t & char_data;

public:	
	/** Создать пустой столбец строк, с типом значений */
	ColumnString()
		: ColumnArray(new ColumnUInt8()),
		char_data(static_cast<ColumnUInt8 &>(*data).getData())
	{
	}

	std::string getName() const { return "ColumnString"; }

	ColumnPtr cloneEmpty() const
	{
		return new ColumnString;
	}
	
	ColumnUInt8::Container_t & getDataVector() { return char_data; }
	const ColumnUInt8::Container_t & getDataVector() const { return char_data; }
	
	Field operator[](size_t n) const
	{
		return Field(&char_data[offsetAt(n)], sizeAt(n) - 1);
	}

	void get(size_t n, Field & res) const
	{
		res.assignString(&char_data[offsetAt(n)], sizeAt(n) - 1);
	}

	StringRef getDataAt(size_t n) const
	{
		return StringRef(&char_data[offsetAt(n)], sizeAt(n) - 1);
	}

	StringRef getDataAtWithTerminatingZero(size_t n) const
	{
		return StringRef(&char_data[offsetAt(n)], sizeAt(n));
	}

	void insert(const Field & x)
	{
		const String & s = DB::get<const String &>(x);
		size_t old_size = char_data.size();
		size_t size_to_append = s.size() + 1;
		
		char_data.resize(old_size + size_to_append);
		memcpy(&char_data[old_size], s.c_str(), size_to_append);
		getOffsets().push_back((getOffsets().size() == 0 ? 0 : getOffsets().back()) + size_to_append);
	}

	void insertFrom(const IColumn & src_, size_t n)
	{
		const ColumnString & src = static_cast<const ColumnString &>(src_);
		size_t old_size = char_data.size();
		size_t size_to_append = src.sizeAt(n);
		size_t offset = src.offsetAt(n);
		
		char_data.resize(old_size + size_to_append);
		memcpy(&char_data[old_size], &src.char_data[offset], size_to_append);
		getOffsets().push_back((getOffsets().size() == 0 ? 0 : getOffsets().back()) + size_to_append);
	}

	void insertData(const char * pos, size_t length)
	{
		size_t old_size = char_data.size();

		char_data.resize(old_size + length + 1);
		memcpy(&char_data[old_size], pos, length);
		char_data[old_size + length] = 0;
		getOffsets().push_back((getOffsets().size() == 0 ? 0 : getOffsets().back()) + length + 1);
	}

	void insertDataWithTerminatingZero(const char * pos, size_t length)
	{
		size_t old_size = char_data.size();

		char_data.resize(old_size + length);
		memcpy(&char_data[old_size], pos, length);
		getOffsets().push_back((getOffsets().size() == 0 ? 0 : getOffsets().back()) + length);
	}

	void filter(const Filter & filt)
	{
		size_t size = getOffsets().size();
		if (size != filt.size())
			throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		if (size == 0)
			return;

		ColumnUInt8::Container_t tmp_chars;
		Offsets_t tmp_offsets;
		tmp_chars.reserve(char_data.size());
		tmp_offsets.reserve(size);

		Offset_t current_new_offset = 0;

		for (size_t i = 0; i < size; ++i)
		{
			if (!filt[i])
				continue;
			
			size_t string_offset = i == 0 ? 0 : getOffsets()[i - 1];
			size_t string_size = getOffsets()[i] - string_offset;

			current_new_offset += string_size;
			tmp_offsets.push_back(current_new_offset);

			tmp_chars.resize(tmp_chars.size() + string_size);
			memcpy(&tmp_chars[tmp_chars.size() - string_size], &char_data[string_offset], string_size);
		}

		tmp_chars.swap(char_data);
		tmp_offsets.swap(getOffsets());
	}

	void permute(const Permutation & perm)
	{
		size_t size = getOffsets().size();
		if (size != perm.size())
			throw Exception("Size of permutation doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		if (size == 0)
			return;

		ColumnUInt8::Container_t tmp_chars(char_data.size());
		Offsets_t tmp_offsets(size);

		Offset_t current_new_offset = 0;

		for (size_t i = 0; i < size; ++i)
		{
			size_t j = perm[i];
			size_t string_offset = j == 0 ? 0 : getOffsets()[j - 1];
			size_t string_size = getOffsets()[j] - string_offset;

			memcpy(&tmp_chars[current_new_offset], &char_data[string_offset], string_size);

			current_new_offset += string_size;
			tmp_offsets[i] = current_new_offset;
		}

		tmp_chars.swap(char_data);
		tmp_offsets.swap(getOffsets());
	}

	void insertDefault()
	{
		char_data.push_back(0);
		getOffsets().push_back(getOffsets().size() == 0 ? 1 : (getOffsets().back() + 1));
	}

	int compareAt(size_t n, size_t m, const IColumn & rhs_) const
	{
		const ColumnString & rhs = static_cast<const ColumnString &>(rhs_);

		/** Для производительности, строки сравниваются до первого нулевого байта.
		  * (если нулевой байт в середине строки, то то, что после него - игнорируется)
		  * Замечу, что завершающий нулевой байт всегда есть.
		  */
		return strcmp(
			reinterpret_cast<const char *>(&char_data[offsetAt(n)]),
			reinterpret_cast<const char *>(&rhs.char_data[rhs.offsetAt(m)]));
	}

	struct less
	{
		const ColumnString & parent;
		less(const ColumnString & parent_) : parent(parent_) {}
		bool operator()(size_t lhs, size_t rhs) const
		{
			return 0 > strcmp(
				reinterpret_cast<const char *>(&parent.char_data[parent.offsetAt(lhs)]),
				reinterpret_cast<const char *>(&parent.char_data[parent.offsetAt(rhs)]));
		}
	};

	Permutation getPermutation() const
	{
		size_t s = getOffsets().size();
		Permutation res(s);
		for (size_t i = 0; i < s; ++i)
			res[i] = i;

		std::sort(res.begin(), res.end(), less(*this));
		return res;
	}

	void replicate(const Offsets_t & replicate_offsets)
	{
		size_t col_size = size();
		if (col_size != replicate_offsets.size())
			throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		ColumnUInt8::Container_t tmp_chars;
		Offsets_t tmp_offsets;
		tmp_chars.reserve(char_data.size() / col_size * replicate_offsets.back());
		tmp_offsets.reserve(replicate_offsets.back());

		Offset_t prev_replicate_offset = 0;
		Offset_t prev_string_offset = 0;
		Offset_t current_new_offset = 0;

		for (size_t i = 0; i < col_size; ++i)
		{
			size_t size_to_replicate = replicate_offsets[i] - prev_replicate_offset;
			size_t string_size = getOffsets()[i] - prev_string_offset;

			for (size_t j = 0; j < size_to_replicate; ++j)
			{
				current_new_offset += string_size;
				tmp_offsets.push_back(current_new_offset);

				tmp_chars.resize(tmp_chars.size() + string_size);
				memcpy(&tmp_chars[tmp_chars.size() - string_size], &char_data[prev_string_offset], string_size);
			}

			prev_replicate_offset = replicate_offsets[i];
			prev_string_offset = getOffsets()[i];
		}

		tmp_chars.swap(char_data);
		tmp_offsets.swap(getOffsets());
	}

	void reserve(size_t n)
	{
		getOffsets().reserve(n);
		char_data.reserve(n * DBMS_APPROX_STRING_SIZE);
	}
};


}
