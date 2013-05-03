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
		char_data(static_cast<ColumnUInt8 &>(*data).getData())
	{
	}

	std::string getName() const { return "ColumnFixedString"; }

	ColumnPtr cloneEmpty() const
	{
		return new ColumnFixedString(n);
	}
	
	Field operator[](size_t index) const
	{
		return String(reinterpret_cast<const char *>(&char_data[n * index]), n);
	}

	void get(size_t index, Field & res) const
	{
		res.assignString(reinterpret_cast<const char *>(&char_data[n * index]), n);
	}

	StringRef getDataAt(size_t index) const
	{
		return StringRef(&char_data[n * index], n);
	}

	void insert(const Field & x)
	{
		const String & s = DB::get<const String &>(x);

		if (s.size() > n)
			throw Exception("Too large string '" + s + "' for FixedString column", ErrorCodes::TOO_LARGE_STRING_SIZE);
		
		size_t old_size = char_data.size();
		char_data.resize(old_size + n);
		memcpy(&char_data[old_size], s.data(), s.size());
	}

	void insertFrom(const IColumn & src_, size_t index)
	{
		const ColumnFixedString & src = static_cast<const ColumnFixedString &>(src_);

		if (n != src.getN())
			throw Exception("Size of FixedString doesn't match", ErrorCodes::SIZE_OF_ARRAY_DOESNT_MATCH_SIZE_OF_FIXEDARRAY_COLUMN);

		size_t old_size = char_data.size();
		char_data.resize(old_size + n);
		memcpy(&char_data[old_size], &src.char_data[n * index], n);
	}

	void insertData(const char * pos, size_t length)
	{
		size_t old_size = char_data.size();
		char_data.resize(old_size + n);
		memcpy(&char_data[old_size], pos, n);
	}

	void insertDefault()
	{
		char_data.resize(char_data.size() + n);
	}

	int compareAt(size_t p1, size_t p2, const IColumn & rhs_) const
	{
		const ColumnFixedString & rhs = static_cast<const ColumnFixedString &>(rhs_);
		return memcmp(&char_data[p1 * n], &rhs.char_data[p2 * n], n);
	}

	struct less
	{
		const ColumnFixedString & parent;
		less(const ColumnFixedString & parent_) : parent(parent_) {}
		bool operator()(size_t lhs, size_t rhs) const
		{
			return 0 > memcmp(&parent.char_data[lhs * parent.n], &parent.char_data[rhs * parent.n], parent.n);
		}
	};

	Permutation getPermutation() const
	{
		size_t s = size();
		Permutation res(s);
		for (size_t i = 0; i < s; ++i)
			res[i] = i;

		std::sort(res.begin(), res.end(), less(*this));
		return res;
	}

	ColumnPtr cut(size_t start, size_t length) const
	{
		ColumnFixedString * res_ = new ColumnFixedString(n);
		ColumnPtr res = res_;
		res_->data = data->cut(n * start, n * length);
		return res;
	}

	ColumnPtr filter(const Filter & filt) const
	{
		size_t size = this->size();
		if (size != filt.size())
			throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		if (size == 0)
			return new ColumnFixedString(n);

		/// Не слишком оптимально. Можно сделать специализацию для массивов известных типов.
		Filter nested_filt(size * n);
		for (size_t i = 0; i < size; ++i)
			if (filt[i])
				memset(&nested_filt[i * n], 1, n);

		ColumnFixedString * res_ = new ColumnFixedString(n);
		ColumnPtr res = res_;
		res_->data = data->filter(nested_filt);
		return res;
	}

	ColumnPtr permute(const Permutation & perm) const
	{
		size_t size = this->size();
		if (size != perm.size())
			throw Exception("Size of permutation doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		if (size == 0)
			return new ColumnFixedString(n);

		Permutation nested_perm(size * n);
		for (size_t i = 0; i < size; ++i)
			for (size_t j = 0; j < n; ++j)
				nested_perm[i * n + j] = perm[i] * n + j;

		ColumnFixedString * res_ = new ColumnFixedString(n);
		ColumnPtr res = res_;
		res_->data = data->permute(nested_perm);
		return res;
	}

	ColumnPtr replicate(const Offsets_t & offsets) const
	{
		size_t col_size = size();
		if (col_size != offsets.size())
			throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		ColumnFixedString * res_ = new ColumnFixedString(n);
		ColumnPtr res = res_;
		
		ColumnUInt8::Container_t & res_chars = res_->char_data;
		res_chars.reserve(n * offsets.back());

		Offset_t prev_offset = 0;
		for (size_t i = 0; i < col_size; ++i)
		{
			size_t size_to_replicate = offsets[i] - prev_offset;
			prev_offset = offsets[i];

			for (size_t j = 0; j < size_to_replicate; ++j)
				for (size_t k = 0; k < n; ++k)
					res_chars.push_back(char_data[i * n + k]);
		}

		return res;
	}
};


}
