#pragma once

#include <string.h> // memcpy

#include <Poco/SharedPtr.h>

#include <DB/Core/Defines.h>
#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/Columns/IColumn.h>
#include <DB/Columns/ColumnsNumber.h>


namespace DB
{

using Poco::SharedPtr;

/** Cтолбeц значений типа вложенная таблица.
  * В памяти это выглядит, как столбцы вложенных типов одинковой длины, равной сумме размеров всех массивов с общим именем,
  *  и как общий для всех столбцов массив смещений, который позволяет достать каждый элемент.
  *
  * Не предназначен для возвращения результа в запросах SELECT. Предполагается, что для SELECT'а будут отдаваться
  * столбцы вида ColumnArray, ссылающиеся на один массив Offset'ов и соответствующий массив с данными.
  *
  * Используется для сериализации вложенной таблицы.
  */
class ColumnNested final : public IColumn
{
public:
	/** По индексу i находится смещение до начала i + 1 -го элемента. */
	typedef ColumnVector<Offset_t> ColumnOffsets_t;

	/** Создать пустой столбец вложенных таблиц, с типом значений, как в столбце nested_column */
	explicit ColumnNested(Columns nested_columns, ColumnPtr offsets_column = nullptr)
		: data(nested_columns), offsets(offsets_column)
	{
		if (!offsets_column)
		{
			offsets = new ColumnOffsets_t;
		}
		else
		{
			if (!typeid_cast<ColumnOffsets_t *>(&*offsets_column))
				throw Exception("offsets_column must be a ColumnVector<UInt64>", ErrorCodes::ILLEGAL_COLUMN);
		}
	}

	std::string getName() const override
	{
		std::string res;
		{
			WriteBufferFromString out(res);

			for (Columns::const_iterator it = data.begin(); it != data.end(); ++it)
			{
				if (it != data.begin())
					writeCString(", ", out);
				writeString((*it)->getName(), out);
			}
		}
		return "ColumnNested(" + res + ")";
	}

	ColumnPtr cloneEmpty() const override
	{
		Columns res(data.size());
		for (size_t i = 0; i < data.size(); ++i)
			res[i] = data[i]->cloneEmpty();
		return new ColumnNested(res);
	}

	size_t size() const override
	{
		return getOffsets().size();
	}

	Field operator[](size_t n) const override
	{
		throw Exception("Method operator[] is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	void get(size_t n, Field & res) const override
	{
		throw Exception("Method get is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	StringRef getDataAt(size_t n) const override
	{
		throw Exception("Method getDataAt is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	void insertData(const char * pos, size_t length) override
	{
		throw Exception("Method insertData is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	ColumnPtr cut(size_t start, size_t length) const override
	{
		if (length == 0)
			return new ColumnNested(data);

		if (start + length > getOffsets().size())
			throw Exception("Parameter out of bound in ColumnNested::cut() method.",
				ErrorCodes::PARAMETER_OUT_OF_BOUND);

		size_t nested_offset = offsetAt(start);
		size_t nested_length = getOffsets()[start + length - 1] - nested_offset;

		ColumnNested * res_ = new ColumnNested(data);
		ColumnPtr res = res_;

		for (size_t i = 0; i < data.size(); ++i)
			res_->data[i] = data[i]->cut(nested_offset, nested_length);

		Offsets_t & res_offsets = res_->getOffsets();

		if (start == 0)
		{
			res_offsets.assign(getOffsets().begin(), getOffsets().begin() + length);
		}
		else
		{
			res_offsets.resize(length);

			for (size_t i = 0; i < length; ++i)
				res_offsets[i] = getOffsets()[start + i] - nested_offset;
		}

		return res;
	}

	void insert(const Field & x) override
	{
		throw Exception("Method insert is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	void insertFrom(const IColumn & src_, size_t n) override
	{
		const ColumnNested & src = static_cast<const ColumnNested &>(src_);

		if (data.size() != src.getData().size())
			throw Exception("Number of columns in nested tables do not match.", ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH);

		size_t size = src.sizeAt(n);
		size_t offset = src.offsetAt(n);

		for (size_t i = 0; i < data.size(); ++i)
		{
			if (data[i]->getName() != src.getData()[i]->getName())
				throw Exception("Types of columns in nested tables do not match.", ErrorCodes::TYPE_MISMATCH);

			for (size_t j = 0; j < size; ++j)
				data[i]->insertFrom(*src.getData()[i], offset + j);
		}

		getOffsets().push_back((getOffsets().size() == 0 ? 0 : getOffsets().back()) + size);
	}

	void insertDefault() override
	{
		for (size_t i = 0; i < data.size(); ++i)
			data[i]->insertDefault();
		getOffsets().push_back(getOffsets().size() == 0 ? 1 : (getOffsets().back() + 1));
	}

	ColumnPtr filter(const Filter & filt) const override
	{
		size_t size = getOffsets().size();
		if (size != filt.size())
			throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		if (size == 0)
			return new ColumnNested(data);

		/// Не слишком оптимально. Можно сделать специализацию для массивов известных типов.
		Filter nested_filt(getOffsets().back());
		for (size_t i = 0; i < size; ++i)
		{
			if (filt[i])
				memset(&nested_filt[offsetAt(i)], 1, sizeAt(i));
			else
				memset(&nested_filt[offsetAt(i)], 0, sizeAt(i));
		}

		ColumnNested * res_ = new ColumnNested(data);
		ColumnPtr res = res_;
		for (size_t i = 0; i < data.size(); ++i)
			res_->data[i] = data[i]->filter(nested_filt);

		Offsets_t & res_offsets = res_->getOffsets();
		res_offsets.reserve(size);

		size_t current_offset = 0;
		for (size_t i = 0; i < size; ++i)
		{
			if (filt[i])
			{
				current_offset += sizeAt(i);
				res_offsets.push_back(current_offset);
			}
		}

		return res;
	}

	ColumnPtr replicate(const Offsets_t & offsets) const override
	{
		throw Exception("Replication of ColumnNested is not implemented.", ErrorCodes::NOT_IMPLEMENTED);
	}

	ColumnPtr permute(const Permutation & perm, size_t limit) const override
	{
		size_t size = getOffsets().size();
		if (size != perm.size())
			throw Exception("Size of permutation doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		if (limit == 0)
			limit = size;
		else
			limit = std::min(size, limit);

		if (perm.size() < limit)
			throw Exception("Size of permutation is less than required.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		if (limit == 0)
 			return new ColumnNested(data);

		Permutation nested_perm(getOffsets().back());

		Columns cloned_columns(data.size());
		for (size_t i = 0; i < data.size(); ++i)
			cloned_columns[i] = data[i]->cloneEmpty();

		ColumnNested * res_ = new ColumnNested(cloned_columns);
		ColumnPtr res = res_;

		Offsets_t & res_offsets = res_->getOffsets();
		res_offsets.resize(limit);
		size_t current_offset = 0;

		for (size_t i = 0; i < limit; ++i)
		{
			for (size_t j = 0; j < sizeAt(perm[i]); ++j)
				nested_perm[current_offset + j] = offsetAt(perm[i]) + j;
			current_offset += sizeAt(perm[i]);
			res_offsets[i] = current_offset;
		}

		if (current_offset != 0)
			for (size_t i = 0; i < data.size(); ++i)
				res_->data[i] = data[i]->permute(nested_perm, current_offset);

		return res;
	}

	int compareAt(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const override
	{
		throw Exception("Method compareAt is not supported for ColumnNested.", ErrorCodes::NOT_IMPLEMENTED);
	}

	void getPermutation(bool reverse, size_t limit, Permutation & res) const override
	{
		throw Exception("Method getPermutation is not supported for ColumnNested.", ErrorCodes::NOT_IMPLEMENTED);
	}

	void reserve(size_t n) override
	{
		getOffsets().reserve(n);
		for (Columns::iterator it = data.begin(); it != data.end(); ++it)
			(*it)->reserve(n);
	}

	size_t byteSize() const override
	{
		size_t size = getOffsets().size() * sizeof(getOffsets()[0]);
		for (Columns::const_iterator it = data.begin(); it != data.end(); ++it)
			size += (*it)->byteSize();
		return size;
	}

	void getExtremes(Field & min, Field & max) const override
	{
		throw Exception("Method getExtremes is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	/** Более эффективные методы манипуляции */
	Columns & getData() { return data; }
	const Columns & getData() const { return data; }

	Offsets_t & ALWAYS_INLINE getOffsets()
	{
		return static_cast<ColumnOffsets_t &>(*offsets.get()).getData();
	}

	const Offsets_t & ALWAYS_INLINE getOffsets() const
	{
		return static_cast<const ColumnOffsets_t &>(*offsets.get()).getData();
	}

	ColumnPtr & getOffsetsColumn() { return offsets; }
	const ColumnPtr & getOffsetsColumn() const { return offsets; }

private:
	Columns data;
	ColumnPtr offsets;

	size_t ALWAYS_INLINE offsetAt(size_t i) const	{ return i == 0 ? 0 : getOffsets()[i - 1]; }
	size_t ALWAYS_INLINE sizeAt(size_t i) const	{ return i == 0 ? getOffsets()[0] : (getOffsets()[i] - getOffsets()[i - 1]); }
};


}
