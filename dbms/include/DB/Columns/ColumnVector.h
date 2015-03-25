#pragma once

#include <string.h>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/Columns/IColumn.h>


namespace DB
{


/** Штука для сравнения чисел.
  * Целые числа сравниваются как обычно.
  * Числа с плавающей запятой сравниваются так, что NaN-ы всегда оказываются в конце
  *  (если этого не делать, то сортировка не работала бы вообще).
  */
template <typename T>
struct CompareHelper
{
	static bool less(T a, T b) { return a < b; }
	static bool greater(T a, T b) { return a > b; }

	/** Сравнивает два числа. Выдаёт число меньше нуля, равное нулю, или больше нуля, если a < b, a == b, a > b, соответственно.
	  * Если одно из значений является NaN, то:
	  * - если nan_direction_hint == -1 - NaN считаются меньше всех чисел;
	  * - если nan_direction_hint == 1 - NaN считаются больше всех чисел;
	  * По-сути: nan_direction_hint == -1 говорит, что сравнение идёт для сортировки по убыванию.
	  */
	static int compare(T a, T b, int nan_direction_hint)
	{
		return a > b ? 1 : (a < b ? -1 : 0);
	}
};

template <typename T>
struct FloatCompareHelper
{
	static bool less(T a, T b)
	{
		if (unlikely(isnan(b)))
			return !isnan(a);
		return a < b;
	}

	static bool greater(T a, T b)
	{
		if (unlikely(isnan(b)))
			return !isnan(a);
		return a > b;
	}

	static int compare(T a, T b, int nan_direction_hint)
	{
		bool isnan_a = isnan(a);
		bool isnan_b = isnan(b);
		if (unlikely(isnan_a || isnan_b))
		{
			if (isnan_a && isnan_b)
				return 0;

			return isnan_a
				? nan_direction_hint
				: -nan_direction_hint;
		}

		return (T(0) < (a - b)) - ((a - b) < T(0));
	}
};

template <> struct CompareHelper<Float32> : public FloatCompareHelper<Float32> {};
template <> struct CompareHelper<Float64> : public FloatCompareHelper<Float64> {};


/** Для реализации функции get64.
  */
template <typename T>
inline UInt64 unionCastToUInt64(T x) { return x; }

template <> inline UInt64 unionCastToUInt64(Float64 x)
{
	union
	{
		Float64 src;
		UInt64 res;
	};

	src = x;
	return res;
}

template <> inline UInt64 unionCastToUInt64(Float32 x)
{
	union
	{
		Float32 src;
		UInt64 res;
	};

	res = 0;
	src = x;
	return res;
}


/** Шаблон столбцов, которые используют для хранения простой массив.
  */
template <typename T>
class ColumnVector final : public IColumn
{
private:
	typedef ColumnVector<T> Self;
public:
	typedef T value_type;
	typedef PODArray<value_type> Container_t;

	ColumnVector() {}
	ColumnVector(const size_t n) : data{n} {}
	ColumnVector(const size_t n, const value_type x) : data{n, x} {}

	bool isNumeric() const override { return IsNumber<T>::value; }
	bool isFixed() const override { return IsNumber<T>::value; }

	size_t sizeOfField() const override { return sizeof(T); }

	size_t size() const override
	{
		return data.size();
	}

	StringRef getDataAt(size_t n) const override
	{
		return StringRef(reinterpret_cast<const char *>(&data[n]), sizeof(data[n]));
	}

	void insertFrom(const IColumn & src, size_t n) override
	{
		data.push_back(static_cast<const Self &>(src).getData()[n]);
	}

	void insertData(const char * pos, size_t length) override
	{
		data.push_back(*reinterpret_cast<const T *>(pos));
	}

	void insertDefault() override
	{
		data.push_back(T());
	}

	size_t byteSize() const override
	{
		return data.size() * sizeof(data[0]);
	}

	void insert(const T value)
	{
		data.push_back(value);
	}

	int compareAt(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const override
	{
		return CompareHelper<T>::compare(data[n], static_cast<const Self &>(rhs_).data[m], nan_direction_hint);
	}

	struct less
	{
		const Self & parent;
		less(const Self & parent_) : parent(parent_) {}
		bool operator()(size_t lhs, size_t rhs) const { return CompareHelper<T>::less(parent.data[lhs], parent.data[rhs]); }
	};

	struct greater
	{
		const Self & parent;
		greater(const Self & parent_) : parent(parent_) {}
		bool operator()(size_t lhs, size_t rhs) const { return CompareHelper<T>::greater(parent.data[lhs], parent.data[rhs]); }
	};

	void getPermutation(bool reverse, size_t limit, Permutation & res) const override
	{
		size_t s = data.size();
		res.resize(s);
		for (size_t i = 0; i < s; ++i)
			res[i] = i;

		if (limit >= s)
			limit = 0;

		if (limit)
		{
			if (reverse)
				std::partial_sort(res.begin(), res.begin() + limit, res.end(), greater(*this));
			else
				std::partial_sort(res.begin(), res.begin() + limit, res.end(), less(*this));
		}
		else
		{
			if (reverse)
				std::sort(res.begin(), res.end(), greater(*this));
			else
				std::sort(res.begin(), res.end(), less(*this));
		}
	}

	void reserve(size_t n) override
	{
		data.reserve(n);
	}

	std::string getName() const override { return "ColumnVector<" + TypeName<T>::get() + ">"; }

	ColumnPtr cloneEmpty() const override
	{
		return new ColumnVector<T>;
	}

	Field operator[](size_t n) const override
	{
		return typename NearestFieldType<T>::Type(data[n]);
	}

	void get(size_t n, Field & res) const override
	{
		res = typename NearestFieldType<T>::Type(data[n]);
	}

	UInt64 get64(size_t n) const override
	{
		return unionCastToUInt64(data[n]);
	}

	void insert(const Field & x) override
	{
		data.push_back(DB::get<typename NearestFieldType<T>::Type>(x));
	}

	ColumnPtr cut(size_t start, size_t length) const override
	{
		if (start + length > data.size())
			throw Exception("Parameters start = "
				+ toString(start) + ", length = "
				+ toString(length) + " are out of bound in IColumnVector<T>::cut() method"
				" (data.size() = " + toString(data.size()) + ").",
				ErrorCodes::PARAMETER_OUT_OF_BOUND);

		Self * res = new Self(length);
		memcpy(&res->getData()[0], &data[start], length * sizeof(data[0]));
		return res;
	}

	ColumnPtr filter(const IColumn::Filter & filt) const override
	{
		size_t size = data.size();
		if (size != filt.size())
			throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		Self * res_ = new Self;
		ColumnPtr res = res_;
		typename Self::Container_t & res_data = res_->getData();
		res_data.reserve(size);

		/** Чуть более оптимизированная версия.
		  * Исходит из допущения, что часто куски последовательно идущих значений
		  *  полностью проходят или полностью не проходят фильтр.
		  * Поэтому, будем оптимистично проверять куски по 16 значений.
		  */
		const UInt8 * filt_pos = &filt[0];
		const UInt8 * filt_end = filt_pos + size;
		const UInt8 * filt_end_sse = filt_pos + size / 16 * 16;
		const T * data_pos = &data[0];

		const __m128i zero16 = _mm_setzero_si128();

		while (filt_pos < filt_end_sse)
		{
			int mask = _mm_movemask_epi8(_mm_cmpgt_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i *>(filt_pos)), zero16));

			if (0 == mask)
			{
				/// Ничего не вставляем.
			}
			else if (0xFFFF == mask)
			{
				res_data.insert_assume_reserved(data_pos, data_pos + 16);
			}
			else
			{
				for (size_t i = 0; i < 16; ++i)
					if (filt_pos[i])
						res_data.push_back(data_pos[i]);
			}

			filt_pos += 16;
			data_pos += 16;
		}

		while (filt_pos < filt_end)
		{
			if (*filt_pos)
				res_data.push_back(*data_pos);

			++filt_pos;
			++data_pos;
		}

		return res;
	}

	ColumnPtr permute(const IColumn::Permutation & perm, size_t limit) const override
	{
		size_t size = data.size();

		if (limit == 0)
			limit = size;
		else
			limit = std::min(size, limit);

		if (perm.size() < limit)
			throw Exception("Size of permutation is less than required.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		Self * res_ = new Self(limit);
		ColumnPtr res = res_;
		typename Self::Container_t & res_data = res_->getData();
		for (size_t i = 0; i < limit; ++i)
			res_data[i] = data[perm[i]];

		return res;
	}

	ColumnPtr replicate(const IColumn::Offsets_t & offsets) const override
	{
		size_t size = data.size();
		if (size != offsets.size())
			throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		if (0 == size)
			return new Self;

		Self * res_ = new Self;
		ColumnPtr res = res_;
		typename Self::Container_t & res_data = res_->getData();
		res_data.reserve(offsets.back());

		IColumn::Offset_t prev_offset = 0;
		for (size_t i = 0; i < size; ++i)
		{
			size_t size_to_replicate = offsets[i] - prev_offset;
			prev_offset = offsets[i];

			for (size_t j = 0; j < size_to_replicate; ++j)
				res_data.push_back(data[i]);
		}

		return res;
	}

	void getExtremes(Field & min, Field & max) const override
	{
		size_t size = data.size();

		if (size == 0)
		{
			min = typename NearestFieldType<T>::Type(0);
			max = typename NearestFieldType<T>::Type(0);
			return;
		}

		T cur_min = data[0];
		T cur_max = data[0];

		for (size_t i = 1; i < size; ++i)
		{
			if (data[i] < cur_min)
				cur_min = data[i];

			if (data[i] > cur_max)
				cur_max = data[i];
		}

		min = typename NearestFieldType<T>::Type(cur_min);
		max = typename NearestFieldType<T>::Type(cur_max);
	}

	/** Более эффективные методы манипуляции */
	Container_t & getData()
	{
		return data;
	}

	const Container_t & getData() const
	{
		return data;
	}

protected:
	Container_t data;
};


}
