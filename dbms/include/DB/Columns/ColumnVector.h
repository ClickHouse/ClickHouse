#pragma once

#include <cstring>
#include <cmath>

#include <DB/Common/Exception.h>
#include <DB/Common/Arena.h>
#include <DB/Common/SipHash.h>

#include <DB/IO/WriteBuffer.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/Columns/IColumn.h>

#if defined(__x86_64__)
	#include <emmintrin.h>
#endif


namespace DB
{

namespace ErrorCodes
{
	extern const int PARAMETER_OUT_OF_BOUND;
	extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}


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
		if (unlikely(std::isnan(b)))
			return !std::isnan(a);
		return a < b;
	}

	static bool greater(T a, T b)
	{
		if (unlikely(std::isnan(b)))
			return !std::isnan(a);
		return a > b;
	}

	static int compare(T a, T b, int nan_direction_hint)
	{
		bool isnan_a = std::isnan(a);
		bool isnan_b = std::isnan(b);
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


/// To be sure, that this function is zero-cost for non-floating point types.
template <typename T>
inline bool isNaN(T x)
{
	return std::is_floating_point<T>::value ? std::isnan(x) : false;
}


template <typename T>
typename std::enable_if<std::is_floating_point<T>::value, T>::type NaNOrZero()
{
	return std::numeric_limits<T>::quiet_NaN();
}

template <typename T>
typename std::enable_if<!std::is_floating_point<T>::value, T>::type NaNOrZero()
{
	return 0;
}


/** Шаблон столбцов, которые используют для хранения простой массив.
  */
template <typename T>
class ColumnVector final : public IColumn
{
private:
	using Self = ColumnVector<T>;
public:
	using value_type = T;
	using Container_t = PaddedPODArray<value_type>;

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

	void popBack(size_t n) override
	{
		data.resize_assume_reserved(data.size() - n);
	}

	StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override
	{
		auto pos = arena.allocContinue(sizeof(T), begin);
		memcpy(pos, &data[n], sizeof(T));
		return StringRef(pos, sizeof(T));
	}

	const char * deserializeAndInsertFromArena(const char * pos) override
	{
		data.push_back(*reinterpret_cast<const T *>(pos));
		return pos + sizeof(T);
	}

	void updateHashWithValue(size_t n, SipHash & hash) const override
	{
		hash.update(reinterpret_cast<const char *>(&data[n]), sizeof(T));
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

	ColumnPtr cloneResized(size_t size) const override
	{
		ColumnPtr new_col_holder = std::make_shared<Self>();

		if (size > 0)
		{
			auto & new_col = static_cast<Self &>(*new_col_holder);
			new_col.data.resize(size);

			size_t count = std::min(this->size(), size);
			memcpy(&new_col.data[0], &data[0], count * sizeof(data[0]));

			if (size > count)
				memset(&new_col.data[count], value_type(), size - count);
		}

		return new_col_holder;
	}

	Field operator[](size_t n) const override
	{
		return typename NearestFieldType<T>::Type(data[n]);
	}

	void get(size_t n, Field & res) const override
	{
		res = typename NearestFieldType<T>::Type(data[n]);
	}

	const T & getElement(size_t n) const
	{
		return data[n];
	}

	T & getElement(size_t n)
	{
		return data[n];
	}

	UInt64 get64(size_t n) const override
	{
		return unionCastToUInt64(data[n]);
	}

	void insert(const Field & x) override
	{
		data.push_back(DB::get<typename NearestFieldType<T>::Type>(x));
	}

	void insertRangeFrom(const IColumn & src, size_t start, size_t length) override
	{
		const ColumnVector & src_vec = static_cast<const ColumnVector &>(src);

		if (start + length > src_vec.data.size())
			throw Exception("Parameters start = "
				+ toString(start) + ", length = "
				+ toString(length) + " are out of bound in ColumnVector::insertRangeFrom method"
				" (data.size() = " + toString(src_vec.data.size()) + ").",
				ErrorCodes::PARAMETER_OUT_OF_BOUND);

		size_t old_size = data.size();
		data.resize(old_size + length);
		memcpy(&data[old_size], &src_vec.data[start], length * sizeof(data[0]));
	}

	ColumnPtr filter(const IColumn::Filter & filt, ssize_t result_size_hint) const override
	{
		size_t size = data.size();
		if (size != filt.size())
			throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		std::shared_ptr<Self> res = std::make_shared<Self>();
		typename Self::Container_t & res_data = res->getData();

		if (result_size_hint)
			res_data.reserve(result_size_hint > 0 ? result_size_hint : size);

		const UInt8 * filt_pos = &filt[0];
		const UInt8 * filt_end = filt_pos + size;
		const T * data_pos = &data[0];

#if defined(__x86_64__)
		/** Чуть более оптимизированная версия.
		 * Исходит из допущения, что часто куски последовательно идущих значений
		 *  полностью проходят или полностью не проходят фильтр.
		 * Поэтому, будем оптимистично проверять куски по SIMD_BYTES значений.
		 */

		static constexpr size_t SIMD_BYTES = 16;
		const __m128i zero16 = _mm_setzero_si128();
		const UInt8 * filt_end_sse = filt_pos + size / SIMD_BYTES * SIMD_BYTES;

		while (filt_pos < filt_end_sse)
		{
			int mask = _mm_movemask_epi8(_mm_cmpgt_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i *>(filt_pos)), zero16));

			if (0 == mask)
			{
				/// Ничего не вставляем.
			}
			else if (0xFFFF == mask)
			{
				res_data.insert(data_pos, data_pos + SIMD_BYTES);
			}
			else
			{
				for (size_t i = 0; i < SIMD_BYTES; ++i)
					if (filt_pos[i])
						res_data.push_back(data_pos[i]);
			}

			filt_pos += SIMD_BYTES;
			data_pos += SIMD_BYTES;
		}
#endif

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

		std::shared_ptr<Self> res = std::make_shared<Self>(limit);
		typename Self::Container_t & res_data = res->getData();
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
			return std::make_shared<Self>();

		std::shared_ptr<Self> res = std::make_shared<Self>();
		typename Self::Container_t & res_data = res->getData();
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

		bool has_value = false;

		/** Skip all NaNs in extremes calculation.
		  * If all values are NaNs, then return NaN.
		  * NOTE: There exist many different NaNs.
		  * Different NaN could be returned: not bit-exact value as one of NaNs from column.
		  */

		T cur_min = NaNOrZero<T>();
		T cur_max = NaNOrZero<T>();

		for (const T x : data)
		{
			if (isNaN(x))
				continue;

			if (!has_value)
			{
				cur_min = x;
				cur_max = x;
				has_value = true;
				continue;
			}

			if (x < cur_min)
				cur_min = x;

			if (x > cur_max)
				cur_max = x;
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
