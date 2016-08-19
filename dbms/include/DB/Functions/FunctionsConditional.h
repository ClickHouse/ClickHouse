#pragma once

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeArray.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeFixedString.h>
#include <DB/DataTypes/DataTypeTuple.h>
#include <DB/Columns/ColumnVector.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Columns/ColumnArray.h>
#include <DB/Columns/ColumnFixedString.h>
#include <DB/Columns/ColumnTuple.h>
#include <DB/Functions/IFunction.h>
#include <DB/Functions/NumberTraits.h>
#include <DB/Functions/DataTypeTraits.h>

/// The following includes are needed for the function multiIf.
#include <DB/Functions/Conditional/CondException.h>
#include <DB/Functions/Conditional/common.h>
#include <DB/Functions/Conditional/ArgsInfo.h>
#include <DB/Functions/Conditional/NumericPerformer.h>
#include <DB/Functions/Conditional/StringEvaluator.h>
#include <DB/Functions/Conditional/StringArrayEvaluator.h>

namespace DB
{

/** Функция выбора по условию: if(cond, then, else).
  * cond - UInt8
  * then, else - числовые типы, для которых есть общий тип, либо даты, даты-с-временем, либо строки, либо массивы таких типов.
  */


template <typename A, typename B, typename ResultType>
struct NumIfImpl
{
private:
	static PaddedPODArray<ResultType> & result_vector(Block & block, size_t result, size_t size)
	{
		auto col_res = std::make_shared<ColumnVector<ResultType>>();
		block.getByPosition(result).column = col_res;

		typename ColumnVector<ResultType>::Container_t & vec_res = col_res->getData();
		vec_res.resize(size);

		return vec_res;
	}
public:
	static void vector_vector(
		const PaddedPODArray<UInt8> & cond,
		const PaddedPODArray<A> & a, const PaddedPODArray<B> & b,
		Block & block,
		size_t result)
	{
		size_t size = cond.size();
		PaddedPODArray<ResultType> & res = result_vector(block, result, size);
		for (size_t i = 0; i < size; ++i)
			res[i] = cond[i] ? static_cast<ResultType>(a[i]) : static_cast<ResultType>(b[i]);
	}

	static void vector_constant(
		const PaddedPODArray<UInt8> & cond,
		const PaddedPODArray<A> & a, B b,
		Block & block,
		size_t result)
	{
		size_t size = cond.size();
		PaddedPODArray<ResultType> & res = result_vector(block, result, size);
		for (size_t i = 0; i < size; ++i)
			res[i] = cond[i] ? static_cast<ResultType>(a[i]) : static_cast<ResultType>(b);
	}

	static void constant_vector(
		const PaddedPODArray<UInt8> & cond,
		A a, const PaddedPODArray<B> & b,
		Block & block,
		size_t result)
	{
		size_t size = cond.size();
		PaddedPODArray<ResultType> & res = result_vector(block, result, size);
		for (size_t i = 0; i < size; ++i)
			res[i] = cond[i] ? static_cast<ResultType>(a) : static_cast<ResultType>(b[i]);
	}

	static void constant_constant(
		const PaddedPODArray<UInt8> & cond,
		A a, B b,
		Block & block,
		size_t result)
	{
		size_t size = cond.size();
		PaddedPODArray<ResultType> & res = result_vector(block, result, size);
		for (size_t i = 0; i < size; ++i)
			res[i] = cond[i] ? static_cast<ResultType>(a) : static_cast<ResultType>(b);
	}
};

template <typename A, typename B>
struct NumIfImpl<A, B, NumberTraits::Error>
{
private:
	static void throw_error()
	{
		throw Exception("Internal logic error: invalid types of arguments 2 and 3 of if", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
public:
	static void vector_vector(
		const PaddedPODArray<UInt8> & cond,
		const PaddedPODArray<A> & a, const PaddedPODArray<B> & b,
		Block & block,
		size_t result)
	{
		throw_error();
	}

	static void vector_constant(
		const PaddedPODArray<UInt8> & cond,
		const PaddedPODArray<A> & a, B b,
		Block & block,
		size_t result)
	{
		throw_error();
	}

	static void constant_vector(
		const PaddedPODArray<UInt8> & cond,
		A a, const PaddedPODArray<B> & b,
		Block & block,
		size_t result)
	{
		throw_error();
	}

	static void constant_constant(
		const PaddedPODArray<UInt8> & cond,
		A a, B b,
		Block & block,
		size_t result)
	{
		throw_error();
	}
};


struct StringIfImpl
{
	static void vector_vector(
		const PaddedPODArray<UInt8> & cond,
		const ColumnString::Chars_t & a_data, const ColumnString::Offsets_t & a_offsets,
		const ColumnString::Chars_t & b_data, const ColumnString::Offsets_t & b_offsets,
		ColumnString::Chars_t & c_data, ColumnString::Offsets_t & c_offsets)
	{
		size_t size = cond.size();
		c_offsets.resize(size);
		c_data.reserve(std::max(a_data.size(), b_data.size()));

		ColumnString::Offset_t a_prev_offset = 0;
		ColumnString::Offset_t b_prev_offset = 0;
		ColumnString::Offset_t c_prev_offset = 0;

		for (size_t i = 0; i < size; ++i)
		{
			if (cond[i])
			{
				size_t size_to_write = a_offsets[i] - a_prev_offset;
				c_data.resize(c_data.size() + size_to_write);
				memcpySmallAllowReadWriteOverflow15(&c_data[c_prev_offset], &a_data[a_prev_offset], size_to_write);
				c_prev_offset += size_to_write;
				c_offsets[i] = c_prev_offset;
			}
			else
			{
				size_t size_to_write = b_offsets[i] - b_prev_offset;
				c_data.resize(c_data.size() + size_to_write);
				memcpySmallAllowReadWriteOverflow15(&c_data[c_prev_offset], &b_data[b_prev_offset], size_to_write);
				c_prev_offset += size_to_write;
				c_offsets[i] = c_prev_offset;
			}

			a_prev_offset = a_offsets[i];
			b_prev_offset = b_offsets[i];
		}
	}

	static void vector_fixed_vector_fixed(
		const PaddedPODArray<UInt8> & cond,
		const ColumnFixedString::Chars_t & a_data,
		const ColumnFixedString::Chars_t & b_data,
		const size_t N,
		ColumnFixedString::Chars_t & c_data)
	{
		size_t size = cond.size();
		c_data.resize(a_data.size());

		for (size_t i = 0; i < size; ++i)
		{
			if (cond[i])
				memcpySmallAllowReadWriteOverflow15(&c_data[i * N], &a_data[i * N], N);
			else
				memcpySmallAllowReadWriteOverflow15(&c_data[i * N], &b_data[i * N], N);
		}
	}

	template <bool negative>
	static void vector_vector_fixed_impl(
		const PaddedPODArray<UInt8> & cond,
		const ColumnString::Chars_t & a_data, const ColumnString::Offsets_t & a_offsets,
		const ColumnFixedString::Chars_t & b_data, const size_t b_N,
		ColumnString::Chars_t & c_data, ColumnString::Offsets_t & c_offsets)
	{
		size_t size = cond.size();
		c_offsets.resize(size);
		c_data.reserve(std::max(a_data.size(), b_data.size() + size));

		ColumnString::Offset_t a_prev_offset = 0;
		ColumnString::Offset_t c_prev_offset = 0;

		for (size_t i = 0; i < size; ++i)
		{
			if (negative != cond[i])
			{
				size_t size_to_write = a_offsets[i] - a_prev_offset;
				c_data.resize(c_data.size() + size_to_write);
				memcpySmallAllowReadWriteOverflow15(&c_data[c_prev_offset], &a_data[a_prev_offset], size_to_write);
				c_prev_offset += size_to_write;
				c_offsets[i] = c_prev_offset;
			}
			else
			{
				size_t size_to_write = b_N;
				c_data.resize(c_data.size() + size_to_write + 1);
				memcpySmallAllowReadWriteOverflow15(&c_data[c_prev_offset], &b_data[i * b_N], size_to_write);
				c_data.back() = 0;
				c_prev_offset += size_to_write + 1;
				c_offsets[i] = c_prev_offset;
			}

			a_prev_offset = a_offsets[i];
		}
	}

	static void vector_vector_fixed(
		const PaddedPODArray<UInt8> & cond,
		const ColumnString::Chars_t & a_data, const ColumnString::Offsets_t & a_offsets,
		const ColumnFixedString::Chars_t & b_data, const size_t b_N,
		ColumnString::Chars_t & c_data, ColumnString::Offsets_t & c_offsets)
	{
		vector_vector_fixed_impl<false>(cond, a_data, a_offsets, b_data, b_N, c_data, c_offsets);
	}

	static void vector_fixed_vector(
		const PaddedPODArray<UInt8> & cond,
		const ColumnFixedString::Chars_t & a_data, const size_t a_N,
		const ColumnString::Chars_t & b_data, const ColumnString::Offsets_t & b_offsets,
		ColumnString::Chars_t & c_data, ColumnString::Offsets_t & c_offsets)
	{
		vector_vector_fixed_impl<true>(cond, b_data, b_offsets, a_data, a_N, c_data, c_offsets);
	}

	template <bool negative>
	static void vector_constant_impl(
		const PaddedPODArray<UInt8> & cond,
		const ColumnString::Chars_t & a_data, const ColumnString::Offsets_t & a_offsets,
		const String & b,
		ColumnString::Chars_t & c_data, ColumnString::Offsets_t & c_offsets)
	{
		size_t size = cond.size();
		c_offsets.resize(size);
		c_data.reserve(a_data.size());

		ColumnString::Offset_t a_prev_offset = 0;
		ColumnString::Offset_t c_prev_offset = 0;

		for (size_t i = 0; i < size; ++i)
		{
			if (negative != cond[i])
			{
				size_t size_to_write = a_offsets[i] - a_prev_offset;
				c_data.resize(c_data.size() + size_to_write);
				memcpySmallAllowReadWriteOverflow15(&c_data[c_prev_offset], &a_data[a_prev_offset], size_to_write);
				c_prev_offset += size_to_write;
				c_offsets[i] = c_prev_offset;
			}
			else
			{
				size_t size_to_write = b.size() + 1;
				c_data.resize(c_data.size() + size_to_write);
				memcpy(&c_data[c_prev_offset], b.data(), size_to_write);
				c_prev_offset += size_to_write;
				c_offsets[i] = c_prev_offset;
			}

			a_prev_offset = a_offsets[i];
		}
	}

	static void vector_constant(
		const PaddedPODArray<UInt8> & cond,
		const ColumnString::Chars_t & a_data, const ColumnString::Offsets_t & a_offsets,
		const String & b,
		ColumnString::Chars_t & c_data, ColumnString::Offsets_t & c_offsets)
	{
		return vector_constant_impl<false>(cond, a_data, a_offsets, b, c_data, c_offsets);
	}

	static void constant_vector(
		const PaddedPODArray<UInt8> & cond,
		const String & a,
		const ColumnString::Chars_t & b_data, const ColumnString::Offsets_t & b_offsets,
		ColumnString::Chars_t & c_data, ColumnString::Offsets_t & c_offsets)
	{
		return vector_constant_impl<true>(cond, b_data, b_offsets, a, c_data, c_offsets);
	}

	template <bool negative>
	static void vector_fixed_constant_impl(
		const PaddedPODArray<UInt8> & cond,
		const ColumnFixedString::Chars_t & a_data, const size_t a_N,
		const String & b,
		ColumnString::Chars_t & c_data, ColumnString::Offsets_t & c_offsets)
	{
		size_t size = cond.size();
		c_offsets.resize(size);
		c_data.reserve(a_data.size());

		ColumnString::Offset_t c_prev_offset = 0;

		for (size_t i = 0; i < size; ++i)
		{
			if (negative != cond[i])
			{
				size_t size_to_write = a_N;
				c_data.resize(c_data.size() + size_to_write + 1);
				memcpySmallAllowReadWriteOverflow15(&c_data[c_prev_offset], &a_data[i * a_N], size_to_write);
				c_data.back() = 0;
				c_prev_offset += size_to_write + 1;
				c_offsets[i] = c_prev_offset;
			}
			else
			{
				size_t size_to_write = b.size() + 1;
				c_data.resize(c_data.size() + size_to_write);
				memcpy(&c_data[c_prev_offset], b.data(), size_to_write);
				c_prev_offset += size_to_write;
				c_offsets[i] = c_prev_offset;
			}
		}
	}

	static void vector_fixed_constant(
		const PaddedPODArray<UInt8> & cond,
		const ColumnFixedString::Chars_t & a_data, const size_t N,
		const String & b,
		ColumnString::Chars_t & c_data, ColumnString::Offsets_t & c_offsets)
	{
		vector_fixed_constant_impl<false>(cond, a_data, N, b, c_data, c_offsets);
	}

	static void constant_vector_fixed(
		const PaddedPODArray<UInt8> & cond,
		const String & a,
		const ColumnFixedString::Chars_t & b_data, const size_t N,
		ColumnString::Chars_t & c_data, ColumnString::Offsets_t & c_offsets)
	{
		vector_fixed_constant_impl<true>(cond, b_data, N, a, c_data, c_offsets);
	}

	static void constant_constant(
		const PaddedPODArray<UInt8> & cond,
		const String & a, const String & b,
		ColumnString::Chars_t & c_data, ColumnString::Offsets_t & c_offsets)
	{
		size_t size = cond.size();
		c_offsets.resize(size);
		c_data.reserve((std::max(a.size(), b.size()) + 1) * size);

		ColumnString::Offset_t c_prev_offset = 0;

		for (size_t i = 0; i < size; ++i)
		{
			if (cond[i])
			{
				size_t size_to_write = a.size() + 1;
				c_data.resize(c_data.size() + size_to_write);
				memcpy(&c_data[c_prev_offset], a.data(), size_to_write);
				c_prev_offset += size_to_write;
				c_offsets[i] = c_prev_offset;
			}
			else
			{
				size_t size_to_write = b.size() + 1;
				c_data.resize(c_data.size() + size_to_write);
				memcpy(&c_data[c_prev_offset], b.data(), size_to_write);
				c_prev_offset += size_to_write;
				c_offsets[i] = c_prev_offset;
			}
		}
	}
};


template <typename A, typename B, typename ResultType>
struct NumArrayIfImpl
{
	template <typename FromT>
	static ALWAYS_INLINE void copy_from_vector(
		size_t i,
		const PaddedPODArray<FromT> & from_data, const ColumnArray::Offsets_t & from_offsets, ColumnArray::Offset_t from_prev_offset,
		PaddedPODArray<ResultType> & to_data, ColumnArray::Offsets_t & to_offsets, ColumnArray::Offset_t & to_prev_offset)
	{
		size_t size_to_write = from_offsets[i] - from_prev_offset;
		to_data.resize(to_data.size() + size_to_write);

		for (size_t i = 0; i < size_to_write; ++i)
			to_data[to_prev_offset + i] = static_cast<ResultType>(from_data[from_prev_offset + i]);

		to_prev_offset += size_to_write;
		to_offsets[i] = to_prev_offset;
	}

	static ALWAYS_INLINE void copy_from_constant(
		size_t i,
		const PaddedPODArray<ResultType> & from_data,
		PaddedPODArray<ResultType> & to_data, ColumnArray::Offsets_t & to_offsets, ColumnArray::Offset_t & to_prev_offset)
	{
		size_t size_to_write = from_data.size();
		to_data.resize(to_data.size() + size_to_write);
		memcpy(&to_data[to_prev_offset], from_data.data(), size_to_write * sizeof(from_data[0]));
		to_prev_offset += size_to_write;
		to_offsets[i] = to_prev_offset;
	}

	static void create_result_column(
		Block & block, size_t result,
		PaddedPODArray<ResultType> ** c_data, ColumnArray::Offsets_t ** c_offsets)
	{
		auto col_res_vec = std::make_shared<ColumnVector<ResultType>>();
		auto col_res_array = std::make_shared<ColumnArray>(col_res_vec);
		block.getByPosition(result).column = col_res_array;

		*c_data = &col_res_vec->getData();
		*c_offsets = &col_res_array->getOffsets();
	}


	static void vector_vector(
		const PaddedPODArray<UInt8> & cond,
		const PaddedPODArray<A> & a_data, const ColumnArray::Offsets_t & a_offsets,
		const PaddedPODArray<B> & b_data, const ColumnArray::Offsets_t & b_offsets,
		Block & block, size_t result)
	{
		PaddedPODArray<ResultType> * c_data = nullptr;
		ColumnArray::Offsets_t * c_offsets = nullptr;
		create_result_column(block, result, &c_data, &c_offsets);

		size_t size = cond.size();
		c_offsets->resize(size);
		c_data->reserve(std::max(a_data.size(), b_data.size()));

		ColumnArray::Offset_t a_prev_offset = 0;
		ColumnArray::Offset_t b_prev_offset = 0;
		ColumnArray::Offset_t c_prev_offset = 0;

		for (size_t i = 0; i < size; ++i)
		{
			if (cond[i])
				copy_from_vector(i, a_data, a_offsets, a_prev_offset, *c_data, *c_offsets, c_prev_offset);
			else
				copy_from_vector(i, b_data, b_offsets, b_prev_offset, *c_data, *c_offsets, c_prev_offset);

			a_prev_offset = a_offsets[i];
			b_prev_offset = b_offsets[i];
		}
	}

	static void vector_constant(
		const PaddedPODArray<UInt8> & cond,
		const PaddedPODArray<A> & a_data, const ColumnArray::Offsets_t & a_offsets,
		const Array & b,
		Block & block, size_t result)
	{
		PaddedPODArray<ResultType> * c_data = nullptr;
		ColumnArray::Offsets_t * c_offsets = nullptr;
		create_result_column(block, result, &c_data, &c_offsets);

		PaddedPODArray<ResultType> b_converted(b.size());
		for (size_t i = 0, size = b.size(); i < size; ++i)
			b_converted[i] = b[i].get<typename NearestFieldType<B>::Type>();

		size_t size = cond.size();
		c_offsets->resize(size);
		c_data->reserve(a_data.size());

		ColumnArray::Offset_t a_prev_offset = 0;
		ColumnArray::Offset_t c_prev_offset = 0;

		for (size_t i = 0; i < size; ++i)
		{
			if (cond[i])
				copy_from_vector(i, a_data, a_offsets, a_prev_offset, *c_data, *c_offsets, c_prev_offset);
			else
				copy_from_constant(i, b_converted, *c_data, *c_offsets, c_prev_offset);

			a_prev_offset = a_offsets[i];
		}
	}

	static void constant_vector(
		const PaddedPODArray<UInt8> & cond,
		const Array & a,
		const PaddedPODArray<B> & b_data, const ColumnArray::Offsets_t & b_offsets,
		Block & block, size_t result)
	{
		PaddedPODArray<ResultType> * c_data = nullptr;
		ColumnArray::Offsets_t * c_offsets = nullptr;
		create_result_column(block, result, &c_data, &c_offsets);

		PaddedPODArray<ResultType> a_converted(a.size());
		for (size_t i = 0, size = a.size(); i < size; ++i)
			a_converted[i] = a[i].get<typename NearestFieldType<A>::Type>();

		size_t size = cond.size();
		c_offsets->resize(size);
		c_data->reserve(b_data.size());

		ColumnArray::Offset_t b_prev_offset = 0;
		ColumnArray::Offset_t c_prev_offset = 0;

		for (size_t i = 0; i < size; ++i)
		{
			if (cond[i])
				copy_from_constant(i, a_converted, *c_data, *c_offsets, c_prev_offset);
			else
				copy_from_vector(i, b_data, b_offsets, b_prev_offset, *c_data, *c_offsets, c_prev_offset);

			b_prev_offset = b_offsets[i];
		}
	}

	static void constant_constant(
		const PaddedPODArray<UInt8> & cond,
		const Array & a, const Array & b,
		Block & block, size_t result)
	{
		PaddedPODArray<ResultType> * c_data = nullptr;
		ColumnArray::Offsets_t * c_offsets = nullptr;
		create_result_column(block, result, &c_data, &c_offsets);

		PaddedPODArray<ResultType> a_converted(a.size());
		for (size_t i = 0, size = a.size(); i < size; ++i)
			a_converted[i] = a[i].get<typename NearestFieldType<A>::Type>();

		PaddedPODArray<ResultType> b_converted(b.size());
		for (size_t i = 0, size = b.size(); i < size; ++i)
			b_converted[i] = b[i].get<typename NearestFieldType<B>::Type>();

		size_t size = cond.size();
		c_offsets->resize(size);
		c_data->reserve((std::max(a.size(), b.size())) * size);

		ColumnArray::Offset_t c_prev_offset = 0;

		for (size_t i = 0; i < size; ++i)
		{
			if (cond[i])
				copy_from_constant(i, a_converted, *c_data, *c_offsets, c_prev_offset);
			else
				copy_from_constant(i, b_converted, *c_data, *c_offsets, c_prev_offset);
		}
	}
};

template <typename A, typename B>
struct NumArrayIfImpl<A, B, NumberTraits::Error>
{
private:
	static void throw_error()
	{
		throw Exception("Internal logic error: invalid types of arguments 2 and 3 of if", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
public:
	static void vector_vector(
		const PaddedPODArray<UInt8> & cond,
		const PaddedPODArray<A> & a_data, const ColumnArray::Offsets_t & a_offsets,
		const PaddedPODArray<B> & b_data, const ColumnArray::Offsets_t & b_offsets,
		Block & block, size_t result)
	{
		throw_error();
	}

	static void vector_constant(
		const PaddedPODArray<UInt8> & cond,
		const PaddedPODArray<A> & a_data, const ColumnArray::Offsets_t & a_offsets,
		const Array & b,
		Block & block, size_t result)
	{
		throw_error();
	}

	static void constant_vector(
		const PaddedPODArray<UInt8> & cond,
		const Array & a,
		const PaddedPODArray<B> & b_data, const ColumnArray::Offsets_t & b_offsets,
		Block & block, size_t result)
	{
		throw_error();
	}

	static void constant_constant(
		const PaddedPODArray<UInt8> & cond,
		const Array & a, const Array & b,
		Block & block, size_t result)
	{
		throw_error();
	}
};


/** Реализация для массивов строк.
  * NOTE: Код слишком сложный, потому что он работает в внутренностями массивов строк.
  * NOTE: Массивы из FixedString не поддерживаются.
  */
struct StringArrayIfImpl
{
	static ALWAYS_INLINE void copy_from_vector(
		size_t i,
		const ColumnString::Chars_t & from_data,
		const ColumnString::Offsets_t & from_string_offsets,
		const ColumnArray::Offsets_t & from_array_offsets,
		const ColumnArray::Offset_t & from_array_prev_offset,
		const ColumnString::Offset_t & from_string_prev_offset,
		ColumnString::Chars_t & to_data,
		ColumnString::Offsets_t & to_string_offsets,
		ColumnArray::Offsets_t & to_array_offsets,
		ColumnArray::Offset_t & to_array_prev_offset,
		ColumnString::Offset_t & to_string_prev_offset)
	{
		size_t array_size = from_array_offsets[i] - from_array_prev_offset;

		size_t bytes_to_copy = 0;
		size_t from_string_prev_offset_local = from_string_prev_offset;
		for (size_t j = 0; j < array_size; ++j)
		{
			size_t string_size = from_string_offsets[from_array_prev_offset + j] - from_string_prev_offset_local;

			to_string_prev_offset += string_size;
			to_string_offsets.push_back(to_string_prev_offset);

			from_string_prev_offset_local += string_size;
			bytes_to_copy += string_size;
		}

		size_t to_data_old_size = to_data.size();
		to_data.resize(to_data_old_size + bytes_to_copy);
		memcpy(&to_data[to_data_old_size], &from_data[from_string_prev_offset], bytes_to_copy);

		to_array_prev_offset += array_size;
		to_array_offsets[i] = to_array_prev_offset;
	}

	static ALWAYS_INLINE void copy_from_constant(
		size_t i,
		const Array & from_data,
		ColumnString::Chars_t & to_data,
		ColumnString::Offsets_t & to_string_offsets,
		ColumnArray::Offsets_t & to_array_offsets,
		ColumnArray::Offset_t & to_array_prev_offset,
		ColumnString::Offset_t & to_string_prev_offset)
	{
		size_t array_size = from_data.size();

		for (size_t j = 0; j < array_size; ++j)
		{
			const String & str = from_data[j].get<const String &>();
			size_t string_size = str.size() + 1;	/// Включая 0 на конце.

			to_data.resize(to_string_prev_offset + string_size);
			memcpy(&to_data[to_string_prev_offset], str.data(), string_size);

			to_string_prev_offset += string_size;
			to_string_offsets.push_back(to_string_prev_offset);
		}

		to_array_prev_offset += array_size;
		to_array_offsets[i] = to_array_prev_offset;
	}


	static void vector_vector(
		const PaddedPODArray<UInt8> & cond,
		const ColumnString::Chars_t & a_data, const ColumnString::Offsets_t & a_string_offsets, const ColumnArray::Offsets_t & a_array_offsets,
		const ColumnString::Chars_t & b_data, const ColumnString::Offsets_t & b_string_offsets, const ColumnArray::Offsets_t & b_array_offsets,
		ColumnString::Chars_t & c_data, ColumnString::Offsets_t & c_string_offsets, ColumnArray::Offsets_t & c_array_offsets)
	{
		size_t size = cond.size();
		c_array_offsets.resize(size);
		c_string_offsets.reserve(std::max(a_string_offsets.size(), b_string_offsets.size()));
		c_data.reserve(std::max(a_data.size(), b_data.size()));

		ColumnArray::Offset_t a_array_prev_offset = 0;
		ColumnArray::Offset_t b_array_prev_offset = 0;
		ColumnArray::Offset_t c_array_prev_offset = 0;

		ColumnString::Offset_t a_string_prev_offset = 0;
		ColumnString::Offset_t b_string_prev_offset = 0;
		ColumnString::Offset_t c_string_prev_offset = 0;

		for (size_t i = 0; i < size; ++i)
		{
			if (cond[i])
				copy_from_vector(i,
					a_data, a_string_offsets, a_array_offsets, a_array_prev_offset, a_string_prev_offset,
					c_data, c_string_offsets, c_array_offsets, c_array_prev_offset, c_string_prev_offset);
			else
				copy_from_vector(i,
					b_data, b_string_offsets, b_array_offsets, b_array_prev_offset, b_string_prev_offset,
					c_data, c_string_offsets, c_array_offsets, c_array_prev_offset, c_string_prev_offset);

			a_array_prev_offset = a_array_offsets[i];
			b_array_prev_offset = b_array_offsets[i];

			if (a_array_prev_offset)
				a_string_prev_offset = a_string_offsets[a_array_prev_offset - 1];

			if (b_array_prev_offset)
				b_string_prev_offset = b_string_offsets[b_array_prev_offset - 1];
		}
	}

	template <bool reverse>
	static void vector_constant_impl(
		const PaddedPODArray<UInt8> & cond,
		const ColumnString::Chars_t & a_data, const ColumnString::Offsets_t & a_string_offsets, const ColumnArray::Offsets_t & a_array_offsets,
		const Array & b,
		ColumnString::Chars_t & c_data, ColumnString::Offsets_t & c_string_offsets, ColumnArray::Offsets_t & c_array_offsets)
	{
		size_t size = cond.size();
		c_array_offsets.resize(size);
		c_string_offsets.reserve(a_string_offsets.size());
		c_data.reserve(a_data.size());

		ColumnArray::Offset_t a_array_prev_offset = 0;
		ColumnArray::Offset_t c_array_prev_offset = 0;

		ColumnString::Offset_t a_string_prev_offset = 0;
		ColumnString::Offset_t c_string_prev_offset = 0;

		for (size_t i = 0; i < size; ++i)
		{
			if (reverse != cond[i])
				copy_from_vector(i,
					a_data, a_string_offsets, a_array_offsets, a_array_prev_offset, a_string_prev_offset,
					c_data, c_string_offsets, c_array_offsets, c_array_prev_offset, c_string_prev_offset);
			else
				copy_from_constant(i,
					 b,
					 c_data, c_string_offsets, c_array_offsets, c_array_prev_offset, c_string_prev_offset);

			a_array_prev_offset = a_array_offsets[i];

			if (a_array_prev_offset)
				a_string_prev_offset = a_string_offsets[a_array_prev_offset - 1];
		}
	}

	static void vector_constant(
		const PaddedPODArray<UInt8> & cond,
		const ColumnString::Chars_t & a_data, const ColumnString::Offsets_t & a_string_offsets, const ColumnArray::Offsets_t & a_array_offsets,
		const Array & b,
		ColumnString::Chars_t & c_data, ColumnString::Offsets_t & c_string_offsets, ColumnArray::Offsets_t & c_array_offsets)
	{
		vector_constant_impl<false>(cond, a_data, a_string_offsets, a_array_offsets, b, c_data, c_string_offsets, c_array_offsets);
	}

	static void constant_vector(
		const PaddedPODArray<UInt8> & cond,
		const Array & a,
		const ColumnString::Chars_t & b_data, const ColumnString::Offsets_t & b_string_offsets, const ColumnArray::Offsets_t & b_array_offsets,
		ColumnString::Chars_t & c_data, ColumnString::Offsets_t & c_string_offsets, ColumnArray::Offsets_t & c_array_offsets)
	{
		vector_constant_impl<true>(cond, b_data, b_string_offsets, b_array_offsets, a, c_data, c_string_offsets, c_array_offsets);
	}

	static void constant_constant(
		const PaddedPODArray<UInt8> & cond,
		const Array & a,
		const Array & b,
		ColumnString::Chars_t & c_data, ColumnString::Offsets_t & c_string_offsets, ColumnArray::Offsets_t & c_array_offsets)
	{
		size_t size = cond.size();
		c_array_offsets.resize(size);
		c_string_offsets.reserve(std::max(a.size(), b.size()) * size);

		size_t sum_size_a = 0;
		for (const auto & s : a)
			sum_size_a += s.get<const String &>().size() + 1;

		size_t sum_size_b = 0;
		for (const auto & s : b)
			sum_size_b += s.get<const String &>().size() + 1;

		c_data.reserve(std::max(sum_size_a, sum_size_b) * size);

		ColumnArray::Offset_t c_array_prev_offset = 0;
		ColumnString::Offset_t c_string_prev_offset = 0;

		for (size_t i = 0; i < size; ++i)
		{
			if (cond[i])
				copy_from_constant(i,
					a,
					c_data, c_string_offsets, c_array_offsets, c_array_prev_offset, c_string_prev_offset);
			else
				copy_from_constant(i,
					b,
					c_data, c_string_offsets, c_array_offsets, c_array_prev_offset, c_string_prev_offset);
		}
	}
};


class FunctionIf : public IFunction
{
public:
	static constexpr auto name = "if";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionIf>(); }

private:
	template <typename T0, typename T1>
	bool checkRightType(const DataTypes & arguments, DataTypePtr & type_res) const
	{
		if (typeid_cast<const T1 *>(&*arguments[2]))
		{
			using ResultType = typename NumberTraits::ResultOfIf<typename T0::FieldType, typename T1::FieldType>::Type;
			type_res = DataTypeTraits::DataTypeFromFieldTypeOrError<ResultType>::getDataType();
			if (!type_res)
				throw Exception("Arguments 2 and 3 of function " + getName() + " are not upscalable to a common type without loss of precision: "
					+ arguments[1]->getName() + " and " + arguments[2]->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
			return true;
		}
		return false;
	}

	template <typename T0>
	bool checkLeftType(const DataTypes & arguments, DataTypePtr & type_res) const
	{
		if (typeid_cast<const T0 *>(&*arguments[1]))
		{
			if (	checkRightType<T0, DataTypeUInt8>(arguments, type_res)
				||	checkRightType<T0, DataTypeUInt16>(arguments, type_res)
				||	checkRightType<T0, DataTypeUInt32>(arguments, type_res)
				||	checkRightType<T0, DataTypeUInt64>(arguments, type_res)
				||	checkRightType<T0, DataTypeInt8>(arguments, type_res)
				||	checkRightType<T0, DataTypeInt16>(arguments, type_res)
				||	checkRightType<T0, DataTypeInt32>(arguments, type_res)
				||	checkRightType<T0, DataTypeInt64>(arguments, type_res)
				||	checkRightType<T0, DataTypeFloat32>(arguments, type_res)
				||	checkRightType<T0, DataTypeFloat64>(arguments, type_res))
				return true;
			else
				throw Exception("Illegal type " + arguments[2]->getName() + " of third argument of function " + getName(),
					ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
		}
		return false;
	}

	template <typename T0, typename T1>
	bool executeRightType(
		const ColumnUInt8 * cond_col,
		Block & block,
		const ColumnNumbers & arguments,
		size_t result,
		const ColumnVector<T0> * col_left)
	{
		const ColumnVector<T1> * col_right_vec = typeid_cast<const ColumnVector<T1> *>(block.getByPosition(arguments[2]).column.get());
		const ColumnConst<T1> * col_right_const = typeid_cast<const ColumnConst<T1> *>(block.getByPosition(arguments[2]).column.get());

		if (!col_right_vec && !col_right_const)
			return false;

		using ResultType = typename NumberTraits::ResultOfIf<T0, T1>::Type;

		if (col_right_vec)
			NumIfImpl<T0, T1, ResultType>::vector_vector(cond_col->getData(), col_left->getData(), col_right_vec->getData(), block, result);
		else
			NumIfImpl<T0, T1, ResultType>::vector_constant(cond_col->getData(), col_left->getData(), col_right_const->getData(), block, result);

		return true;
	}

	template <typename T0, typename T1>
	bool executeConstRightType(
		const ColumnUInt8 * cond_col,
		Block & block,
		const ColumnNumbers & arguments,
		size_t result,
		const ColumnConst<T0> * col_left)
	{
		const ColumnVector<T1> * col_right_vec = typeid_cast<const ColumnVector<T1> *>(block.getByPosition(arguments[2]).column.get());
		const ColumnConst<T1> * col_right_const = typeid_cast<const ColumnConst<T1> *>(block.getByPosition(arguments[2]).column.get());

		if (!col_right_vec && !col_right_const)
			return false;

		using ResultType = typename NumberTraits::ResultOfIf<T0, T1>::Type;

		if (col_right_vec)
			NumIfImpl<T0, T1, ResultType>::constant_vector(cond_col->getData(), col_left->getData(), col_right_vec->getData(), block, result);
		else
			NumIfImpl<T0, T1, ResultType>::constant_constant(cond_col->getData(), col_left->getData(), col_right_const->getData(), block, result);

		return true;
	}

	template <typename T0, typename T1>
	bool executeRightTypeArray(
		const ColumnUInt8 * cond_col,
		Block & block,
		const ColumnNumbers & arguments,
		size_t result,
		const ColumnArray * col_left_array,
		const ColumnVector<T0> * col_left)
	{
		const IColumn * col_right_untyped = block.getByPosition(arguments[2]).column.get();

		const ColumnArray * col_right_array = typeid_cast<const ColumnArray *>(col_right_untyped);
		const ColumnConstArray * col_right_const_array = typeid_cast<const ColumnConstArray *>(col_right_untyped);

		if (!col_right_array && !col_right_const_array)
			return false;

		using ResultType = typename NumberTraits::ResultOfIf<T0, T1>::Type;

		if (col_right_array)
		{
			const ColumnVector<T1> * col_right_vec = typeid_cast<const ColumnVector<T1> *>(&col_right_array->getData());

			if (!col_right_vec)
				return false;

			NumArrayIfImpl<T0, T1, ResultType>::vector_vector(
				cond_col->getData(),
				col_left->getData(), col_left_array->getOffsets(),
				col_right_vec->getData(), col_right_array->getOffsets(),
				block, result);
		}
		else
		{
			if (!typeid_cast<const typename DataTypeFromFieldType<T1>::Type *>(
				typeid_cast<const DataTypeArray &>(*col_right_const_array->getDataType()).getNestedType().get()))
				return false;

			NumArrayIfImpl<T0, T1, ResultType>::vector_constant(
				cond_col->getData(),
				col_left->getData(), col_left_array->getOffsets(),
				col_right_const_array->getData(),
				block, result);
		}

		return true;
	}

	template <typename T0, typename T1>
	bool executeConstRightTypeArray(
		const ColumnUInt8 * cond_col,
		Block & block,
		const ColumnNumbers & arguments,
		size_t result,
		const ColumnConstArray * col_left_const_array)
	{
		const IColumn * col_right_untyped = block.getByPosition(arguments[2]).column.get();

		const ColumnArray * col_right_array = typeid_cast<const ColumnArray *>(col_right_untyped);
		const ColumnConstArray * col_right_const_array = typeid_cast<const ColumnConstArray *>(col_right_untyped);

		if (!col_right_array && !col_right_const_array)
			return false;

		using ResultType = typename NumberTraits::ResultOfIf<T0, T1>::Type;

		if (col_right_array)
		{
			const ColumnVector<T1> * col_right_vec = typeid_cast<const ColumnVector<T1> *>(&col_right_array->getData());

			if (!col_right_vec)
				return false;

			NumArrayIfImpl<T0, T1, ResultType>::constant_vector(
				cond_col->getData(),
				col_left_const_array->getData(),
				col_right_vec->getData(), col_right_array->getOffsets(),
				block, result);
		}
		else
		{
			if (!typeid_cast<const typename DataTypeFromFieldType<T1>::Type *>(
				typeid_cast<const DataTypeArray &>(*col_right_const_array->getDataType()).getNestedType().get()))
				return false;

			NumArrayIfImpl<T0, T1, ResultType>::constant_constant(
				cond_col->getData(),
				col_left_const_array->getData(),
				col_right_const_array->getData(),
				block, result);
		}

		return true;
	}

	template <typename T0>
	bool executeLeftType(const ColumnUInt8 * cond_col, Block & block, const ColumnNumbers & arguments, size_t result)
	{
		const IColumn * col_left_untyped = block.getByPosition(arguments[1]).column.get();

		const ColumnVector<T0> * col_left = nullptr;
		const ColumnConst<T0> * col_const_left = nullptr;
		const ColumnArray * col_arr_left = nullptr;
		const ColumnVector<T0> * col_arr_left_elems = nullptr;
		const ColumnConstArray * col_const_arr_left = nullptr;

		col_left = typeid_cast<const ColumnVector<T0> *>(col_left_untyped);
		if (!col_left)
		{
			col_const_left = typeid_cast<const ColumnConst<T0> *>(col_left_untyped);
			if (!col_const_left)
			{
				col_arr_left = typeid_cast<const ColumnArray *>(col_left_untyped);

				if (col_arr_left)
					col_arr_left_elems = typeid_cast<const ColumnVector<T0> *>(&col_arr_left->getData());
				else
					col_const_arr_left = typeid_cast<const ColumnConstArray *>(col_left_untyped);
			}
		}

		if (col_left)
		{
			if (	executeRightType<T0, UInt8>(cond_col, block, arguments, result, col_left)
				||	executeRightType<T0, UInt16>(cond_col, block, arguments, result, col_left)
				||	executeRightType<T0, UInt32>(cond_col, block, arguments, result, col_left)
				||	executeRightType<T0, UInt64>(cond_col, block, arguments, result, col_left)
				||	executeRightType<T0, Int8>(cond_col, block, arguments, result, col_left)
				||	executeRightType<T0, Int16>(cond_col, block, arguments, result, col_left)
				||	executeRightType<T0, Int32>(cond_col, block, arguments, result, col_left)
				||	executeRightType<T0, Int64>(cond_col, block, arguments, result, col_left)
				||	executeRightType<T0, Float32>(cond_col, block, arguments, result, col_left)
				||	executeRightType<T0, Float64>(cond_col, block, arguments, result, col_left))
				return true;
			else
				throw Exception("Illegal column " + block.getByPosition(arguments[2]).column->getName()
					+ " of third argument of function " + getName(),
					ErrorCodes::ILLEGAL_COLUMN);
		}
		else if (col_const_left)
		{
			if (	executeConstRightType<T0, UInt8>(cond_col, block, arguments, result, col_const_left)
				||	executeConstRightType<T0, UInt16>(cond_col, block, arguments, result, col_const_left)
				||	executeConstRightType<T0, UInt32>(cond_col, block, arguments, result, col_const_left)
				||	executeConstRightType<T0, UInt64>(cond_col, block, arguments, result, col_const_left)
				||	executeConstRightType<T0, Int8>(cond_col, block, arguments, result, col_const_left)
				||	executeConstRightType<T0, Int16>(cond_col, block, arguments, result, col_const_left)
				||	executeConstRightType<T0, Int32>(cond_col, block, arguments, result, col_const_left)
				||	executeConstRightType<T0, Int64>(cond_col, block, arguments, result, col_const_left)
				||	executeConstRightType<T0, Float32>(cond_col, block, arguments, result, col_const_left)
				||	executeConstRightType<T0, Float64>(cond_col, block, arguments, result, col_const_left))
				return true;
			else
				throw Exception("Illegal column " + block.getByPosition(arguments[2]).column->getName()
					+ " of third argument of function " + getName(),
					ErrorCodes::ILLEGAL_COLUMN);
		}
		else if (col_arr_left && col_arr_left_elems)
		{
			if (	executeRightTypeArray<T0, UInt8>(cond_col, block, arguments, result, col_arr_left, col_arr_left_elems)
				||	executeRightTypeArray<T0, UInt16>(cond_col, block, arguments, result, col_arr_left, col_arr_left_elems)
				||	executeRightTypeArray<T0, UInt32>(cond_col, block, arguments, result, col_arr_left, col_arr_left_elems)
				||	executeRightTypeArray<T0, UInt64>(cond_col, block, arguments, result, col_arr_left, col_arr_left_elems)
				||	executeRightTypeArray<T0, Int8>(cond_col, block, arguments, result, col_arr_left, col_arr_left_elems)
				||	executeRightTypeArray<T0, Int16>(cond_col, block, arguments, result, col_arr_left, col_arr_left_elems)
				||	executeRightTypeArray<T0, Int32>(cond_col, block, arguments, result, col_arr_left, col_arr_left_elems)
				||	executeRightTypeArray<T0, Int64>(cond_col, block, arguments, result, col_arr_left, col_arr_left_elems)
				||	executeRightTypeArray<T0, Float32>(cond_col, block, arguments, result, col_arr_left, col_arr_left_elems)
				||	executeRightTypeArray<T0, Float64>(cond_col, block, arguments, result, col_arr_left, col_arr_left_elems))
				return true;
			else
				throw Exception("Illegal column " + block.getByPosition(arguments[2]).column->getName()
					+ " of third argument of function " + getName(),
					ErrorCodes::ILLEGAL_COLUMN);
		}
		else if (col_const_arr_left
			&& typeid_cast<const typename DataTypeFromFieldType<T0>::Type *>(
				typeid_cast<const DataTypeArray &>(*col_const_arr_left->getDataType()).getNestedType().get()))
		{
			if (	executeConstRightTypeArray<T0, UInt8>(cond_col, block, arguments, result, col_const_arr_left)
				||	executeConstRightTypeArray<T0, UInt16>(cond_col, block, arguments, result, col_const_arr_left)
				||	executeConstRightTypeArray<T0, UInt32>(cond_col, block, arguments, result, col_const_arr_left)
				||	executeConstRightTypeArray<T0, UInt64>(cond_col, block, arguments, result, col_const_arr_left)
				||	executeConstRightTypeArray<T0, Int8>(cond_col, block, arguments, result, col_const_arr_left)
				||	executeConstRightTypeArray<T0, Int16>(cond_col, block, arguments, result, col_const_arr_left)
				||	executeConstRightTypeArray<T0, Int32>(cond_col, block, arguments, result, col_const_arr_left)
				||	executeConstRightTypeArray<T0, Int64>(cond_col, block, arguments, result, col_const_arr_left)
				||	executeConstRightTypeArray<T0, Float32>(cond_col, block, arguments, result, col_const_arr_left)
				||	executeConstRightTypeArray<T0, Float64>(cond_col, block, arguments, result, col_const_arr_left))
				return true;
			else
				throw Exception("Illegal column " + block.getByPosition(arguments[2]).column->getName()
					+ " of third argument of function " + getName(),
					ErrorCodes::ILLEGAL_COLUMN);
		}

		return false;
	}

	bool executeString(const ColumnUInt8 * cond_col, Block & block, const ColumnNumbers & arguments, size_t result)
	{
		const IColumn * col_then_untyped = block.getByPosition(arguments[1]).column.get();
		const IColumn * col_else_untyped = block.getByPosition(arguments[2]).column.get();

		const ColumnString * col_then = typeid_cast<const ColumnString *>(col_then_untyped);
		const ColumnString * col_else = typeid_cast<const ColumnString *>(col_else_untyped);
		const ColumnFixedString * col_then_fixed = typeid_cast<const ColumnFixedString *>(col_then_untyped);
		const ColumnFixedString * col_else_fixed = typeid_cast<const ColumnFixedString *>(col_else_untyped);
		const ColumnConstString * col_then_const = typeid_cast<const ColumnConstString *>(col_then_untyped);
		const ColumnConstString * col_else_const = typeid_cast<const ColumnConstString *>(col_else_untyped);

		if ((col_then || col_then_const || col_then_fixed) && (col_else || col_else_const || col_else_fixed))
		{
			if (col_then_fixed && col_else_fixed)
			{
				/// Результат - FixedString.

				if (col_then_fixed->getN() != col_else_fixed->getN())
					throw Exception("FixedString columns as 'then' and 'else' arguments of function 'if' has different sizes", ErrorCodes::ILLEGAL_COLUMN);

				size_t N = col_then_fixed->getN();

				auto col_res = std::make_shared<ColumnFixedString>(N);
				block.getByPosition(result).column = col_res;

				ColumnFixedString::Chars_t & res_vec = col_res->getChars();

				StringIfImpl::vector_fixed_vector_fixed(
					cond_col->getData(),
					col_then_fixed->getChars(),
					col_else_fixed->getChars(),
					N,
					res_vec);
			}
			else
			{
				/// Результат - String.
				std::shared_ptr<ColumnString> col_res = std::make_shared<ColumnString>();
				block.getByPosition(result).column = col_res;

				ColumnString::Chars_t & res_vec = col_res->getChars();
				ColumnString::Offsets_t & res_offsets = col_res->getOffsets();

				if (col_then && col_else)
					StringIfImpl::vector_vector(
						cond_col->getData(),
						col_then->getChars(), col_then->getOffsets(),
						col_else->getChars(), col_else->getOffsets(),
						res_vec, res_offsets);
				else if (col_then && col_else_const)
					StringIfImpl::vector_constant(
						cond_col->getData(),
						col_then->getChars(), col_then->getOffsets(),
						col_else_const->getData(),
						res_vec, res_offsets);
				else if (col_then_const && col_else)
					StringIfImpl::constant_vector(
						cond_col->getData(),
						col_then_const->getData(),
						col_else->getChars(), col_else->getOffsets(),
						res_vec, res_offsets);
				else if (col_then_const && col_else_const)
					StringIfImpl::constant_constant(
						cond_col->getData(),
						col_then_const->getData(),
						col_else_const->getData(),
						res_vec, res_offsets);
				else if (col_then && col_else_fixed)
					StringIfImpl::vector_vector_fixed(
						cond_col->getData(),
						col_then->getChars(), col_then->getOffsets(),
						col_else_fixed->getChars(), col_else_fixed->getN(),
						res_vec, res_offsets);
				else if (col_then_fixed && col_else)
					StringIfImpl::vector_fixed_vector(
						cond_col->getData(),
						col_then_fixed->getChars(), col_then_fixed->getN(),
						col_else->getChars(), col_else->getOffsets(),
						res_vec, res_offsets);
				else if (col_then_const && col_else_fixed)
					StringIfImpl::constant_vector_fixed(
						cond_col->getData(),
						col_then_const->getData(),
						col_else_fixed->getChars(), col_else_fixed->getN(),
						res_vec, res_offsets);
				else if (col_then_fixed && col_else_const)
					StringIfImpl::vector_fixed_constant(
						cond_col->getData(),
						col_then_fixed->getChars(), col_then_fixed->getN(),
						col_else_const->getData(),
						res_vec, res_offsets);
				else
					return false;
			}

			return true;
		}

		const ColumnArray * col_arr_then = typeid_cast<const ColumnArray *>(col_then_untyped);
		const ColumnArray * col_arr_else = typeid_cast<const ColumnArray *>(col_else_untyped);
		const ColumnConstArray * col_arr_then_const = typeid_cast<const ColumnConstArray *>(col_then_untyped);
		const ColumnConstArray * col_arr_else_const = typeid_cast<const ColumnConstArray *>(col_else_untyped);
		const ColumnString * col_then_elements = col_arr_then ? typeid_cast<const ColumnString *>(&col_arr_then->getData()) : nullptr;
		const ColumnString * col_else_elements = col_arr_else ? typeid_cast<const ColumnString *>(&col_arr_else->getData()) : nullptr;

		if (((col_arr_then && col_then_elements) || col_arr_then_const)
			&& ((col_arr_else && col_else_elements) || col_arr_else_const))
		{
			auto col_res_elements = std::make_shared<ColumnString>();
			auto col_res = std::make_shared<ColumnArray>(col_res_elements);
			block.getByPosition(result).column = col_res;

			ColumnString::Chars_t & res_chars = col_res_elements->getChars();
			ColumnString::Offsets_t & res_string_offsets = col_res_elements->getOffsets();
			ColumnArray::Offsets_t & res_array_offsets = col_res->getOffsets();

			if (col_then_elements && col_else_elements)
				StringArrayIfImpl::vector_vector(
					cond_col->getData(),
					col_then_elements->getChars(), col_then_elements->getOffsets(), col_arr_then->getOffsets(),
					col_else_elements->getChars(), col_else_elements->getOffsets(), col_arr_else->getOffsets(),
					res_chars, res_string_offsets, res_array_offsets);
			else if (col_then_elements && col_arr_else_const)
				StringArrayIfImpl::vector_constant(
					cond_col->getData(),
					col_then_elements->getChars(), col_then_elements->getOffsets(), col_arr_then->getOffsets(),
					col_arr_else_const->getData(),
					res_chars, res_string_offsets, res_array_offsets);
			else if (col_arr_then_const && col_else_elements)
				StringArrayIfImpl::constant_vector(
					cond_col->getData(),
					col_arr_then_const->getData(),
					col_else_elements->getChars(), col_else_elements->getOffsets(), col_arr_else->getOffsets(),
					res_chars, res_string_offsets, res_array_offsets);
			else if (col_arr_then_const && col_arr_else_const)
				StringArrayIfImpl::constant_constant(
					cond_col->getData(),
					col_arr_then_const->getData(),
					col_arr_else_const->getData(),
					res_chars, res_string_offsets, res_array_offsets);
			else
				return false;

			return true;
		}

		return false;
	}

	bool executeTuple(const ColumnUInt8 * cond_col, Block & block, const ColumnNumbers & arguments, size_t result)
	{
		/// Calculate function for each corresponding elements of tuples.

		const ColumnWithTypeAndName & arg1 = block.getByPosition(arguments[1]);
		const ColumnWithTypeAndName & arg2 = block.getByPosition(arguments[2]);

		ColumnPtr col1_holder;
		ColumnPtr col2_holder;

		if (typeid_cast<const ColumnTuple *>(arg1.column.get()))
			col1_holder = arg1.column;
		else if (const ColumnConstTuple * const_tuple = typeid_cast<const ColumnConstTuple *>(arg1.column.get()))
			col1_holder = const_tuple->convertToTupleOfConstants();
		else
			return false;

		if (typeid_cast<const ColumnTuple *>(arg2.column.get()))
			col2_holder = arg2.column;
		else if (const ColumnConstTuple * const_tuple = typeid_cast<const ColumnConstTuple *>(arg2.column.get()))
			col2_holder = const_tuple->convertToTupleOfConstants();
		else
			return false;

		const ColumnTuple * col1 = static_cast<const ColumnTuple *>(col1_holder.get());
		const ColumnTuple * col2 = static_cast<const ColumnTuple *>(col2_holder.get());

		const DataTypeTuple & type1 = static_cast<const DataTypeTuple &>(*arg1.type);
		const DataTypeTuple & type2 = static_cast<const DataTypeTuple &>(*arg2.type);

		Block temporary_block;
		temporary_block.insert(block.getByPosition(arguments[0]));

		size_t tuple_size = type1.getElements().size();

		for (size_t i = 0; i < tuple_size; ++i)
		{
			temporary_block.insert({nullptr,
				getReturnType({std::make_shared<DataTypeUInt8>(), type1.getElements()[i], type2.getElements()[i]}),
				{}});

			temporary_block.insert({col1->getData().getByPosition(i).column, type1.getElements()[i], {}});
			temporary_block.insert({col2->getData().getByPosition(i).column, type2.getElements()[i], {}});

			/// temporary_block will be: cond, res_0, ..., res_i, then_i, else_i
			execute(temporary_block, {0, i + 2, i + 3}, i + 1);
			temporary_block.erase(i + 3);
			temporary_block.erase(i + 2);
		}

		/// temporary_block is: cond, res_0, res_1, res_2...

		temporary_block.erase(0);
		block.getByPosition(result).column = std::make_shared<ColumnTuple>(temporary_block);
		return true;
	}

public:
	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	/// Получить типы результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (arguments.size() != 3)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 3.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!typeid_cast<const DataTypeUInt8 *>(&*arguments[0]))
			throw Exception("Illegal type of first argument (condition) of function if. Must be UInt8.",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		const DataTypeArray * type_arr1 = typeid_cast<const DataTypeArray *>(arguments[1].get());
		const DataTypeArray * type_arr2 = typeid_cast<const DataTypeArray *>(arguments[2].get());

		const DataTypeTuple * type_tuple1 = typeid_cast<const DataTypeTuple *>(arguments[1].get());
		const DataTypeTuple * type_tuple2 = typeid_cast<const DataTypeTuple *>(arguments[2].get());

		if (arguments[1]->behavesAsNumber() && arguments[2]->behavesAsNumber())
		{
			DataTypePtr type_res;
			if (!(	checkLeftType<DataTypeUInt8>(arguments, type_res)
				||	checkLeftType<DataTypeUInt16>(arguments, type_res)
				||	checkLeftType<DataTypeUInt32>(arguments, type_res)
				||	checkLeftType<DataTypeUInt64>(arguments, type_res)
				||	checkLeftType<DataTypeInt8>(arguments, type_res)
				||	checkLeftType<DataTypeInt16>(arguments, type_res)
				||	checkLeftType<DataTypeInt32>(arguments, type_res)
				||	checkLeftType<DataTypeInt64>(arguments, type_res)
				||	checkLeftType<DataTypeFloat32>(arguments, type_res)
				||	checkLeftType<DataTypeFloat64>(arguments, type_res)))
				throw Exception("Internal error: unexpected type " + arguments[1]->getName() + " of first argument of function " + getName(),
					ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
			return type_res;
		}
		else if (type_arr1 && type_arr2)
		{
			/// NOTE Сообщения об ошибках будут относится к типам элементов массивов, что немного некорректно.
			return std::make_shared<DataTypeArray>(getReturnType({arguments[0], type_arr1->getNestedType(), type_arr2->getNestedType()}));
		}
		else if (type_tuple1 && type_tuple2)
		{
			const size_t tuple_size = type_tuple1->getElements().size();

			if (tuple_size != type_tuple2->getElements().size())
				throw Exception("Different sizes of tuples in 'then' and 'else' argument of function if",
					ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

			DataTypes result_tuple(tuple_size);

			for (size_t i = 0; i < tuple_size; ++i)
				result_tuple[i] = getReturnType({arguments[0], type_tuple1->getElements()[i], type_tuple2->getElements()[i]});

			return std::make_shared<DataTypeTuple>(std::move(result_tuple));
		}
		else if (arguments[1]->getName() != arguments[2]->getName())
		{
			const DataTypeString * type_string1 = typeid_cast<const DataTypeString *>(arguments[1].get());
			const DataTypeString * type_string2 = typeid_cast<const DataTypeString *>(arguments[2].get());
			const DataTypeFixedString * type_fixed_string1 = typeid_cast<const DataTypeFixedString *>(arguments[1].get());
			const DataTypeFixedString * type_fixed_string2 = typeid_cast<const DataTypeFixedString *>(arguments[2].get());

			if (type_fixed_string1 && type_fixed_string2)
			{
				if (type_fixed_string1->getN() != type_fixed_string2->getN())
					throw Exception("FixedString types as 'then' and 'else' arguments of function 'if' has different sizes",
						ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

				return std::make_shared<DataTypeFixedString>(type_fixed_string1->getN());
			}
			else if ((type_string1 || type_fixed_string1) && (type_string2 || type_fixed_string2))
			{
				return std::make_shared<DataTypeString>();
			}

			throw Exception{
				"Incompatible second and third arguments for function " + getName() + ": " +
					arguments[1]->getName() + " and " + arguments[2]->getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
			};
		}

		return arguments[1];
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		const ColumnUInt8 * cond_col = typeid_cast<const ColumnUInt8 *>(block.getByPosition(arguments[0]).column.get());
		const ColumnConst<UInt8> * cond_const_col = typeid_cast<const ColumnConst<UInt8> *>(block.getByPosition(arguments[0]).column.get());
		ColumnPtr materialized_cond_col;

		const ColumnWithTypeAndName & arg_then = block.getByPosition(arguments[1]);
		const ColumnWithTypeAndName & arg_else = block.getByPosition(arguments[2]);

		if (cond_const_col)
		{
			if (arg_then.type->getName() == arg_else.type->getName())
			{
				block.getByPosition(result).column = cond_const_col->getData()
					? arg_then.column
					: arg_else.column;
				return;
			}
			else
			{
				materialized_cond_col = cond_const_col->convertToFullColumn();
				cond_col = typeid_cast<const ColumnUInt8 *>(&*materialized_cond_col);
			}
		}

		if (cond_col)
		{
			if (!(	executeLeftType<UInt8>(cond_col, block, arguments, result)
				||	executeLeftType<UInt16>(cond_col, block, arguments, result)
				||	executeLeftType<UInt32>(cond_col, block, arguments, result)
				||	executeLeftType<UInt64>(cond_col, block, arguments, result)
				||	executeLeftType<Int8>(cond_col, block, arguments, result)
				||	executeLeftType<Int16>(cond_col, block, arguments, result)
				||	executeLeftType<Int32>(cond_col, block, arguments, result)
				||	executeLeftType<Int64>(cond_col, block, arguments, result)
				||	executeLeftType<Float32>(cond_col, block, arguments, result)
				||	executeLeftType<Float64>(cond_col, block, arguments, result)
				|| 	executeString(cond_col, block, arguments, result)
				||  executeTuple(cond_col, block, arguments, result)))
				throw Exception("Illegal columns " + arg_then.column->getName()
					+ " and " + arg_else.column->getName()
					+ " of second (then) and third (else) arguments of function " + getName(),
					ErrorCodes::ILLEGAL_COLUMN);
		}
		else
			throw Exception("Illegal column " + cond_col->getName() + " of first argument of function " + getName()
				+ ". Must be ColumnUInt8 or ColumnConstUInt8.",
				ErrorCodes::ILLEGAL_COLUMN);
	}
};


/// Function multiIf, which generalizes the function if.
///
/// Syntax: multiIf(cond_1, then_1, ..., cond_N, then_N, else)
/// where N >= 1.
///
/// For all 1 <= i <= N, "cond_i" has type UInt8.
/// Types of all the branches "then_i" and "else" are either of the following:
///    - numeric types for which there exists a common type;
///    - dates;
///    - dates with time;
///    - strings;
///    - arrays of such types.
class FunctionMultiIf final : public IFunction
{
public:
	static constexpr auto name = "multiIf";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionMultiIf>(); }

public:
	String getName() const override
	{
		return is_case_mode ? "CASE" : name;
	}

	void setCaseMode()
	{
		is_case_mode = true;
	}

	DataTypePtr getReturnType(const DataTypes & args) const override
	{
		DataTypePtr data_type;

		try
		{
			data_type = getReturnTypeImpl(args);
		}
		catch (const Conditional::CondException & ex)
		{
			rethrowContextually(ex);
		}

		return data_type;
	}

	void execute(Block & block, const ColumnNumbers & args, size_t result) override
	{
		try
		{
			if (performTrivialCase(block, args, result))
				return;

			if (Conditional::NumericPerformer::perform(block, args, result))
				return;

			if (Conditional::StringEvaluator::perform(block, args, result))
				return;

			if (Conditional::StringArrayEvaluator::perform(block, args, result))
				return;

			if (is_case_mode)
				throw Exception{"Some THEN/ELSE clauses in CASE construction have "
					"illegal or incompatible types", ErrorCodes::ILLEGAL_COLUMN};
			else
				throw Exception{"One or more branch (then, else) columns of function "
					+ getName() + " have illegal or incompatible types",
					ErrorCodes::ILLEGAL_COLUMN};
		}
		catch (const Conditional::CondException & ex)
		{
			rethrowContextually(ex);
		}
	}

private:
	DataTypePtr getReturnTypeImpl(const DataTypes & args) const
	{
		if (!Conditional::hasValidArgCount(args))
		{
			if (is_case_mode)
				throw Exception{"Some mandatory parameters are missing in the CASE "
					"construction", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};
			else
				throw Exception{"Invalid number of arguments for function " + getName(),
					ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};
		}

		/// Check conditions types.
		for (size_t i = Conditional::firstCond(); i < Conditional::elseArg(args); i = Conditional::nextCond(i))
		{
			if (!typeid_cast<const DataTypeUInt8 *>(&*args[i]))
			{
				if (is_case_mode)
					throw Exception{"In CASE construction, illegal type of WHEN clause "
					+ toString(i / 2) + ". Must be UInt8", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
				else
					throw Exception{"Illegal type of argument " + toString(i) + " (condition) "
						"of function " + getName() + ". Must be UInt8.",
						ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
			}
		}

		if (Conditional::hasArithmeticBranches(args))
			return Conditional::getReturnTypeForArithmeticArgs(args);
		else if (Conditional::hasArrayBranches(args))
		{
			/// NOTE Сообщения об ошибках будут относится к типам элементов массивов, что немного некорректно.
			DataTypes new_args;
			new_args.reserve(args.size());

			auto push_branch_arg = [&args, &new_args](size_t i)
			{
				const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(args[i].get());
				if (type_arr == nullptr)
					throw Exception{"Internal error", ErrorCodes::LOGICAL_ERROR};
				new_args.push_back(type_arr->getNestedType());
			};

			for (size_t i = 0; i < Conditional::elseArg(args); ++i)
			{
				if (Conditional::isCond(i))
					new_args.push_back(args[i]);
				else
					push_branch_arg(i);
			}

			push_branch_arg(Conditional::elseArg(args));

			return std::make_shared<DataTypeArray>(getReturnType(new_args));
		}
		else if (!Conditional::hasIdenticalTypes(args))
		{
			if (Conditional::hasFixedStrings(args))
			{
				if (!Conditional::hasFixedStringsOfIdenticalLength(args))
				{
					if (is_case_mode)
						throw Exception{"THEN/ELSE clauses in CASE construction "
							"have FixedString type and different sizes",
							ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
					else
						throw Exception{"Branch (then, else) arguments of function " + getName() +
							" have FixedString type and different sizes",
							ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
				}

				const IDataType * data = args[Conditional::firstThen()].get();
				const auto * fixed_str = typeid_cast<const DataTypeFixedString *>(data);

				if (fixed_str == nullptr)
					throw Exception{"Internal error", ErrorCodes::LOGICAL_ERROR};

				return std::make_shared<DataTypeFixedString>(fixed_str->getN());
			}
			else if (Conditional::hasStrings(args))
				return std::make_shared<DataTypeString>();
			else
			{
				if (is_case_mode)
					throw Exception{"THEN/ELSE clauses in CASE construction "
						"have incompatible arguments", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
				else
					throw Exception{
						"Incompatible branch (then, else) arguments for function " + getName(),
						ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
					};
			}
		}
		else
			return args[Conditional::firstThen()];
	}

	bool performTrivialCase(Block & block, const ColumnNumbers & args, size_t result)
	{
		size_t else_arg = Conditional::elseArg(args);
		auto first_type_name = block.getByPosition(args[Conditional::firstThen()]).type->getName();

		for (size_t i = Conditional::secondThen(); i < else_arg; i = Conditional::nextThen(i))
		{
			if (block.getByPosition(args[i]).type->getName() != first_type_name)
				return false;
		}

		if (block.getByPosition(args.back()).type->getName() != first_type_name)
			return false;

		auto & res_col = block.getByPosition(result).column;

		for (size_t i = Conditional::firstCond(); i < else_arg; i = Conditional::nextCond(i))
		{
			auto cond_const_col = typeid_cast<const ColumnConst<UInt8> *>(block.getByPosition(args[i]).column.get());
			if (!cond_const_col)
				return false;

			bool has_triggered_cond = cond_const_col->getData();
			if (has_triggered_cond)
			{
				res_col = block.getByPosition(args[Conditional::thenFromCond(i)]).column;
				return true;
			}
		}

		res_col = block.getByPosition(args[else_arg]).column;
		return true;
	}

	/// Translate a context-free error into a contextual error.
	void rethrowContextually(const Conditional::CondException & ex) const
	{
		if (is_case_mode)
		{
			/// CASE construction context.
			if (ex.getCode() == Conditional::CondErrorCodes::TYPE_DEDUCER_ILLEGAL_COLUMN_TYPE)
				throw Exception{"Illegal type of column " + ex.getMsg1() +
					" in CASE construction", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
			else if (ex.getCode() == Conditional::CondErrorCodes::TYPE_DEDUCER_UPSCALING_ERROR)
				throw Exception{"THEN/ELSE clause parameters in CASE construction are not upscalable to a "
					"common type without loss of precision: " + ex.getMsg1(),
					ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
			else if (ex.getCode() == Conditional::CondErrorCodes::NUMERIC_PERFORMER_ILLEGAL_COLUMN)
			{
				size_t i = std::stoul(ex.getMsg1());
				if ((i % 2) == 1)
					throw Exception{"Illegal THEN clause " + toString(1 + (i - 1) / 2)
						+ " in CASE construction", ErrorCodes::ILLEGAL_COLUMN};
				else
					throw Exception{"Illegal ELSE clause in CASE construction",
						ErrorCodes::ILLEGAL_COLUMN};
			}
			else if (ex.getCode() == Conditional::CondErrorCodes::COND_SOURCE_ILLEGAL_COLUMN)
			{
				size_t i = std::stoul(ex.getMsg2());
				if ((i % 2) == 1)
					throw Exception{"Illegal column " + ex.getMsg1() + " of THEN clause "
						+ toString(1 + (i - 1) / 2) + " in CASE construction."
						"Must be ColumnUInt8 or ColumnConstUInt8", ErrorCodes::ILLEGAL_COLUMN};
				else
					throw Exception{"Illegal column " + ex.getMsg1() + " of ELSE clause "
						" in CASE construction. Must be ColumnUInt8 or ColumnConstUInt8",
						ErrorCodes::ILLEGAL_COLUMN};
			}
			else if (ex.getCode() == Conditional::CondErrorCodes::NUMERIC_EVALUATOR_ILLEGAL_ARGUMENT)
			{
				size_t i = std::stoul(ex.getMsg1());
				if ((i % 2) == 1)
					throw Exception{"Illegal type of THEN clause " + toString(1 + (i - 1) / 2)
						+ " in CASE construction", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
				else
					throw Exception{"Illegal type of ELSE clause in CASE construction",
						ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
			}
			else if (ex.getCode() == Conditional::CondErrorCodes::ARRAY_EVALUATOR_INVALID_TYPES)
				throw Exception{"Internal logic error: one or more THEN/ELSE clauses of "
					"CASE construction have invalid types", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
			else
				throw Exception{"An unexpected error has occurred in CASE construction",
					ErrorCodes::LOGICAL_ERROR};
		}
		else
		{
			/// multiIf function context.
			if (ex.getCode() == Conditional::CondErrorCodes::TYPE_DEDUCER_ILLEGAL_COLUMN_TYPE)
				throw Exception{"Illegal type of column " + ex.getMsg1() +
					" of function multiIf", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
			else if (ex.getCode() == Conditional::CondErrorCodes::TYPE_DEDUCER_UPSCALING_ERROR)
				throw Exception{"Arguments of function multiIf are not upscalable to a "
					"common type without loss of precision: " + ex.getMsg1(),
					ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
			else if (ex.getCode() == Conditional::CondErrorCodes::NUMERIC_PERFORMER_ILLEGAL_COLUMN)
				throw Exception{"Illegal argument " + ex.getMsg1() + " of function multiIf",
					ErrorCodes::ILLEGAL_COLUMN};
			else if (ex.getCode() == Conditional::CondErrorCodes::COND_SOURCE_ILLEGAL_COLUMN)
				throw Exception{"Illegal column " + ex.getMsg1() + " of argument "
					+ ex.getMsg2() + " of function multiIf"
					"Must be ColumnUInt8 or ColumnConstUInt8.", ErrorCodes::ILLEGAL_COLUMN};
			else if (ex.getCode() == Conditional::CondErrorCodes::NUMERIC_EVALUATOR_ILLEGAL_ARGUMENT)
				throw Exception{"Illegal type of argument " + ex.getMsg1() + " of function multiIf",
					ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
			else if (ex.getCode() == Conditional::CondErrorCodes::ARRAY_EVALUATOR_INVALID_TYPES)
				throw Exception{"Internal logic error: one or more arguments of function "
					"multiIf have invalid types", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
			else
				throw Exception{"An unexpected error has occurred while performing multiIf",
					ErrorCodes::LOGICAL_ERROR};
		}
	}

private:
	bool is_case_mode = false;
};

}
