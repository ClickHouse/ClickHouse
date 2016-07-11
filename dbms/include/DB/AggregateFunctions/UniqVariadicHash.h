#pragma once

#include <city.h>
#include <DB/Core/Defines.h>
#include <DB/Common/SipHash.h>
#include <DB/Common/UInt128.h>
#include <DB/Columns/ColumnTuple.h>


namespace DB
{

/** Хэширует набор аргументов агрегатной функции
  *  для вычисления количества уникальных значений
  *  и добавляет их в множество.
  *
  * Четыре варианта (2 x 2):
  *
  * - для приближённого вычисления, использует некриптографическую 64-битную хэш-функцию;
  * - для точного вычисления, использует криптографическую 128-битную хэш-функцию;
  *
  * - для нескольких аргументов, переданных обычным способом;
  * - для одного аргумента-кортежа.
  */

template <bool exact, bool for_tuple>
struct UniqVariadicHash;


template <>
struct UniqVariadicHash<false, false>
{
	static inline UInt64 apply(size_t num_args, const IColumn ** columns, size_t row_num)
	{
		UInt64 hash;

		const IColumn ** column = columns;
		const IColumn ** columns_end = column + num_args;

		{
			StringRef value = (*column)->getDataAt(row_num);
			hash = CityHash64(value.data, value.size);
			++column;
		}

		while (column < columns_end)
		{
			StringRef value = (*column)->getDataAt(row_num);
			hash = Hash128to64(uint128(CityHash64(value.data, value.size), hash));
			++column;
		}

		return hash;
	}
};

template <>
struct UniqVariadicHash<false, true>
{
	static inline UInt64 apply(size_t num_args, const IColumn ** columns, size_t row_num)
	{
		UInt64 hash;

		const Columns & tuple_columns = static_cast<const ColumnTuple *>(columns[0])->getColumns();

		const ColumnPtr * column = tuple_columns.data();
		const ColumnPtr * columns_end = column + num_args;

		{
			StringRef value = column->get()->getDataAt(row_num);
			hash = CityHash64(value.data, value.size);
			++column;
		}

		while (column < columns_end)
		{
			StringRef value = column->get()->getDataAt(row_num);
			hash = Hash128to64(uint128(CityHash64(value.data, value.size), hash));
			++column;
		}

		return hash;
	}
};

template <>
struct UniqVariadicHash<true, false>
{
	static inline UInt128 apply(size_t num_args, const IColumn ** columns, size_t row_num)
	{
		const IColumn ** column = columns;
		const IColumn ** columns_end = column + num_args;

		SipHash hash;

		while (column < columns_end)
		{
			(*column)->updateHashWithValue(row_num, hash);
			++column;
		}

		UInt128 key;
		hash.get128(key.first, key.second);
		return key;
	}
};

template <>
struct UniqVariadicHash<true, true>
{
	static inline UInt128 apply(size_t num_args, const IColumn ** columns, size_t row_num)
	{
		const Columns & tuple_columns = static_cast<const ColumnTuple *>(columns[0])->getColumns();

		const ColumnPtr * column = tuple_columns.data();
		const ColumnPtr * columns_end = column + num_args;

		SipHash hash;

		while (column < columns_end)
		{
			(*column)->updateHashWithValue(row_num, hash);
			++column;
		}

		UInt128 key;
		hash.get128(key.first, key.second);
		return key;
	}
};

}
