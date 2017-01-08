#pragma once

#include <city.h>
#include <openssl/md5.h>

#include <DB/Common/SipHash.h>
#include <DB/Common/Arena.h>
#include <DB/Common/UInt128.h>
#include <DB/Core/Defines.h>
#include <DB/Core/StringRef.h>
#include <DB/Columns/IColumn.h>
#include <DB/Columns/ColumnsNumber.h>
#include <DB/Columns/ColumnFixedString.h>
#include <DB/Columns/ColumnNullable.h>


template <>
struct DefaultHash<StringRef> : public StringRefHash {};


namespace DB
{

using Sizes = std::vector<size_t>;

/// When packing the values of nullable columns at a given row, we have to
/// store the fact that these values are nullable or not. This is achieved
/// by encoding this information as a bitmap. Let S be the size in bytes of
/// a packed values binary blob and T the number of bytes we may place into
/// this blob, the size that the bitmap shall occupy in the blob is equal to:
/// ceil(T/8). Thus we must have: S = T + ceil(T/8). Below we indicate for
/// each value of S, the corresponding value of T, and the bitmap size:
///
/// 32,28,4
/// 16,14,2
/// 8,7,1
/// 4,3,1
/// 2,1,1
///

namespace
{

template <typename T>
constexpr auto getBitmapSize()
{
	return
		(sizeof(T) == 32) ?
			4 :
		(sizeof(T) == 16) ?
			2 :
		((sizeof(T) == 8) ?
			1 :
		((sizeof(T) == 4) ?
			1 :
		((sizeof(T) == 2) ?
			1 :
		0)));
}

}

template <typename T>
using KeysNullMap = std::array<UInt8, getBitmapSize<T>()>;

/// Pack into a binary blob of type T a set of fixed-size keys. Granted that all the keys fit into the
/// binary blob, they are disposed in it consecutively.
template <typename T>
static inline T ALWAYS_INLINE packFixed(
	size_t i, size_t keys_size, const ConstColumnPlainPtrs & key_columns, const Sizes & key_sizes)
{
	union
	{
		T key;
		char bytes[sizeof(key)] = {};
	};

	size_t offset = 0;

	for (size_t j = 0; j < keys_size; ++j)
	{
		switch (key_sizes[j])
		{
			case 1:
				memcpy(bytes + offset, &static_cast<const ColumnUInt8 *>(key_columns[j])->getData()[i], 1);
				offset += 1;
				break;
			case 2:
				memcpy(bytes + offset, &static_cast<const ColumnUInt16 *>(key_columns[j])->getData()[i], 2);
				offset += 2;
				break;
			case 4:
				memcpy(bytes + offset, &static_cast<const ColumnUInt32 *>(key_columns[j])->getData()[i], 4);
				offset += 4;
				break;
			case 8:
				memcpy(bytes + offset, &static_cast<const ColumnUInt64 *>(key_columns[j])->getData()[i], 8);
				offset += 8;
				break;
			default:
				memcpy(bytes + offset, &static_cast<const ColumnFixedString *>(key_columns[j])->getChars()[i * key_sizes[j]], key_sizes[j]);
				offset += key_sizes[j];
		}
	}

	return key;
}

/// Similar as above but supports nullable values.
template <typename T>
static inline T ALWAYS_INLINE packFixed(
	size_t i, size_t keys_size, const ConstColumnPlainPtrs & key_columns, const Sizes & key_sizes,
	const KeysNullMap<T> & bitmap)
{
	union
	{
		T key;
		char bytes[sizeof(key)] = {};
	};

	size_t offset = 0;

	static constexpr auto bitmap_size = std::tuple_size<KeysNullMap<T>>::value;
	static constexpr bool has_bitmap = bitmap_size > 0;

	if (has_bitmap)
	{
		memcpy(bytes + offset, bitmap.data(), bitmap_size * sizeof(UInt8));
		offset += bitmap_size;
	}

	for (size_t j = 0; j < keys_size; ++j)
	{
		bool is_null;

		if (!has_bitmap)
			is_null = false;
		else
		{
			size_t bucket = j / 8;
			size_t off = j % 8;
			is_null = ((bitmap[bucket] >> off) & 1) == 1;
		}

		if (is_null)
			continue;

		switch (key_sizes[j])
		{
			case 1:
				memcpy(bytes + offset, &static_cast<const ColumnUInt8 *>(key_columns[j])->getData()[i], 1);
				offset += 1;
				break;
			case 2:
				memcpy(bytes + offset, &static_cast<const ColumnUInt16 *>(key_columns[j])->getData()[i], 2);
				offset += 2;
				break;
			case 4:
				memcpy(bytes + offset, &static_cast<const ColumnUInt32 *>(key_columns[j])->getData()[i], 4);
				offset += 4;
				break;
			case 8:
				memcpy(bytes + offset, &static_cast<const ColumnUInt64 *>(key_columns[j])->getData()[i], 8);
				offset += 8;
				break;
			default:
				memcpy(bytes + offset, &static_cast<const ColumnFixedString *>(key_columns[j])->getChars()[i * key_sizes[j]], key_sizes[j]);
				offset += key_sizes[j];
		}
	}

	return key;
}


/// Hash a set of keys into a UInt128 value.
static inline UInt128 ALWAYS_INLINE hash128(
	size_t i, size_t keys_size, const ConstColumnPlainPtrs & key_columns, StringRefs & keys)
{
	UInt128 key;
	SipHash hash;

	for (size_t j = 0; j < keys_size; ++j)
	{
		/// Хэшируем ключ.
		keys[j] = key_columns[j]->getDataAtWithTerminatingZero(i);
		hash.update(keys[j].data, keys[j].size);
	}

    hash.get128(key.first, key.second);

	return key;
}


/// Almost the same as above but it doesn't return any reference to key data.
static inline UInt128 ALWAYS_INLINE hash128(
	size_t i, size_t keys_size, const ConstColumnPlainPtrs & key_columns)
{
	UInt128 key;
	SipHash hash;

	for (size_t j = 0; j < keys_size; ++j)
		key_columns[j]->updateHashWithValue(i, hash);

    hash.get128(key.first, key.second);

	return key;
}


/// Скопировать ключи в пул. Потом разместить в пуле StringRef-ы на них и вернуть указатель на первый.
static inline StringRef * ALWAYS_INLINE placeKeysInPool(
	size_t i, size_t keys_size, StringRefs & keys, Arena & pool)
{
	for (size_t j = 0; j < keys_size; ++j)
	{
		char * place = pool.alloc(keys[j].size);
		memcpy(place, keys[j].data, keys[j].size);		/// TODO padding в Arena и memcpySmall
		keys[j].data = place;
	}

	/// Размещаем в пуле StringRef-ы на только что скопированные ключи.
	char * res = pool.alloc(keys_size * sizeof(StringRef));
	memcpy(res, &keys[0], keys_size * sizeof(StringRef));

	return reinterpret_cast<StringRef *>(res);
}


/// Скопировать ключи в пул. Потом разместить в пуле StringRef-ы на них и вернуть указатель на первый.
static inline StringRef * ALWAYS_INLINE extractKeysAndPlaceInPool(
	size_t i, size_t keys_size, const ConstColumnPlainPtrs & key_columns, StringRefs & keys, Arena & pool)
{
	for (size_t j = 0; j < keys_size; ++j)
	{
		keys[j] = key_columns[j]->getDataAtWithTerminatingZero(i);
		char * place = pool.alloc(keys[j].size);
		memcpy(place, keys[j].data, keys[j].size);
		keys[j].data = place;
	}

	/// Размещаем в пуле StringRef-ы на только что скопированные ключи.
	char * res = pool.alloc(keys_size * sizeof(StringRef));
	memcpy(res, &keys[0], keys_size * sizeof(StringRef));

	return reinterpret_cast<StringRef *>(res);
}


/// Place the specified keys into a continuous memory chunk. The implementation
/// of this function depends on whether some keys are nullable or not. See comments
/// below for the specialized implementations.
template <bool has_nullable_keys>
static StringRef extractKeysAndPlaceInPoolContiguous(
	size_t i, size_t keys_size, const ConstColumnPlainPtrs & key_columns, StringRefs & keys, Arena & pool);

/// Implementation for the case when there are no nullable keys.
/// Copy the specified keys to a continuous memory chunk of a pool.
/// Subsequently append StringRef objects referring to each key.
///
/// [key1][key2]...[keyN][ref1][ref2]...[refN]
///   ^     ^        :     |     |
///   +-----|--------:-----+     |
///   :     +--------:-----------+
///   :              :
///   <-------------->
///        (1)
///
/// Return a StringRef object, referring to the area (1) of the memory
/// chunk that contains the keys. In other words, we ignore their StringRefs.
template <>
inline StringRef ALWAYS_INLINE extractKeysAndPlaceInPoolContiguous<false>(
	size_t i, size_t keys_size, const ConstColumnPlainPtrs & key_columns, StringRefs & keys, Arena & pool)
{
	size_t sum_keys_size = 0;
	for (size_t j = 0; j < keys_size; ++j)
	{
		keys[j] = key_columns[j]->getDataAtWithTerminatingZero(i);
		sum_keys_size += keys[j].size;
	}

	char * res = pool.alloc(sum_keys_size + keys_size * sizeof(StringRef));
	char * place = res;

	for (size_t j = 0; j < keys_size; ++j)
	{
		memcpy(place, keys[j].data, keys[j].size);
		keys[j].data = place;
		place += keys[j].size;
	}

	/// Размещаем в пуле StringRef-ы на только что скопированные ключи.
	memcpy(place, &keys[0], keys_size * sizeof(StringRef));

	return {res, sum_keys_size};
}

/// Implementation for the case where there is at least one nullable key.
/// Inside a continuous memory chunk of a pool, put a bitmap that indicates
/// for each specified key whether its value is null or not. Copy the keys
/// whose values are not nulls to the memory chunk. Subsequently append
/// StringRef objects referring to each key, even those who contain a null.
///
/// [bitmap][key1][key2][key4]...[keyN][ref1][ref2][ref3 (null)]...[refN]
///   :       ^     ^              :     |     |
///   :       +-----|--------------:-----+     |
///   :             +--------------:-----------+
///   :                            :
///   <---------------------------->
///                  (1)
///
/// Return a StringRef object, referring to the area (1) of the memory
/// chunk that contains the bitmap and the keys. In other words, we ignore
/// the keys' StringRefs.
template <>
inline StringRef ALWAYS_INLINE extractKeysAndPlaceInPoolContiguous<true>(
	size_t i, size_t keys_size, const ConstColumnPlainPtrs & key_columns, StringRefs & keys, Arena & pool)
{
	size_t bitmap_size = keys_size / 8;
	if ((keys_size % 8) != 0) { ++bitmap_size; }
	std::vector<UInt8> bitmap(bitmap_size);

	/// Prepare the keys to be stored. Create the bitmap.
	size_t keys_bytes = 0;
	for (size_t j = 0; j < keys_size; ++j)
	{
		const IColumn * observed_column;
		bool is_null;

		if (key_columns[j]->isNullable())
		{
			const ColumnNullable & nullable_col = static_cast<const ColumnNullable &>(*key_columns[j]);
			observed_column = nullable_col.getNestedColumn().get();
			const auto & null_map = nullable_col.getNullMap();
			is_null = null_map[i] == 1;
		}
		else
		{
			observed_column = key_columns[j];
			is_null = false;
		}

		if (is_null)
		{
			size_t bucket = j / 8;
			size_t offset = j % 8;
			bitmap[bucket] |= UInt8(1) << offset;

			keys[j] = StringRef{};
		}
		else
		{
			keys[j] = observed_column->getDataAtWithTerminatingZero(i);
			keys_bytes += keys[j].size;
		}
	}

	/// Allocate space for bitmap + non-null keys + StringRef objects.
	char * res = pool.alloc(bitmap_size + keys_bytes + keys_size * sizeof(StringRef));
	char * place = res;

	/// Store the bitmap.
	memcpy(place, bitmap.data(), bitmap.size());
	place += bitmap.size();

	/// Store the non-null keys data.
	for (size_t j = 0; j < keys_size; ++j)
	{
		size_t bucket = j / 8;
		size_t offset = j % 8;
		if (((bitmap[bucket] >> offset) & 1) == 0)
		{
			memcpy(place, keys[j].data, keys[j].size);
			keys[j].data = place;
			place += keys[j].size;
		}
	}

	/// Store StringRef objects for all the keys, i.e. even for those
	/// whose value is null.
	memcpy(place, &keys[0], keys_size * sizeof(StringRef));

	return {res, bitmap_size + keys_bytes};
}

/** Сериализовать ключи в непрерывный кусок памяти.
  */
static inline StringRef ALWAYS_INLINE serializeKeysToPoolContiguous(
	size_t i, size_t keys_size, const ConstColumnPlainPtrs & key_columns, StringRefs & keys, Arena & pool)
{
	const char * begin = nullptr;

	size_t sum_size = 0;
	for (size_t j = 0; j < keys_size; ++j)
		sum_size += key_columns[j]->serializeValueIntoArena(i, pool, begin).size;

	return {begin, sum_size};
}


}
