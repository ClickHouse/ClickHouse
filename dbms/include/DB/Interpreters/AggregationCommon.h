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


template <>
struct DefaultHash<StringRef> : public StringRefHash {};


namespace DB
{

typedef std::vector<size_t> Sizes;


/// Записать набор ключей фиксированной длины в T, уложив их подряд (при допущении, что они помещаются).
template <typename T>
static inline T ALWAYS_INLINE packFixed(
	size_t i, size_t keys_size, const ConstColumnPlainPtrs & key_columns, const Sizes & key_sizes)
{
	union
	{
		T key;
		char bytes[sizeof(key)];
	};

	memset(bytes, 0, sizeof(key));
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


/// Хэшировать набор ключей в UInt128.
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


/// То же самое, но без возврата ссылок на данные ключей.
static inline UInt128 ALWAYS_INLINE hash128(
	size_t i, size_t keys_size, const ConstColumnPlainPtrs & key_columns)
{
	UInt128 key;
	SipHash hash;

	for (size_t j = 0; j < keys_size; ++j)
	{
		/// Хэшируем ключ.
		StringRef key = key_columns[j]->getDataAtWithTerminatingZero(i);
		hash.update(key.data, key.size);
	}

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
		memcpy(place, keys[j].data, keys[j].size);
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


/** Скопировать ключи в пул в непрерывный кусок памяти.
  * Потом разместить в пуле StringRef-ы на них.
  *
  * [key1][key2]...[keyN][ref1][ref2]...[refN]
  * ^---------------------|     |
  *       ^---------------------|
  * ^---return-value----^
  *
  * Вернуть StringRef на кусок памяти с ключами (без учёта StringRef-ов после них).
  */
static inline StringRef ALWAYS_INLINE extractKeysAndPlaceInPoolContiguous(
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
