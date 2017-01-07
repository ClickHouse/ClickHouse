#pragma once

#include <DB/Columns/ColumnNullable.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Interpreters/AggregationCommon.h>

#include <DB/Common/Arena.h>
#include <DB/Common/HashTable/HashSet.h>
#include <DB/Common/UInt128.h>

namespace DB
{

/** Методы для разных вариантов реализации множеств.
  * Используются в качестве параметра шаблона.
  */


/// Для случая, когда есть один числовой ключ.
template <typename FieldType, typename TData>	/// UInt8/16/32/64 для любых типов соответствующей битности.
struct SetMethodOneNumber
{
	using Data = TData;
	using Key = typename Data::key_type;

	Data data;

	/// Для использования одного Method в разных потоках, используйте разные State.
	struct State
	{
		const FieldType * vec;

		/** Вызывается в начале обработки каждого блока.
		  * Устанавливает переменные, необходимые для остальных методов, вызываемых во внутренних циклах.
		  */
		void init(const ConstColumnPlainPtrs & key_columns)
		{
			vec = &static_cast<const ColumnVector<FieldType> *>(key_columns[0])->getData()[0];
		}

		/// Достать из ключевых столбцов ключ для вставки в хэш-таблицу.
		Key getKey(
			const ConstColumnPlainPtrs & key_columns,	/// Ключевые столбцы.
			size_t keys_size,							/// Количество ключевых столбцов.
			size_t i,						/// Из какой строки блока достать ключ.
			const Sizes & key_sizes) const	/// Если ключи фиксированной длины - их длины. Не используется в методах по ключам переменной длины.
		{
			return unionCastToUInt64(vec[i]);
		}
	};

	/** Разместить дополнительные данные, если это необходимо, в случае, когда в хэш-таблицу был вставлен новый ключ.
	  */
	static void onNewKey(typename Data::value_type & value, size_t keys_size, size_t i, Arena & pool) {}
};

/// Для случая, когда есть один строковый ключ.
template <typename TData>
struct SetMethodString
{
	using Data = TData;
	using Key = typename Data::key_type;

	Data data;

	struct State
	{
		const ColumnString::Offsets_t * offsets;
		const ColumnString::Chars_t * chars;

		void init(const ConstColumnPlainPtrs & key_columns)
		{
			const IColumn & column = *key_columns[0];
			const ColumnString & column_string = static_cast<const ColumnString &>(column);
			offsets = &column_string.getOffsets();
			chars = &column_string.getChars();
		}

		Key getKey(
			const ConstColumnPlainPtrs & key_columns,
			size_t keys_size,
			size_t i,
			const Sizes & key_sizes) const
		{
			return StringRef(
				&(*chars)[i == 0 ? 0 : (*offsets)[i - 1]],
				(i == 0 ? (*offsets)[i] : ((*offsets)[i] - (*offsets)[i - 1])) - 1);
		}
	};

	static void onNewKey(typename Data::value_type & value, size_t keys_size, size_t i, Arena & pool)
	{
		value.data = pool.insert(value.data, value.size);
	}
};

/// Для случая, когда есть один строковый ключ фиксированной длины.
template <typename TData>
struct SetMethodFixedString
{
	using Data = TData;
	using Key = typename Data::key_type;

	Data data;

	struct State
	{
		size_t n;
		const ColumnFixedString::Chars_t * chars;

		void init(const ConstColumnPlainPtrs & key_columns)
		{
			const IColumn & column = *key_columns[0];
			const ColumnFixedString & column_string = static_cast<const ColumnFixedString &>(column);
			n = column_string.getN();
			chars = &column_string.getChars();
		}

		Key getKey(
			const ConstColumnPlainPtrs & key_columns,
			size_t keys_size,
			size_t i,
			const Sizes & key_sizes) const
		{
			return StringRef(&(*chars)[i * n], n);
		}
	};

	static void onNewKey(typename Data::value_type & value, size_t keys_size, size_t i, Arena & pool)
	{
		value.data = pool.insert(value.data, value.size);
	}
};

namespace set_impl
{

/// This class is designed to provide the functionality that is required for
/// supporting nullable keys in SetMethodKeysFixed. If there are
/// no nullable keys, this class is merely implemented as an empty shell.
template <typename Key, bool has_nullable_keys>
class BaseStateKeysFixed;

/// Case where nullable keys are supported.
template <typename Key>
class BaseStateKeysFixed<Key, true>
{
protected:
	void init(const ConstColumnPlainPtrs & key_columns)
	{
		null_maps.reserve(key_columns.size());
		actual_columns.reserve(key_columns.size());

		for (const auto & col : key_columns)
		{
			if (col->isNullable())
			{
				const auto & nullable_col = static_cast<const ColumnNullable &>(*col);
				actual_columns.push_back(nullable_col.getNestedColumn().get());
				null_maps.push_back(nullable_col.getNullMapColumn().get());
			}
			else
			{
				actual_columns.push_back(col);
				null_maps.push_back(nullptr);
			}
		}
	}

	/// Return the columns which actually contain the values of the keys.
	/// For a given key column, if it is nullable, we return its nested
	/// column. Otherwise we return the key column itself.
	inline const ConstColumnPlainPtrs & getActualColumns() const
	{
		return actual_columns;
	}

	/// Create a bitmap that indicates whether, for a particular row,
	/// a key column bears a null value or not.
	KeysNullMap<Key> createBitmap(size_t row) const
	{
		KeysNullMap<Key> bitmap{};

		for (size_t k = 0; k < null_maps.size(); ++k)
		{
			if (null_maps[k] != nullptr)
			{
				const auto & null_map = static_cast<const ColumnUInt8 &>(*null_maps[k]).getData();
				if (null_map[row] == 1)
				{
					size_t bucket = k / 8;
					size_t offset = k % 8;
					bitmap[bucket] |= UInt8(1) << offset;
				}
			}
		}

		return bitmap;
	}

private:
	ConstColumnPlainPtrs actual_columns;
	ConstColumnPlainPtrs null_maps;
};

/// Case where nullable keys are not supported.
template <typename Key>
class BaseStateKeysFixed<Key, false>
{
protected:
	void init(const ConstColumnPlainPtrs & key_columns)
	{
		throw Exception{"Internal error: calling init() for non-nullable"
			" keys is forbidden", ErrorCodes::LOGICAL_ERROR};
	}

	const ConstColumnPlainPtrs & getActualColumns() const
	{
		throw Exception{"Internal error: calling getActualColumns() for non-nullable"
			" keys is forbidden", ErrorCodes::LOGICAL_ERROR};
	}

	KeysNullMap<Key> createBitmap(size_t row) const
	{
		throw Exception{"Internal error: calling createBitmap() for non-nullable keys"
			" is forbidden", ErrorCodes::LOGICAL_ERROR};
	}
};

}

/// Для случая, когда все ключи фиксированной длины, и они помещаются в N (например, 128) бит.
template <typename TData, bool has_nullable_keys_ = false>
struct SetMethodKeysFixed
{
	using Data = TData;
	using Key = typename Data::key_type;
	static constexpr bool has_nullable_keys = has_nullable_keys_;

	Data data;

	class State : private set_impl::BaseStateKeysFixed<Key, has_nullable_keys>
	{
	public:
		using Base = set_impl::BaseStateKeysFixed<Key, has_nullable_keys>;

		void init(const ConstColumnPlainPtrs & key_columns)
		{
			if (has_nullable_keys)
				Base::init(key_columns);
		}

		Key getKey(
			const ConstColumnPlainPtrs & key_columns,
			size_t keys_size,
			size_t i,
			const Sizes & key_sizes) const
		{
			if (has_nullable_keys)
			{
				auto bitmap = Base::createBitmap(i);
				return packFixed<Key>(i, keys_size, Base::getActualColumns(), key_sizes, bitmap);
			}
			else
				return packFixed<Key>(i, keys_size, key_columns, key_sizes);
		}
	};

	static void onNewKey(typename Data::value_type & value, size_t keys_size, size_t i, Arena & pool) {}
};

/// Для остальных случаев. По 128 битному хэшу от ключа. (При этом, строки, содержащие нули посередине, могут склеиться.)
template <typename TData>
struct SetMethodHashed
{
	using Data = TData;
	using Key = typename Data::key_type;

	Data data;

	struct State
	{
		void init(const ConstColumnPlainPtrs & key_columns)
		{
		}

		Key getKey(
			const ConstColumnPlainPtrs & key_columns,
			size_t keys_size,
			size_t i,
			const Sizes & key_sizes) const
		{
			return hash128(i, keys_size, key_columns);
		}
	};

	static void onNewKey(typename Data::value_type & value, size_t keys_size, size_t i, Arena & pool) {}
};


/** Разные варианты реализации множества.
  */
struct SetVariants
{
	/// TODO Использовать для этих двух вариантов bit- или byte- set.
	std::unique_ptr<SetMethodOneNumber<UInt8, HashSet<UInt8, TrivialHash, HashTableFixedGrower<8>>>> 	key8;
	std::unique_ptr<SetMethodOneNumber<UInt16, HashSet<UInt16, TrivialHash, HashTableFixedGrower<16>>>> key16;

	/** Также для эксперимента проверялась возможность использовать SmallSet,
	  *  пока количество элементов в множестве небольшое (и, при необходимости, конвертировать в полноценный HashSet).
	  * Но этот эксперимент показал, что преимущество есть только в редких случаях.
	  */
	std::unique_ptr<SetMethodOneNumber<UInt32, HashSet<UInt32, HashCRC32<UInt32>>>> key32;
	std::unique_ptr<SetMethodOneNumber<UInt64, HashSet<UInt64, HashCRC32<UInt64>>>> key64;
	std::unique_ptr<SetMethodString<HashSetWithSavedHash<StringRef>>> 				key_string;
	std::unique_ptr<SetMethodFixedString<HashSetWithSavedHash<StringRef>>> 			key_fixed_string;
	std::unique_ptr<SetMethodKeysFixed<HashSet<UInt128, UInt128HashCRC32>>> 		keys128;
	std::unique_ptr<SetMethodKeysFixed<HashSet<UInt256, UInt256HashCRC32>>> 		keys256;
	std::unique_ptr<SetMethodHashed<HashSet<UInt128, UInt128TrivialHash>>> 			hashed;

	/// Support for nullable keys.
	std::unique_ptr<SetMethodKeysFixed<HashSet<UInt128, UInt128HashCRC32>, true>> 		nullable_keys128;
	std::unique_ptr<SetMethodKeysFixed<HashSet<UInt256, UInt256HashCRC32>, true>> 		nullable_keys256;

	/** В отличие от Aggregator, здесь не используется метод concat.
	  * Это сделано потому что метод hashed, хоть и медленнее, но в данном случае, использует меньше оперативки.
	  *  так как при его использовании, сами значения ключей не сохраняются.
	  */

	Arena string_pool;

	#define APPLY_FOR_SET_VARIANTS(M) \
		M(key8) 			\
		M(key16) 			\
		M(key32) 			\
		M(key64) 			\
		M(key_string) 		\
		M(key_fixed_string) \
		M(keys128) 			\
		M(keys256) 			\
		M(nullable_keys128)	\
		M(nullable_keys256)	\
		M(hashed)

	enum class Type
	{
		EMPTY,

	#define M(NAME) NAME,
		APPLY_FOR_SET_VARIANTS(M)
	#undef M
	};

	Type type = Type::EMPTY;

	bool empty() const { return type == Type::EMPTY; }

	static Type chooseMethod(const ConstColumnPlainPtrs & key_columns, Sizes & key_sizes);

	void init(Type type_);

	size_t getTotalRowCount() const;
	/// Считает размер в байтах буфера Set и размер string_pool'а
	size_t getTotalByteCount() const;
};

}
