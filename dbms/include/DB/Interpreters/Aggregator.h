#pragma once

#include <mutex>
#include <memory>
#include <functional>

#include <Yandex/logger_useful.h>
#include <statdaemons/threadpool.hpp>

#include <DB/Core/StringRef.h>
#include <DB/Common/Arena.h>
#include <DB/Common/HashTable/HashMap.h>
#include <DB/Common/HashTable/TwoLevelHashMap.h>

#include <DB/DataStreams/IBlockInputStream.h>

#include <DB/Interpreters/AggregateDescription.h>
#include <DB/Interpreters/AggregationCommon.h>
#include <DB/Interpreters/Limits.h>
#include <DB/Interpreters/Compiler.h>

#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnFixedString.h>
#include <DB/Columns/ColumnAggregateFunction.h>
#include <DB/Columns/ColumnVector.h>



namespace DB
{


/** Разные структуры данных, которые могут использоваться для агрегации
  * Для эффективности, сами данные для агрегации кладутся в пул.
  * Владение данными (состояний агрегатных функций) и пулом
  *  захватывается позднее - в функции convertToBlocks, объектом ColumnAggregateFunction.
  *
  * Большинство структур данных существует в двух вариантах: обычном и двухуровневом (TwoLevel).
  * Двухуровневая хэш-таблица работает чуть медленнее при маленьком количестве различных ключей,
  *  но при большом количестве различных ключей лучше масштабируется, так как позволяет
  *  распараллелить некоторые операции (слияние, пост-обработку) естественным образом.
  *
  * Чтобы обеспечить эффективную работу в большом диапазоне условий,
  *  сначала используются одноуровневые хэш-таблицы,
  *  а при достижении количеством различных ключей достаточно большого размера,
  *  они конвертируются в двухуровневые.
  *
  * PS. Существует много различных подходов к эффективной реализации параллельной и распределённой агрегации,
  *  лучшим образом подходящих для разных случаев, и этот подход - всего лишь один из них, выбранный по совокупности причин.
  */
typedef AggregateDataPtr AggregatedDataWithoutKey;

typedef HashMap<UInt64, AggregateDataPtr, HashCRC32<UInt64>> AggregatedDataWithUInt64Key;
typedef HashMapWithSavedHash<StringRef, AggregateDataPtr> AggregatedDataWithStringKey;
typedef HashMap<UInt128, AggregateDataPtr, UInt128HashCRC32> AggregatedDataWithKeys128;
typedef HashMap<UInt256, AggregateDataPtr, UInt256HashCRC32> AggregatedDataWithKeys256;
typedef HashMap<UInt128, std::pair<StringRef*, AggregateDataPtr>, UInt128TrivialHash> AggregatedDataHashed;

typedef TwoLevelHashMap<UInt64, AggregateDataPtr, HashCRC32<UInt64>> AggregatedDataWithUInt64KeyTwoLevel;
typedef TwoLevelHashMapWithSavedHash<StringRef, AggregateDataPtr> AggregatedDataWithStringKeyTwoLevel;
typedef TwoLevelHashMap<UInt128, AggregateDataPtr, UInt128HashCRC32> AggregatedDataWithKeys128TwoLevel;
typedef TwoLevelHashMap<UInt256, AggregateDataPtr, UInt256HashCRC32> AggregatedDataWithKeys256TwoLevel;
typedef TwoLevelHashMap<UInt128, std::pair<StringRef*, AggregateDataPtr>, UInt128TrivialHash> AggregatedDataHashedTwoLevel;

typedef HashMap<UInt64, AggregateDataPtr, TrivialHash, HashTableFixedGrower<8>> AggregatedDataWithUInt8Key;
typedef HashMap<UInt64, AggregateDataPtr, TrivialHash, HashTableFixedGrower<16>> AggregatedDataWithUInt16Key;


/// Для случая, когда есть один числовой ключ.
template <typename FieldType, typename TData>	/// UInt8/16/32/64 для любых типов соответствующей битности.
struct AggregationMethodOneNumber
{
	typedef TData Data;
	typedef typename Data::key_type Key;
	typedef typename Data::mapped_type Mapped;
	typedef typename Data::iterator iterator;
	typedef typename Data::const_iterator const_iterator;

	Data data;

	AggregationMethodOneNumber() {}

	template <typename Other>
	AggregationMethodOneNumber(const Other & other) : data(other.data) {}

	/// Для использования одного Method в разных потоках, используйте разные State.
	struct State
	{
		const FieldType * vec;

		/** Вызывается в начале обработки каждого блока.
		  * Устанавливает переменные, необходимые для остальных методов, вызываемых во внутренних циклах.
		  */
		void init(ConstColumnPlainPtrs & key_columns)
		{
			vec = &static_cast<const ColumnVector<FieldType> *>(key_columns[0])->getData()[0];
		}

		/// Достать из ключевых столбцов ключ для вставки в хэш-таблицу.
		Key getKey(
			const ConstColumnPlainPtrs & key_columns,	/// Ключевые столбцы.
			size_t keys_size,							/// Количество ключевых столбцов.
			size_t i,					/// Из какой строки блока достать ключ.
			const Sizes & key_sizes,	/// Если ключи фиксированной длины - их длины. Не используется в методах агрегации по ключам переменной длины.
			StringRefs & keys,			/// Сюда могут быть записаны ссылки на данные ключей в столбцах. Они могут быть использованы в дальнейшем.
			Arena & pool) const
		{
			return unionCastToUInt64(vec[i]);
		}
	};

	/// Из значения в хэш-таблице получить AggregateDataPtr.
	static AggregateDataPtr & getAggregateData(Mapped & value) 				{ return value; }
	static const AggregateDataPtr & getAggregateData(const Mapped & value) 	{ return value; }

	/** Разместить дополнительные данные, если это необходимо, в случае, когда в хэш-таблицу был вставлен новый ключ.
	  */
	static void onNewKey(typename Data::value_type & value, size_t keys_size, size_t i, StringRefs & keys, Arena & pool)
	{
	}

	/** Действие, которое нужно сделать, если ключ не новый. Например, откатить выделение памяти в пуле.
	  */
	static void onExistingKey(const Key & key, StringRefs & keys, Arena & pool) {}

	/** Не использовать оптимизацию для идущих подряд ключей.
	  */
	static const bool no_consecutive_keys_optimization = false;

	/** Вставить ключ из хэш-таблицы в столбцы.
	  */
	static void insertKeyIntoColumns(const typename Data::value_type & value, ColumnPlainPtrs & key_columns, size_t keys_size, const Sizes & key_sizes)
	{
		static_cast<ColumnVector<FieldType> *>(key_columns[0])->insertData(reinterpret_cast<const char *>(&value.first), sizeof(value.first));
	}
};


/// Для случая, когда есть один строковый ключ.
template <typename TData>
struct AggregationMethodString
{
	typedef TData Data;
	typedef typename Data::key_type Key;
	typedef typename Data::mapped_type Mapped;
	typedef typename Data::iterator iterator;
	typedef typename Data::const_iterator const_iterator;

	Data data;

	AggregationMethodString() {}

	template <typename Other>
	AggregationMethodString(const Other & other) : data(other.data) {}

	struct State
	{
		const ColumnString::Offsets_t * offsets;
		const ColumnString::Chars_t * chars;

		void init(ConstColumnPlainPtrs & key_columns)
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
			const Sizes & key_sizes,
			StringRefs & keys,
			Arena & pool) const
		{
			return StringRef(
				&(*chars)[i == 0 ? 0 : (*offsets)[i - 1]],
				(i == 0 ? (*offsets)[i] : ((*offsets)[i] - (*offsets)[i - 1])) - 1);
		}
	};

	static AggregateDataPtr & getAggregateData(Mapped & value) 				{ return value; }
	static const AggregateDataPtr & getAggregateData(const Mapped & value) 	{ return value; }

	static void onNewKey(typename Data::value_type & value, size_t keys_size, size_t i, StringRefs & keys, Arena & pool)
	{
		value.first.data = pool.insert(value.first.data, value.first.size);
	}

	static void onExistingKey(const Key & key, StringRefs & keys, Arena & pool) {}

	static const bool no_consecutive_keys_optimization = false;

	static void insertKeyIntoColumns(const typename Data::value_type & value, ColumnPlainPtrs & key_columns, size_t keys_size, const Sizes & key_sizes)
	{
		key_columns[0]->insertData(value.first.data, value.first.size);
	}
};


/// Для случая, когда есть один строковый ключ фиксированной длины.
template <typename TData>
struct AggregationMethodFixedString
{
	typedef TData Data;
	typedef typename Data::key_type Key;
	typedef typename Data::mapped_type Mapped;
	typedef typename Data::iterator iterator;
	typedef typename Data::const_iterator const_iterator;

	Data data;

	AggregationMethodFixedString() {}

	template <typename Other>
	AggregationMethodFixedString(const Other & other) : data(other.data) {}

	struct State
	{
		size_t n;
		const ColumnFixedString::Chars_t * chars;

		void init(ConstColumnPlainPtrs & key_columns)
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
			const Sizes & key_sizes,
			StringRefs & keys,
			Arena & pool) const
		{
			return StringRef(&(*chars)[i * n], n);
		}
	};

	static AggregateDataPtr & getAggregateData(Mapped & value) 				{ return value; }
	static const AggregateDataPtr & getAggregateData(const Mapped & value) 	{ return value; }

	static void onNewKey(typename Data::value_type & value, size_t keys_size, size_t i, StringRefs & keys, Arena & pool)
	{
		value.first.data = pool.insert(value.first.data, value.first.size);
	}

	static void onExistingKey(const Key & key, StringRefs & keys, Arena & pool) {}

	static const bool no_consecutive_keys_optimization = false;

	static void insertKeyIntoColumns(const typename Data::value_type & value, ColumnPlainPtrs & key_columns, size_t keys_size, const Sizes & key_sizes)
	{
		key_columns[0]->insertData(value.first.data, value.first.size);
	}
};


/// Для случая, когда все ключи фиксированной длины, и они помещаются в N (например, 128) бит.
template <typename TData>
struct AggregationMethodKeysFixed
{
	typedef TData Data;
	typedef typename Data::key_type Key;
	typedef typename Data::mapped_type Mapped;
	typedef typename Data::iterator iterator;
	typedef typename Data::const_iterator const_iterator;

	Data data;

	AggregationMethodKeysFixed() {}

	template <typename Other>
	AggregationMethodKeysFixed(const Other & other) : data(other.data) {}

	struct State
	{
		void init(ConstColumnPlainPtrs & key_columns)
		{
		}

		Key getKey(
			const ConstColumnPlainPtrs & key_columns,
			size_t keys_size,
			size_t i,
			const Sizes & key_sizes,
			StringRefs & keys,
			Arena & pool) const
		{
			return packFixed<Key>(i, keys_size, key_columns, key_sizes);
		}
	};

	static AggregateDataPtr & getAggregateData(Mapped & value) 				{ return value; }
	static const AggregateDataPtr & getAggregateData(const Mapped & value) 	{ return value; }

	static void onNewKey(typename Data::value_type & value, size_t keys_size, size_t i, StringRefs & keys, Arena & pool)
	{
	}

	static void onExistingKey(const Key & key, StringRefs & keys, Arena & pool) {}

	static const bool no_consecutive_keys_optimization = false;

	static void insertKeyIntoColumns(const typename Data::value_type & value, ColumnPlainPtrs & key_columns, size_t keys_size, const Sizes & key_sizes)
	{
		size_t offset = 0;
		for (size_t i = 0; i < keys_size; ++i)
		{
			size_t size = key_sizes[i];
			key_columns[i]->insertData(reinterpret_cast<const char *>(&value.first) + offset, size);
			offset += size;
		}
	}
};


/// Для остальных случаев. Агрегирует по конкатенации ключей. (При этом, строки, содержащие нули посередине, могут склеиться.)
template <typename TData>
struct AggregationMethodConcat
{
	typedef TData Data;
	typedef typename Data::key_type Key;
	typedef typename Data::mapped_type Mapped;
	typedef typename Data::iterator iterator;
	typedef typename Data::const_iterator const_iterator;

	Data data;

	AggregationMethodConcat() {}

	template <typename Other>
	AggregationMethodConcat(const Other & other) : data(other.data) {}

	struct State
	{
		void init(ConstColumnPlainPtrs & key_columns)
		{
		}

		Key getKey(
			const ConstColumnPlainPtrs & key_columns,
			size_t keys_size,
			size_t i,
			const Sizes & key_sizes,
			StringRefs & keys,
			Arena & pool) const
		{
			return extractKeysAndPlaceInPoolContiguous(i, keys_size, key_columns, keys, pool);
		}
	};

	static AggregateDataPtr & getAggregateData(Mapped & value) 				{ return value; }
	static const AggregateDataPtr & getAggregateData(const Mapped & value) 	{ return value; }

	static void onNewKey(typename Data::value_type & value, size_t keys_size, size_t i, StringRefs & keys, Arena & pool)
	{
	}

	static void onExistingKey(const Key & key, StringRefs & keys, Arena & pool)
	{
		pool.rollback(key.size + keys.size() * sizeof(keys[0]));
	}

	/// Если ключ уже был, то он удаляется из пула (затирается), и сравнить с ним следующий ключ уже нельзя.
	static const bool no_consecutive_keys_optimization = true;

	static void insertKeyIntoColumns(const typename Data::value_type & value, ColumnPlainPtrs & key_columns, size_t keys_size, const Sizes & key_sizes)
	{
		/// См. функцию extractKeysAndPlaceInPoolContiguous.
		const StringRef * key_refs = reinterpret_cast<const StringRef *>(value.first.data + value.first.size);

		if (unlikely(0 == value.first.size))
		{
			/** Исправление, если все ключи - пустые массивы. Для них в хэш-таблицу записывается StringRef нулевой длины, но с ненулевым указателем.
			  * Но при вставке в хэш-таблицу, такой StringRef оказывается равен другому ключу нулевой длины,
			  *  у которого указатель на данные может быть любым мусором и использовать его нельзя.
			  */
			for (size_t i = 0; i < keys_size; ++i)
				key_columns[i]->insertDefault();
		}
		else
		{
			for (size_t i = 0; i < keys_size; ++i)
				key_columns[i]->insertDataWithTerminatingZero(key_refs[i].data, key_refs[i].size);
		}
	}
};


/// Для остальных случаев. Агрегирует по 128 битному хэшу от ключа. (При этом, строки, содержащие нули посередине, могут склеиться.)
template <typename TData>
struct AggregationMethodHashed
{
	typedef TData Data;
	typedef typename Data::key_type Key;
	typedef typename Data::mapped_type Mapped;
	typedef typename Data::iterator iterator;
	typedef typename Data::const_iterator const_iterator;

	Data data;

	AggregationMethodHashed() {}

	template <typename Other>
	AggregationMethodHashed(const Other & other) : data(other.data) {}

	struct State
	{
		void init(ConstColumnPlainPtrs & key_columns)
		{
		}

		Key getKey(
			const ConstColumnPlainPtrs & key_columns,
			size_t keys_size,
			size_t i,
			const Sizes & key_sizes,
			StringRefs & keys,
			Arena & pool) const
		{
			return hash128(i, keys_size, key_columns, keys);
		}
	};

	static AggregateDataPtr & getAggregateData(Mapped & value) 				{ return value.second; }
	static const AggregateDataPtr & getAggregateData(const Mapped & value) 	{ return value.second; }

	static void onNewKey(typename Data::value_type & value, size_t keys_size, size_t i, StringRefs & keys, Arena & pool)
	{
		value.second.first = placeKeysInPool(i, keys_size, keys, pool);
	}

	static void onExistingKey(const Key & key, StringRefs & keys, Arena & pool) {}

	static const bool no_consecutive_keys_optimization = false;

	static void insertKeyIntoColumns(const typename Data::value_type & value, ColumnPlainPtrs & key_columns, size_t keys_size, const Sizes & key_sizes)
	{
		for (size_t i = 0; i < keys_size; ++i)
			key_columns[i]->insertDataWithTerminatingZero(value.second.first[i].data, value.second.first[i].size);
	}
};


class Aggregator;

struct AggregatedDataVariants : private boost::noncopyable
{
	/** Работа с состояниями агрегатных функций в пуле устроена следующим (неудобным) образом:
	  * - при агрегации, состояния создаются в пуле с помощью функции IAggregateFunction::create (внутри - placement new произвольной структуры);
	  * - они должны быть затем уничтожены с помощью IAggregateFunction::destroy (внутри - вызов деструктора произвольной структуры);
	  * - если агрегация завершена, то, в функции Aggregator::convertToBlocks, указатели на состояния агрегатных функций
	  *   записываются в ColumnAggregateFunction; ColumnAggregateFunction "захватывает владение" ими, то есть - вызывает destroy в своём деструкторе.
	  * - если при агрегации, до вызова Aggregator::convertToBlocks вылетело исключение,
	  *   то состояния агрегатных функций всё-равно должны быть уничтожены,
	  *   иначе для сложных состояний (наприемер, AggregateFunctionUniq), будут утечки памяти;
	  * - чтобы, в этом случае, уничтожить состояния, в деструкторе вызывается метод Aggregator::destroyAggregateStates,
	  *   но только если переменная aggregator (см. ниже) не nullptr;
	  * - то есть, пока вы не передали владение состояниями агрегатных функций в ColumnAggregateFunction, установите переменную aggregator,
	  *   чтобы при возникновении исключения, состояния были корректно уничтожены.
	  *
	  * PS. Это можно исправить, сделав пул, который знает о том, какие состояния агрегатных функций и в каком порядке в него уложены, и умеет сам их уничтожать.
	  * Но это вряд ли можно просто сделать, так как в этот же пул планируется класть строки переменной длины.
	  * В этом случае, пул не сможет знать, по каким смещениям хранятся объекты.
	  */
	Aggregator * aggregator = nullptr;

	size_t keys_size;	/// Количество ключей NOTE нужно ли это поле?
	Sizes key_sizes;	/// Размеры ключей, если ключи фиксированной длины

	/// Пулы для состояний агрегатных функций. Владение потом будет передано в ColumnAggregateFunction.
	Arenas aggregates_pools;
	Arena * aggregates_pool;	/// Пул, который сейчас используется для аллокации.

	/** Специализация для случая, когда ключи отсутствуют, и для ключей, не попавших в max_rows_to_group_by.
	  */
	AggregatedDataWithoutKey without_key = nullptr;

	std::unique_ptr<AggregationMethodOneNumber<UInt8, AggregatedDataWithUInt8Key>>			key8;
	std::unique_ptr<AggregationMethodOneNumber<UInt16, AggregatedDataWithUInt16Key>>		key16;

	std::unique_ptr<AggregationMethodOneNumber<UInt32, AggregatedDataWithUInt64Key>>		key32;
	std::unique_ptr<AggregationMethodOneNumber<UInt64, AggregatedDataWithUInt64Key>>		key64;
	std::unique_ptr<AggregationMethodString<AggregatedDataWithStringKey>> 					key_string;
	std::unique_ptr<AggregationMethodFixedString<AggregatedDataWithStringKey>> 				key_fixed_string;
	std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys128>> 					keys128;
	std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys256>> 					keys256;
	std::unique_ptr<AggregationMethodHashed<AggregatedDataHashed>> 							hashed;
	std::unique_ptr<AggregationMethodConcat<AggregatedDataWithStringKey>> 					concat;

	std::unique_ptr<AggregationMethodOneNumber<UInt32, AggregatedDataWithUInt64KeyTwoLevel>>	key32_two_level;
	std::unique_ptr<AggregationMethodOneNumber<UInt64, AggregatedDataWithUInt64KeyTwoLevel>>	key64_two_level;
	std::unique_ptr<AggregationMethodString<AggregatedDataWithStringKeyTwoLevel>>				key_string_two_level;
	std::unique_ptr<AggregationMethodFixedString<AggregatedDataWithStringKeyTwoLevel>> 			key_fixed_string_two_level;
	std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys128TwoLevel>> 				keys128_two_level;
	std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys256TwoLevel>> 				keys256_two_level;
	std::unique_ptr<AggregationMethodHashed<AggregatedDataHashedTwoLevel>> 						hashed_two_level;
	std::unique_ptr<AggregationMethodConcat<AggregatedDataWithStringKeyTwoLevel>> 				concat_two_level;

	/// В этом и подобных макросах, вариант without_key не учитывается.
	#define APPLY_FOR_AGGREGATED_VARIANTS(M) \
		M(key8,					false) \
		M(key16,				false) \
		M(key32,				false) \
		M(key64,				false) \
		M(key_string,			false) \
		M(key_fixed_string,		false) \
		M(keys128,				false) \
		M(keys256,				false) \
		M(hashed,				false) \
		M(concat,				false) \
		M(key32_two_level,				true) \
		M(key64_two_level,				true) \
		M(key_string_two_level,			true) \
		M(key_fixed_string_two_level,	true) \
		M(keys128_two_level,			true) \
		M(keys256_two_level,			true) \
		M(hashed_two_level,				true) \
		M(concat_two_level,				true)

	enum class Type
	{
		EMPTY = 0,
		without_key,

	#define M(NAME, IS_TWO_LEVEL) NAME,
		APPLY_FOR_AGGREGATED_VARIANTS(M)
	#undef M
	};
	Type type = Type::EMPTY;

	AggregatedDataVariants() : aggregates_pools(1, new Arena), aggregates_pool(&*aggregates_pools.back()) {}
	bool empty() const { return type == Type::EMPTY; }
	void invalidate() { type = Type::EMPTY; }

	~AggregatedDataVariants();

	void init(Type type_)
	{
		switch (type_)
		{
			case Type::EMPTY:		break;
			case Type::without_key:	break;

		#define M(NAME, IS_TWO_LEVEL) \
			case Type::NAME: NAME.reset(new decltype(NAME)::element_type); break;
			APPLY_FOR_AGGREGATED_VARIANTS(M)
		#undef M

			default:
				throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
		}

		type = type_;
	}

	size_t size() const
	{
		switch (type)
		{
			case Type::EMPTY:		return 0;
			case Type::without_key:	return 1;

		#define M(NAME, IS_TWO_LEVEL) \
			case Type::NAME: return NAME->data.size() + (without_key != nullptr);
			APPLY_FOR_AGGREGATED_VARIANTS(M)
		#undef M

			default:
				throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
		}
	}

	/// Размер без учёта строчки, в которую записываются данные для расчёта TOTALS.
	size_t sizeWithoutOverflowRow() const
	{
		switch (type)
		{
			case Type::EMPTY:		return 0;
			case Type::without_key:	return 1;

			#define M(NAME, IS_TWO_LEVEL) \
			case Type::NAME: return NAME->data.size();
			APPLY_FOR_AGGREGATED_VARIANTS(M)
			#undef M

			default:
				throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
		}
	}

	const char * getMethodName() const
	{
		switch (type)
		{
			case Type::EMPTY:		return "EMPTY";
			case Type::without_key:	return "without_key";

		#define M(NAME, IS_TWO_LEVEL) \
			case Type::NAME: return #NAME;
			APPLY_FOR_AGGREGATED_VARIANTS(M)
		#undef M

			default:
				throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
		}
	}

	bool isTwoLevel() const
	{
		switch (type)
		{
			case Type::EMPTY:		return false;
			case Type::without_key:	return false;

		#define M(NAME, IS_TWO_LEVEL) \
			case Type::NAME: return IS_TWO_LEVEL;
			APPLY_FOR_AGGREGATED_VARIANTS(M)
		#undef M

			default:
				throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
		}
	}

	#define APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M) \
		M(key32)			\
		M(key64)			\
		M(key_string)		\
		M(key_fixed_string)	\
		M(keys128)			\
		M(keys256)			\
		M(hashed)			\
		M(concat)

	#define APPLY_FOR_VARIANTS_NOT_CONVERTIBLE_TO_TWO_LEVEL(M) \
		M(key8)				\
		M(key16)			\

	bool isConvertibleToTwoLevel() const
	{
		switch (type)
		{
		#define M(NAME) \
			case Type::NAME: return true;

			APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M)

		#undef M
			default:
				return false;
		}
	}

	void convertToTwoLevel();

	#define APPLY_FOR_VARIANTS_TWO_LEVEL(M) \
			M(key32_two_level)				\
			M(key64_two_level)				\
			M(key_string_two_level)			\
			M(key_fixed_string_two_level)	\
			M(keys128_two_level)			\
			M(keys256_two_level)			\
			M(hashed_two_level)				\
			M(concat_two_level)
};

typedef SharedPtr<AggregatedDataVariants> AggregatedDataVariantsPtr;
typedef std::vector<AggregatedDataVariantsPtr> ManyAggregatedDataVariants;


/** Агрегирует источник блоков.
  */
class Aggregator
{
public:
	Aggregator(const Names & key_names_, const AggregateDescriptions & aggregates_, bool overflow_row_,
		size_t max_rows_to_group_by_, OverflowMode group_by_overflow_mode_, Compiler * compiler_, UInt32 min_count_to_compile_,
		size_t group_by_two_level_threshold_)
		: key_names(key_names_), aggregates(aggregates_), aggregates_size(aggregates.size()),
		overflow_row(overflow_row_),
		max_rows_to_group_by(max_rows_to_group_by_), group_by_overflow_mode(group_by_overflow_mode_),
		compiler(compiler_), min_count_to_compile(min_count_to_compile_), group_by_two_level_threshold(group_by_two_level_threshold_),
		isCancelled([]() { return false; })
	{
		std::sort(key_names.begin(), key_names.end());
		key_names.erase(std::unique(key_names.begin(), key_names.end()), key_names.end());
		keys_size = key_names.size();
	}

	/// Агрегировать источник. Получить результат в виде одной из структур данных.
	void execute(BlockInputStreamPtr stream, AggregatedDataVariants & result);

	using AggregateColumns = std::vector<ConstColumnPlainPtrs>;
	using AggregateColumnsData = std::vector<ColumnAggregateFunction::Container_t *>;
	using AggregateFunctionsPlainPtrs = std::vector<IAggregateFunction *>;

	/// Обработать один блок. Вернуть false, если обработку следует прервать (при group_by_overflow_mode = 'break').
	bool executeOnBlock(Block & block, AggregatedDataVariants & result,
		ConstColumnPlainPtrs & key_columns, AggregateColumns & aggregate_columns,	/// Передаются, чтобы не создавать их заново на каждый блок
		Sizes & key_sizes, StringRefs & keys,										/// - передайте соответствующие объекты, которые изначально пустые.
		bool & no_more_keys);

	/** Преобразовать структуру данных агрегации в блок.
	  * Если overflow_row = true, то агрегаты для строк, не попавших в max_rows_to_group_by, кладутся в первый блок.
	  *
	  * Если final = false, то в качестве столбцов-агрегатов создаются ColumnAggregateFunction с состоянием вычислений,
	  *  которые могут быть затем объединены с другими состояниями (для распределённой обработки запроса).
	  * Если final = true, то в качестве столбцов-агрегатов создаются столбцы с готовыми значениями.
	  */
	BlocksList convertToBlocks(AggregatedDataVariants & data_variants, bool final, size_t max_threads);

	/** Объединить несколько структур данных агрегации в одну. (В первый непустой элемент массива.)
	  * После объединения, все стркутуры агрегации (а не только те, в которую они будут слиты) должны жить,
	  *  пока не будет вызвана функция convertToBlocks.
	  * Это нужно, так как в слитом результате могут остаться указатели на память в пуле, которым владеют другие структуры агрегации.
	  */
	AggregatedDataVariantsPtr merge(ManyAggregatedDataVariants & data_variants, size_t max_threads);

	/** Объединить поток частично агрегированных блоков в одну структуру данных.
	  * (Доагрегировать несколько блоков, которые представляют собой результат независимых агрегаций с удалённых серверов.)
	  */
	void mergeStream(BlockInputStreamPtr stream, AggregatedDataVariants & result, size_t max_threads);

	/** Объединить несколько частично агрегированных блоков в один.
	  */
	Block mergeBlocks(BlocksList & blocks, bool final);

	using CancellationHook = std::function<bool()>;

	/** Установить функцию, которая проверяет, можно ли прервать текущую задачу.
	  */
	void setCancellationHook(const CancellationHook cancellation_hook);

	/// Для IBlockInputStream.
	String getID() const;

	size_t getNumberOfKeys() const { return keys_size; }
	size_t getNumberOfAggregates() const { return aggregates_size; }

protected:
	friend struct AggregatedDataVariants;

	ColumnNumbers keys;
	Names key_names;
	AggregateDescriptions aggregates;
	AggregateFunctionsPlainPtrs aggregate_functions;
	size_t keys_size;
	size_t aggregates_size;
	/// Нужно ли класть в AggregatedDataVariants::without_key агрегаты для ключей, не попавших в max_rows_to_group_by.
	bool overflow_row;

	Sizes offsets_of_aggregate_states;	/// Смещение до n-ой агрегатной функции в строке из агрегатных функций.
	size_t total_size_of_aggregate_states = 0;	/// Суммарный размер строки из агрегатных функций.
	bool all_aggregates_has_trivial_destructor = false;

	/// Для инициализации от первого блока при конкуррентном использовании.
	bool initialized = false;
	std::mutex mutex;

	size_t max_rows_to_group_by;
	OverflowMode group_by_overflow_mode;

	Block sample;

	Logger * log = &Logger::get("Aggregator");


	/** Для динамической компиляции, если предусмотрено. */
	Compiler * compiler = nullptr;
	UInt32 min_count_to_compile;

	/** Динамически скомпилированная библиотека для агрегации, если есть.
	  * Смысл динамической компиляции в том, чтобы специализировать код
	  *  под конкретный список агрегатных функций.
	  * Это позволяет развернуть цикл по созданию и обновлению состояний агрегатных функций,
	  *  а также использовать вместо виртуальных вызовов inline-код.
	  */
	struct CompiledData
	{
		SharedLibraryPtr compiled_aggregator;

		/// Получены с помощью dlsym. Нужно ещё сделать reinterpret_cast в указатель на функцию.
		void * compiled_method_ptr = nullptr;
		void * compiled_two_level_method_ptr = nullptr;
	};
	/// shared_ptr - чтобы передавать в callback, который может пережить Aggregator.
	std::shared_ptr<CompiledData> compiled_data { new CompiledData };

	bool compiled_if_possible = false;
	void compileIfPossible(AggregatedDataVariants::Type type);

	/** При каком количестве ключей, начинает использоваться двухуровневая агрегация.
	  * 0 - никогда не использовать.
	  */
	size_t group_by_two_level_threshold;

	/// Возвращает true, если можно прервать текущую задачу.
	CancellationHook isCancelled;

	/** Если заданы только имена столбцов (key_names, а также aggregates[i].column_name), то вычислить номера столбцов.
	  * Сформировать блок - пример результата.
	  */
	void initialize(Block & block);

	/** Выбрать способ агрегации на основе количества и типов ключей. */
	AggregatedDataVariants::Type chooseAggregationMethod(const ConstColumnPlainPtrs & key_columns, Sizes & key_sizes);

	/** Создать состояния агрегатных функций для одного ключа.
	  */
	void createAggregateStates(AggregateDataPtr & aggregate_data) const;

	/** Вызвать методы destroy для состояний агрегатных функций.
	  * Используется в обработчике исключений при агрегации, так как RAII в данном случае не применим.
	  */
	void destroyAllAggregateStates(AggregatedDataVariants & result);


	/// Обработать один блок данных, агрегировать данные в хэш-таблицу.
	template <typename Method>
	void executeImpl(
		Method & method,
		Arena * aggregates_pool,
		size_t rows,
		ConstColumnPlainPtrs & key_columns,
		AggregateColumns & aggregate_columns,
		const Sizes & key_sizes,
		StringRefs & keys,
		bool no_more_keys,
		AggregateDataPtr overflow_row) const;

	/// Специализация для конкретного значения no_more_keys.
	template <bool no_more_keys, typename Method>
	void executeImplCase(
		Method & method,
		typename Method::State & state,
		Arena * aggregates_pool,
		size_t rows,
		ConstColumnPlainPtrs & key_columns,
		AggregateColumns & aggregate_columns,
		const Sizes & key_sizes,
		StringRefs & keys,
		AggregateDataPtr overflow_row) const;

	/// Для случая, когда нет ключей (всё агрегировать в одну строку).
	void executeWithoutKeyImpl(
		AggregatedDataWithoutKey & res,
		size_t rows,
		AggregateColumns & aggregate_columns) const;

public:
	/// Шаблоны, инстанцирующиеся путём динамической компиляции кода - см. SpecializedAggregator.h

	template <typename Method, typename AggregateFunctionsList>
	void executeSpecialized(
		Method & method,
		Arena * aggregates_pool,
		size_t rows,
		ConstColumnPlainPtrs & key_columns,
		AggregateColumns & aggregate_columns,
		const Sizes & key_sizes,
		StringRefs & keys,
		bool no_more_keys,
		AggregateDataPtr overflow_row) const;

	template <bool no_more_keys, typename Method, typename AggregateFunctionsList>
	void executeSpecializedCase(
		Method & method,
		typename Method::State & state,
		Arena * aggregates_pool,
		size_t rows,
		ConstColumnPlainPtrs & key_columns,
		AggregateColumns & aggregate_columns,
		const Sizes & key_sizes,
		StringRefs & keys,
		AggregateDataPtr overflow_row) const;

	template <typename AggregateFunctionsList>
	void executeSpecializedWithoutKey(
		AggregatedDataWithoutKey & res,
		size_t rows,
		AggregateColumns & aggregate_columns) const;

protected:
	/// Слить данные из хэш-таблицы src в dst.
	template <typename Method, typename Table>
	void mergeDataImpl(
		Table & table_dst,
		Table & table_src) const;

	void mergeWithoutKeyDataImpl(
		ManyAggregatedDataVariants & non_empty_data) const;

	template <typename Method>
	void mergeSingleLevelDataImpl(
		ManyAggregatedDataVariants & non_empty_data) const;

	template <typename Method>
	void mergeTwoLevelDataImpl(
		ManyAggregatedDataVariants & many_data,
		boost::threadpool::pool * thread_pool) const;

	template <typename Method, typename Table>
	void convertToBlockImpl(
		Method & method,
		Table & data,
		ColumnPlainPtrs & key_columns,
		AggregateColumnsData & aggregate_columns,
		ColumnPlainPtrs & final_aggregate_columns,
		const Sizes & key_sizes,
		bool final) const;

	template <typename Method, typename Table>
	void convertToBlockImplFinal(
		Method & method,
		Table & data,
		ColumnPlainPtrs & key_columns,
		ColumnPlainPtrs & final_aggregate_columns,
		const Sizes & key_sizes) const;

	template <typename Method, typename Table>
	void convertToBlockImplNotFinal(
		Method & method,
		Table & data,
		ColumnPlainPtrs & key_columns,
		AggregateColumnsData & aggregate_columns,
		const Sizes & key_sizes) const;

	template <typename Filler>
	Block prepareBlockAndFill(
		AggregatedDataVariants & data_variants,
		bool final,
		size_t rows,
		Filler && filler) const;

	BlocksList prepareBlocksAndFillWithoutKey(AggregatedDataVariants & data_variants, bool final) const;
	BlocksList prepareBlocksAndFillSingleLevel(AggregatedDataVariants & data_variants, bool final) const;
	BlocksList prepareBlocksAndFillTwoLevel(AggregatedDataVariants & data_variants, bool final, boost::threadpool::pool * thread_pool) const;

	template <typename Method>
	BlocksList prepareBlocksAndFillTwoLevelImpl(
		AggregatedDataVariants & data_variants,
		Method & method,
		bool final,
		boost::threadpool::pool * thread_pool) const;

	template <typename Method, typename Table>
	void mergeStreamsImpl(
		Block & block,
		const Sizes & key_sizes,
		Arena * aggregates_pool,
		Method & method,
		Table & data) const;

	void mergeWithoutKeyStreamsImpl(
		Block & block,
		AggregatedDataVariants & result) const;

	template <typename Method>
	void destroyImpl(
		Method & method) const;
};


/** Достать вариант агрегации по его типу. */
template <typename Method> Method & getDataVariant(AggregatedDataVariants & variants);

#define M(NAME, IS_TWO_LEVEL) \
	template <> inline decltype(AggregatedDataVariants::NAME)::element_type & getDataVariant<decltype(AggregatedDataVariants::NAME)::element_type>(AggregatedDataVariants & variants) { return *variants.NAME; }

APPLY_FOR_AGGREGATED_VARIANTS(M)

#undef M


}
