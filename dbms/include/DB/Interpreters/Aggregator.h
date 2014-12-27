#pragma once

#include <map>
#include <unordered_map>

#include <Poco/Mutex.h>

#include <Yandex/logger_useful.h>
#include <statdaemons/threadpool.hpp>

#include <DB/Core/ColumnNumbers.h>
#include <DB/Core/Names.h>
#include <DB/Core/StringRef.h>
#include <DB/Common/Arena.h>

#include <DB/DataStreams/IBlockInputStream.h>

#include <DB/AggregateFunctions/IAggregateFunction.h>

#include <DB/Interpreters/AggregationCommon.h>
#include <DB/Interpreters/Limits.h>

#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnFixedString.h>
#include <DB/Columns/ColumnAggregateFunction.h>
#include <DB/Columns/ColumnVector.h>



namespace DB
{


struct AggregateDescription
{
	AggregateFunctionPtr function;
	Array parameters;		/// Параметры (параметрической) агрегатной функции.
	ColumnNumbers arguments;
	Names argument_names;	/// Используются, если arguments не заданы.
	String column_name;		/// Какое имя использовать для столбца со значениями агрегатной функции
};

typedef std::vector<AggregateDescription> AggregateDescriptions;


/** Разные структуры данных, которые могут использоваться для агрегации
  * Для эффективности сами данные для агрегации кладутся в пул.
  * Владение данными (состояний агрегатных функций) и пулом
  *  захватывается позднее - в функции convertToBlock, объектом ColumnAggregateFunction.
  */
typedef AggregateDataPtr AggregatedDataWithoutKey;
typedef TwoLevelHashMap<UInt64, AggregateDataPtr, HashCRC32<UInt64>> AggregatedDataWithUInt64Key;
typedef TwoLevelHashMapWithSavedHash<StringRef, AggregateDataPtr> AggregatedDataWithStringKey;
typedef TwoLevelHashMap<UInt128, AggregateDataPtr, UInt128HashCRC32> AggregatedDataWithKeys128;
typedef TwoLevelHashMap<UInt128, std::pair<StringRef*, AggregateDataPtr>, UInt128TrivialHash> AggregatedDataHashed;


/// Специализации для UInt8, UInt16.
struct TrivialHash
{
	template <typename T>
	size_t operator() (T key) const
	{
		return key;
	}
};

/// Превращает хэш-таблицу в что-то типа lookup-таблицы. Остаётся неоптимальность - в ячейках хранятся ключи.
template <size_t key_bits>
struct HashTableFixedGrower
{
	size_t bufSize() const				{ return 1 << key_bits; }
	size_t place(size_t x) const 		{ return x; }
	/// Тут можно было бы написать __builtin_unreachable(), но компилятор не до конца всё оптимизирует, и получается менее эффективно.
	size_t next(size_t pos) const		{ return pos + 1; }
	bool overflow(size_t elems) const	{ return false; }

	void increaseSize() { __builtin_unreachable(); }
	void set(size_t num_elems) {}
	void setBufSize(size_t buf_size_) {}
};

typedef HashMap<UInt64, AggregateDataPtr, TrivialHash, HashTableFixedGrower<8>> AggregatedDataWithUInt8Key;
typedef HashMap<UInt64, AggregateDataPtr, TrivialHash, HashTableFixedGrower<16>> AggregatedDataWithUInt16Key;

template <typename FieldType>
struct AggregatedDataWithUIntKey
{
	using Type = AggregatedDataWithUInt64Key;
	static constexpr bool never_overflows = false;
};

template <>
struct AggregatedDataWithUIntKey<UInt8>
{
	using Type = AggregatedDataWithUInt8Key;
	static constexpr bool never_overflows = true;	/// Говорит о том, что в результате агрегации не может быть много записей.
};

template <>
struct AggregatedDataWithUIntKey<UInt16>
{
	using Type = AggregatedDataWithUInt16Key;
	static constexpr bool never_overflows = true;
};


/// Для случая, когда есть один числовой ключ.
template <typename FieldType>	/// UInt8/16/32/64 для любых типов соответствующей битности.
struct AggregationMethodOneNumber
{
	typedef typename AggregatedDataWithUIntKey<FieldType>::Type Data;
	typedef typename Data::key_type Key;
	typedef typename Data::mapped_type Mapped;
	typedef typename Data::iterator iterator;
	typedef typename Data::const_iterator const_iterator;

	static constexpr bool never_overflows = AggregatedDataWithUIntKey<FieldType>::never_overflows;

	Data data;

	const FieldType * column;

	/** Вызывается в начале обработки каждого блока.
	  * Устанавливает переменные, необходимые для остальных методов, вызываемых во внутренних циклах.
	  */
	void init(ConstColumnPlainPtrs & key_columns)
	{
		column = &static_cast<const ColumnVector<FieldType> *>(key_columns[0])->getData()[0];
	}

	/// Достать из ключевых столбцов ключ для вставки в хэш-таблицу.
	Key getKey(
		const ConstColumnPlainPtrs & key_columns,	/// Ключевые столбцы.
		size_t keys_size,							/// Количество ключевых столбцов.
		size_t i,					/// Из какой строки блока достать ключ.
		const Sizes & key_sizes,	/// Если ключи фиксированной длины - их длины. Не используется в методах агрегации по ключам переменной длины.
		StringRefs & keys) const	/// Сюда могут быть записаны ссылки на данные ключей в столбцах. Они могут быть использованы в дальнейшем.
	{
		return get64(column[i]);
	}

	/// Из значения в хэш-таблице получить AggregateDataPtr.
	static AggregateDataPtr & getAggregateData(Mapped & value) 				{ return value; }
	static const AggregateDataPtr & getAggregateData(const Mapped & value) 	{ return value; }

	/** Разместить дополнительные данные, если это необходимо, в случае, когда в хэш-таблицу был вставлен новый ключ.
	  */
	static void onNewKey(iterator & it, size_t keys_size, size_t i, StringRefs & keys, Arena & pool)
	{
	}

	/** Вставить ключ из хэш-таблицы в столбцы.
	  */
	static void insertKeyIntoColumns(const_iterator & it, ColumnPlainPtrs & key_columns, size_t keys_size, const Sizes & key_sizes)
	{
		static_cast<ColumnVector<FieldType> *>(key_columns[0])->insertData(reinterpret_cast<const char *>(&it->first), sizeof(it->first));
	}

private:
	UInt64 get64(FieldType x) const
	{
		return x;
	}
};

template <>
inline UInt64 AggregationMethodOneNumber<Float64>::get64(Float64 x) const
{
	union
	{
		Float64 src;
		UInt64 res;
	};

	src = x;
	return res;
}

template <>
inline UInt64 AggregationMethodOneNumber<Float32>::get64(Float32 x) const
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


/// Для случая, когда есть один строковый ключ.
struct AggregationMethodString
{
	typedef AggregatedDataWithStringKey Data;
	typedef Data::key_type Key;
	typedef Data::mapped_type Mapped;
	typedef Data::iterator iterator;
	typedef Data::const_iterator const_iterator;

	static constexpr bool never_overflows = false;

	Data data;

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
		StringRefs & keys) const
	{
		return StringRef(&(*chars)[i == 0 ? 0 : (*offsets)[i - 1]], (i == 0 ? (*offsets)[i] : ((*offsets)[i] - (*offsets)[i - 1])) - 1);
	}

	static AggregateDataPtr & getAggregateData(Mapped & value) 				{ return value; }
	static const AggregateDataPtr & getAggregateData(const Mapped & value) 	{ return value; }

	static void onNewKey(iterator & it, size_t keys_size, size_t i, StringRefs & keys, Arena & pool)
	{
		it->first.data = pool.insert(it->first.data, it->first.size);
	}

	static void insertKeyIntoColumns(const_iterator & it, ColumnPlainPtrs & key_columns, size_t keys_size, const Sizes & key_sizes)
	{
		key_columns[0]->insertData(it->first.data, it->first.size);
	}
};


/// Для случая, когда есть один строковый ключ фиксированной длины.
struct AggregationMethodFixedString
{
	typedef AggregatedDataWithStringKey Data;
	typedef Data::key_type Key;
	typedef Data::mapped_type Mapped;
	typedef Data::iterator iterator;
	typedef Data::const_iterator const_iterator;

	static constexpr bool never_overflows = false;

	Data data;

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
		StringRefs & keys) const
	{
		return StringRef(&(*chars)[i * n], n);
	}

	static AggregateDataPtr & getAggregateData(Mapped & value) 				{ return value; }
	static const AggregateDataPtr & getAggregateData(const Mapped & value) 	{ return value; }

	static void onNewKey(iterator & it, size_t keys_size, size_t i, StringRefs & keys, Arena & pool)
	{
		it->first.data = pool.insert(it->first.data, it->first.size);
	}

	static void insertKeyIntoColumns(const_iterator & it, ColumnPlainPtrs & key_columns, size_t keys_size, const Sizes & key_sizes)
	{
		key_columns[0]->insertData(it->first.data, it->first.size);
	}
};


/// Для случая, когда все ключи фиксированной длины, и они помещаются в 128 бит.
struct AggregationMethodKeys128
{
	typedef AggregatedDataWithKeys128 Data;
	typedef Data::key_type Key;
	typedef Data::mapped_type Mapped;
	typedef Data::iterator iterator;
	typedef Data::const_iterator const_iterator;

	static constexpr bool never_overflows = false;

	Data data;

	void init(ConstColumnPlainPtrs & key_columns)
	{
	}

	Key getKey(
		const ConstColumnPlainPtrs & key_columns,
		size_t keys_size,
		size_t i,
		const Sizes & key_sizes,
		StringRefs & keys) const
	{
		return pack128(i, keys_size, key_columns, key_sizes);
	}

	static AggregateDataPtr & getAggregateData(Mapped & value) 				{ return value; }
	static const AggregateDataPtr & getAggregateData(const Mapped & value) 	{ return value; }

	static void onNewKey(iterator & it, size_t keys_size, size_t i, StringRefs & keys, Arena & pool)
	{
	}

	static void insertKeyIntoColumns(const_iterator & it, ColumnPlainPtrs & key_columns, size_t keys_size, const Sizes & key_sizes)
	{
		size_t offset = 0;
		for (size_t i = 0; i < keys_size; ++i)
		{
			size_t size = key_sizes[i];
			key_columns[i]->insertData(reinterpret_cast<const char *>(&it->first) + offset, size);
			offset += size;
		}
	}
};


/// Для остальных случаев. Агрегирует по 128 битному хэшу от ключа. (При этом, строки, содержащие нули посередине, могут склеиться.)
struct AggregationMethodHashed
{
	typedef AggregatedDataHashed Data;
	typedef Data::key_type Key;
	typedef Data::mapped_type Mapped;
	typedef Data::iterator iterator;
	typedef Data::const_iterator const_iterator;

	static constexpr bool never_overflows = false;

	Data data;

	void init(ConstColumnPlainPtrs & key_columns)
	{
	}

	Key getKey(
		const ConstColumnPlainPtrs & key_columns,
		size_t keys_size,
		size_t i,
		const Sizes & key_sizes,
		StringRefs & keys) const
	{
		return hash128(i, keys_size, key_columns, keys);
	}

	static AggregateDataPtr & getAggregateData(Mapped & value) 				{ return value.second; }
	static const AggregateDataPtr & getAggregateData(const Mapped & value) 	{ return value.second; }

	static void onNewKey(iterator & it, size_t keys_size, size_t i, StringRefs & keys, Arena & pool)
	{
		it->second.first = placeKeysInPool(i, keys_size, keys, pool);
	}

	static void insertKeyIntoColumns(const_iterator & it, ColumnPlainPtrs & key_columns, size_t keys_size, const Sizes & key_sizes)
	{
		for (size_t i = 0; i < keys_size; ++i)
			key_columns[i]->insertDataWithTerminatingZero(it->second.first[i].data, it->second.first[i].size);
	}
};


class Aggregator;

struct AggregatedDataVariants : private boost::noncopyable
{
	/** Работа с состояниями агрегатных функций в пуле устроена следующим (неудобным) образом:
	  * - при агрегации, состояния создаются в пуле с помощью функции IAggregateFunction::create (внутри - placement new произвольной структуры);
	  * - они должны быть затем уничтожены с помощью IAggregateFunction::destroy (внутри - вызов деструктора произвольной структуры);
	  * - если агрегация завершена, то, в функции Aggregator::convertToBlock, указатели на состояния агрегатных функций
	  *   записываются в ColumnAggregateFunction; ColumnAggregateFunction "захватывает владение" ими, то есть - вызывает destroy в своём деструкторе.
	  * - если при агрегации, до вызова Aggregator::convertToBlock вылетело исключение, то состояния агрегатных функций всё-равно должны быть уничтожены,
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

	std::unique_ptr<AggregationMethodOneNumber<UInt8>>	key8;
	std::unique_ptr<AggregationMethodOneNumber<UInt16>>	key16;
	std::unique_ptr<AggregationMethodOneNumber<UInt32>>	key32;
	std::unique_ptr<AggregationMethodOneNumber<UInt64>>	key64;
	std::unique_ptr<AggregationMethodString> 		key_string;
	std::unique_ptr<AggregationMethodFixedString> 	key_fixed_string;
	std::unique_ptr<AggregationMethodKeys128> 		keys128;
	std::unique_ptr<AggregationMethodHashed> 		hashed;

	enum Type
	{
		EMPTY = 0,
		WITHOUT_KEY,
		KEY_8,
		KEY_16,
		KEY_32,
		KEY_64,
		KEY_STRING,
		KEY_FIXED_STRING,
		KEYS_128,
		HASHED,
	};
	Type type = EMPTY;

	AggregatedDataVariants() : aggregates_pools(1, new Arena), aggregates_pool(&*aggregates_pools.back()) {}
	bool empty() const { return type == EMPTY; }

	~AggregatedDataVariants();

	void init(Type type_)
	{
		type = type_;

		switch (type)
		{
			case EMPTY:				break;
			case WITHOUT_KEY:		break;
			case KEY_8:				key8			.reset(new decltype(key8)::element_type); 			break;
			case KEY_16:			key16			.reset(new decltype(key16)::element_type); 			break;
			case KEY_32:			key32			.reset(new decltype(key32)::element_type); 			break;
			case KEY_64:			key64			.reset(new decltype(key64)::element_type); 			break;
			case KEY_STRING:		key_string		.reset(new decltype(key_string)::element_type); 	break;
			case KEY_FIXED_STRING:	key_fixed_string.reset(new decltype(key_fixed_string)::element_type); break;
			case KEYS_128:			keys128			.reset(new decltype(keys128)::element_type); 		break;
			case HASHED:			hashed			.reset(new decltype(hashed)::element_type);	 		break;

			default:
				throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
		}
	}

	size_t size() const
	{
		switch (type)
		{
			case EMPTY:				return 0;
			case WITHOUT_KEY:		return 1;
			case KEY_8:				return key8->data.size() 				+ (without_key != nullptr);
			case KEY_16:			return key16->data.size() 				+ (without_key != nullptr);
			case KEY_32:			return key32->data.size() 				+ (without_key != nullptr);
			case KEY_64:			return key64->data.size() 				+ (without_key != nullptr);
			case KEY_STRING:		return key_string->data.size() 			+ (without_key != nullptr);
			case KEY_FIXED_STRING:	return key_fixed_string->data.size() 	+ (without_key != nullptr);
			case KEYS_128:			return keys128->data.size() 			+ (without_key != nullptr);
			case HASHED:			return hashed->data.size() 				+ (without_key != nullptr);

			default:
				throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
		}
	}

	const char * getMethodName() const
	{
		switch (type)
		{
			case EMPTY:				return "EMPTY";
			case WITHOUT_KEY:		return "WITHOUT_KEY";
			case KEY_8:				return "KEY_8";
			case KEY_16:			return "KEY_16";
			case KEY_32:			return "KEY_32";
			case KEY_64:			return "KEY_64";
			case KEY_STRING:		return "KEY_STRING";
			case KEY_FIXED_STRING:	return "KEY_FIXED_STRING";
			case KEYS_128:			return "KEYS_128";
			case HASHED:			return "HASHED";

			default:
				throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
		}
	}
};

typedef SharedPtr<AggregatedDataVariants> AggregatedDataVariantsPtr;
typedef std::vector<AggregatedDataVariantsPtr> ManyAggregatedDataVariants;


/** Достать вариант агрегации по его типу. */
template <typename Method> Method & getDataVariant(AggregatedDataVariants & variants);

template <> inline AggregationMethodOneNumber<UInt8> & 	getDataVariant<AggregationMethodOneNumber<UInt8>>(AggregatedDataVariants & variants)
	{ return *variants.key8; }
template <> inline AggregationMethodOneNumber<UInt16> & getDataVariant<AggregationMethodOneNumber<UInt16>>(AggregatedDataVariants & variants)
	{ return *variants.key16; }
template <> inline AggregationMethodOneNumber<UInt32> & getDataVariant<AggregationMethodOneNumber<UInt32>>(AggregatedDataVariants & variants)
	{ return *variants.key32; }
template <> inline AggregationMethodOneNumber<UInt64> & getDataVariant<AggregationMethodOneNumber<UInt64>>(AggregatedDataVariants & variants)
	{ return *variants.key64; }
template <> inline AggregationMethodString & 			getDataVariant<AggregationMethodString>(AggregatedDataVariants & variants)
	{ return *variants.key_string; }
template <> inline AggregationMethodFixedString & 		getDataVariant<AggregationMethodFixedString>(AggregatedDataVariants & variants)
	{ return *variants.key_fixed_string; }
template <> inline AggregationMethodKeys128 & 			getDataVariant<AggregationMethodKeys128>(AggregatedDataVariants & variants)
	{ return *variants.keys128; }
template <> inline AggregationMethodHashed & 			getDataVariant<AggregationMethodHashed>(AggregatedDataVariants & variants)
	{ return *variants.hashed; }


/** Агрегирует источник блоков.
  */
class Aggregator
{
public:
	Aggregator(const ColumnNumbers & keys_, const AggregateDescriptions & aggregates_, bool overflow_row_,
		size_t max_rows_to_group_by_ = 0, OverflowMode group_by_overflow_mode_ = OverflowMode::THROW)
		: keys(keys_), aggregates(aggregates_), aggregates_size(aggregates.size()),
		overflow_row(overflow_row_),
		max_rows_to_group_by(max_rows_to_group_by_), group_by_overflow_mode(group_by_overflow_mode_),
		log(&Logger::get("Aggregator"))
	{
		std::sort(keys.begin(), keys.end());
		keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
		keys_size = keys.size();
	}

	Aggregator(const Names & key_names_, const AggregateDescriptions & aggregates_, bool overflow_row_,
		size_t max_rows_to_group_by_ = 0, OverflowMode group_by_overflow_mode_ = OverflowMode::THROW)
		: key_names(key_names_), aggregates(aggregates_), aggregates_size(aggregates.size()),
		overflow_row(overflow_row_),
		max_rows_to_group_by(max_rows_to_group_by_), group_by_overflow_mode(group_by_overflow_mode_),
		log(&Logger::get("Aggregator"))
	{
		std::sort(key_names.begin(), key_names.end());
		key_names.erase(std::unique(key_names.begin(), key_names.end()), key_names.end());
		keys_size = key_names.size();
	}

	/// Агрегировать источник. Получить результат в виде одной из структур данных.
	void execute(BlockInputStreamPtr stream, AggregatedDataVariants & result);

	typedef std::vector<ConstColumnPlainPtrs> AggregateColumns;
	typedef std::vector<ColumnAggregateFunction::Container_t *> AggregateColumnsData;

	/// Обработать один блок. Вернуть false, если обработку следует прервать (при group_by_overflow_mode = 'break').
	bool executeOnBlock(Block & block, AggregatedDataVariants & result,
		ConstColumnPlainPtrs & key_columns, AggregateColumns & aggregate_columns,	/// Передаются, чтобы не создавать их заново на каждый блок
		Sizes & key_sizes, StringRefs & keys,										/// - передайте соответствующие объекты, которые изначально пустые.
		bool & no_more_keys);

	/** Преобразовать структуру данных агрегации в блок.
	  * Если overflow_row = true, то агрегаты для строк, не попавших в max_rows_to_group_by, кладутся в первую строчку возвращаемого блока.
	  *
	  * Если final = false, то в качестве столбцов-агрегатов создаются ColumnAggregateFunction с состоянием вычислений,
	  *  которые могут быть затем объединены с другими состояниями (для распределённой обработки запроса).
	  * Если final = true, то в качестве столбцов-агрегатов создаются столбцы с готовыми значениями.
	  */
	Block convertToBlock(AggregatedDataVariants & data_variants, bool final);

	/** Объединить несколько структур данных агрегации в одну. (В первый непустой элемент массива.) Все варианты агрегации должны быть одинаковыми!
	  * После объединения, все стркутуры агрегации (а не только те, в которую они будут слиты) должны жить, пока не будет вызвана функция convertToBlock.
	  * Это нужно, так как в слитом результате могут остаться указатели на память в пуле, которым владеют другие структуры агрегации.
	  */
	AggregatedDataVariantsPtr merge(ManyAggregatedDataVariants & data_variants, size_t max_threads);

	/** Объединить несколько агрегированных блоков в одну структуру данных.
	  * (Доагрегировать несколько блоков, которые представляют собой результат независимых агрегаций с удалённых серверов.)
	  * Если overflow_row = true, то предполагается, что агрегаты для строк, не попавших в max_rows_to_group_by, расположены в первой строке каждого блока.
	  */
	void merge(BlockInputStreamPtr stream, AggregatedDataVariants & result);

	/// Для IBlockInputStream.
	String getID() const;

protected:
	friend struct AggregatedDataVariants;

	ColumnNumbers keys;
	Names key_names;
	AggregateDescriptions aggregates;
	std::vector<IAggregateFunction *> aggregate_functions;
	size_t keys_size;
	size_t aggregates_size;
	/// Нужно ли класть в AggregatedDataVariants::without_key агрегаты для ключей, не попавших в max_rows_to_group_by.
	bool overflow_row;

	Sizes offsets_of_aggregate_states;	/// Смещение до n-ой агрегатной функции в строке из агрегатных функций.
	size_t total_size_of_aggregate_states = 0;	/// Суммарный размер строки из агрегатных функций.
	bool all_aggregates_has_trivial_destructor = false;

	/// Для инициализации от первого блока при конкуррентном использовании.
	bool initialized = false;
	Poco::FastMutex mutex;

	size_t max_rows_to_group_by;
	OverflowMode group_by_overflow_mode;

	Block sample;

	Logger * log;

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

	template <typename Method>
	void convertToBlockImpl(
		Method & method,
		ColumnPlainPtrs & key_columns,
		AggregateColumnsData & aggregate_columns,
		ColumnPlainPtrs & final_aggregate_columns,
		const Sizes & key_sizes,
		size_t start_row, bool final) const;

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

	template <typename Method>
	void mergeStreamsImpl(
		Method & method,
		Arena * aggregates_pool,
		size_t start_row,
		size_t rows,
		ConstColumnPlainPtrs & key_columns,
		AggregateColumnsData & aggregate_columns,
		const Sizes & key_sizes,
		StringRefs & keys) const;

	template <typename Method>
	void destroyImpl(
		Method & method) const;
};


}
