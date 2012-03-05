#pragma once

#include <city.h>

#include <map>
#include <tr1/unordered_map>

#include <Poco/Mutex.h>

#include <DB/Core/ColumnNumbers.h>
#include <DB/Core/Names.h>
#include <DB/DataStreams/IBlockInputStream.h>
#include <DB/AggregateFunctions/IAggregateFunction.h>
#include <DB/Interpreters/HashMap.h>


namespace DB
{


struct AggregateDescription
{
	AggregateFunctionPtr function;
	ColumnNumbers arguments;
	Names argument_names;	/// Используются, если arguments не заданы.
	String column_name;		/// Какое имя использовать для столбца со значениями агрегатной функции
};

typedef std::vector<AggregateDescription> AggregateDescriptions;


/// Для агрегации по md5.
struct UInt128
{
	UInt64 first;
	UInt64 second;

	bool operator== (const UInt128 rhs) const { return first == rhs.first && second == rhs.second; }
	bool operator!= (const UInt128 rhs) const { return first != rhs.first || second != rhs.second; }
};

struct UInt128Hash
{
	default_hash<UInt64> hash64;
	size_t operator()(UInt128 x) const { return hash64(x.first ^ 0xB15652B8790A0D36ULL) ^ hash64(x.second); }
};

struct UInt128ZeroTraits
{
	static inline bool check(UInt128 x) { return x.first == 0 && x.second == 0; }
	static inline void set(UInt128 & x) { x.first = 0; x.second = 0; }
};


/// Немного быстрее стандартного
struct StringHash
{
	size_t operator()(const String & x) const { return CityHash64(x.data(), x.size()); }
};


/** Разные структуры данных, которые могут использоваться для агрегации
  * Для эффективности используются "голые" указатели на IAggregateFunction,
  *  владение будет захвачено после агрегации (в AggregatingBlockInputStream).
  */
typedef std::map<Row, AggregateFunctionsPlainPtrs> AggregatedData;
typedef AggregateFunctionsPlainPtrs AggregatedDataWithoutKey;
typedef HashMap<UInt64, AggregateFunctionsPlainPtrs> AggregatedDataWithUInt64Key;
typedef std::tr1::unordered_map<String, AggregateFunctionsPlainPtrs, StringHash> AggregatedDataWithStringKey;
typedef HashMap<UInt128, std::pair<Row, AggregateFunctionsPlainPtrs>, UInt128Hash, UInt128ZeroTraits> AggregatedDataHashed;


struct AggregatedDataVariants
{
	/// Наиболее общий вариант. Самый медленный. На данный момент, не используется.
	AggregatedData generic;

	/// Специализация для случая, когда ключи отсутствуют.
	AggregatedDataWithoutKey without_key;

	/// Специализация для случая, когда есть один числовой ключ (не с плавающей запятой).
	AggregatedDataWithUInt64Key key64;

	/// Специализация для случая, когда есть один строковый ключ.
	AggregatedDataWithStringKey key_string;

	/** Агрегирует по 128 битному хэшу от ключа.
	  * Если все ключи фиксированной длины, влезающие целиком в 128 бит, то укладывает их без изменений в 128 бит.
	  * Иначе - вычисляет md5 от набора из всех ключей.
	  * (При этом, строки, содержащие нули посередине, могут склеиться.)
	  */ 
	AggregatedDataHashed hashed;

	enum Type
	{
		GENERIC,
		WITHOUT_KEY,
		KEY_64,
		KEY_STRING,
		HASHED,
	};
	Type type;
};

typedef SharedPtr<AggregatedDataVariants> AggregatedDataVariantsPtr;
typedef std::vector<AggregatedDataVariantsPtr> ManyAggregatedDataVariants;


/** Агрегирует источник блоков.
  */
class Aggregator
{
public:
	Aggregator(const ColumnNumbers & keys_, AggregateDescriptions & aggregates_) : keys(keys_), aggregates(aggregates_), initialized(false) {};
	Aggregator(const Names & key_names_, AggregateDescriptions & aggregates_) : key_names(key_names_), aggregates(aggregates_), initialized(false) {};

	/// Агрегировать источник. Получить результат в виде одной из структур данных.
	void execute(BlockInputStreamPtr stream, AggregatedDataVariants & result);

	/// Получить пример блока, описывающего результат. Следует вызывать только после execute.
	Block getSampleBlock() { return sample; }

	/// Преобразовать структуру данных агрегации в блок.
	Block convertToBlock(AggregatedDataVariants & data_variants);

	/// Объединить несколько структуру данных агрегации в одну. (В первый элемент массива.) Все варианты агрегации должны быть одинаковыми!
	AggregatedDataVariantsPtr merge(ManyAggregatedDataVariants & data_variants);

private:
	ColumnNumbers keys;
	Names key_names;
	AggregateDescriptions aggregates;

	/// Для инициализации от первого блока при конкуррентном использовании.
	bool initialized;
	Poco::FastMutex mutex;

	Block sample;

	/** Если заданы только имена столбцов (key_names, а также aggregates[i].column_name), то вычислить номера столбцов.
	  * Сформировать блок - пример результата.
	  */
	void initialize(Block & block);
};


}
