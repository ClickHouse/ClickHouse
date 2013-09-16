#pragma once

#include <map>
#include <tr1/unordered_map>

#include <Poco/Mutex.h>

#include <Yandex/logger_useful.h>

#include <DB/Core/ColumnNumbers.h>
#include <DB/Core/Names.h>
#include <DB/Core/StringRef.h>
#include <DB/Common/Arena.h>

#include <DB/DataStreams/IBlockInputStream.h>

#include <DB/AggregateFunctions/IAggregateFunction.h>

#include <DB/Interpreters/AggregationCommon.h>
#include <DB/Interpreters/Limits.h>


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


/** Разные структуры данных, которые могут использоваться для агрегации
  * Для эффективности сами данные для агрегации кладутся в пул.
  * Владение данными (состояний агрегатных функций) и пулом
  *  захватывается позднее - в функции ConvertToBlock, объектом ColumnAggregateFunction.
  */
typedef AggregateDataPtr AggregatedDataWithoutKey;
typedef HashMap<UInt64, AggregateDataPtr> AggregatedDataWithUInt64Key;
typedef HashMap<StringRef, AggregateDataPtr, StringRefHash, StringRefZeroTraits> AggregatedDataWithStringKey;
typedef HashMap<UInt128, AggregateDataPtr, UInt128Hash, UInt128ZeroTraits> AggregatedDataWithKeys128;
typedef HashMap<UInt128, std::pair<StringRef*, AggregateDataPtr>, UInt128TrivialHash, UInt128ZeroTraits> AggregatedDataHashed;

class Aggregator;


struct AggregatedDataVariants
{
	/** Работа с состояниями агрегатных функций в пуле устроена следующим (неудобным) образом:
	  * - при агрегации, состояния создаются в пуле с помощью функции IAggregateFunction::create (внутри - placement new произвольной структуры);
	  * - они должны быть затем уничтожены с помощью IAggregateFunction::destroy (внутри - вызов деструктора произвольной структуры);
	  * - если агрегация завершена, то, в функции Aggregator::convertToBlock, указатели на состояния агрегатных функций
	  *   записываются в ColumnAggregateFunction; ColumnAggregateFunction "захватывает владение" ими, то есть - вызывает destroy в своём деструкторе.
	  * - если при агрегации, до вызова Aggregator::convertToBlock вылетело исключение, то состояния агрегатных функций всё-равно должны быть уничтожены,
	  *   иначе для сложных состояний (наприемер, AggregateFunctionUniq), будут утечки памяти;
	  * - чтобы, в этом случае, уничтожить состояния, в деструкторе вызывается метод Aggregator::destroyAggregateStates,
	  *   но только если переменная aggregator (см. ниже) не NULL;
	  * - то есть, пока вы не передали владение состояниями агрегатных функций в ColumnAggregateFunction, установите переменную aggregator,
	  *   чтобы при возникновении исключения, состояния были корректно уничтожены.
	  *
	  * PS. Это можно исправить, сделав пул, который знает о том, какие состояния агрегатных функций и в каком порядке в него уложены, и умеет сам их уничтожать.
	  * Но это вряд ли можно просто сделать, так как в этот же пул планируется класть строки переменной длины.
	  * В этом случае, пул не сможет знать, по каким смещениям хранятся объекты.
	  */
	Aggregator * aggregator;
	
	/// Пулы для состояний агрегатных функций. Владение потом будет передано в ColumnAggregateFunction.
	Arenas aggregates_pools;
	Arena * aggregates_pool;	/// Пул, который сейчас используется для аллокации.

	/** Специализация для случая, когда ключи отсутствуют.
	  * А также в этой стркутуре хранятся "тотальные" агрегаты, когда указано with_totals (GROUP BY ... WITH TOTALS в запросе).
	  */
	AggregatedDataWithoutKey without_key;

	/// Специализация для случая, когда есть один числовой ключ.
	AggregatedDataWithUInt64Key key64;

	/// Специализация для случая, когда есть один строковый ключ.
	AggregatedDataWithStringKey key_string;
	Arena string_pool;

	size_t keys_size;	/// Количество ключей
	Sizes key_sizes;	/// Размеры ключей, если ключи фиксированной длины

	/// Специализация для случая, когда ключи фискированной длины помещаются в 128 бит.
	AggregatedDataWithKeys128 keys128;

	/** Агрегирует по 128 битному хэшу от ключа.
	  * (При этом, строки, содержащие нули посередине, могут склеиться.)
	  */ 
	AggregatedDataHashed hashed;
	Arena keys_pool;
	
	enum Type
	{
		EMPTY 		= 0,
		WITHOUT_KEY = 1,
		KEY_64		= 2,
		KEY_STRING	= 3,
		KEYS_128	= 4,
		HASHED		= 5,
	};
	Type type;

	AggregatedDataVariants() : aggregator(NULL), aggregates_pools(1, new Arena), aggregates_pool(&*aggregates_pools.back()), without_key(NULL), type(EMPTY) {}
	bool empty() const { return type == EMPTY; }

	~AggregatedDataVariants();

	size_t size() const
	{
		switch (type)
		{
			case EMPTY:			return 0;
			case WITHOUT_KEY:	return 1;
			case KEY_64:		return key64.size() 		+ (without_key != NULL);
			case KEY_STRING:	return key_string.size() 	+ (without_key != NULL);
			case KEYS_128:		return keys128.size() 		+ (without_key != NULL);
			case HASHED:		return hashed.size() 		+ (without_key != NULL);

			default:
				throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
		}
	}

	const char * getMethodName() const
	{
		switch (type)
		{
			case EMPTY:			return "EMPTY";
			case WITHOUT_KEY:	return "WITHOUT_KEY";
			case KEY_64:		return "KEY_64";
			case KEY_STRING:	return "KEY_STRING";
			case KEYS_128:		return "KEYS_128";
			case HASHED:		return "HASHED";

			default:
				throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
		}
	}
};

typedef SharedPtr<AggregatedDataVariants> AggregatedDataVariantsPtr;
typedef std::vector<AggregatedDataVariantsPtr> ManyAggregatedDataVariants;


/** Агрегирует источник блоков.
  */
class Aggregator
{
public:
	Aggregator(const ColumnNumbers & keys_, const AggregateDescriptions & aggregates_, bool with_totals_,
		size_t max_rows_to_group_by_ = 0, Limits::OverflowMode group_by_overflow_mode_ = Limits::THROW)
		: keys(keys_), aggregates(aggregates_), aggregates_size(aggregates.size()),
		with_totals(with_totals_), total_size_of_aggregate_states(0), initialized(false),
		max_rows_to_group_by(max_rows_to_group_by_), group_by_overflow_mode(group_by_overflow_mode_),
		log(&Logger::get("Aggregator"))
	{
		std::sort(keys.begin(), keys.end());
		keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
		keys_size = keys.size();
	}

	Aggregator(const Names & key_names_, const AggregateDescriptions & aggregates_, bool with_totals_,
		size_t max_rows_to_group_by_ = 0, Limits::OverflowMode group_by_overflow_mode_ = Limits::THROW)
		: key_names(key_names_), aggregates(aggregates_), aggregates_size(aggregates.size()),
		with_totals(with_totals_), total_size_of_aggregate_states(0), initialized(false),
		max_rows_to_group_by(max_rows_to_group_by_), group_by_overflow_mode(group_by_overflow_mode_),
		log(&Logger::get("Aggregator"))
	{
		std::sort(key_names.begin(), key_names.end());
		key_names.erase(std::unique(key_names.begin(), key_names.end()), key_names.end());
		keys_size = key_names.size();
	}

	/// Агрегировать источник. Получить результат в виде одной из структур данных.
	void execute(BlockInputStreamPtr stream, AggregatedDataVariants & result);

	/** Преобразовать структуру данных агрегации в блок.
	  * Если with_totals = true и serarate_totals = true, то тотальные значения кладутся в totals.
	  * Если with_totals = true и serarate_totals = false, то тотальные значения кладутся в первую строчку возвращаемого блока.
	  */
	Block convertToBlock(AggregatedDataVariants & data_variants, bool separate_totals, Block & totals);

	/** Объединить несколько структур данных агрегации в одну. (В первый элемент массива.) Все варианты агрегации должны быть одинаковыми!
	  * После объединения, все стркутуры агрегации (а не только те, в которую они будут слиты) должны жить, пока не будет вызвана функция convertToBlock.
	  * Это нужно, так как в слитом результате могут остаться указатели на память в пуле, которым владеют другие структуры агрегации.
	  */
	AggregatedDataVariantsPtr merge(ManyAggregatedDataVariants & data_variants);

	/** Объединить несколько агрегированных блоков в одну структуру данных.
	  * (Доагрегировать несколько блоков, которые представляют собой результат независимых агрегаций с удалённых серверов.)
	  * Если with_totals = true, то предполагается, что тотальные значения расположены в первой строке каждого блока.
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
	bool with_totals;

	Sizes offsets_of_aggregate_states;	/// Смещение до n-ой агрегатной функции в строке из агрегатных функций.
	size_t total_size_of_aggregate_states;	/// Суммарный размер строки из агрегатных функций.

	/// Для инициализации от первого блока при конкуррентном использовании.
	bool initialized;
	Poco::FastMutex mutex;

	size_t max_rows_to_group_by;
	Limits::OverflowMode group_by_overflow_mode;

	Block sample;

	Logger * log;

	/** Если заданы только имена столбцов (key_names, а также aggregates[i].column_name), то вычислить номера столбцов.
	  * Сформировать блок - пример результата.
	  */
	void initialize(Block & block);

	/** Выбрать способ агрегации на основе количества и типов ключей. */
	AggregatedDataVariants::Type chooseAggregationMethod(const ConstColumnPlainPtrs & key_columns, Sizes & key_sizes);

	/** Вызвать методы destroy для состояний агрегатных функций.
	  * Используется в обработчике исключений при агрегации, так как RAII в данном случае не применим.
	  */
	void destroyAggregateStates(AggregatedDataVariants & result);
};


}
