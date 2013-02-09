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
typedef std::map<Row, AggregateDataPtr> AggregatedData;
typedef AggregateDataPtr AggregatedDataWithoutKey;
typedef HashMap<UInt64, AggregateDataPtr> AggregatedDataWithUInt64Key;
typedef HashMap<StringRef, AggregateDataPtr, StringRefHash, StringRefZeroTraits> AggregatedDataWithStringKey;
typedef HashMap<UInt128, std::pair<Field*, AggregateDataPtr>, UInt128Hash, UInt128ZeroTraits> AggregatedDataHashed;


struct AggregatedDataVariants
{
	// TODO exception safety при работе с агрегатными функциями и ключами в пуле
	/// Пулы для состояний агрегатных функций. Владение потом будет передано в ColumnAggregateFunction.
	Arenas aggregates_pools;
	Arena * aggregates_pool;	/// Последний пул, который используется для аллокации.
	
	/// Наиболее общий вариант. Самый медленный. На данный момент, не используется.
	AggregatedData generic;

	/// Специализация для случая, когда ключи отсутствуют.
	AggregatedDataWithoutKey without_key;

	/// Специализация для случая, когда есть один числовой ключ (не с плавающей запятой).
	AggregatedDataWithUInt64Key key64;

	/// Специализация для случая, когда есть один строковый ключ.
	AggregatedDataWithStringKey key_string;
	Arena string_pool;

	/** Агрегирует по 128 битному хэшу от ключа.
	  * Если все ключи фиксированной длины, влезающие целиком в 128 бит, то укладывает их без изменений в 128 бит.
	  * Иначе - вычисляет SipHash от набора из всех ключей.
	  * (При этом, строки, содержащие нули посередине, могут склеиться.)
	  */ 
	AggregatedDataHashed hashed;
	Arena keys_pool;	// TODO: складывать ключи в пул не в виде Field, а в виде плоского набора байт.
	size_t keys_size;

	enum Type
	{
		EMPTY 		= 0,
		GENERIC 	= 1,
		WITHOUT_KEY = 2,
		KEY_64		= 3,
		KEY_STRING	= 4,
		HASHED		= 5,
	};
	Type type;

	AggregatedDataVariants() : aggregates_pools(1, new Arena), aggregates_pool(&*aggregates_pools.back()), without_key(NULL), type(EMPTY) {}
	bool empty() const { return type == EMPTY; }

	~AggregatedDataVariants()
	{
		if (type == HASHED)
		{
			/// Уничтожаем ключи из keys_pool.
			for (AggregatedDataHashed::iterator it = hashed.begin(); it != hashed.end(); ++it)
				if (it->second.first != NULL)	/// Они могли быть перенесены в другой AggregatedDataVariants, с занулением указателя.
					for (size_t i = 0; i < keys_size; ++i)
						it->second.first[i].~Field();
		}
	}

	size_t size() const
	{
		switch (type)
		{
			case EMPTY:			return 0;
			case GENERIC:		return generic.size();
			case WITHOUT_KEY:	return 1;
			case KEY_64:		return key64.size();
			case KEY_STRING:	return key_string.size();
			case HASHED:		return hashed.size();

			default:
				throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
		}
	}

	const char * getMethodName() const
	{
		switch (type)
		{
			case EMPTY:			return "EMPTY";
			case GENERIC:		return "GENERIC";
			case WITHOUT_KEY:	return "WITHOUT_KEY";
			case KEY_64:		return "KEY_64";
			case KEY_STRING:	return "KEY_STRING";
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
	Aggregator(const ColumnNumbers & keys_, AggregateDescriptions & aggregates_,
		size_t max_rows_to_group_by_ = 0, Limits::OverflowMode group_by_overflow_mode_ = Limits::THROW)
		: keys(keys_), aggregates(aggregates_), keys_size(keys.size()), aggregates_size(aggregates.size()),
		total_size_of_aggregate_states(0), initialized(false),
		max_rows_to_group_by(max_rows_to_group_by_), group_by_overflow_mode(group_by_overflow_mode_),
		log(&Logger::get("Aggregator"))
	{
	}

	Aggregator(const Names & key_names_, AggregateDescriptions & aggregates_,
		size_t max_rows_to_group_by_ = 0, Limits::OverflowMode group_by_overflow_mode_ = Limits::THROW)
		: key_names(key_names_), aggregates(aggregates_), keys_size(key_names.size()), aggregates_size(aggregates.size()),
		total_size_of_aggregate_states(0), initialized(false),
		max_rows_to_group_by(max_rows_to_group_by_), group_by_overflow_mode(group_by_overflow_mode_),
		log(&Logger::get("Aggregator"))
	{
	}

	/// Агрегировать источник. Получить результат в виде одной из структур данных.
	void execute(BlockInputStreamPtr stream, AggregatedDataVariants & result);

	/// Получить пример блока, описывающего результат. Следует вызывать только после execute.
	Block getSampleBlock() { return sample; }

	/// Преобразовать структуру данных агрегации в блок.
	Block convertToBlock(AggregatedDataVariants & data_variants);

	/** Объединить несколько структур данных агрегации в одну. (В первый элемент массива.) Все варианты агрегации должны быть одинаковыми!
	  * После объединения, все стркутуры агрегации (а не только те, в которую они будут слиты) должны жить, пока не будет вызвана функция convertToBlock.
	  * Это нужно, так как в слитом результате могут остаться указатели на память в пуле, которым владеют другие структуры агрегации.
	  */
	AggregatedDataVariantsPtr merge(ManyAggregatedDataVariants & data_variants);

	/** Объединить несколько агрегированных блоков в одну структуру данных.
	  * (Доагрегировать несколько блоков, которые представляют собой результат независимых агрегаций.)
	  */
	void merge(BlockInputStreamPtr stream, AggregatedDataVariants & result);

private:
	ColumnNumbers keys;
	Names key_names;
	AggregateDescriptions aggregates;
	std::vector<IAggregateFunction *> aggregate_functions;
	size_t keys_size;
	size_t aggregates_size;

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
	AggregatedDataVariants::Type chooseAggregationMethod(const ConstColumnPlainPtrs & key_columns, bool & keys_fit_128_bits, Sizes & key_sizes);
};


}
