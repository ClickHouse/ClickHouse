#pragma once

#include <map>
#include <unordered_map>

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
typedef HashMap<UInt64, AggregateDataPtr> AggregatedDataWithUInt64Key;
typedef HashMap<StringRef, AggregateDataPtr, StringRefHash, StringRefZeroTraits> AggregatedDataWithStringKey;
typedef HashMap<UInt128, AggregateDataPtr, UInt128Hash, UInt128ZeroTraits> AggregatedDataWithKeys128;
typedef HashMap<UInt128, std::pair<StringRef*, AggregateDataPtr>, UInt128TrivialHash, UInt128ZeroTraits> AggregatedDataHashed;

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
	
	/// Пулы для состояний агрегатных функций. Владение потом будет передано в ColumnAggregateFunction.
	Arenas aggregates_pools;
	Arena * aggregates_pool;	/// Пул, который сейчас используется для аллокации.

	/** Специализация для случая, когда ключи отсутствуют, и для ключей, не попавших в max_rows_to_group_by.
	  */
	AggregatedDataWithoutKey without_key = nullptr;

	/// Специализация для случая, когда есть один числовой ключ.
	/// auto_ptr - для ленивой инициализации (так как иначе HashMap в конструкторе выделяет и зануляет слишком много памяти).
	std::auto_ptr<AggregatedDataWithUInt64Key> key64;

	/// Специализация для случая, когда есть один строковый ключ.
	std::auto_ptr<AggregatedDataWithStringKey> key_string;
	Arena string_pool;

	size_t keys_size;	/// Количество ключей
	Sizes key_sizes;	/// Размеры ключей, если ключи фиксированной длины

	/// Специализация для случая, когда ключи фискированной длины помещаются в 128 бит.
	std::auto_ptr<AggregatedDataWithKeys128> keys128;

	/** Агрегирует по 128 битному хэшу от ключа.
	  * (При этом, строки, содержащие нули посередине, могут склеиться.)
	  */ 
	std::auto_ptr<AggregatedDataHashed> hashed;
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
	Type type = EMPTY;

	AggregatedDataVariants() : aggregates_pools(1, new Arena), aggregates_pool(&*aggregates_pools.back()) {}
	bool empty() const { return type == EMPTY; }

	~AggregatedDataVariants();

	void init(Type type_)
	{
		type = type_;

		switch (type)
		{
			case EMPTY:			break;
			case WITHOUT_KEY:	break;
			case KEY_64:		key64		.reset(new AggregatedDataWithUInt64Key); 	break;
			case KEY_STRING:	key_string	.reset(new AggregatedDataWithStringKey); 	break;
			case KEYS_128:		keys128		.reset(new AggregatedDataWithKeys128); 		break;
			case HASHED:		hashed		.reset(new AggregatedDataHashed);	 		break;

			default:
				throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
		}
	}

	size_t size() const
	{
		switch (type)
		{
			case EMPTY:			return 0;
			case WITHOUT_KEY:	return 1;
			case KEY_64:		return key64->size() 		+ (without_key != nullptr);
			case KEY_STRING:	return key_string->size() 	+ (without_key != nullptr);
			case KEYS_128:		return keys128->size() 		+ (without_key != nullptr);
			case HASHED:		return hashed->size() 		+ (without_key != nullptr);

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
	Aggregator(const ColumnNumbers & keys_, const AggregateDescriptions & aggregates_, bool overflow_row_,
		size_t max_rows_to_group_by_ = 0, OverflowMode group_by_overflow_mode_ = OverflowMode::THROW)
		: keys(keys_), aggregates(aggregates_), aggregates_size(aggregates.size()),
		overflow_row(overflow_row_), total_size_of_aggregate_states(0), all_aggregates_has_trivial_destructor(false), initialized(false),
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
		overflow_row(overflow_row_), total_size_of_aggregate_states(0), all_aggregates_has_trivial_destructor(false), initialized(false),
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
	AggregatedDataVariantsPtr merge(ManyAggregatedDataVariants & data_variants);

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
	size_t total_size_of_aggregate_states;	/// Суммарный размер строки из агрегатных функций.
	bool all_aggregates_has_trivial_destructor;

	/// Для инициализации от первого блока при конкуррентном использовании.
	bool initialized;
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

	/** Вызвать методы destroy для состояний агрегатных функций.
	  * Используется в обработчике исключений при агрегации, так как RAII в данном случае не применим.
	  */
	void destroyAggregateStates(AggregatedDataVariants & result);
};


}
