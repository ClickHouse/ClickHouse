#include <iostream>
#include <iomanip>
#include <vector>

#include <tr1/unordered_map>

#include <google/dense_hash_map>
#include <google/sparse_hash_map>

#include <statdaemons/Stopwatch.h>
/*
#define DBMS_HASH_MAP_COUNT_COLLISIONS
*/
#include <DB/Core/Types.h>
#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/CompressedReadBuffer.h>
#include <DB/Interpreters/HashMap.h>
#include <DB/AggregateFunctions/IAggregateFunction.h>
#include <DB/AggregateFunctions/AggregateFunctionFactory.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>


/** Тест проверяет скорость работы хэш-таблиц, имитируя их использование для агрегации.
  * Первым аргументом указывается количество элементов, которое будет вставлено.
  * Вторым аргументом может быть указано число от 1 до 4 - номер тестируемой структуры данных.
  * Это важно, так как если запускать все тесты один за другим, то результаты будут некорректными.
  * (Из-за особенностей работы аллокатора, первый тест получает преимущество.)
  *
  * В зависимости от USE_AUTO_ARRAY, выбирается одна из структур в качестве значения.
  * USE_AUTO_ARRAY = 0 - используется std::vector (сложно-копируемая структура, sizeof = 24 байта).
  * USE_AUTO_ARRAY = 1 - используется DB::AutoArray (структура специально разработанная для таких случаев, sizeof = 8 байт).
  *
  * То есть, тест также позволяет сравнить DB::AutoArray и std::vector.
  *
  * Если USE_AUTO_ARRAY = 0, то DB::HashMap уверенно обгоняет всех.
  * Если USE_AUTO_ARRAY = 1, то DB::HashMap чуть менее серьёзно (20%) обгоняет google::dense_hash_map.
  *
  * Если IMITATE_UPDATES = 1, то будет производиться обновление агрегатных функций, что сглаживает разницу между разными тестами.
  *
  * При использовании DB::HashMap, AutoArray имеет довольно серьёзное (40%) преимущество перед std::vector.
  * А при использовании других хэш-таблиц, AutoArray ещё более серьёзно обгоняет std::vector
  *  (до трёх c половиной раз в случае std::unordered_map и google::sparse_hash_map).
  *
  * DB::HashMap, в отличие от google::dense_hash_map, гораздо больше зависит от качества хэш-функции.
  *
  * PS. Измеряйте всё сами, а то я почти запутался.
  */

#define USE_AUTO_ARRAY	0
#define IMITATE_UPDATES	0


int main(int argc, char ** argv)
{
	typedef DB::UInt64 Key;

#if USE_AUTO_ARRAY
	typedef DB::AggregateFunctionsPlainPtrs Value;
#else
	typedef std::vector<DB::AggregateFunctionPlainPtr> Value;
#endif

	size_t n = argc < 2 ? 10000000 : atoi(argv[1]);
	//size_t m = atoi(argv[2]);

	DB::AggregateFunctionFactory factory;
	DB::DataTypes data_types_empty;
	DB::DataTypes data_types_uint64;
	data_types_uint64.push_back(new DB::DataTypeUInt64);
	
	std::vector<Key> data(n);
	Value value;

	DB::AggregateFunctionPtr func_count = factory.get("count", data_types_empty);
	DB::AggregateFunctionPtr func_avg = factory.get("avg", data_types_uint64);
	DB::AggregateFunctionPtr func_uniq = factory.get("uniq", data_types_uint64);

	#define INIT				\
	{							\
		value.resize(3);		\
								\
		value[0] = func_count;	\
		value[1] = func_avg;	\
		value[2] = func_uniq;	\
	}

	INIT;

#ifndef USE_AUTO_ARRAY
	#undef INIT
	#define INIT
#endif

	DB::Row row(1);
	row[0] = DB::UInt64(0);

	std::cerr << "sizeof(Key) = " << sizeof(Key) << ", sizeof(Value) = " << sizeof(Value) << std::endl;

	{
		Stopwatch watch;
	/*	for (size_t i = 0; i < n; ++i)
			data[i] = rand() % m;

		for (size_t i = 0; i < n; i += 10)
			data[i] = 0;*/

		DB::ReadBufferFromFile in1("UniqID.bin");
		DB::CompressedReadBuffer in2(in1);

		in2.readStrict(reinterpret_cast<char*>(&data[0]), sizeof(data[0]) * n);

		watch.stop();
		std::cerr << std::fixed << std::setprecision(2)
			<< "Vector. Size: " << n
			<< ", elapsed: " << watch.elapsedSeconds()
			<< " (" << n / watch.elapsedSeconds() << " elem/sec.)"
			<< std::endl;
	}

	if (argc < 3 || atoi(argv[2]) == 1)
	{
		Stopwatch watch;

		DB::HashMap<Key, Value> map;
		DB::HashMap<Key, Value>::iterator it;
		bool inserted;

		for (size_t i = 0; i < n; ++i)
		{
			map.emplace(data[i], it, inserted);
			if (inserted)
			{
				new(&it->second) Value(value);
				INIT;
			}
#if IMITATE_UPDATES
			it->second[0]->add(row);
			it->second[1]->add(row);
			it->second[2]->add(row);
#endif
		}

		watch.stop();
		std::cerr << std::fixed << std::setprecision(2)
			<< "DB::HashMap. Size: " << map.size()
			<< ", elapsed: " << watch.elapsedSeconds()
			<< " (" << n / watch.elapsedSeconds() << " elem/sec.)"
#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
			<< ", collisions: " << map.getCollisions()
#endif
			<< std::endl;
	}

	if (argc < 3 || atoi(argv[2]) == 2)
	{
		Stopwatch watch;

		std::tr1::unordered_map<Key, Value, DB::default_hash<Key> > map;
		std::tr1::unordered_map<Key, Value, DB::default_hash<Key> >::iterator it;
		for (size_t i = 0; i < n; ++i)
		{
			it = map.insert(std::make_pair(data[i], value)).first;
			INIT;
#if IMITATE_UPDATES
			it->second[0]->add(row);
			it->second[1]->add(row);
			it->second[2]->add(row);
#endif
		}

		watch.stop();
		std::cerr << std::fixed << std::setprecision(2)
			<< "std::tr1::unordered_map. Size: " << map.size()
			<< ", elapsed: " << watch.elapsedSeconds()
			<< " (" << n / watch.elapsedSeconds() << " elem/sec.)"
			<< std::endl;
	}

	if (argc < 3 || atoi(argv[2]) == 3)
	{
		Stopwatch watch;

		google::dense_hash_map<Key, Value, DB::default_hash<Key> > map;
		google::dense_hash_map<Key, Value, DB::default_hash<Key> >::iterator it;
		map.set_empty_key(-1ULL);
		for (size_t i = 0; i < n; ++i)
		{
			it = map.insert(std::make_pair(data[i], value)).first;
			INIT;
#if IMITATE_UPDATES
			it->second[0]->add(row);
			it->second[1]->add(row);
			it->second[2]->add(row);
#endif
		}

		watch.stop();
		std::cerr << std::fixed << std::setprecision(2)
			<< "google::dense_hash_map. Size: " << map.size()
			<< ", elapsed: " << watch.elapsedSeconds()
			<< " (" << n / watch.elapsedSeconds() << " elem/sec.)"
			<< std::endl;
	}

	if (argc < 3 || atoi(argv[2]) == 4)
	{
		Stopwatch watch;

		google::sparse_hash_map<Key, Value, DB::default_hash<Key> > map;
		google::sparse_hash_map<Key, Value, DB::default_hash<Key> >::iterator it;
		for (size_t i = 0; i < n; ++i)
		{
			map.insert(std::make_pair(data[i], value));
			INIT;
#if IMITATE_UPDATES
			it->second[0]->add(row);
			it->second[1]->add(row);
			it->second[2]->add(row);
#endif
		}

		watch.stop();
		std::cerr << std::fixed << std::setprecision(2)
			<< "google::sparse_hash_map. Size: " << map.size()
			<< ", elapsed: " << watch.elapsedSeconds()
			<< " (" << n / watch.elapsedSeconds() << " elem/sec.)"
			<< std::endl;
	}
	
	return 0;
}
