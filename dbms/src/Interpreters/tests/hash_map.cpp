#include <iostream>
#include <iomanip>
#include <vector>

#include <tr1/unordered_map>

#include <google/dense_hash_map>
#include <google/sparse_hash_map>

#include <statdaemons/Stopwatch.h>

#include <DB/Core/Types.h>
#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/CompressedReadBuffer.h>
#include <DB/Interpreters/HashMap.h>
#include <DB/AggregateFunctions/IAggregateFunction.h>
#include <DB/AggregateFunctions/AggregateFunctionFactory.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>


int main(int argc, char ** argv)
{
	typedef DB::UInt64 Key;
	typedef DB::AggregateFunctions Value;
	
	size_t n = atoi(argv[1]);
	//size_t m = atoi(argv[2]);

	DB::AggregateFunctionFactory factory;
	DB::DataTypes data_types_empty;
	DB::DataTypes data_types_uint64;
	data_types_uint64.push_back(new DB::DataTypeUInt64);
	
	std::vector<Key> data(n);
	Value value;

	value.push_back(factory.get("count", data_types_empty));
	value.push_back(factory.get("avg", data_types_uint64));
	value.push_back(factory.get("uniq", data_types_uint64));

	std::cerr << "sizeof(Key) = " << sizeof(Key) << ", sizeof(Value) = " << sizeof(Value) << std::endl;

	{
		Stopwatch watch;
	/*	for (size_t i = 0; i < n; ++i)
			data[i] = rand() % m;

		for (size_t i = 0; i < n; i += 10)
			data[i] = 0;*/

		DB::ReadBufferFromFile in1("UniqID.bin");
		DB::CompressedReadBuffer in2(in1);

		size_t size = in2.read(reinterpret_cast<char*>(&data[0]), sizeof(data[0]) * n);

		watch.stop();
		std::cerr << std::fixed << std::setprecision(2)
			<< "Vector. Size: " << n
			<< ", elapsed: " << watch.elapsedSeconds()
			<< " (" << n / watch.elapsedSeconds() << " elem/sec.)"
			<< std::endl;
	}

	{
		Stopwatch watch;

		DB::HashMap<Key, Value> map;
		DB::HashMap<Key, Value>::iterator it;
		bool inserted;
		
		for (size_t i = 0; i < n; ++i)
		{
			map.emplace(data[i], it, inserted);
			if (inserted)
				new(&it->second) Value(value);
		}

		watch.stop();
		std::cerr << std::fixed << std::setprecision(2)
			<< "DB::HashMap. Size: " << map.size()
			<< ", elapsed: " << watch.elapsedSeconds()
			<< " (" << n / watch.elapsedSeconds() << " elem/sec.)"
			<< std::endl;
	}

	{
		Stopwatch watch;

		std::tr1::unordered_map<Key, Value> map;
		for (size_t i = 0; i < n; ++i)
			map.insert(std::make_pair(data[i], value));

		watch.stop();
		std::cerr << std::fixed << std::setprecision(2)
			<< "std::tr1::unordered_map. Size: " << map.size()
			<< ", elapsed: " << watch.elapsedSeconds()
			<< " (" << n / watch.elapsedSeconds() << " elem/sec.)"
			<< std::endl;
	}

	{
		Stopwatch watch;

		google::dense_hash_map<Key, Value> map;
		map.set_empty_key(-1ULL);
		for (size_t i = 0; i < n; ++i)
			map.insert(std::make_pair(data[i], value));

		watch.stop();
		std::cerr << std::fixed << std::setprecision(2)
			<< "google::dense_hash_map. Size: " << map.size()
			<< ", elapsed: " << watch.elapsedSeconds()
			<< " (" << n / watch.elapsedSeconds() << " elem/sec.)"
			<< std::endl;
	}

	/*{
		Stopwatch watch;

		google::sparse_hash_map<Key, Value> map;
		for (size_t i = 0; i < n; ++i)
			map.insert(std::make_pair(data[i], value));

		watch.stop();
		std::cerr << std::fixed << std::setprecision(2)
			<< "google::sparse_hash_map. Size: " << map.size()
			<< ", elapsed: " << watch.elapsedSeconds()
			<< " (" << n / watch.elapsedSeconds() << " elem/sec.)"
			<< std::endl;
	}*/
	
	return 0;
}
