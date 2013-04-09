#include <iostream>
#include <iomanip>
#include <vector>

#include <tr1/unordered_map>

#include <sparsehash/dense_hash_map>
#include <sparsehash/sparse_hash_map>

#include <statdaemons/Stopwatch.h>

#define DBMS_HASH_MAP_COUNT_COLLISIONS

#include <DB/Core/Types.h>
#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/CompressedReadBuffer.h>
#include <DB/Interpreters/HashMap.h>


int main(int argc, char ** argv)
{
	typedef DB::UInt64 Key;
	typedef DB::UInt64 Value;
	
	size_t n = atoi(argv[1]);
	//size_t m = atoi(argv[2]);

	std::vector<Key> data(n);

	std::cerr << "sizeof(Key) = " << sizeof(Key) << ", sizeof(Value) = " << sizeof(Value) << std::endl;

	{
		Stopwatch watch;
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

	{
		Stopwatch watch;

		DB::HashMap<Key, Value> map;
		DB::HashMap<Key, Value>::iterator it;
		bool inserted;

		for (size_t i = 0; i < n; ++i)
		{
			map.emplace(data[i], it, inserted);
			if (inserted)
				it->second = 0;
			++it->second;
		}

		watch.stop();
		std::cerr << std::fixed << std::setprecision(2)
			<< "DB::HashMap. Size: " << map.size()
			<< ", elapsed: " << watch.elapsedSeconds()
			<< " (" << n / watch.elapsedSeconds() << " elem/sec.)"
			<< ", collisions: " << map.getCollisions()
			<< std::endl;
	}

	{
		Stopwatch watch;

		std::tr1::unordered_map<Key, Value, DB::default_hash<Key> > map;
		for (size_t i = 0; i < n; ++i)
			++map[data[i]];
		
		watch.stop();
		std::cerr << std::fixed << std::setprecision(2)
			<< "std::tr1::unordered_map. Size: " << map.size()
			<< ", elapsed: " << watch.elapsedSeconds()
			<< " (" << n / watch.elapsedSeconds() << " elem/sec.)"
			<< std::endl;
	}

	{
		Stopwatch watch;

		google::dense_hash_map<Key, Value, DB::default_hash<Key> > map;
		map.set_empty_key(-1ULL);
		for (size_t i = 0; i < n; ++i)
  			++map[data[i]];
		
		watch.stop();
		std::cerr << std::fixed << std::setprecision(2)
			<< "google::dense_hash_map. Size: " << map.size()
			<< ", elapsed: " << watch.elapsedSeconds()
			<< " (" << n / watch.elapsedSeconds() << " elem/sec.)"
			<< std::endl;
	}

	{
		Stopwatch watch;

		google::sparse_hash_map<Key, Value, DB::default_hash<Key> > map;
		for (size_t i = 0; i < n; ++i)
			++map[data[i]];

		watch.stop();
		std::cerr << std::fixed << std::setprecision(2)
			<< "google::sparse_hash_map. Size: " << map.size()
			<< ", elapsed: " << watch.elapsedSeconds()
			<< " (" << n / watch.elapsedSeconds() << " elem/sec.)"
			<< std::endl;
	}
	
	return 0;
}
