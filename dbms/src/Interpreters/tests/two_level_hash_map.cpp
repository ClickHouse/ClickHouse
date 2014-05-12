#include <iostream>
#include <iomanip>
#include <vector>

#include <unordered_map>

#include <sparsehash/dense_hash_map>
#include <sparsehash/sparse_hash_map>

//#define DBMS_HASH_MAP_DEBUG_RESIZES

#include <statdaemons/Stopwatch.h>
#include <stats/UniquesHashSet.h>

#include <DB/Core/Types.h>
#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/CompressedReadBuffer.h>
#include <DB/Common/HashTable/TwoLevelHashTable.h>
#include <DB/Common/HashTable/HashMap.h>


typedef UInt64 Key;
typedef UInt64 Value;


int main(int argc, char ** argv)
{
	size_t n = atoi(argv[1]);

	std::vector<Key> data(n);

	std::cerr << "sizeof(Key) = " << sizeof(Key) << ", sizeof(Value) = " << sizeof(Value) << std::endl;

	{
		Stopwatch watch;
		DB::ReadBufferFromFileDescriptor in1(STDIN_FILENO);
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

		std::cerr << sizeof(HashMapCell<Key, Value, DefaultHash<Key> >) << std::endl;

		typedef TwoLevelHashTable<Key, HashMapCell<Key, Value, DefaultHash<Key> >, DefaultHash<Key>, HashTableGrower<8>, HashTableAllocator> Map;

		Map map;
		Map::iterator it;
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
			<< "HashMap. Size: " << map.size()
			<< ", elapsed: " << watch.elapsedSeconds()
			<< " (" << n / watch.elapsedSeconds() << " elem/sec.)"
			<< std::endl;

		size_t sum_counts = 0;
		size_t elems = 0;
		for (const auto & kv : map)
		{
			sum_counts += kv.second;
			++elems;
		}

		std::cerr << "sum_counts: " << sum_counts << ", elems: " << elems << std::endl;
	}

	{
		Stopwatch watch;

		typedef TwoLevelHashTable<Key, HashMapCell<Key, Value, DefaultHash<Key> >, DefaultHash<Key>, HashTableGrower<8>, HashTableAllocator> Map;
		//typedef HashMap<Key, Value, UniquesHashSetDefaultHash> Map;

		Map map;
		Map::iterator it;
		bool inserted;

		for (size_t i = 0; i < n; ++i)
		{
			map.emplace(i, it, inserted);
			if (inserted)
				it->second = 0;
			++it->second;
		}

		watch.stop();
		std::cerr << std::fixed << std::setprecision(2)
			<< "HashMap. Size: " << map.size()
			<< ", elapsed: " << watch.elapsedSeconds()
			<< " (" << n / watch.elapsedSeconds() << " elem/sec.)"
			<< std::endl;

		size_t sum_counts = 0;
		size_t elems = 0;
		for (const auto & kv : map)
		{
			sum_counts += kv.second;
			++elems;

			if (kv.first > n)
				std::cerr << kv.first << std::endl;
		}

		std::cerr << "sum_counts: " << sum_counts << ", elems: " << elems << std::endl;

		if (sum_counts != n)
			std::cerr << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" << std::endl;
	}

	return 0;
}
