#include <iostream>
#include <iomanip>

#include <DB/Interpreters/AggregationCommon.h>

#include <DB/Common/HashTable/HashMap.h>
#include <DB/Common/HashTable/TwoLevelHashTable.h>

#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/CompressedReadBuffer.h>

#include <statdaemons/Stopwatch.h>
#include <statdaemons/threadpool.hpp>


typedef UInt64 Key;
typedef UInt64 Value;

typedef std::vector<Key> Source;

typedef HashMap<Key, Value> Map1;


struct TwoLevelGrower : public HashTableGrower
{
	static const size_t initial_size_degree = 8;
	TwoLevelGrower() { size_degree = initial_size_degree; }
};

template
<
	typename Key,
	typename Cell,
	typename Hash = DefaultHash<Key>,
	typename Grower = TwoLevelGrower,
	typename Allocator = HashTableAllocator
>
class TwoLevelHashMapTable : public TwoLevelHashTable<Key, Cell, Hash, Grower, Allocator, HashMapTable<Key, Cell, Hash, Grower, Allocator> >
{
public:
	typedef Key key_type;
	typedef typename Cell::Mapped mapped_type;
	typedef typename Cell::value_type value_type;

	mapped_type & operator[](Key x)
	{
		typename TwoLevelHashMapTable::iterator it;
		bool inserted;
		this->emplace(x, it, inserted);

		if (inserted)
			new(&it->second) mapped_type();

		return it->second;
	}
};

template
<
	typename Key,
	typename Mapped,
	typename Hash = DefaultHash<Key>,
	typename Grower = TwoLevelGrower,
	typename Allocator = HashTableAllocator
>
using TwoLevelHashMap = TwoLevelHashMapTable<Key, HashMapCell<Key, Mapped, Hash>, Hash, Grower, Allocator>;

typedef TwoLevelHashMap<Key, Value> Map2;


void aggregate1(Map1 & map, Source::const_iterator begin, Source::const_iterator end)
{
	for (auto it = begin; it != end; ++it)
		++map[*it];
}

void aggregate2(Map2 & map, Source::const_iterator begin, Source::const_iterator end)
{
	for (auto it = begin; it != end; ++it)
		++map[*it];
}

void merge2(Map2 * maps, size_t num_threads, size_t bucket)
{
	for (size_t i = 1; i < num_threads; ++i)
		for (auto it = maps[i].impls[bucket].begin(); it != maps[i].impls[bucket].end(); ++it)
			maps[0].impls[bucket][it->first] += it->second;
}


int main(int argc, char ** argv)
{
	size_t n = atoi(argv[1]);
	size_t num_threads = atoi(argv[2]);

	std::cerr << std::fixed << std::setprecision(2);

	boost::threadpool::pool pool(num_threads);

	Source data(n);

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
		/** Вариант 1.
		  * В разных потоках агрегируем независимо в разные хэш-таблицы.
		  * Затем сливаем их вместе.
		  */

		Map1 maps[num_threads];

		Stopwatch watch;

		for (size_t i = 0; i < num_threads; ++i)
			pool.schedule(std::bind(aggregate1,
				std::ref(maps[i]),
				data.begin() + (data.size() * i) / num_threads,
				data.begin() + (data.size() * (i + 1)) / num_threads));

		pool.wait();

		watch.stop();
		double time_aggregated = watch.elapsedSeconds();
		std::cerr
			<< "Aggregated in " << time_aggregated
			<< " (" << n / time_aggregated << " elem/sec.)"
			<< std::endl;

		std::cerr << "Sizes: ";
		for (size_t i = 0; i < num_threads; ++i)
			std::cerr << (i == 0 ? "" : ", ") << maps[i].size();
		std::cerr << std::endl;

		watch.restart();

		for (size_t i = 1; i < num_threads; ++i)
			for (auto it = maps[i].begin(); it != maps[i].end(); ++it)
				maps[0][it->first] += it->second;

		watch.stop();
		double time_merged = watch.elapsedSeconds();
		std::cerr
			<< "Merged in " << time_merged
			<< " (" << n / time_merged << " elem/sec.)"
			<< std::endl;

		double time_total = time_aggregated + time_merged;
		std::cerr
			<< "Total in " << time_total
			<< " (" << n / time_total << " elem/sec.)"
			<< std::endl;
		std::cerr << "Size: " << maps[0].size() << std::endl;
	}

	{
		/** Вариант 2.
		  * В разных потоках агрегируем независимо в разные two-level хэш-таблицы.
		  * Затем сливаем их вместе, распараллелив по bucket-ам первого уровня.
		  */

		Map2 maps[num_threads];

		Stopwatch watch;

		for (size_t i = 0; i < num_threads; ++i)
			pool.schedule(std::bind(aggregate2,
				std::ref(maps[i]),
				data.begin() + (data.size() * i) / num_threads,
				data.begin() + (data.size() * (i + 1)) / num_threads));

		pool.wait();

		watch.stop();
		double time_aggregated = watch.elapsedSeconds();
		std::cerr
			<< "Aggregated in " << time_aggregated
			<< " (" << n / time_aggregated << " elem/sec.)"
			<< std::endl;

		std::cerr << "Sizes: ";
		for (size_t i = 0; i < num_threads; ++i)
			std::cerr << (i == 0 ? "" : ", ") << maps[i].size();
		std::cerr << std::endl;

		watch.restart();

		for (size_t i = 0; i < Map2::NUM_BUCKETS; ++i)
			pool.schedule(std::bind(merge2,
				&maps[0], num_threads, i));

		pool.wait();

		watch.stop();
		double time_merged = watch.elapsedSeconds();
		std::cerr
			<< "Merged in " << time_merged
			<< " (" << n / time_merged << " elem/sec.)"
			<< std::endl;

		double time_total = time_aggregated + time_merged;
		std::cerr
			<< "Total in " << time_total
			<< " (" << n / time_total << " elem/sec.)"
			<< std::endl;

		size_t sum_size = 0;
		for (size_t i = 0; i < Map2::NUM_BUCKETS; ++i)
			sum_size += maps[0].impls[i].size();

		std::cerr << "Size: " << sum_size << std::endl;
	}

	return 0;
}
