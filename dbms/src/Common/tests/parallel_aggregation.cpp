#include <iostream>
#include <iomanip>
#include <mutex>
#include <atomic>

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


struct __attribute__((__aligned__(64))) SmallLock
{
	std::atomic<bool> locked {false};
	char dummy[64 - sizeof(locked)];

	bool tryLock()
	{
		bool expected = false;
		return locked.compare_exchange_strong(expected, true, std::memory_order_acquire);
	}

	void unlock()
	{
		locked.store(false, std::memory_order_release);
	}
};


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

void aggregate3(Map1 & local_map, Map1 & global_map, SmallLock & mutex, Source::const_iterator begin, Source::const_iterator end)
{
	static constexpr size_t threshold = 65536;

	for (auto it = begin; it != end; ++it)
	{
		Map1::iterator found = local_map.find(*it);

		if (found != local_map.end())
			++found->second;
		else if (local_map.size() < threshold)
			++local_map[*it];	/// TODO Можно было бы делать один lookup, а не два.
		else
		{
			if (mutex.tryLock())
			{
				++global_map[*it];
				mutex.unlock();
			}
			else
				++local_map[*it];
		}
	}
}

void aggregate4(Map1 & local_map, Map2 & global_map, SmallLock * mutexes, Source::const_iterator begin, Source::const_iterator end)
{
	DefaultHash<Key> hash;
	static constexpr size_t threshold = 65536;

	for (auto it = begin; it != end; ++it)
	{
		Map1::iterator found = local_map.find(*it);

		if (found != local_map.end())
			++found->second;
		else if (local_map.size() < threshold)
			++local_map[*it];	/// TODO Можно было бы делать один lookup, а не два.
		else
		{
			size_t hash_value = hash(*it);
			size_t bucket = hash_value >> 24;

			if (mutexes[bucket].tryLock())
			{
				Map2::Impl::iterator inserted_it;
				bool inserted;
				global_map.impls[bucket].emplace(*it, inserted_it, inserted, hash_value);
				mutexes[bucket].unlock();
			}
			else
				++local_map[*it];
		}
	}
}


int main(int argc, char ** argv)
{
	size_t n = atoi(argv[1]);
	size_t num_threads = atoi(argv[2]);
	size_t method = argc <= 3 ? 0 : atoi(argv[3]);

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

	if (!method || method == 1)
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

		size_t size_before_merge = 0;
		std::cerr << "Sizes: ";
		for (size_t i = 0; i < num_threads; ++i)
		{
			std::cerr << (i == 0 ? "" : ", ") << maps[i].size();
			size_before_merge += maps[i].size();
		}
		std::cerr << std::endl;

		watch.restart();

		for (size_t i = 1; i < num_threads; ++i)
			for (auto it = maps[i].begin(); it != maps[i].end(); ++it)
				maps[0][it->first] += it->second;

		watch.stop();
		double time_merged = watch.elapsedSeconds();
		std::cerr
			<< "Merged in " << time_merged
			<< " (" << size_before_merge / time_merged << " elem/sec.)"
			<< std::endl;

		double time_total = time_aggregated + time_merged;
		std::cerr
			<< "Total in " << time_total
			<< " (" << n / time_total << " elem/sec.)"
			<< std::endl;
		std::cerr << "Size: " << maps[0].size() << std::endl << std::endl;
	}

	if (!method || method == 2)
	{
		/** Вариант 2.
		  * В разных потоках агрегируем независимо в разные two-level хэш-таблицы.
		  * Затем сливаем их вместе, распараллелив по bucket-ам первого уровня.
		  * При использовании хэш-таблиц больших размеров (10 млн. элементов и больше),
		  *  и большого количества потоков (8-32), слияние является узким местом,
		  *  и преимущество в производительности достигает 4 раз.
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

		size_t size_before_merge = 0;
		std::cerr << "Sizes: ";
		for (size_t i = 0; i < num_threads; ++i)
		{
			std::cerr << (i == 0 ? "" : ", ") << maps[i].size();
			size_before_merge += maps[i].size();
		}
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
			<< " (" << size_before_merge / time_merged << " elem/sec.)"
			<< std::endl;

		double time_total = time_aggregated + time_merged;
		std::cerr
			<< "Total in " << time_total
			<< " (" << n / time_total << " elem/sec.)"
			<< std::endl;

		size_t sum_size = 0;
		for (size_t i = 0; i < Map2::NUM_BUCKETS; ++i)
			sum_size += maps[0].impls[i].size();

		std::cerr << "Size: " << sum_size << std::endl << std::endl;
	}

	if (!method || method == 3)
	{
		/** Вариант 3.
		  * В разных потоках агрегируем независимо в разные хэш-таблицы,
		  *  пока их размер не станет достаточно большим.
		  * Если размер локальной хэш-таблицы большой, и в ней нет элемента,
		  *  то вставляем его в одну глобальную хэш-таблицу, защищённую mutex-ом,
		  *  а если mutex не удалось захватить, то вставляем в локальную.
		  * Затем сливаем все локальные хэш-таблицы в глобальную.
		  * Этот метод плохой - много contention-а.
		  */

		Map1 local_maps[num_threads];
		Map1 global_map;
		SmallLock mutex;

		Stopwatch watch;

		for (size_t i = 0; i < num_threads; ++i)
			pool.schedule(std::bind(aggregate3,
				std::ref(local_maps[i]),
				std::ref(global_map),
				std::ref(mutex),
				data.begin() + (data.size() * i) / num_threads,
				data.begin() + (data.size() * (i + 1)) / num_threads));

		pool.wait();

		watch.stop();
		double time_aggregated = watch.elapsedSeconds();
		std::cerr
			<< "Aggregated in " << time_aggregated
			<< " (" << n / time_aggregated << " elem/sec.)"
			<< std::endl;

		size_t size_before_merge = 0;
		std::cerr << "Sizes (local): ";
		for (size_t i = 0; i < num_threads; ++i)
		{
			std::cerr << (i == 0 ? "" : ", ") << local_maps[i].size();
			size_before_merge += local_maps[i].size();
		}
		std::cerr << std::endl;
		std::cerr << "Size (global): " << global_map.size() << std::endl;
		size_before_merge += global_map.size();

		watch.restart();

		for (size_t i = 0; i < num_threads; ++i)
			for (auto it = local_maps[i].begin(); it != local_maps[i].end(); ++it)
				global_map[it->first] += it->second;

		pool.wait();

		watch.stop();
		double time_merged = watch.elapsedSeconds();
		std::cerr
			<< "Merged in " << time_merged
			<< " (" << size_before_merge / time_merged << " elem/sec.)"
			<< std::endl;

		double time_total = time_aggregated + time_merged;
		std::cerr
			<< "Total in " << time_total
			<< " (" << n / time_total << " elem/sec.)"
			<< std::endl;

		std::cerr << "Size: " << global_map.size() << std::endl << std::endl;
	}

	if (!method || method == 4)
	{
		/** Вариант 4.
		  * В разных потоках агрегируем независимо в разные хэш-таблицы,
		  *  пока их размер не станет достаточно большим.
		  * Если размер локальной хэш-таблицы большой, и в ней нет элемента,
		  *  то вставляем его в одну из 256 глобальных хэш-таблиц, каждая из которых под своим mutex-ом.
		  * Затем сливаем все локальные хэш-таблицы в глобальную.
		  * Этот метод тоже плохой.
		  */

		Map1 local_maps[num_threads];
		Map2 global_map;
		SmallLock mutexes[Map2::NUM_BUCKETS];

		Stopwatch watch;

		for (size_t i = 0; i < num_threads; ++i)
			pool.schedule(std::bind(aggregate4,
				std::ref(local_maps[i]),
				std::ref(global_map),
				&mutexes[0],
				data.begin() + (data.size() * i) / num_threads,
				data.begin() + (data.size() * (i + 1)) / num_threads));

		pool.wait();

		watch.stop();
		double time_aggregated = watch.elapsedSeconds();
		std::cerr
			<< "Aggregated in " << time_aggregated
			<< " (" << n / time_aggregated << " elem/sec.)"
			<< std::endl;

		size_t size_before_merge = 0;
		std::cerr << "Sizes (local): ";
		for (size_t i = 0; i < num_threads; ++i)
		{
			std::cerr << (i == 0 ? "" : ", ") << local_maps[i].size();
			size_before_merge += local_maps[i].size();
		}
		std::cerr << std::endl;

		size_t sum_size = 0;
		for (size_t i = 0; i < Map2::NUM_BUCKETS; ++i)
			sum_size += global_map.impls[i].size();

		std::cerr << "Size (global): " << sum_size << std::endl;
		size_before_merge += sum_size;

		watch.restart();

		for (size_t i = 0; i < num_threads; ++i)
			for (auto it = local_maps[i].begin(); it != local_maps[i].end(); ++it)
				global_map[it->first] += it->second;

		pool.wait();

		watch.stop();
		double time_merged = watch.elapsedSeconds();
		std::cerr
			<< "Merged in " << time_merged
			<< " (" << size_before_merge / time_merged << " elem/sec.)"
			<< std::endl;

		double time_total = time_aggregated + time_merged;
		std::cerr
			<< "Total in " << time_total
			<< " (" << n / time_total << " elem/sec.)"
			<< std::endl;

		sum_size = 0;
		for (size_t i = 0; i < Map2::NUM_BUCKETS; ++i)
			sum_size += global_map.impls[i].size();

		std::cerr << "Size: " << sum_size << std::endl << std::endl;
	}

	return 0;
}
