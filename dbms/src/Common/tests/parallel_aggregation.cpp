#include <iostream>
#include <iomanip>
#include <mutex>
#include <atomic>

//#define DBMS_HASH_MAP_DEBUG_RESIZES

#include <DB/Interpreters/AggregationCommon.h>

#include <DB/Common/HashTable/HashMap.h>
#include <DB/Common/HashTable/TwoLevelHashMap.h>
//#include <DB/Common/HashTable/HashTableWithSmallLocks.h>
//#include <DB/Common/HashTable/HashTableMerge.h>

#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/CompressedReadBuffer.h>

#include <DB/Common/Stopwatch.h>
#include <threadpool.hpp>


typedef UInt64 Key;
typedef UInt64 Value;

typedef std::vector<Key> Source;

typedef HashMap<Key, Value> Map;
typedef TwoLevelHashMap<Key, Value> MapTwoLevel;


struct SmallLock
{
	std::atomic<int> locked {false};

	bool tryLock()
	{
		int expected = 0;
		return locked.compare_exchange_strong(expected, 1, std::memory_order_acquire);
	}

	void unlock()
	{
		locked.store(0, std::memory_order_release);
	}
};

struct __attribute__((__aligned__(64))) AlignedSmallLock : public SmallLock
{
	char dummy[64 - sizeof(SmallLock)];
};


typedef Poco::FastMutex Mutex;


/*typedef HashTableWithSmallLocks<
	Key,
	HashTableCellWithLock<
		Key,
		HashMapCell<Key, Value, DefaultHash<Key> > >,
	DefaultHash<Key>,
	HashTableGrower<21>,
	HashTableAllocator> MapSmallLocks;*/


void aggregate1(Map & map, Source::const_iterator begin, Source::const_iterator end)
{
	for (auto it = begin; it != end; ++it)
		++map[*it];
}

#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

void aggregate12(Map & map, Source::const_iterator begin, Source::const_iterator end)
{
	Map::iterator found;
	auto prev_it = end;
	for (auto it = begin; it != end; ++it)
	{
		if (*it == *prev_it)
		{
			++found->second;
			continue;
		}
		prev_it = it;

		bool inserted;
		map.emplace(*it, found, inserted);
		++found->second;
	}
}

void aggregate2(MapTwoLevel & map, Source::const_iterator begin, Source::const_iterator end)
{
	for (auto it = begin; it != end; ++it)
		++map[*it];
}

void aggregate22(MapTwoLevel & map, Source::const_iterator begin, Source::const_iterator end)
{
	MapTwoLevel::iterator found;
	auto prev_it = end;
	for (auto it = begin; it != end; ++it)
	{
		if (*it == *prev_it)
		{
			++found->second;
			continue;
		}
		prev_it = it;

		bool inserted;
		map.emplace(*it, found, inserted);
		++found->second;
	}
}

#if !__clang__
#pragma GCC diagnostic pop
#endif

void merge2(MapTwoLevel * maps, size_t num_threads, size_t bucket)
{
	for (size_t i = 1; i < num_threads; ++i)
		for (auto it = maps[i].impls[bucket].begin(); it != maps[i].impls[bucket].end(); ++it)
			maps[0].impls[bucket][it->first] += it->second;
}

void aggregate3(Map & local_map, Map & global_map, Mutex & mutex, Source::const_iterator begin, Source::const_iterator end)
{
	static constexpr size_t threshold = 65536;

	for (auto it = begin; it != end; ++it)
	{
		Map::iterator found = local_map.find(*it);

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

void aggregate33(Map & local_map, Map & global_map, Mutex & mutex, Source::const_iterator begin, Source::const_iterator end)
{
	static constexpr size_t threshold = 65536;

	for (auto it = begin; it != end; ++it)
	{
		Map::iterator found;
		bool inserted;
		local_map.emplace(*it, found, inserted);
		++found->second;

		if (inserted && local_map.size() == threshold)
		{
			Poco::ScopedLock<Mutex> lock(mutex);
			for (auto & value_type : local_map)
				global_map[value_type.first] += value_type.second;

			local_map.clear();
		}
	}
}

void aggregate4(Map & local_map, MapTwoLevel & global_map, Mutex * mutexes, Source::const_iterator begin, Source::const_iterator end)
{
	static constexpr size_t threshold = 65536;
	static constexpr size_t block_size = 8192;

	auto it = begin;
	while (it != end)
	{
		auto block_end = std::min(end, it + block_size);

		if (local_map.size() < threshold)
		{
			for (; it != block_end; ++it)
				++local_map[*it];
		}
		else
		{
			for (; it != block_end; ++it)
			{
				Map::iterator found = local_map.find(*it);

				if (found != local_map.end())
					++found->second;
				else
				{
					size_t hash_value = global_map.hash(*it);
					size_t bucket = global_map.getBucketFromHash(hash_value);

					if (mutexes[bucket].tryLock())
					{
						++global_map.impls[bucket][*it];
						mutexes[bucket].unlock();
					}
					else
						++local_map[*it];
				}
			}
		}
	}
}
/*
void aggregate5(Map & local_map, MapSmallLocks & global_map, Source::const_iterator begin, Source::const_iterator end)
{
	static constexpr size_t threshold = 65536;

	for (auto it = begin; it != end; ++it)
	{
		Map::iterator found = local_map.find(*it);

		if (found != local_map.end())
			++found->second;
		else if (local_map.size() < threshold)
			++local_map[*it];	/// TODO Можно было бы делать один lookup, а не два.
		else
		{
			SmallScopedLock lock;
			MapSmallLocks::iterator insert_it;
			bool inserted;

			if (global_map.tryEmplace(*it, insert_it, inserted, lock))
				++insert_it->second;
			else
				++local_map[*it];
		}
	}
}*/



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
			<< std::endl << std::endl;
	}

	if (!method || method == 1)
	{
		/** Вариант 1.
		  * В разных потоках агрегируем независимо в разные хэш-таблицы.
		  * Затем сливаем их вместе.
		  */

		std::vector<Map> maps(num_threads);

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

	if (!method || method == 12)
	{
		/** То же самое, но с оптимизацией для подряд идущих одинаковых значений.
		  */

		std::vector<Map> maps(num_threads);

		Stopwatch watch;

		for (size_t i = 0; i < num_threads; ++i)
			pool.schedule(std::bind(aggregate12,
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

	if (!method || method == 11)
	{
		/** Вариант 11.
		  * То же, что вариант 1, но при мердже, изменён порядок циклов,
		  *  что потенциально может дать лучшую кэш-локальность.
		  *
		  * На практике, разницы нет.
		  */

		std::vector<Map> maps(num_threads);

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

		std::vector<Map::iterator> iterators(num_threads);
		for (size_t i = 1; i < num_threads; ++i)
			iterators[i] = maps[i].begin();

		while (true)
		{
			bool finish = true;
			for (size_t i = 1; i < num_threads; ++i)
			{
				if (iterators[i] == maps[i].end())
					continue;

				finish = false;
				maps[0][iterators[i]->first] += iterators[i]->second;
				++iterators[i];
			}

			if (finish)
				break;
		}

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

		std::vector<MapTwoLevel> maps(num_threads);

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

		for (size_t i = 0; i < MapTwoLevel::NUM_BUCKETS; ++i)
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

		std::cerr << "Size: " << maps[0].size() << std::endl << std::endl;
	}

	if (!method || method == 22)
	{
		std::vector<MapTwoLevel> maps(num_threads);

		Stopwatch watch;

		for (size_t i = 0; i < num_threads; ++i)
			pool.schedule(std::bind(aggregate22,
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

		for (size_t i = 0; i < MapTwoLevel::NUM_BUCKETS; ++i)
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

		std::cerr << "Size: " << maps[0].size() << std::endl << std::endl;
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

		std::vector<Map> local_maps(num_threads);
		Map global_map;
		Mutex mutex;

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

	if (!method || method == 33)
	{
		/** Вариант 33.
		 * В разных потоках агрегируем независимо в разные хэш-таблицы,
		 *  пока их размер не станет достаточно большим.
		 * Затем сбрасываем данные в глобальную хэш-таблицу, защищённую mutex-ом, и продолжаем.
		 */

		std::vector<Map> local_maps(num_threads);
		Map global_map;
		Mutex mutex;

		Stopwatch watch;

		for (size_t i = 0; i < num_threads; ++i)
			pool.schedule(std::bind(aggregate33,
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
		  * Этот метод не такой уж плохой при большом количестве потоков, но хуже второго.
		  */

		std::vector<Map> local_maps(num_threads);
		MapTwoLevel global_map;
		std::vector<Mutex> mutexes(MapTwoLevel::NUM_BUCKETS);

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

		size_t sum_size = global_map.size();
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

		std::cerr << "Size: " << global_map.size() << std::endl << std::endl;
	}

/*	if (!method || method == 5)
	{
	*/	/** Вариант 5.
		  * В разных потоках агрегируем независимо в разные хэш-таблицы,
		  *  пока их размер не станет достаточно большим.
		  * Если размер локальной хэш-таблицы большой, и в ней нет элемента,
		  *  то вставляем его в одну глобальную хэш-таблицу, содержащую маленькие защёлки в каждой ячейке,
		  *  а если защёлку не удалось захватить, то вставляем в локальную.
		  * Затем сливаем все локальные хэш-таблицы в глобальную.
		  */
/*
		Map local_maps[num_threads];
		MapSmallLocks global_map;

		Stopwatch watch;

		for (size_t i = 0; i < num_threads; ++i)
			pool.schedule(std::bind(aggregate5,
				std::ref(local_maps[i]),
				std::ref(global_map),
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
				global_map.insert(std::make_pair(it->first, 0)).first->second += it->second;

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
	}*/

	/*if (!method || method == 6)
	{
		*//** Вариант 6.
		  * В разных потоках агрегируем независимо в разные хэш-таблицы.
		  * Затем "сливаем" их, проходя по ним в одинаковом порядке ключей.
		  * Довольно тормозной вариант.
		  */
/*
		std::vector<Map> maps(num_threads);

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

		typedef std::vector<Map *> Maps;
		Maps maps_to_merge(num_threads);
		for (size_t i = 0; i < num_threads; ++i)
			maps_to_merge[i] = &maps[i];

		size_t size = 0;

		for (size_t i = 0; i < 100; ++i)
		processMergedHashTables(maps_to_merge,
			[] (Map::value_type & dst, const Map::value_type & src) { dst.second += src.second; },
			[&] (const Map::value_type & dst) { ++size; });

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
		std::cerr << "Size: " << size << std::endl << std::endl;
	}*/

	return 0;
}
