#include <DB/Storages/StorageMergeTree.h>

using namespace DB;
using namespace std;

struct DataPart
{
	int time; ///левая граница куска
	size_t size; /// количество строк в куске
	int currently_merging; /// 0 если не мерджится, иначе номер "потока"

	DataPart () : time(0), size(0), currently_merging(0) {};
	DataPart (int time, size_t val) : time(time), size(val), currently_merging(0) {};
	DataPart (int time, size_t val, int currently_merging) : time(time), size(val), currently_merging(currently_merging) {};
};

bool operator < (const DataPart &a, const DataPart &b)
{
	if (a.time != b.time)
		return a.time < b.time;
	return a.size < b.size;
}

typedef Poco::SharedPtr<DataPart> DataPtr;
struct DataPtrLess { bool operator() (const DataPtr & lhs, const DataPtr & rhs) const { return *lhs < *rhs; } };
typedef std::set<DataPtr, DataPtrLess> DataParts;

DataParts copy(const DataParts &a)
{
	DataParts res;
	for (DataParts::iterator it = a.begin(); it != a.end(); it ++)
		res.insert(new DataPart((*it)->time, (*it)->size, (*it)->currently_merging));
	return res;
}

const int RowsPerSec = 55000;

StorageMergeTreeSettings settings;

/// Чему он равен ?
int index_granularity = 0;

/// Time, Type, Value
set<pair<int, pair<int, int> > > events;

/// Текущие части в merge tree
DataParts data_parts;

/// Первый свободный номер потока для мерджа
int uniqId = 1;

/// Разные статистики
long long totalMergeTime = 0, totalSize = 0;
DataParts maxCount, maxMerging, maxThreads;
int maxCountMoment, maxMergingMoment, maxThreadsMoment;

int genRand(int l, int r)
{
	return l + rand() % (r - l + 1);
}

/// Используется дли инициализации рандсида
long long rdtsc()
{
	asm("rdtsc");
}

/// Скопировано с минимальной потерей логики
bool selectPartsToMerge(std::vector<DataPtr> & parts)
{
	size_t min_max = -1U;
	size_t min_min = -1U;
	int max_len = 0;
	DataParts::iterator best_begin;
	bool found = false;

	/// Сколько кусков, начиная с текущего, можно включить в валидный отрезок, начинающийся левее текущего куска.
	/// Нужно для определения максимальности по включению.
	int max_count_from_left = 0;

	/// Левый конец отрезка.
	for (DataParts::iterator it = data_parts.begin(); it != data_parts.end(); ++it)
	{
		const DataPtr & first_part = *it;

		max_count_from_left = std::max(0, max_count_from_left - 1);

		/// Кусок не занят и достаточно мал.
		if (first_part->currently_merging ||
			first_part->size * index_granularity > settings.max_rows_to_merge_parts)
			continue;

		/// Самый длинный валидный отрезок, начинающийся здесь.
		size_t cur_longest_max = -1U;
		size_t cur_longest_min = -1U;
		int cur_longest_len = 0;

		/// Текущий отрезок, не обязательно валидный.
		size_t cur_max = first_part->size;
		size_t cur_min = first_part->size;
		size_t cur_sum = first_part->size;
		int cur_len = 1;

		/// Правый конец отрезка.
		DataParts::iterator jt = it;
		for (++jt; jt != data_parts.end() && cur_len < static_cast<int>(settings.max_parts_to_merge_at_once); ++jt)
		{
			const DataPtr & last_part = *jt;

			/// Кусок не занят, достаточно мал и в одном правильном месяце.
			if (last_part->currently_merging ||
				last_part->size * index_granularity > settings.max_rows_to_merge_parts)
				break;

			cur_max = std::max(cur_max, last_part->size);
			cur_min = std::min(cur_min, last_part->size);
			cur_sum += last_part->size;
			++cur_len;

			/// Если отрезок валидный, то он самый длинный валидный, начинающийся тут.
			if (cur_len >= 2 &&
				(static_cast<double>(cur_max) / (cur_sum - cur_max) < settings.max_size_ratio_to_merge_parts))
			{
				cur_longest_max = cur_max;
				cur_longest_min = cur_min;
				cur_longest_len = cur_len;
			}
		}

		/// Это максимальный по включению валидный отрезок.
		if (cur_longest_len > max_count_from_left)
		{
			max_count_from_left = cur_longest_len;

			if (!found ||
				std::make_pair(std::make_pair(cur_longest_max, cur_longest_min), -cur_longest_len) <
				std::make_pair(std::make_pair(min_max, min_min), -max_len))
			{
				found = true;
				min_max = cur_longest_max;
				min_min = cur_longest_min;
				max_len = cur_longest_len;
				best_begin = it;
			}
		}
	}

	if (found)
	{
		parts.clear();

		DataParts::iterator it = best_begin;
		for (int i = 0; i < max_len; ++i)
		{
			parts.push_back(*it);
			parts.back()->currently_merging = true;
			++it;
		}

//		LOG_DEBUG(log, "Selected " << parts.size() << " parts from " << parts.front()->time << " to " << parts.back()->time);
	}
	else
	{
//		LOG_DEBUG(log, "No parts to merge");
	}

	return found;
}

/// выбрать кого мерджить, оценить время и добавить событие об окончании
void makeMerge(int curTime) {
	std::vector<DataPtr> e;
	if (!selectPartsToMerge(e)) return;
	int curId = uniqId ++;
	size_t size = 0;
	for (size_t i = 0; i < e.size(); ++i)
	{
		e[i]->currently_merging = curId;
		size += e[i]->size;
	}
	size_t needTime = (size + RowsPerSec - 1) / RowsPerSec;
	totalMergeTime += needTime;
	events.insert(make_pair(curTime + needTime, make_pair(2, curId)));
}

/// Запустить потоки мерджа
void merge(int curTime, int cnt)
{
	for (int i = 0; i < cnt; ++i)
		makeMerge(curTime);
}

/// Обработать событие
void process(pair<int, pair<int, int> > ev)
{
	int curTime = ev.first;
	int type = ev.second.first;
	int val = ev.second.second;

	/// insert
	if (type == 1)
	{
		data_parts.insert(new DataPart(curTime, val));
		merge(curTime, 2);
		totalSize += val;
		return;
	}

	/// merge done
	if (type == 2)
	{
		size_t size = 0;
		int st = (int)1e9;
		DataParts newData;
		for (DataParts::iterator it = data_parts.begin(); it != data_parts.end();)
			if ((*it)->currently_merging == val)
			{
				size += (*it)->size;
				st = min(st, (*it)->time);
				DataParts::iterator nxt = it;
				nxt ++;
				data_parts.erase(it);
				it = nxt;
			} else
				it ++;
		data_parts.insert(new DataPart(st, size));
		return;
	}
}

int getMergeSize(const DataParts &a)
{
	int res = 0;
	for (DataParts::iterator it = a.begin(); it != a.end(); ++it)
		if ((*it)->currently_merging)
			res += (*it)->size;
	return res;
}

int getThreads(const DataParts &a)
{
	set<int> res;
	for (DataParts::iterator it = a.begin(); it != a.end(); ++it)
		res.insert((*it)->currently_merging);
	res.erase(0);
	return res.size();
}

void writeParts(const DataParts &a)
{
	for (DataParts::iterator it = a.begin(); it != a.end(); ++it)
		printf("(%d, %d) ", (int)(*it)->size, (int)(*it)->currently_merging);
	puts("\n");
}

void updateStat(int time)
{
	if (maxCount.size() < data_parts.size())
	{
		maxCount = copy(data_parts);
		maxCountMoment = time;
	}
	if (getMergeSize(maxMerging) < getMergeSize(data_parts))
	{
		maxMerging = copy(data_parts);
		maxMergingMoment = time;
	}
	if (getThreads(maxThreads) < getThreads(data_parts))
	{
		maxThreads = copy(data_parts);
		maxThreadsMoment = time;
	}
}

int main()
{
	srand(rdtsc());
	for (int i = 0; i < 10000; ++i)
	{
		if (rand() & 15)
			events.insert(make_pair(i * 10, make_pair(1, genRand(65000, 75000))));
		else {
			events.insert(make_pair(2 + i * 10, make_pair(1, genRand(1000, 20000))));
			events.insert(make_pair(5 + i * 10, make_pair(1, genRand(1000, 20000))));
			events.insert(make_pair(8 + i * 10, make_pair(1, genRand(1000, 20000))));
		}
	}
	int iter = 0;
	int curTime = 0;
	maxCount = data_parts;
	puts("________________________________________________________________________________________________________");
	puts("A couple of moments from the process log:");
	while (events.size() > 0)
	{
		curTime = events.begin()->first;
		updateStat(curTime);
		iter ++;
		if (iter % 3000 == 0)
		{
			printf("Current time: %d\n", curTime);
			printf("Current parts:");
			writeParts(data_parts);
		}
		process(*events.begin());
		events.erase(*events.begin());
	}
	puts("________________________________________________________________________________________________________");
	puts("During whole process: ");
	printf("Max number of alive parts was at %d second with %d parts\n", maxCountMoment, (int) maxCount.size());
	writeParts(maxCount);
	printf("Max total size of merging parts was at %d second with %d rows in merge\n", maxMergingMoment, getMergeSize(maxMerging));
	writeParts(maxMerging);
	printf("Max number of active threads was at %d second with %d threads\n", maxThreadsMoment, getThreads(maxThreads));
	writeParts(maxThreads);
	printf("Total merge time %lld sec\n", totalMergeTime);
	printf("Total time %d sec\n", curTime);
	printf("Total parts size %lld\n", totalSize);
	printf("Total merged Rows / total rows %0.5lf \n", 1.0 * totalMergeTime * RowsPerSec / totalSize);
	puts("________________________________________________________________________________________________________");
	puts("Result configuration:\n");
	writeParts(data_parts);

	return 0;
}

/*
less /var/log/clickhouse-server/clickhouse-server.log
    2  2013-11-22 15:42:46 du -b --max-depth=1 /opt/clickhouse/data/merge/hits/ | less
    3  2013-11-22 15:43:29 less /var/log/clickhouse-server/clickhouse-server.log
    4  2013-11-22 15:44:00 du -b --max-depth=1 /opt/clickhouse/data/merge/hits/ | less
    5  2013-11-22 15:44:15 less /var/log/clickhouse-server/clickhouse-server.log
    6  2013-11-22 15:44:30 ls -l /opt/clickhouse/data/merge/hits/ | less
    7  2013-11-22 15:44:42 less /var/log/clickhouse-server/clickhouse-server.log.1
    8  2013-11-22 15:44:45 less /var/log/clickhouse-server/clickhouse-server.log.2.gz
    9  2013-11-22 15:44:50 less /var/log/clickhouse-server/clickhouse-server.log.3.gz
   10  2013-11-22 15:45:23 less /var/log/clickhouse-server/clickhouse-server.log.2.gz
   11  2013-11-22 15:45:41 less /var/log/clickhouse-server/clickhouse-server.log.3.gz
   12  2013-11-22 15:46:07 less /var/log/clickhouse-server/clickhouse-server.log.4.gz
   13  2013-11-22 15:47:12 du -b --max-depth=1 /opt/clickhouse/data/merge/hits/ | less
   14  2013-11-22 15:47:48 less /var/log/clickhouse-server/clickhouse-server.log.1
   15  2013-11-26 17:16:54 ls -l /opt/clickhouse/data/merge/hits/ | less
   16  2013-11-26 17:17:11 du -b --max-depth=1 /opt/clickhouse/data/merge/hits/ | less
   17  2013-11-26 17:17:52 less /var/log/clickhouse-server/clickhouse-server.log

2013.11.26 16:31:48 [ 321 ] <Debug> StorageMergeTree: visits: Selecting parts to merge
2013.11.26 16:31:48 [ 321 ] <Debug> StorageMergeTree: visits: Selected 4 parts from 20131126_20131126_46035_46099_33 to 20131126_20131126_46102_46102_0
2013.11.26 16:31:48 [ 321 ] <Debug> StorageMergeTree: visits: Merging 4 parts: from 20131126_20131126_46035_46099_33 to 20131126_20131126_46102_46102_0
2013.11.26 16:31:48 [ 321 ] <Trace> StorageMergeTree: visits: Reading 1 ranges from part 20131126_20131126_46035_46099_33, up to 876544 rows starting from 0
2013.11.26 16:31:48 [ 321 ] <Trace> StorageMergeTree: visits: Reading 1 ranges from part 20131126_20131126_46100_46100_0, up to 16384 rows starting from 0
2013.11.26 16:31:48 [ 321 ] <Trace> StorageMergeTree: visits: Reading 1 ranges from part 20131126_20131126_46101_46101_0, up to 81920 rows starting from 0
2013.11.26 16:31:48 [ 321 ] <Trace> StorageMergeTree: visits: Reading 1 ranges from part 20131126_20131126_46102_46102_0, up to 81920 rows starting from 0
2013.11.26 16:31:48 [ 304 ] <Information> TCPHandler: Processed in 5.820 sec.
2013.11.26 16:31:48 [ 311 ] <Debug> MergingSortedBlockInputStream: Merge sorted 8 blocks, 72809 rows in 1.22 sec., 59667.15 rows/sec., 89.11 MiB/sec.
2013.11.26 16:31:48 [ 305 ] <Information> TCPHandler: Processed in 5.726 sec.
2013.11.26 16:31:48 [ 306 ] <Information> TCPHandler: Processed in 5.589 sec.
2013.11.26 16:31:48 [ 311 ] <Trace> StorageMergeTree: clicks: Merged 3 parts: from 20131126_20131126_41232_41501_192 to 20131126_20131126_41503_41503_0
2013.11.26 16:31:48 [ 311 ] <Trace> StorageMergeTree: clicks: Clearing old parts
2013.11.26 16:31:48 [ 311 ] <Debug> StorageMergeTree: clicks: 'Removing' part 20131126_20131126_41232_41501_192 (prepending old_ to its name)
2013.11.26 16:31:48 [ 311 ] <Debug> StorageMergeTree: clicks: 'Removing' part 20131126_20131126_41502_41502_0 (prepending old_ to its name)
2013.11.26 16:31:48 [ 311 ] <Debug> StorageMergeTree: clicks: 'Removing' part 20131126_20131126_41503_41503_0 (prepending old_ to its name)
2013.11.26 16:31:48 [ 307 ] <Information> TCPHandler: Processed in 5.492 sec.


2013.11.26 16:51:44 [ 309 ] <Trace> StorageMergeTree: hits: Merged 5 parts: from 20131126_20131126_45719_46125_25 to 20131126_20131126_46206_46206_0


2013.11.26 16:30:00 [ 321 ] <Debug> MergingSortedBlockInputStream: Merge sorted 74 blocks, 731029 rows in 13.45 sec., 54334.14 rows/sec., 86.64 MiB/sec.
2013.11.26 16:30:00 [ 321 ] <Trace> StorageMergeTree: visits: Merged 3 parts: from 20131126_20131126_46035_46085_28 to 20131126_20131126_46087_46087_0
*/


