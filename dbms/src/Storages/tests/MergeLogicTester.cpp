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


int index_granularity = 1;

/// Time, Type, Value
multiset<pair<int, pair<int, int> > > events;

/// Текущие части в merge tree
DataParts data_parts;

/// Первый свободный номер потока для мерджа
int uniqId = 1;

/// Разные статистики
long long totalMergeTime = 0, totalSize = 0;
DataParts maxCount, maxMerging, maxThreads;
int maxCountMoment, maxMergingMoment, maxThreadsMoment, maxScheduledThreadsMoment;
int maxScheduledThreads;

int mergeScheduled = 0;

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


	/// NOTE
	/// Сейчас всегда true, поскольку в настоящем mergeTree этой эвристики нет
	bool is_anything_merging = true;
	for (DataParts::iterator it = data_parts.begin(); it != data_parts.end(); ++it)
		if ((*it)->currently_merging)
			is_anything_merging = true;

	/// Левый конец отрезка.
	for (DataParts::iterator it = data_parts.begin(); it != data_parts.end(); ++it)
	{
		const DataPtr & first_part = *it;

		max_count_from_left = std::max(0, max_count_from_left - 1);

		/// Кусок не занят и достаточно мал.
		if (first_part->currently_merging || (is_anything_merging &&
			first_part->size * index_granularity > settings.max_rows_to_merge_parts))
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
			if (last_part->currently_merging || (is_anything_merging &&
				last_part->size * index_granularity > settings.max_rows_to_merge_parts))
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
	if (maxScheduledThreads < mergeScheduled)
	{
		maxScheduledThreads = mergeScheduled;
		maxScheduledThreadsMoment = time;
	}
}


/// выбрать кого мерджить, оценить время и добавить событие об окончании
bool makeMerge(int cur_time) {
	if (getThreads(data_parts) >= settings.merging_threads) return 0;
	if (mergeScheduled == 0) return 0;
	mergeScheduled --;
	std::vector<DataPtr> e;
	if (!selectPartsToMerge(e)) return 1;
	int curId = uniqId ++;
	size_t size = 0;
	for (size_t i = 0; i < e.size(); ++i)
	{
		e[i]->currently_merging = curId;
		size += e[i]->size;
	}
	size_t need_time = (size + RowsPerSec - 1) / RowsPerSec;
	totalMergeTime += need_time;
	events.insert(make_pair(cur_time + need_time, make_pair(2, curId)));
	return 1;
}

/// Запустить потоки мерджа
void merge(int cur_time, int cnt)
{
	mergeScheduled += cnt;
}

/// Обработать событие
void process(pair<int, pair<int, int> > ev)
{
	int cur_time = ev.first;
	int type = ev.second.first;
	int val = ev.second.second;

	/// insert
	if (type == 1)
	{
		data_parts.insert(new DataPart(cur_time, val));
		totalSize += val;
		merge(cur_time, 2);
	} else if (type == 2) /// merge done
	{
		size_t size = 0;
		int st = (int)1e9;
		DataParts newData;
		for (DataParts::iterator it = data_parts.begin(); it != data_parts.end();)
		{
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
		}
		data_parts.insert(new DataPart(st, size));
	} else if (type == 3) /// do merge
	{
		merge(cur_time, val);
	}

	while (makeMerge(cur_time));
}


int main()
{
	srand(rdtsc());
	int delay = 15;
	for (int i = 0; i < 60*60*30/delay; ++i)
	{
		if (rand() & 7)
			events.insert(make_pair(i * delay, make_pair(1, genRand(75000, 85000))));
		else {
			events.insert(make_pair(-4 + i * delay, make_pair(1, genRand(10000, 30000))));
			events.insert(make_pair(+4 + i * delay, make_pair(1, genRand(10000, 30000))));
		}
	}

	int iter = 0;
	int cur_time = 0;
	maxCount = data_parts;
	puts("________________________________________________________________________________________________________");
	puts("A couple of moments from the process log:");
	while (events.size() > 0)
	{
		cur_time = events.begin()->first;
		updateStat(cur_time);
		iter ++;
		if (iter % 3000 == 0)
		{
			printf("Current time: %d\n", cur_time);
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
	printf("Max number of running threads was at %d second with %d threads\n", maxThreadsMoment, getThreads(maxThreads));
	writeParts(maxThreads);
	printf("Max number of scheduled threads was at %d second with %d threads\n", maxScheduledThreadsMoment, maxScheduledThreads);
	printf("Total merge time %lld sec\n", totalMergeTime);
	printf("Total time %d sec\n", cur_time);
	printf("Total parts size %lld\n", totalSize);
	printf("Total merged Rows / total rows %0.5lf \n", 1.0 * totalMergeTime * RowsPerSec / totalSize);
	puts("________________________________________________________________________________________________________");
	puts("Result configuration:\n");
	writeParts(data_parts);

	return 0;
}
