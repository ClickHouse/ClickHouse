#include <DB/Storages/StorageMergeTree.h>

using namespace DB;
using namespace std;

struct DataPart
{
	int time; ///левая граница куска
	size_t size; /// количество строк в куске
	int currently_merging; /// 0 если не мерджится, иначе номер "потока"
	int modification_time;

	DataPart () : time(0), size(0), currently_merging(0), modification_time(0) {};
	DataPart (int time, size_t val) : time(time), size(val), currently_merging(0), modification_time(time) {};
	DataPart (int time, size_t val, int currently_merging) : time(time), size(val), currently_merging(currently_merging), modification_time(time) {};
	DataPart (int time, size_t val, int currently_merging, int modification_time) : time(time), size(val), currently_merging(currently_merging), modification_time(modification_time) {};
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

const int RowsPerSec = 100000;

MergeTreeSettings settings;


size_t index_granularity = 1;

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

double averageNumberOfParts = 0.0;

int mergeScheduled = 0;

int cur_time = 0;


int genRand(int l, int r)
{
	return l + rand() % (r - l + 1);
}

bool selectPartsToMerge(std::vector<DataPtr> & parts)
{
//	LOG_DEBUG(log, "Selecting parts to merge");

	size_t min_max = -1U;
	size_t min_min = -1U;
	int max_len = 0;
	DataParts::iterator best_begin;
	bool found = false;

	/// Сколько кусков, начиная с текущего, можно включить в валидный отрезок, начинающийся левее текущего куска.
	/// Нужно для определения максимальности по включению.
	int max_count_from_left = 0;

	size_t cur_max_rows_to_merge_parts = settings.max_rows_to_merge_parts;

	/// Если ночь, можем мерджить сильно большие куски
//	if (now_hour >= 1 && now_hour <= 5)
//		cur_max_rows_to_merge_parts *= settings.merge_parts_at_night_inc;

	/// Если есть активный мердж крупных кусков, то ограничаемся мерджем только маленьких частей.
	for (DataParts::iterator it = data_parts.begin(); it != data_parts.end(); ++it)
		if ((*it)->currently_merging && (*it)->size * index_granularity > 25 * 1024 * 1024)
			cur_max_rows_to_merge_parts = settings.max_rows_to_merge_parts_second;

	/// Найдем суммарный размер еще не пройденных кусков (то есть всех).
	size_t size_of_remaining_parts = 0;
	for (const auto & part : data_parts)
		size_of_remaining_parts += part->size;

	/// Левый конец отрезка.
	for (DataParts::iterator it = data_parts.begin(); it != data_parts.end(); ++it)
	{
		const DataPtr & first_part = *it;

		max_count_from_left = std::max(0, max_count_from_left - 1);
		size_of_remaining_parts -= first_part->size;

		/// Кусок не занят и достаточно мал.
		if (first_part->currently_merging ||
			first_part->size * index_granularity > cur_max_rows_to_merge_parts)
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

		int oldest_modification_time = first_part->modification_time;

		/// Правый конец отрезка.
		DataParts::iterator jt = it;
		for (++jt; jt != data_parts.end() && cur_len < static_cast<int>(settings.max_parts_to_merge_at_once); ++jt)
		{
			const DataPtr & last_part = *jt;

			/// Кусок не занят, достаточно мал и в одном правильном месяце.
			if (last_part->currently_merging ||
				last_part->size * index_granularity > cur_max_rows_to_merge_parts)
				break;

			oldest_modification_time = std::max(oldest_modification_time, last_part->modification_time);
			cur_max = std::max(cur_max, last_part->size);
			cur_min = std::min(cur_min, last_part->size);
			cur_sum += last_part->size;
			++cur_len;

			int min_len = 2;
			int cur_age_in_sec = cur_time - oldest_modification_time;

			/// Если куски примерно больше 1 Gb и образовались меньше 6 часов назад, то мерджить не меньше чем по 3.
			if (cur_max * index_granularity * 150 > 1024*1024*1024 && cur_age_in_sec < 6*3600)
				min_len = 3;

			/// Размер кусков после текущих, делить на максимальный из текущих кусков. Чем меньше, тем новее текущие куски.
			size_t oldness_coef = (size_of_remaining_parts + first_part->size - cur_sum + 0.0) / cur_max;

			/// Эвристика: если после этой группы кусков еще накопилось мало строк, не будем соглашаться на плохо
			///  сбалансированные слияния, расчитывая, что после будущих вставок данных появятся более привлекательные слияния.
			double ratio = (oldness_coef + 1) * settings.size_ratio_coefficient_to_merge_parts;

			/// Если отрезок валидный, то он самый длинный валидный, начинающийся тут.
			if (cur_len >= min_len &&
				(static_cast<double>(cur_max) / (cur_sum - cur_max) < ratio))
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
	}

	return found;
}


size_t getMergeSize(const DataParts &a)
{
	size_t res = 0;
	for (DataParts::iterator it = a.begin(); it != a.end(); ++it)
		if ((*it)->currently_merging)
			res += (*it)->size;
	return res;
}

size_t getThreads(const DataParts &a)
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
	int curId = ++uniqId;
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
				++nxt;
				data_parts.erase(it);
				it = nxt;
			} else
				++it;
		}
		data_parts.insert(new DataPart(st, size, 0, cur_time));
	} else if (type == 3) /// do merge
	{
		merge(cur_time, val);
	}

	while (makeMerge(cur_time));
}


int main()
{
	srand(time(0));
	int delay = 12;
	int total_time = 60*60*30;
	for (int i = 0; i < total_time/delay; ++i)
	{
		int cur_time = genRand(0, total_time);
		if (rand() & 7)
			events.insert(make_pair(cur_time, make_pair(1, genRand(70000, 80000))));
		else {
			events.insert(make_pair(cur_time, make_pair(1, genRand(10000, 30000))));
		}
	}

	int iter = 0;
	maxCount = data_parts;
	puts("________________________________________________________________________________________________________");
	puts("Couple moments from the process log:");
	while (events.size() > 0)
	{
		int last_time = cur_time;
		cur_time = events.begin()->first;
		averageNumberOfParts += 1.0 * (cur_time - last_time) * data_parts.size();
		if (cur_time > total_time) break;
		updateStat(cur_time);
		++iter;
		if (iter % 3000 == 0)
		{
			printf("Current time: %d\n", cur_time);
			printf("Current parts:");
			writeParts(data_parts);
		}
		process(*events.begin());
		events.erase(*events.begin());
	}
	total_time = cur_time;
	averageNumberOfParts /= cur_time;
	puts("________________________________________________________________________________________________________");
	puts("During whole process: ");
	printf("Max number of alive parts was at %d second with %d parts\n", maxCountMoment, (int) maxCount.size());
	writeParts(maxCount);
	printf("Max total size of merging parts was at %d second with %d rows in merge\n", maxMergingMoment, (int)getMergeSize(maxMerging));
	writeParts(maxMerging);
	printf("Max number of running threads was at %d second with %d threads\n", maxThreadsMoment, (int)getThreads(maxThreads));
	writeParts(maxThreads);
	printf("Max number of scheduled threads was at %d second with %d threads\n", maxScheduledThreadsMoment, maxScheduledThreads);
	printf("Total merge time %lld sec\n", totalMergeTime);
	printf("Total time %d sec\n", total_time);
	printf("Total parts size %lld\n", totalSize);
	printf("Total merged Rows / total rows %0.5lf \n", 1.0 * totalMergeTime * RowsPerSec / totalSize);
	printf("Average number of data parts is %0.5lf\n", averageNumberOfParts);
	puts("________________________________________________________________________________________________________");
	puts("Result configuration:\n");
	writeParts(data_parts);


	return 0;
}
