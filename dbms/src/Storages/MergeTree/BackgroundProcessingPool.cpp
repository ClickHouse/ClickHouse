#include <DB/Common/Exception.h>
#include <DB/Core/ErrorCodes.h>
#include <DB/Common/setThreadName.h>
#include <DB/IO/WriteHelpers.h>
#include <common/logger_useful.h>
#include <DB/Storages/MergeTree/BackgroundProcessingPool.h>


namespace DB
{


constexpr double BackgroundProcessingPool::sleep_seconds;
constexpr double BackgroundProcessingPool::sleep_seconds_random_part;


void BackgroundProcessingPool::TaskInfo::wake()
{
	if (removed)
		return;

	std::unique_lock<std::mutex> lock(pool.mutex);
	pool.tasks.splice(pool.tasks.begin(), pool.tasks, iterator);

	/// Если эта задача в прошлый раз ничего не сделала, и ей было назначено спать, то отменим время сна.
	time_t current_time = time(0);
	if (next_time_to_execute > current_time)
		next_time_to_execute = current_time;

	/// Если все потоки сейчас выполняют работу, этот вызов никого не разбудит.
	pool.wake_event.notify_one();
}


BackgroundProcessingPool::BackgroundProcessingPool(int size_) : size(size_)
{
	LOG_INFO(&Logger::get("BackgroundProcessingPool"), "Create BackgroundProcessingPool with " << size << " threads");

	threads.resize(size);
	for (auto & thread : threads)
			thread = std::thread([this] { threadFunction(); });
}


int BackgroundProcessingPool::getCounter(const String & name)
{
	std::unique_lock<std::mutex> lock(mutex);
	return counters[name];
}

BackgroundProcessingPool::TaskHandle BackgroundProcessingPool::addTask(const Task & task)
{
	TaskHandle res(new TaskInfo(*this, task));

	{
		std::unique_lock<std::mutex> lock(mutex);
		res->iterator = tasks.insert(tasks.begin(), res);
	}

	wake_event.notify_all();

	return res;
}

void BackgroundProcessingPool::removeTask(const TaskHandle & task)
{
	task->removed = true;

	/// Дождёмся завершения всех выполнений этой задачи.
	{
		Poco::ScopedWriteRWLock wlock(task->rwlock);
	}

	{
		std::unique_lock<std::mutex> lock(mutex);
		tasks.erase(task->iterator);
	}
}

BackgroundProcessingPool::~BackgroundProcessingPool()
{
	try
	{
		shutdown = true;
		wake_event.notify_all();
		for (std::thread & thread : threads)
			thread.join();
	}
	catch (...)
	{
		tryLogCurrentException(__PRETTY_FUNCTION__);
	}
}


void BackgroundProcessingPool::threadFunction()
{
	setThreadName("BackgrProcPool");

	std::mt19937 rng(reinterpret_cast<intptr_t>(&rng));
	std::this_thread::sleep_for(std::chrono::duration<double>(std::uniform_real_distribution<double>(0, sleep_seconds_random_part)(rng)));

	while (!shutdown)
	{
		Counters counters_diff;
		bool has_exception = false;

		try
		{
			TaskHandle task;
			time_t min_time = std::numeric_limits<time_t>::max();

			{
				std::unique_lock<std::mutex> lock(mutex);

				if (!tasks.empty())
				{
					/// O(n), n - число задач. По сути, количество таблиц. Обычно их мало.
					for (const auto & handle : tasks)
					{
						if (handle->removed)
							continue;

						time_t next_time_to_execute = handle->next_time_to_execute;

						if (next_time_to_execute < min_time)
						{
							min_time = next_time_to_execute;
							task = handle;
						}
					}

					if (task)	/// Переложим в конец очереди (уменьшим приоритет среди задач с одинаковым next_time_to_execute).
						tasks.splice(tasks.end(), tasks, task->iterator);
				}
			}

			if (shutdown)
				break;

			if (!task)
			{
				std::unique_lock<std::mutex> lock(mutex);
				wake_event.wait_for(lock,
					std::chrono::duration<double>(sleep_seconds
						+ std::uniform_real_distribution<double>(0, sleep_seconds_random_part)(rng)));
				continue;
			}

			/// Лучшей задачи не нашлось, а эта задача в прошлый раз ничего не сделала, и поэтому ей назначено некоторое время спать.
			time_t current_time = time(0);
			if (min_time > current_time)
			{
				std::unique_lock<std::mutex> lock(mutex);
				wake_event.wait_for(lock, std::chrono::duration<double>(
					min_time - current_time + std::uniform_real_distribution<double>(0, sleep_seconds_random_part)(rng)));
			}

			Poco::ScopedReadRWLock rlock(task->rwlock);

			if (task->removed)
				continue;

			Context context(*this, counters_diff);
			bool done_work = task->function(context);

			/// Если задача сделала полезную работу, то она сможет выполняться в следующий раз хоть сразу.
			/// Если нет - добавляем задержку перед повторным исполнением.
			task->next_time_to_execute = time(0) + (done_work ? 0 : sleep_seconds);
		}
		catch (...)
		{
			has_exception = true;
			tryLogCurrentException(__PRETTY_FUNCTION__);
		}

		/// Вычтем все счётчики обратно.
		if (!counters_diff.empty())
		{
			std::unique_lock<std::mutex> lock(mutex);
			for (const auto & it : counters_diff)
				counters[it.first] -= it.second;
		}

		if (shutdown)
			break;

		if (has_exception)
		{
			std::unique_lock<std::mutex> lock(mutex);
			wake_event.wait_for(lock, std::chrono::duration<double>(sleep_seconds));
		}
	}
}

}
