#pragma once

#include <thread>
#include <set>
#include <map>
#include <list>
#include <condition_variable>
#include <Poco/Mutex.h>
#include <Poco/RWLock.h>
#include <Poco/Event.h>
#include <DB/Core/Types.h>
#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>
#include <DB/IO/WriteHelpers.h>
#include <Yandex/logger_useful.h>

namespace DB
{

/** Используя фиксированное количество потоков, выполнять произвольное количество задач в бесконечном цикле.
  * При этом, одна задача может выполняться одновременно из разных потоков.
  * Предназначена для задач, выполняющих постоянную фоновую работу (например, слияния).
  * Задача - функция, возвращающая bool - сделала ли она какую-либо работу.
  * Если не сделала, то в следующий раз будет выполнена позже.
  *
  * Также, задача во время выполнения может временно увеличить какой-либо счётчик, относящийся ко всем задачам
  *  - например, число одновременно идующих слияний.
  */
class BackgroundProcessingPool
{
public:
	typedef std::map<String, int> Counters;

	/** Используется изнутри задачи. Позволяет инкрементировать какие-нибудь счетчики.
	  * После завершения задачи, все изменения откатятся.
	  * Например, чтобы можно было узнавать количество потоков, выполняющих большое слияние,
	  *  можно в таске, выполняющей большое слияние, инкрементировать счетчик. Декрементировать обратно его не нужно.
	  */
	class Context
	{
	public:
		void incrementCounter(const String & name, int value = 1)
		{
			std::unique_lock<std::mutex> lock(pool.mutex);
			local_counters[name] += value;
			pool.counters[name] += value;
		}

	private:
		friend class BackgroundProcessingPool;

		Context(BackgroundProcessingPool & pool_, Counters & local_counters_) : pool(pool_), local_counters(local_counters_) {}

		BackgroundProcessingPool & pool;
		Counters & local_counters;
	};

	/// Возвращает true, если что-то получилось сделать. В таком случае поток не будет спать перед следующим вызовом.
	typedef std::function<bool (Context & context)> Task;


	class TaskInfo
	{
	public:
		/// Разбудить какой-нибудь поток.
		void wake()
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

	private:
		friend class BackgroundProcessingPool;

		BackgroundProcessingPool & pool;
		Task function;

		/// При выполнении задачи, держится read lock.
		Poco::RWLock rwlock;
		volatile bool removed = false;
		volatile time_t next_time_to_execute = 0;	/// Приоритет задачи. Для совпадающего времени в секундах берётся первая по списку задача.

		std::list<std::shared_ptr<TaskInfo>>::iterator iterator;

		TaskInfo(BackgroundProcessingPool & pool_, const Task & function_) : pool(pool_), function(function_) {}
	};

	typedef std::shared_ptr<TaskInfo> TaskHandle;


	BackgroundProcessingPool(int size_) : size(size_)
	{
		LOG_INFO(&Logger::get("BackgroundProcessingPool"), "Create BackgroundProcessingPool with " << size << " threads");

		threads.resize(size);
		for (auto & thread : threads)
			 thread = std::thread([this] { threadFunction(); });
	}


	size_t getNumberOfThreads() const
	{
		return size;
	}

	int getCounter(const String & name)
	{
		std::unique_lock<std::mutex> lock(mutex);
		return counters[name];
	}

	TaskHandle addTask(const Task & task)
	{
		TaskHandle res(new TaskInfo(*this, task));

		{
			std::unique_lock<std::mutex> lock(mutex);
			res->iterator = tasks.insert(tasks.begin(), res);
		}

		wake_event.notify_all();

		return res;
	}

	void removeTask(const TaskHandle & task)
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

	~BackgroundProcessingPool()
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

private:
	typedef std::list<TaskHandle> Tasks;
	typedef std::vector<std::thread> Threads;

	const size_t size;
	enum { sleep_seconds = 10 };

	Tasks tasks; 		/// Задачи в порядке, в котором мы планируем их выполнять.
	Counters counters;
	std::mutex mutex;	/// Для работы со списком tasks, а также с counters (когда threads не пустой).

	Threads threads;

	volatile bool shutdown = false;
	std::condition_variable wake_event;


	void threadFunction()
	{
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
					wake_event.wait_for(lock, std::chrono::duration<double>(sleep_seconds));
					continue;
				}

				if (task->removed)
					continue;

				/// Лучшей задачи не нашлось, а эта задача в прошлый раз ничего не сделала, и поэтому ей назначено некоторое время спать.
				time_t current_time = time(0);
				if (min_time > current_time)
				{
					std::unique_lock<std::mutex> lock(mutex);
					wake_event.wait_for(lock, std::chrono::duration<double>(min_time - current_time));
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
};

typedef Poco::SharedPtr<BackgroundProcessingPool> BackgroundProcessingPoolPtr;

}
