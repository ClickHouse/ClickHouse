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
  * Предназначена для задач, выполняющих постоянную фоновую работу (например, слияния).
  * Задача - функция, возвращающая bool - сделала ли она какую-либо работу.
  * Если сделала - надо выполнить ещё раз. Если нет - надо подождать несколько секунд, или до события wake, и выполнить ещё раз.
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
		/// Переставить таск в начало очереди и разбудить какой-нибудь поток.
		void wake()
		{
			Poco::ScopedReadRWLock rlock(rwlock);
			if (removed)
				return;

			std::unique_lock<std::mutex> lock(pool.mutex);
			pool.tasks.splice(pool.tasks.begin(), pool.tasks, iterator);

			/// Не очень надёжно: если все потоки сейчас выполняют работу, этот вызов никого не разбудит,
			///  и все будут спать в конце итерации.
			pool.wake_event.notify_one();
		}

	private:
		friend class BackgroundProcessingPool;

		BackgroundProcessingPool & pool;
		Task function;

		/// При выполнении задачи, держится read lock. Переменная removed меняется под write lock-ом.
		Poco::RWLock rwlock;
		volatile bool removed = false;

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
			tasks.push_back(res);
			res->iterator = --tasks.end();
		}

		wake_event.notify_all();

		return res;
	}

	void removeTask(const TaskHandle & task)
	{
		/// Дождёмся завершения всех выполнений этой задачи.
		{
			Poco::ScopedWriteRWLock wlock(task->rwlock);
			task->removed = true;
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
			bool need_sleep = false;

			try
			{
				TaskHandle task;

				{
					std::unique_lock<std::mutex> lock(mutex);

					if (!tasks.empty())
					{
						need_sleep = true;
						task = tasks.front();
						tasks.splice(tasks.end(), tasks, tasks.begin());
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

				Poco::ScopedReadRWLock rlock(task->rwlock);
				if (task->removed)
					continue;

				Context context(*this, counters_diff);

				if (task->function(context))
				{
					/// Если у задачи получилось выполнить какую-то работу, запустим её снова без паузы.
					need_sleep = false;

					std::unique_lock<std::mutex> lock(mutex);
					tasks.splice(tasks.begin(), tasks, task->iterator);
				}
			}
			catch (...)
			{
				need_sleep = true;
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

			if (need_sleep)
			{
				std::unique_lock<std::mutex> lock(mutex);
				wake_event.wait_for(lock, std::chrono::duration<double>(sleep_seconds));
			}
		}
	}
};

typedef Poco::SharedPtr<BackgroundProcessingPool> BackgroundProcessingPoolPtr;

}
