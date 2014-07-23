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

namespace DB
{

/** В нескольких потоках в бесконечном цикле выполняет указанные функции.
  */
class BackgroundProcessingPool
{
public:
	typedef std::map<String, int> Counters;

	/** Используется изнутри таски. Позволяет инкрементировать какие-нибудь счетчики.
	  * После завершения таски, все изменения откатятся.
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
			std::unique_lock<std::mutex> lock(pool.mutex);
			pool.tasks.splice(pool.tasks.begin(), pool.tasks, iterator);

			/// Не очень надежно: если все потоки сейчас выполняют работу, этот вызов никого не разбудит,
			///  и все будут спать в конце итерации.
			pool.wake_event.notify_one();
		}

	private:
		friend class BackgroundProcessingPool;

		BackgroundProcessingPool & pool;
		Task function;
		Poco::RWLock lock;
		volatile bool removed;
		std::list<std::shared_ptr<TaskInfo>>::iterator iterator;

		TaskInfo(BackgroundProcessingPool & pool_, const Task & function_) : pool(pool_), function(function_), removed(false) {}
	};

	typedef std::shared_ptr<TaskInfo> TaskHandle;


	BackgroundProcessingPool(int size_) : size(size_), sleep_seconds(10), shutdown(false) {}

	void setNumberOfThreads(int size_)
	{
		if (size_ <= 0)
			throw Exception("Invalid number of threads: " + toString(size_), ErrorCodes::ARGUMENT_OUT_OF_BOUND);

		std::unique_lock<std::mutex> tlock(threads_mutex);
		std::unique_lock<std::mutex> lock(mutex);

		if (size_ == size)
			return;

		if (threads.empty())
		{
			size = size_;
			return;
		}

		throw Exception("setNumberOfThreads is not implemented for non-empty pool", ErrorCodes::NOT_IMPLEMENTED);
	}

	int getNumberOfThreads()
	{
		std::unique_lock<std::mutex> lock(mutex);
		return size;
	}

	void setSleepTime(double seconds)
	{
		std::unique_lock<std::mutex> lock(mutex);
		sleep_seconds = seconds;
	}

	int getCounter(const String & name)
	{
		std::unique_lock<std::mutex> lock(mutex);
		return counters[name];
	}

	TaskHandle addTask(const Task & task)
	{
		std::unique_lock<std::mutex> lock(threads_mutex);

		TaskHandle res(new TaskInfo(*this, task));

		{
			std::unique_lock<std::mutex> lock(mutex);
			tasks.push_back(res);
			res->iterator = --tasks.end();
		}

		if (threads.empty())
		{
			shutdown = false;
			counters.clear();
			threads.resize(size);
			for (std::thread & thread : threads)
				 thread = std::thread(std::bind(&BackgroundProcessingPool::threadFunction, this));
		}

		return res;
	}

	void removeTask(const TaskHandle & task)
	{
		std::unique_lock<std::mutex> tlock(threads_mutex);

		/// Дождемся завершения всех выполнений этой задачи.
		{
			Poco::ScopedWriteRWLock wlock(task->lock);
			task->removed = true;
		}

		{
			std::unique_lock<std::mutex> lock(mutex);
			auto it = std::find(tasks.begin(), tasks.end(), task);
			if (it == tasks.end())
				throw Exception("Task not found", ErrorCodes::LOGICAL_ERROR);
			tasks.erase(it);
		}

		if (tasks.empty())
		{
			shutdown = true;
			wake_event.notify_all();
			for (std::thread & thread : threads)
				thread.join();
			threads.clear();
			counters.clear();
		}
	}

	~BackgroundProcessingPool()
	{
		try
		{
			std::unique_lock<std::mutex> lock(threads_mutex);
			if (!threads.empty())
			{
				LOG_ERROR(&Logger::get("~BackgroundProcessingPool"), "Destroying non-empty BackgroundProcessingPool");
				shutdown = true;
				wake_event.notify_all();
				for (std::thread & thread : threads)
					thread.join();
			}
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);
		}
	}

private:
	typedef std::list<TaskHandle> Tasks;
	typedef std::vector<std::thread> Threads;

	std::mutex threads_mutex;
	std::mutex mutex;
	int size;
	Tasks tasks; /// Таски в порядке, в котором мы планируем их выполнять.
	Threads threads;
	Counters counters;
	double sleep_seconds;

	volatile bool shutdown;
	std::condition_variable wake_event;

	void threadFunction()
	{
		while (!shutdown)
		{
			Counters counters_diff;
			bool need_sleep = false;
			size_t tasks_count = 1;

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
					std::this_thread::sleep_for(std::chrono::duration<double>(sleep_seconds));
					continue;
				}

				Poco::ScopedReadRWLock rlock(task->lock);
				if (task->removed)
					continue;

				Context context(*this, counters_diff);

				if (task->function(context))
				{
					/// Если у таска получилось выполнить какую-то работу, запустим его снова без паузы.
					std::unique_lock<std::mutex> lock(mutex);

					auto it = std::find(tasks.begin(), tasks.end(), task);
					if (it != tasks.end())
					{
						need_sleep = false;
						tasks.splice(tasks.begin(), tasks, it);
					}
				}
			}
			catch (...)
			{
				need_sleep = true;
				tryLogCurrentException(__PRETTY_FUNCTION__);
			}

			/// Вычтем все счетчики обратно.
			if (!counters_diff.empty())
			{
				std::unique_lock<std::mutex> lock(mutex);
				for (const auto & it : counters_diff)
				{
					counters[it.first] -= it.second;
				}
			}

			if (shutdown)
				break;

			if (need_sleep)
			{
				std::unique_lock<std::mutex> lock(mutex);
				wake_event.wait_for(lock, std::chrono::duration<double>(sleep_seconds / tasks_count));
			}
		}
	}
};

typedef Poco::SharedPtr<BackgroundProcessingPool> BackgroundProcessingPoolPtr;

}
