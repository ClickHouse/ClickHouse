#pragma once

#include <thread>
#include <set>
#include <map>
#include <Poco/Mutex.h>
#include <Poco/RWLock.h>
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
			Poco::ScopedLock<Poco::FastMutex> lock(pool.mutex);
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

	typedef std::shared_ptr<void> TaskHandle;

	BackgroundProcessingPool(int size_) : size(size_), sleep_seconds(10), shutdown(false) {}

	void setNumberOfThreads(int size_)
	{
		if (size_ <= 0)
			throw Exception("Invalid number of threads: " + toString(size_), ErrorCodes::ARGUMENT_OUT_OF_BOUND);

		Poco::ScopedLock<Poco::FastMutex> tlock(threads_mutex);
		Poco::ScopedLock<Poco::FastMutex> lock(mutex);

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
		Poco::ScopedLock<Poco::FastMutex> lock(mutex);
		return size;
	}

	void setSleepTime(double seconds)
	{
		Poco::ScopedLock<Poco::FastMutex> lock(mutex);
		sleep_seconds = seconds;
	}

	int getCounter(const String & name)
	{
		Poco::ScopedLock<Poco::FastMutex> lock(mutex);
		return counters[name];
	}

	TaskHandle addTask(const Task & task)
	{
		Poco::ScopedLock<Poco::FastMutex> lock(threads_mutex);

		TaskInfoPtr res = std::make_shared<TaskInfo>(task);

		{
			Poco::ScopedLock<Poco::FastMutex> lock(mutex);
			tasks.push_back(res);
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

	void removeTask(const TaskHandle & handle)
	{
		Poco::ScopedLock<Poco::FastMutex> tlock(threads_mutex);

		TaskInfoPtr task = std::static_pointer_cast<TaskInfo>(handle);

		/// Дождемся завершения всех выполнений этой задачи.
		{
			Poco::ScopedWriteRWLock wlock(task->lock);
			task->removed = true;
		}

		{
			Poco::ScopedLock<Poco::FastMutex> lock(mutex);
			auto it = std::find(tasks.begin(), tasks.end(), task);
			if (it == tasks.end())
				throw Exception("Task not found", ErrorCodes::LOGICAL_ERROR);
			tasks.erase(it);
		}

		if (tasks.empty())
		{
			shutdown = true;
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
			Poco::ScopedLock<Poco::FastMutex> lock(threads_mutex);
			if (!threads.empty())
			{
				LOG_ERROR(&Logger::get("~BackgroundProcessingPool"), "Destroying non-empty BackgroundProcessingPool");
				shutdown = true;
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
	struct TaskInfo
	{
		Task function;
		Poco::RWLock lock;
		volatile bool removed;

		TaskInfo(const Task & function_) : function(function_), removed(false) {}
	};

	typedef std::shared_ptr<TaskInfo> TaskInfoPtr;
	typedef std::vector<TaskInfoPtr> Tasks;
	typedef std::vector<std::thread> Threads;

	Poco::FastMutex threads_mutex;
	Poco::FastMutex mutex;
	int size;
	Tasks tasks;
	Threads threads;
	Counters counters;
	double sleep_seconds;
	bool shutdown;

	void threadFunction()
	{
		/// Начнем со случайной задачи (качество rand() не имеет значения).
		size_t i = static_cast<size_t>(rand());

		while (!shutdown)
		{
			Counters counters_diff;
			bool need_sleep = false;
			size_t tasks_count = 1;

			try
			{
				TaskInfoPtr task;

				{
					Poco::ScopedLock<Poco::FastMutex> lock(mutex);

					tasks_count = tasks.size();
					if (!tasks.empty())
					{
						need_sleep = true;
						i %= tasks_count;
						task = tasks[i];
						++i;
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
					/// Если у таска получилось выполнить какую-то работу, запустим его же снова без паузы.
					--i;
					need_sleep = false;
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
				Poco::ScopedLock<Poco::FastMutex> lock(mutex);
				for (const auto & it : counters_diff)
				{
					counters[it.first] -= it.second;
				}
			}

			if (shutdown)
				break;

			if (need_sleep)
			{
				std::this_thread::sleep_for(std::chrono::duration<double>(sleep_seconds / tasks_count));
			}
		}
	}
};

typedef Poco::SharedPtr<BackgroundProcessingPool> BackgroundProcessingPoolPtr;

}
