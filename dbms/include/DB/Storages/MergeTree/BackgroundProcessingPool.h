#pragma once

#include <thread>
#include <set>
#include <map>
#include <list>
#include <condition_variable>
#include <mutex>
#include <Poco/RWLock.h>
#include <Poco/Event.h>
#include <DB/Core/Types.h>

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
	using Counters = std::map<String, int>;

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
	using Task = std::function<bool (Context & context)>;


	class TaskInfo
	{
	public:
		/// Разбудить какой-нибудь поток.
		void wake();

	private:
		friend class BackgroundProcessingPool;

		BackgroundProcessingPool & pool;
		Task function;

		/// При выполнении задачи, держится read lock.
		Poco::RWLock rwlock;
		std::atomic<bool> removed {false};
		std::atomic<time_t> next_time_to_execute {0};	/// Приоритет задачи. Для совпадающего времени в секундах берётся первая по списку задача.

		std::list<std::shared_ptr<TaskInfo>>::iterator iterator;

		TaskInfo(BackgroundProcessingPool & pool_, const Task & function_) : pool(pool_), function(function_) {}
	};

	using TaskHandle = std::shared_ptr<TaskInfo>;


	BackgroundProcessingPool(int size_);

	size_t getNumberOfThreads() const
	{
		return size;
	}

	int getCounter(const String & name);

	TaskHandle addTask(const Task & task);
	void removeTask(const TaskHandle & task);

	~BackgroundProcessingPool();

private:
	using Tasks = std::list<TaskHandle>;
	using Threads = std::vector<std::thread>;

	const size_t size;
	static constexpr double sleep_seconds = 10;
	static constexpr double sleep_seconds_random_part = 1.0;

	Tasks tasks; 		/// Задачи в порядке, в котором мы планируем их выполнять.
	Counters counters;
	std::mutex mutex;	/// Для работы со списком tasks, а также с counters (когда threads не пустой).

	Threads threads;

	std::atomic<bool> shutdown {false};
	std::condition_variable wake_event;


	void threadFunction();
};

using BackgroundProcessingPoolPtr = std::shared_ptr<BackgroundProcessingPool>;

}
