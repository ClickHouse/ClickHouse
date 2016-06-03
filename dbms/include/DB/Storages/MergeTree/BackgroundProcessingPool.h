#pragma once

#include <thread>
#include <set>
#include <map>
#include <list>
#include <condition_variable>
#include <Poco/Mutex.h>
#include <Poco/RWLock.h>
#include <Poco/Event.h>
#include <Poco/SharedPtr.h>
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
		void wake();

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
	typedef std::list<TaskHandle> Tasks;
	typedef std::vector<std::thread> Threads;

	const size_t size;
	static constexpr double sleep_seconds = 10;
	static constexpr double sleep_seconds_random_part = 1.0;

	Tasks tasks; 		/// Задачи в порядке, в котором мы планируем их выполнять.
	Counters counters;
	std::mutex mutex;	/// Для работы со списком tasks, а также с counters (когда threads не пустой).

	Threads threads;

	volatile bool shutdown = false;
	std::condition_variable wake_event;


	void threadFunction();
};

typedef Poco::SharedPtr<BackgroundProcessingPool> BackgroundProcessingPoolPtr;

}
