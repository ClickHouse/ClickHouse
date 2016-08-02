#pragma once

#include <cstdint>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <queue>
#include <vector>


/** Very simple thread pool similar to boost::threadpool.
  * Advantages:
  * - catches exceptions and rethrows on wait.
  */

class ThreadPool
{
private:
	using Job = std::function<void()>;

public:
	ThreadPool(size_t m_size)
		: m_size(m_size)
	{
		threads.reserve(m_size);
		for (size_t i = 0; i < m_size; ++i)
			threads.emplace_back([this] { worker(); });
	}

	void schedule(Job job)
	{
		{
			std::unique_lock<std::mutex> lock(mutex);
			has_free_thread.wait(lock, [this] { return active_jobs < m_size; });
			if (shutdown)
				return;

			jobs.push(std::move(job));
			++active_jobs;
		}
		has_new_job_or_shutdown.notify_one();
	}

	void wait()
	{
		{
			std::unique_lock<std::mutex> lock(mutex);
			has_free_thread.wait(lock, [this] { return active_jobs == 0; });

			if (!exceptions.empty())
				std::rethrow_exception(exceptions.front());
		}
	}

	~ThreadPool()
	{
		{
			std::unique_lock<std::mutex> lock(mutex);
			shutdown = true;
		}

		has_new_job_or_shutdown.notify_all();

		for (auto & thread : threads)
			thread.join();
	}

	size_t size() const { return m_size; }

	size_t active() const
	{
		std::unique_lock<std::mutex> lock(mutex);
		return active_jobs;
	}

private:
	mutable std::mutex mutex;
	std::condition_variable has_free_thread;
	std::condition_variable has_new_job_or_shutdown;

	const size_t m_size;
	size_t active_jobs = 0;
	bool shutdown = false;

	std::queue<Job> jobs;
	std::vector<std::thread> threads;
	std::vector<std::exception_ptr> exceptions;		/// NOTE Saving many exceptions but rethrow just first one.


	void worker()
	{
		while (true)
		{
			Job job;

			{
				std::unique_lock<std::mutex> lock(mutex);
				has_new_job_or_shutdown.wait(lock, [this] { return shutdown || !jobs.empty(); });

				if (!shutdown)
				{
					job = std::move(jobs.front());
					jobs.pop();
				}
			}

			if (!job)
				return;	/// shutdown

			try
			{
				job();
			}
			catch (...)
			{
				{
					std::unique_lock<std::mutex> lock(mutex);
					exceptions.push_back(std::current_exception());
					shutdown = true;
					--active_jobs;
				}
				has_free_thread.notify_one();
				has_new_job_or_shutdown.notify_all();
				return;
			}

			{
				std::unique_lock<std::mutex> lock(mutex);
				--active_jobs;
			}

			has_free_thread.notify_one();
		}
	}
};

