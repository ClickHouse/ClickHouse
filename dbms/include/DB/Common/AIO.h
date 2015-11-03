#pragma once

#include <DB/Common/Exception.h>
#include <common/singleton.h>
#include <boost/range/iterator_range.hpp>
#include <boost/noncopyable.hpp>
#include <future>
#include <mutex>
#include <map>
#include <linux/aio_abi.h>
#include <sys/syscall.h>
#include <unistd.h>


/** Небольшие обёртки для асинхронного ввода-вывода.
  */


inline int io_setup(unsigned nr, aio_context_t * ctxp)
{
	return syscall(__NR_io_setup, nr, ctxp);
}

inline int io_destroy(aio_context_t ctx)
{
	return syscall(__NR_io_destroy, ctx);
}

/// last argument is an array of pointers technically speaking
inline int io_submit(aio_context_t ctx, long nr, struct iocb * iocbpp[])
{
	return syscall(__NR_io_submit, ctx, nr, iocbpp);
}

inline int io_getevents(aio_context_t ctx, long min_nr, long max_nr, io_event *events, struct timespec * timeout)
{
	return syscall(__NR_io_getevents, ctx, min_nr, max_nr, events, timeout);
}


struct AIOContext : private boost::noncopyable
{
	aio_context_t ctx;

	AIOContext(unsigned int nr_events = 128)
	{
		ctx = 0;
		if (io_setup(nr_events, &ctx) < 0)
			DB::throwFromErrno("io_setup failed");
	}

	~AIOContext()
	{
		io_destroy(ctx);
	}
};

namespace DB
{


class AIOContextPool : public Singleton<AIOContextPool>
{
	friend class Singleton<AIOContextPool>;

	static const auto max_concurrent_events = 128;
	static const auto max_timeout_nsec = 1000;

	AIOContext aio_context{max_concurrent_events};

	std::size_t id{};
	mutable std::mutex mutex;
	std::map<std::size_t, std::promise<ssize_t>> promises;
	std::vector<iocb *> queued_requests;

	std::atomic<bool> cancelled{false};
	std::thread man_of_his_word{&AIOContextPool::fulfill_promises, this};

	~AIOContextPool()
	{
		cancelled.store(true, std::memory_order_relaxed);
		man_of_his_word.join();
	}

	void fulfill_promises()
	{
		/// array to hold completion events
		io_event events[max_concurrent_events] {};
		const auto p_events = &events[0];
		timespec timeout{0, max_timeout_nsec};

		/// continue checking for events unless cancelled
		while (!cancelled.load(std::memory_order_relaxed))
		{
			try
			{
				/// number of events signaling on
				auto num_events = 0;

				/// request 1 to `max_concurrent_events` events
				while ((num_events = io_getevents(aio_context.ctx, 1, max_concurrent_events, p_events, &timeout)) < 0)
					if (errno != EINTR)
						throwFromErrno("io_getevents: Failed to wait for asynchronous IO completion",
							ErrorCodes::AIO_COMPLETION_ERROR, errno);

				/// look at returned events and
				for (const auto & event : boost::make_iterator_range(p_events, p_events + num_events))
				{
					/// get id from event
					const auto id = event.data;

					/// find corresponding promise, set result and erase promise from map
					const std::lock_guard<std::mutex> lock{mutex};

					const auto it = promises.find(id);
					it->second.set_value(event.res);
					promises.erase(it);
				}

				if (queued_requests.empty())
					continue;

				const std::lock_guard<std::mutex> lock{mutex};
				auto num_requests = 0;

				/// submit a batch of requests
				while ((num_requests = io_submit(aio_context.ctx, queued_requests.size(), queued_requests.data())) < 0)
					if (!(errno == EINTR || errno == EAGAIN))
						throwFromErrno("io_submit: Failed to submit batch of " +
							std::to_string(queued_requests.size()) + " requests for asynchronous IO",
							ErrorCodes::AIO_SUBMIT_ERROR, errno);

				if (num_requests <= 0)
					continue;

				/// erase submitted requests
				queued_requests.erase(std::begin(queued_requests),
					std::next(std::begin(queued_requests), num_requests));
			}
			catch (...)
			{
				/// there was an error, log it, return to any client and continue
				const std::lock_guard<std::mutex> lock{mutex};

				const auto any_promise_it = std::begin(promises);
				any_promise_it->second.set_exception(std::current_exception());

				tryLogCurrentException("AIOContextPool::fulfill_promises()");
			}
		}
	}

public:
	std::future<ssize_t> post(struct iocb & iocb)
	{
		const std::lock_guard<std::mutex> lock{mutex};

		/// get current id and increment it by one
		const auto request_id = id++;

		/// create a promise and put request in "queue"
		promises.emplace(request_id, std::promise<ssize_t>{});
		/// store id in AIO request for further identification
		iocb.aio_data = request_id;
		queued_requests.push_back(&iocb);

		return promises[request_id].get_future();

	}
};


}
