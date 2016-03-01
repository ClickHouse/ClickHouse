#pragma once

#include <DB/Common/Exception.h>
#include <functional>
#include <memory>
#include <boost/thread/barrier.hpp>

namespace DB
{

class RemoteBlockInputStream;

namespace ErrorCodes
{
	extern const int LOGICAL_ERROR;
}

namespace ClusterProxy
{

struct PreSendHook
{
public:
	using PreProcess = std::function<void(const RemoteBlockInputStream *)>;
	using PostProcess = std::function<void()>;
	using Callback = PreProcess;

public:
	PreSendHook() = default;

	PreSendHook(PreProcess pre_process_)
		: pre_process(pre_process_), is_initialized(pre_process)
	{
	}

	PreSendHook(PreProcess pre_process_, PostProcess post_process_)
		: pre_process(pre_process_), post_process(post_process_),
		is_initialized(pre_process_)
	{
	}

	operator bool() const
	{
		return is_initialized;
	}

	void setupBarrier(size_t count)
	{
		if (!is_initialized)
			throw Exception("PreSendHook: uninitialized object", ErrorCodes::LOGICAL_ERROR);
		if (barrier)
			throw Exception("PreSendHook: barrier already set up", ErrorCodes::LOGICAL_ERROR);

		barrier = std::make_shared<boost::barrier>(count);
	}

	Callback makeCallback()
	{
		if (!is_initialized)
			throw Exception("PreSendHook: uninitialized object", ErrorCodes::LOGICAL_ERROR);

		auto callback = [=](const RemoteBlockInputStream * remote_stream)
		{
			pre_process(remote_stream);
			if (barrier)
			{
				barrier->count_down_and_wait();
				if (post_process)
					post_process();
			}
		};

		return callback;
	}

private:
	PreProcess pre_process;
	PostProcess post_process;
	std::shared_ptr<boost::barrier> barrier;
	bool is_initialized = false;
};

}

}
