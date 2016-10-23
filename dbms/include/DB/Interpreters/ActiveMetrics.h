#pragma once

#include <thread>
#include <mutex>
#include <condition_variable>


namespace DB
{

class Context;


/** Periodically (each minute, starting at 30 seconds offset)
  *  calculates and updates some metrics (see CurrentMetrics.h),
  *  that are not updated automatically (so, need to be actively calculated).
  */
class ActiveMetrics
{
public:
	ActiveMetrics(Context & context_)
		: context(context_), thread([this] { run(); })
	{
	}

	~ActiveMetrics();

private:
	Context & context;
	bool quit {false};
	std::mutex mutex;
	std::condition_variable cond;
	std::thread thread;

	void run();
	void update();
};

}
