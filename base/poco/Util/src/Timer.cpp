//
// Timer.cpp
//
// Library: Util
// Package: Timer
// Module:  Timer
//
// Copyright (c) 2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Util/Timer.h"
#include "Poco/Notification.h"
#include "Poco/ErrorHandler.h"
#include "Poco/Event.h"


using Poco::ErrorHandler;


namespace Poco {
namespace Util {


class TimerNotification: public Poco::Notification
{
public:
	TimerNotification(Poco::TimedNotificationQueue& queue):
		_queue(queue)
	{
	}

	~TimerNotification()
	{
	}

	virtual bool execute() = 0;

	Poco::TimedNotificationQueue& queue()
	{
		return _queue;
	}

private:
	Poco::TimedNotificationQueue& _queue;
};


class StopNotification: public TimerNotification
{
public:
	StopNotification(Poco::TimedNotificationQueue& queue):
		TimerNotification(queue)
	{
	}

	~StopNotification()
	{
	}

	bool execute()
	{
		queue().clear();
		return false;
	}
};


class CancelNotification: public TimerNotification
{
public:
	CancelNotification(Poco::TimedNotificationQueue& queue):
		TimerNotification(queue)
	{
	}

	~CancelNotification()
	{
	}

	bool execute()
	{
		// Check if there's a StopNotification pending.
		Poco::AutoPtr<TimerNotification> pNf = static_cast<TimerNotification*>(queue().dequeueNotification());
		while (pNf)
		{
			if (pNf.cast<StopNotification>())
			{
				queue().clear();
				_finished.set();
				return false;
			}
			pNf = static_cast<TimerNotification*>(queue().dequeueNotification());
		}

		queue().clear();
		_finished.set();
		return true;
	}

	void wait()
	{
		_finished.wait();
	}

private:
	Poco::Event _finished;
};


class TaskNotification: public TimerNotification
{
public:
	TaskNotification(Poco::TimedNotificationQueue& queue, TimerTask::Ptr pTask):
		TimerNotification(queue),
		_pTask(pTask)
	{
	}

	~TaskNotification()
	{
	}

	TimerTask::Ptr task()
	{
		return _pTask;
	}

	bool execute()
	{
		if (!_pTask->isCancelled())
		{
			try
			{
				_pTask->_lastExecution.update();
				_pTask->run();
			}
			catch (Exception& exc)
			{
				ErrorHandler::handle(exc);
			}
			catch (std::exception& exc)
			{
				ErrorHandler::handle(exc);
			}
			catch (...)
			{
				ErrorHandler::handle();
			}
		}
		return true;
	}

private:
	TimerTask::Ptr _pTask;
};


class PeriodicTaskNotification: public TaskNotification
{
public:
	PeriodicTaskNotification(Poco::TimedNotificationQueue& queue, TimerTask::Ptr pTask, long interval):
		TaskNotification(queue, pTask),
		_interval(interval)
	{
	}

	~PeriodicTaskNotification()
	{
	}

	bool execute()
	{
		TaskNotification::execute();

		if (!task()->isCancelled())
		{
			Poco::Clock now;
			Poco::Clock nextExecution;
			nextExecution += static_cast<Poco::Clock::ClockDiff>(_interval)*1000;
			if (nextExecution < now) nextExecution = now;
			queue().enqueueNotification(this, nextExecution);
			duplicate();
		}
		return true;
	}

private:
	long _interval;
};


class FixedRateTaskNotification: public TaskNotification
{
public:
	FixedRateTaskNotification(Poco::TimedNotificationQueue& queue, TimerTask::Ptr pTask, long interval, Poco::Clock clock):
		TaskNotification(queue, pTask),
		_interval(interval),
		_nextExecution(clock)
	{
	}

	~FixedRateTaskNotification()
	{
	}

	bool execute()
	{
		TaskNotification::execute();

		if (!task()->isCancelled())
		{
			Poco::Clock now;
			_nextExecution += static_cast<Poco::Clock::ClockDiff>(_interval)*1000;
			if (_nextExecution < now) _nextExecution = now;
			queue().enqueueNotification(this, _nextExecution);
			duplicate();
		}
		return true;
	}

private:
	long _interval;
	Poco::Clock _nextExecution;
};


Timer::Timer()
{
	_thread.start(*this);
}


Timer::Timer(Poco::Thread::Priority priority)
{
	_thread.setPriority(priority);
	_thread.start(*this);
}


Timer::~Timer()
{
	try
	{
		_queue.enqueueNotification(new StopNotification(_queue), Poco::Clock(0));
		_thread.join();
	}
	catch (...)
	{
		poco_unexpected();
	}
}


void Timer::cancel(bool wait)
{
	Poco::AutoPtr<CancelNotification> pNf = new CancelNotification(_queue);
	_queue.enqueueNotification(pNf, Poco::Clock(0));
	if (wait)
	{
		pNf->wait();
	}
}


void Timer::schedule(TimerTask::Ptr pTask, Poco::Timestamp time)
{
	validateTask(pTask);
	_queue.enqueueNotification(new TaskNotification(_queue, pTask), time);
}


void Timer::schedule(TimerTask::Ptr pTask, Poco::Clock clock)
{
	validateTask(pTask);
	_queue.enqueueNotification(new TaskNotification(_queue, pTask), clock);
}


void Timer::schedule(TimerTask::Ptr pTask, long delay, long interval)
{
	Poco::Clock clock;
	clock += static_cast<Poco::Clock::ClockDiff>(delay)*1000;
	schedule(pTask, clock, interval);
}


void Timer::schedule(TimerTask::Ptr pTask, Poco::Timestamp time, long interval)
{
	validateTask(pTask);
	_queue.enqueueNotification(new PeriodicTaskNotification(_queue, pTask, interval), time);
}


void Timer::schedule(TimerTask::Ptr pTask, Poco::Clock clock, long interval)
{
	validateTask(pTask);
	_queue.enqueueNotification(new PeriodicTaskNotification(_queue, pTask, interval), clock);
}


void Timer::scheduleAtFixedRate(TimerTask::Ptr pTask, long delay, long interval)
{
	Poco::Clock clock;
	clock += static_cast<Poco::Clock::ClockDiff>(delay)*1000;
	scheduleAtFixedRate(pTask, clock, interval);
}


void Timer::scheduleAtFixedRate(TimerTask::Ptr pTask, Poco::Timestamp time, long interval)
{
	validateTask(pTask);
	Poco::Timestamp tsNow;
	Poco::Clock clock;
	Poco::Timestamp::TimeDiff diff = time - tsNow;
	clock += diff;
	_queue.enqueueNotification(new FixedRateTaskNotification(_queue, pTask, interval, clock), clock);
}


void Timer::scheduleAtFixedRate(TimerTask::Ptr pTask, Poco::Clock clock, long interval)
{
	validateTask(pTask);
	_queue.enqueueNotification(new FixedRateTaskNotification(_queue, pTask, interval, clock), clock);
}


void Timer::run()
{
	bool cont = true;
	while (cont)
	{
		Poco::AutoPtr<TimerNotification> pNf = static_cast<TimerNotification*>(_queue.waitDequeueNotification());
		cont = pNf->execute();
	}
}


void Timer::validateTask(const TimerTask::Ptr& pTask)
{
	if (pTask->isCancelled())
	{
		throw Poco::IllegalStateException("A cancelled task must not be rescheduled");
	}
}


} } // namespace Poco::Util
