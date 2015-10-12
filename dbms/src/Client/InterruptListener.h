#pragma once

#include <signal.h>
#include <DB/Common/Exception.h>
#include <DB/Core/ErrorCodes.h>


namespace DB
{

/** Пока существует объект этого класса - блокирует сигнал INT, при этом позволяет узнать, не пришёл ли он.
  * Это нужно, чтобы можно было прервать выполнение запроса с помощью Ctrl+C.
  * В один момент времени используйте только один экземпляр этого класса.
  * Если метод check вернул true (пришёл сигнал), то следующие вызовы будут ждать следующий сигнал.
  */
class InterruptListener
{
private:
	bool active;
	sigset_t sig_set;

public:
	InterruptListener() : active(false)
	{
		if (sigemptyset(&sig_set)
			|| sigaddset(&sig_set, SIGINT))
			throwFromErrno("Cannot manipulate with signal set.", ErrorCodes::CANNOT_MANIPULATE_SIGSET);
		
		block();
	}

	~InterruptListener()
	{
		unblock();
	}

	bool check()
	{
		if (!active)
			return false;
		
		timespec timeout = { 0, 0 };
		
		if (-1 == sigtimedwait(&sig_set, nullptr, &timeout))
		{
			if (errno == EAGAIN)
				return false;
			else
				throwFromErrno("Cannot poll signal (sigtimedwait).", ErrorCodes::CANNOT_WAIT_FOR_SIGNAL);
		}

		return true;
	}

	void block()
	{
		if (!active)
		{
			if (pthread_sigmask(SIG_BLOCK, &sig_set, nullptr))
				throwFromErrno("Cannot block signal.", ErrorCodes::CANNOT_BLOCK_SIGNAL);

			active = true;
		}
	}

	/// Можно прекратить блокировать сигнал раньше, чем в деструкторе.
	void unblock()
	{
		if (active)
		{
			if (pthread_sigmask(SIG_UNBLOCK, &sig_set, nullptr))
				throwFromErrno("Cannot unblock signal.", ErrorCodes::CANNOT_UNBLOCK_SIGNAL);

			active = false;
		}
	}
};

}
