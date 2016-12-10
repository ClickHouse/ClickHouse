#pragma once

#include <signal.h>
#include <DB/Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int CANNOT_MANIPULATE_SIGSET;
	extern const int CANNOT_WAIT_FOR_SIGNAL;
	extern const int CANNOT_BLOCK_SIGNAL;
	extern const int CANNOT_UNBLOCK_SIGNAL;
}

#ifdef __APPLE__
// We only need to support timeout = {0, 0} at this moment
static int sigtimedwait(const sigset_t *set, siginfo_t *info, const struct timespec *timeout) {
	sigset_t pending;
	int signo;
	sigpending(&pending);

	for (signo = 1; signo < NSIG; signo++) {
		if (sigismember(set, signo) && sigismember(&pending, signo)) {
			sigwait(set, &signo);
			if (info) {
				memset(info, 0, sizeof *info);
				info->si_signo = signo;
			}
			return signo;
		}
	}
	errno = EAGAIN;

	return -1;
}
#endif


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
