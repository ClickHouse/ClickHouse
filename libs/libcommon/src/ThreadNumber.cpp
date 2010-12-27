#include <pthread.h>
#include <Yandex/optimization.h>

#include <Yandex/ThreadNumber.h>


static __thread unsigned thread_number = 0;
static unsigned threads = 0;

unsigned ThreadNumber::get()
{
	static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
	if (unlikely(!thread_number))
	{
		pthread_mutex_lock(&mutex);
		if (likely(!thread_number))
			thread_number = ++threads;
		pthread_mutex_unlock(&mutex);
	}
	return thread_number;
}
