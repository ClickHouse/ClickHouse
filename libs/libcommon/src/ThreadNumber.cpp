#include <Yandex/ThreadNumber.h>


__thread unsigned thread_number = 0;
unsigned threads = 0;

unsigned ThreadNumber::get()
{
	if (unlikely(!thread_number))
		thread_number = __sync_add_and_fetch(&threads, 1);
	return thread_number;
}
