#include <netdb.h>
#include "pthread_impl.h"

#undef h_errno
int h_errno;

int *__h_errno_location(void)
{
	if (!__pthread_self()->stack) return &h_errno;
	return &__pthread_self()->h_errno_val;
}
