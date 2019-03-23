#define _BSD_SOURCE
#include <unistd.h>
#include <sys/random.h>
#include <pthread.h>
#include <errno.h>

int getentropy(void *buffer, size_t len)
{
	int cs, ret;
	char *pos = buffer;

	if (len > 256) {
		errno = EIO;
		return -1;
	}

	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &cs);

	while (len) {
		ret = getrandom(pos, len, 0);
		if (ret < 0) {
			if (errno == EINTR) continue;
			else break;
		}
		pos += ret;
		len -= ret;
		ret = 0;
	}

	pthread_setcancelstate(cs, 0);

	return ret;
}
