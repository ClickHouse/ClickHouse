#include <sys/socket.h>
#include <sys/ioctl.h>

int sockatmark(int s)
{
	int ret;
	if (ioctl(s, SIOCATMARK, &ret) < 0)
		return -1;
	return ret;
}
