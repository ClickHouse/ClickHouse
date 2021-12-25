#include <sys/socket.h>
#include "syscall.h"

int listen(int fd, int backlog)
{
	return socketcall(listen, fd, backlog, 0, 0, 0, 0);
}
