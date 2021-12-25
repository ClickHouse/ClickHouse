#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

in_addr_t inet_addr(const char *p)
{
	struct in_addr a;
	if (!__inet_aton(p, &a)) return -1;
	return a.s_addr;
}
