#define _GNU_SOURCE

#include <sys/socket.h>
#include <netdb.h>
#include <string.h>
#include <netinet/in.h>

struct hostent *gethostbyname(const char *name)
{
	return gethostbyname2(name, AF_INET);
}
