#include <netdb.h>

void sethostent(int x)
{
}

struct hostent *gethostent()
{
	return 0;
}

struct netent *getnetent()
{
	return 0;
}

void endhostent(void)
{
}

weak_alias(sethostent, setnetent);
weak_alias(endhostent, endnetent);
