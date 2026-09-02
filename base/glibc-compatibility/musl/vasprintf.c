#define _GNU_SOURCE
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>

int vasprintf(char **s, const char *fmt, va_list ap)
{
	va_list ap2;
	va_copy(ap2, ap);
	int l = vsnprintf(0, 0, fmt, ap2);
	va_end(ap2);

	if (l<0 || !(*s=malloc(l+1U))) return -1;
	return vsnprintf(*s, l+1U, fmt, ap);
}
