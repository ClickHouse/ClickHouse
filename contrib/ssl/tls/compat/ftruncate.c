/*
 * Public domain
 *
 * Kinichiro Inoguchi <inoguchi@openbsd.org>
 */

#include <unistd.h>

int
ftruncate(int fd, off_t length)
{
	return _chsize(fd, length);
}
