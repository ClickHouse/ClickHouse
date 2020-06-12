/*
 * Public domain
 *
 * Kinichiro Inoguchi <inoguchi@openbsd.org>
 */

#include <unistd.h>

uid_t
getuid(void)
{
	/* Windows fstat sets 0 as st_uid */
	return 0;
}
