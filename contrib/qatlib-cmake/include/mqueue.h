/* This is a workaround for a build conflict issue
1. __GLIBC_PREREQ (referenced in OsalServices.c) is only defined in './sysroot/linux-x86_64/include/features.h'
2. mqueue.h only exist under './sysroot/linux-x86_64-musl/'
This cause target_include_directories for _osal has a conflict between './sysroot/linux-x86_64/include' and './sysroot/linux-x86_64-musl/'
hence create mqueue.h separately under ./qatlib-cmake/include as an alternative.
*/

/* Major and minor version number of the GNU C library package.  Use
   these macros to test for features in specific releases.  */
#define	__GLIBC__	2
#define	__GLIBC_MINOR__	27

#define __GLIBC_PREREQ(maj, min) \
	((__GLIBC__ << 16) + __GLIBC_MINOR__ >= ((maj) << 16) + (min))
