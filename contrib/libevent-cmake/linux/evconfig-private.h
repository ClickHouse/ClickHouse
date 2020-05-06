
#ifndef EVCONFIG_PRIVATE_H_INCLUDED_
#define EVCONFIG_PRIVATE_H_INCLUDED_

/* Enable extensions on AIX 3, Interix.  */
/* #undef _ALL_SOURCE */

/* Enable GNU extensions on systems that have them.  */
#define _GNU_SOURCE 1

/* Enable threading extensions on Solaris.  */
/* #undef _POSIX_PTHREAD_SEMANTICS */

/* Enable extensions on HP NonStop.  */
/* #undef _TANDEM_SOURCE */

/* Enable general extensions on Solaris.  */
/* #undef __EXTENSIONS__ */

/* Number of bits in a file offset, on hosts where this is settable. */
/* #undef _FILE_OFFSET_BITS */
/* Define for large files, on AIX-style hosts. */
/* #undef _LARGE_FILES */

/* Define to 1 if on MINIX. */
/* #undef _MINIX */

/* Define to 2 if the system does not provide POSIX.1 features except with
   this defined. */
/* #undef _POSIX_1_SOURCE */

/* Define to 1 if you need to in order for `stat' and other things to work. */
/* #undef _POSIX_SOURCE */

/* Enable POSIX.2 extensions on QNX for getopt */
#ifdef __QNX__
/* #undef __EXT_POSIX2 */
#endif

#endif
