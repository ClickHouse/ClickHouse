/***************************************************************************
 *                                  _   _ ____  _
 *  Project                     ___| | | |  _ \| |
 *                             / __| | | | |_) | |
 *                            | (__| |_| |  _ <| |___
 *                             \___|\___/|_| \_\_____|
 *
 * Copyright (C) 1998 - 2019, Daniel Stenberg, <daniel@haxx.se>, et al.
 *
 * This software is licensed as described in the file COPYING, which
 * you should have received as part of this distribution. The terms
 * are also available at https://curl.haxx.se/docs/copyright.html.
 *
 * You may opt to use, copy, modify, merge, publish, distribute and/or sell
 * copies of the Software, and permit persons to whom the Software is
 * furnished to do so, under the terms of the COPYING file.
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTY OF ANY
 * KIND, either express or implied.
 *
 ***************************************************************************/
#ifdef TIME_WITH_SYS_TIME
/* Time with sys/time test */

#include <sys/types.h>
#include <sys/time.h>
#include <time.h>

int
main ()
{
if ((struct tm *) 0)
return 0;
  ;
  return 0;
}

#endif

#ifdef HAVE_FCNTL_O_NONBLOCK

/* headers for FCNTL_O_NONBLOCK test */
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
/* */
#if defined(sun) || defined(__sun__) || \
    defined(__SUNPRO_C) || defined(__SUNPRO_CC)
# if defined(__SVR4) || defined(__srv4__)
#  define PLATFORM_SOLARIS
# else
#  define PLATFORM_SUNOS4
# endif
#endif
#if (defined(_AIX) || defined(__xlC__)) && !defined(_AIX41)
# define PLATFORM_AIX_V3
#endif
/* */
#if defined(PLATFORM_SUNOS4) || defined(PLATFORM_AIX_V3) || defined(__BEOS__)
#error "O_NONBLOCK does not work on this platform"
#endif

int
main ()
{
      /* O_NONBLOCK source test */
      int flags = 0;
      if(0 != fcntl(0, F_SETFL, flags | O_NONBLOCK))
          return 1;
      return 0;
}
#endif

/* tests for gethostbyaddr_r or gethostbyname_r */
#if defined(HAVE_GETHOSTBYADDR_R_5_REENTRANT) || \
    defined(HAVE_GETHOSTBYADDR_R_7_REENTRANT) || \
    defined(HAVE_GETHOSTBYADDR_R_8_REENTRANT) || \
    defined(HAVE_GETHOSTBYNAME_R_3_REENTRANT) || \
    defined(HAVE_GETHOSTBYNAME_R_5_REENTRANT) || \
    defined(HAVE_GETHOSTBYNAME_R_6_REENTRANT)
#   define _REENTRANT
    /* no idea whether _REENTRANT is always set, just invent a new flag */
#   define TEST_GETHOSTBYFOO_REENTRANT
#endif
#if defined(HAVE_GETHOSTBYADDR_R_5) || \
    defined(HAVE_GETHOSTBYADDR_R_7) || \
    defined(HAVE_GETHOSTBYADDR_R_8) || \
    defined(HAVE_GETHOSTBYNAME_R_3) || \
    defined(HAVE_GETHOSTBYNAME_R_5) || \
    defined(HAVE_GETHOSTBYNAME_R_6) || \
    defined(TEST_GETHOSTBYFOO_REENTRANT)
#include <sys/types.h>
#include <netdb.h>
int main(void)
{
  char *address = "example.com";
  int length = 0;
  int type = 0;
  struct hostent h;
  int rc = 0;
#if defined(HAVE_GETHOSTBYADDR_R_5) || \
    defined(HAVE_GETHOSTBYADDR_R_5_REENTRANT) || \
    \
    defined(HAVE_GETHOSTBYNAME_R_3) || \
    defined(HAVE_GETHOSTBYNAME_R_3_REENTRANT)
  struct hostent_data hdata;
#elif defined(HAVE_GETHOSTBYADDR_R_7) || \
      defined(HAVE_GETHOSTBYADDR_R_7_REENTRANT) || \
      defined(HAVE_GETHOSTBYADDR_R_8) || \
      defined(HAVE_GETHOSTBYADDR_R_8_REENTRANT) || \
      \
      defined(HAVE_GETHOSTBYNAME_R_5) || \
      defined(HAVE_GETHOSTBYNAME_R_5_REENTRANT) || \
      defined(HAVE_GETHOSTBYNAME_R_6) || \
      defined(HAVE_GETHOSTBYNAME_R_6_REENTRANT)
  char buffer[8192];
  int h_errnop;
  struct hostent *hp;
#endif

#ifndef gethostbyaddr_r
  (void)gethostbyaddr_r;
#endif

#if   defined(HAVE_GETHOSTBYADDR_R_5) || \
      defined(HAVE_GETHOSTBYADDR_R_5_REENTRANT)
  rc = gethostbyaddr_r(address, length, type, &h, &hdata);
  (void)rc;
#elif defined(HAVE_GETHOSTBYADDR_R_7) || \
      defined(HAVE_GETHOSTBYADDR_R_7_REENTRANT)
  hp = gethostbyaddr_r(address, length, type, &h, buffer, 8192, &h_errnop);
  (void)hp;
#elif defined(HAVE_GETHOSTBYADDR_R_8) || \
      defined(HAVE_GETHOSTBYADDR_R_8_REENTRANT)
  rc = gethostbyaddr_r(address, length, type, &h, buffer, 8192, &hp, &h_errnop);
  (void)rc;
#endif

#if   defined(HAVE_GETHOSTBYNAME_R_3) || \
      defined(HAVE_GETHOSTBYNAME_R_3_REENTRANT)
  rc = gethostbyname_r(address, &h, &hdata);
#elif defined(HAVE_GETHOSTBYNAME_R_5) || \
      defined(HAVE_GETHOSTBYNAME_R_5_REENTRANT)
  rc = gethostbyname_r(address, &h, buffer, 8192, &h_errnop);
  (void)hp; /* not used for test */
#elif defined(HAVE_GETHOSTBYNAME_R_6) || \
      defined(HAVE_GETHOSTBYNAME_R_6_REENTRANT)
  rc = gethostbyname_r(address, &h, buffer, 8192, &hp, &h_errnop);
#endif

  (void)length;
  (void)type;
  (void)rc;
  return 0;
}
#endif

#ifdef HAVE_SOCKLEN_T
#ifdef _WIN32
#include <ws2tcpip.h>
#else
#include <sys/types.h>
#include <sys/socket.h>
#endif
int
main ()
{
if ((socklen_t *) 0)
  return 0;
if (sizeof (socklen_t))
  return 0;
  ;
  return 0;
}
#endif
#ifdef HAVE_IN_ADDR_T
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>

int
main ()
{
if ((in_addr_t *) 0)
  return 0;
if (sizeof (in_addr_t))
  return 0;
  ;
  return 0;
}
#endif

#ifdef HAVE_BOOL_T
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_STDBOOL_H
#include <stdbool.h>
#endif
int
main ()
{
if (sizeof (bool *) )
  return 0;
  ;
  return 0;
}
#endif

#ifdef STDC_HEADERS
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <float.h>
int main() { return 0; }
#endif
#ifdef RETSIGTYPE_TEST
#include <sys/types.h>
#include <signal.h>
#ifdef signal
# undef signal
#endif
#ifdef __cplusplus
extern "C" void (*signal (int, void (*)(int)))(int);
#else
void (*signal ()) ();
#endif

int
main ()
{
  return 0;
}
#endif
#ifdef HAVE_INET_NTOA_R_DECL
#include <arpa/inet.h>

typedef void (*func_type)();

int main()
{
#ifndef inet_ntoa_r
  func_type func;
  func = (func_type)inet_ntoa_r;
  (void)func;
#endif
  return 0;
}
#endif
#ifdef HAVE_INET_NTOA_R_DECL_REENTRANT
#define _REENTRANT
#include <arpa/inet.h>

typedef void (*func_type)();

int main()
{
#ifndef inet_ntoa_r
  func_type func;
  func = (func_type)&inet_ntoa_r;
  (void)func;
#endif
  return 0;
}
#endif
#ifdef HAVE_GETADDRINFO
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>

int main(void) {
    struct addrinfo hints, *ai;
    int error;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
#ifndef getaddrinfo
    (void)getaddrinfo;
#endif
    error = getaddrinfo("127.0.0.1", "8080", &hints, &ai);
    if (error) {
        return 1;
    }
    return 0;
}
#endif
#ifdef HAVE_FILE_OFFSET_BITS
#ifdef _FILE_OFFSET_BITS
#undef _FILE_OFFSET_BITS
#endif
#define _FILE_OFFSET_BITS 64
#include <sys/types.h>
 /* Check that off_t can represent 2**63 - 1 correctly.
    We can't simply define LARGE_OFF_T to be 9223372036854775807,
    since some C++ compilers masquerading as C compilers
    incorrectly reject 9223372036854775807.  */
#define LARGE_OFF_T (((off_t) 1 << 62) - 1 + ((off_t) 1 << 62))
  int off_t_is_large[(LARGE_OFF_T % 2147483629 == 721
                       && LARGE_OFF_T % 2147483647 == 1)
                      ? 1 : -1];
int main () { ; return 0; }
#endif
#ifdef HAVE_IOCTLSOCKET
/* includes start */
#ifdef HAVE_WINDOWS_H
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN
#  endif
#  include <windows.h>
#  ifdef HAVE_WINSOCK2_H
#    include <winsock2.h>
#  else
#    ifdef HAVE_WINSOCK_H
#      include <winsock.h>
#    endif
#  endif
#endif

int
main ()
{

/* ioctlsocket source code */
 int socket;
 unsigned long flags = ioctlsocket(socket, FIONBIO, &flags);

  ;
  return 0;
}

#endif
#ifdef HAVE_IOCTLSOCKET_CAMEL
/* includes start */
#ifdef HAVE_WINDOWS_H
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN
#  endif
#  include <windows.h>
#  ifdef HAVE_WINSOCK2_H
#    include <winsock2.h>
#  else
#    ifdef HAVE_WINSOCK_H
#      include <winsock.h>
#    endif
#  endif
#endif

int
main ()
{

/* IoctlSocket source code */
    if(0 != IoctlSocket(0, 0, 0))
      return 1;
  ;
  return 0;
}
#endif
#ifdef HAVE_IOCTLSOCKET_CAMEL_FIONBIO
/* includes start */
#ifdef HAVE_WINDOWS_H
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN
#  endif
#  include <windows.h>
#  ifdef HAVE_WINSOCK2_H
#    include <winsock2.h>
#  else
#    ifdef HAVE_WINSOCK_H
#      include <winsock.h>
#    endif
#  endif
#endif

int
main ()
{

/* IoctlSocket source code */
        long flags = 0;
        if(0 != ioctlsocket(0, FIONBIO, &flags))
          return 1;
  ;
  return 0;
}
#endif
#ifdef HAVE_IOCTLSOCKET_FIONBIO
/* includes start */
#ifdef HAVE_WINDOWS_H
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN
#  endif
#  include <windows.h>
#  ifdef HAVE_WINSOCK2_H
#    include <winsock2.h>
#  else
#    ifdef HAVE_WINSOCK_H
#      include <winsock.h>
#    endif
#  endif
#endif

int
main ()
{

        int flags = 0;
        if(0 != ioctlsocket(0, FIONBIO, &flags))
          return 1;

  ;
  return 0;
}
#endif
#ifdef HAVE_IOCTL_FIONBIO
/* headers for FIONBIO test */
/* includes start */
#ifdef HAVE_SYS_TYPES_H
#  include <sys/types.h>
#endif
#ifdef HAVE_UNISTD_H
#  include <unistd.h>
#endif
#ifdef HAVE_SYS_SOCKET_H
#  include <sys/socket.h>
#endif
#ifdef HAVE_SYS_IOCTL_H
#  include <sys/ioctl.h>
#endif
#ifdef HAVE_STROPTS_H
#  include <stropts.h>
#endif

int
main ()
{

        int flags = 0;
        if(0 != ioctl(0, FIONBIO, &flags))
          return 1;

  ;
  return 0;
}
#endif
#ifdef HAVE_IOCTL_SIOCGIFADDR
/* headers for FIONBIO test */
/* includes start */
#ifdef HAVE_SYS_TYPES_H
#  include <sys/types.h>
#endif
#ifdef HAVE_UNISTD_H
#  include <unistd.h>
#endif
#ifdef HAVE_SYS_SOCKET_H
#  include <sys/socket.h>
#endif
#ifdef HAVE_SYS_IOCTL_H
#  include <sys/ioctl.h>
#endif
#ifdef HAVE_STROPTS_H
#  include <stropts.h>
#endif
#include <net/if.h>

int
main ()
{
        struct ifreq ifr;
        if(0 != ioctl(0, SIOCGIFADDR, &ifr))
          return 1;

  ;
  return 0;
}
#endif
#ifdef HAVE_SETSOCKOPT_SO_NONBLOCK
/* includes start */
#ifdef HAVE_WINDOWS_H
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN
#  endif
#  include <windows.h>
#  ifdef HAVE_WINSOCK2_H
#    include <winsock2.h>
#  else
#    ifdef HAVE_WINSOCK_H
#      include <winsock.h>
#    endif
#  endif
#endif
/* includes start */
#ifdef HAVE_SYS_TYPES_H
#  include <sys/types.h>
#endif
#ifdef HAVE_SYS_SOCKET_H
#  include <sys/socket.h>
#endif
/* includes end */

int
main ()
{
        if(0 != setsockopt(0, SOL_SOCKET, SO_NONBLOCK, 0, 0))
          return 1;
  ;
  return 0;
}
#endif
#ifdef HAVE_GLIBC_STRERROR_R
#include <string.h>
#include <errno.h>

void check(char c) {}

int
main () {
  char buffer[1024];
  /* This will not compile if strerror_r does not return a char* */
  check(strerror_r(EACCES, buffer, sizeof(buffer))[0]);
  return 0;
}
#endif
#ifdef HAVE_POSIX_STRERROR_R
#include <string.h>
#include <errno.h>

/* float, because a pointer can't be implicitly cast to float */
void check(float f) {}

int
main () {
  char buffer[1024];
  /* This will not compile if strerror_r does not return an int */
  check(strerror_r(EACCES, buffer, sizeof(buffer)));
  return 0;
}
#endif
#ifdef HAVE_FSETXATTR_6
#include <sys/xattr.h> /* header from libc, not from libattr */
int
main() {
  fsetxattr(0, 0, 0, 0, 0, 0);
  return 0;
}
#endif
#ifdef HAVE_FSETXATTR_5
#include <sys/xattr.h> /* header from libc, not from libattr */
int
main() {
  fsetxattr(0, 0, 0, 0, 0);
  return 0;
}
#endif
#ifdef HAVE_CLOCK_GETTIME_MONOTONIC
#include <time.h>
int
main() {
  struct timespec ts = {0, 0};
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return 0;
}
#endif
#ifdef HAVE_BUILTIN_AVAILABLE
int
main() {
  if(__builtin_available(macOS 10.12, *)) {}
  return 0;
}
#endif
#ifdef HAVE_VARIADIC_MACROS_C99
#define c99_vmacro3(first, ...) fun3(first, __VA_ARGS__)
#define c99_vmacro2(first, ...) fun2(first, __VA_ARGS__)

int fun3(int arg1, int arg2, int arg3);
int fun2(int arg1, int arg2);

int fun3(int arg1, int arg2, int arg3) {
  return arg1 + arg2 + arg3;
}
int fun2(int arg1, int arg2) {
  return arg1 + arg2;
}

int
main() {
  int res3 = c99_vmacro3(1, 2, 3);
  int res2 = c99_vmacro2(1, 2);
  (void)res3;
  (void)res2;
  return 0;
}
#endif
#ifdef HAVE_VARIADIC_MACROS_GCC
#define gcc_vmacro3(first, args...) fun3(first, args)
#define gcc_vmacro2(first, args...) fun2(first, args)

int fun3(int arg1, int arg2, int arg3);
int fun2(int arg1, int arg2);

int fun3(int arg1, int arg2, int arg3) {
  return arg1 + arg2 + arg3;
}
int fun2(int arg1, int arg2) {
  return arg1 + arg2;
}

int
main() {
  int res3 = gcc_vmacro3(1, 2, 3);
  int res2 = gcc_vmacro2(1, 2);
  (void)res3;
  (void)res2;
  return 0;
}
#endif
