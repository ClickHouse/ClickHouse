/* Copyright (C) The c-ares project and its contributors
 * SPDX-License-Identifier: MIT
 */

/* Generated from ares_config.h.cmake */

/* Define if building universal (internal helper macro) */
#undef AC_APPLE_UNIVERSAL_BUILD

/* Defined for build with symbol hiding. */
/* #undef CARES_SYMBOL_HIDING */

/* Use resolver library to configure cares */
/* #undef CARES_USE_LIBRESOLV */

/* if a /etc/inet dir is being used */
#undef ETC_INET

/* Define to the type of arg 2 for gethostname. */
#define GETHOSTNAME_TYPE_ARG2 int

/* Define to the type qualifier of arg 1 for getnameinfo. */
#define GETNAMEINFO_QUAL_ARG1 

/* Define to the type of arg 1 for getnameinfo. */
#define GETNAMEINFO_TYPE_ARG1 struct sockaddr *

/* Define to the type of arg 2 for getnameinfo. */
#define GETNAMEINFO_TYPE_ARG2 socklen_t

/* Define to the type of args 4 and 6 for getnameinfo. */
#define GETNAMEINFO_TYPE_ARG46 socklen_t

/* Define to the type of arg 7 for getnameinfo. */
#define GETNAMEINFO_TYPE_ARG7 int

/* Specifies the number of arguments to getservbyport_r */
#define GETSERVBYPORT_R_ARGS 

/* Specifies the number of arguments to getservbyname_r */
#define GETSERVBYNAME_R_ARGS 

/* Define to 1 if you have AF_INET6. */
#define HAVE_AF_INET6 1

/* Define to 1 if you have the <arpa/inet.h> header file. */
/* #undef HAVE_ARPA_INET_H */

/* Define to 1 if you have the <arpa/nameser_compat.h> header file. */
/* #undef HAVE_ARPA_NAMESER_COMPAT_H */

/* Define to 1 if you have the <arpa/nameser.h> header file. */
/* #undef HAVE_ARPA_NAMESER_H */

/* Define to 1 if you have the <assert.h> header file. */
#define HAVE_ASSERT_H 1

/* Define to 1 if you have the clock_gettime function and monotonic timer. */
#define HAVE_CLOCK_GETTIME_MONOTONIC 1

/* Define to 1 if you have the closesocket function. */
#define HAVE_CLOSESOCKET 1

/* Define to 1 if you have the CloseSocket camel case function. */
/* #undef HAVE_CLOSESOCKET_CAMEL */

/* Define to 1 if you have the connect function. */
#define HAVE_CONNECT 1

/* Define to 1 if you have the connectx function. */
/* #undef HAVE_CONNECTX */

/* define if the compiler supports basic C++11 syntax */
/* #undef HAVE_CXX11 */

/* Define to 1 if you have the <dlfcn.h> header file. */
/* #undef HAVE_DLFCN_H */

/* Define to 1 if you have the <errno.h> header file. */
#define HAVE_ERRNO_H 1

/* Define to 1 if you have the <poll.h> header file. */
/* #undef HAVE_POLL_H */

/* Define to 1 if you have the memmem function. */
/* #undef HAVE_MEMMEM */

/* Define to 1 if you have the poll function. */
/* #undef HAVE_POLL */

/* Define to 1 if you have the pipe function. */
/* #undef HAVE_PIPE */

/* Define to 1 if you have the pipe2 function. */
/* #undef HAVE_PIPE2 */

/* Define to 1 if you have the kqueue function. */
/* #undef HAVE_KQUEUE */

/* Define to 1 if you have the epoll{_create,ctl,wait} functions. */
/* #undef HAVE_EPOLL */

/* Define to 1 if you have the fcntl function. */
/* #undef HAVE_FCNTL */

/* Define to 1 if you have the <fcntl.h> header file. */
#define HAVE_FCNTL_H 1

/* Define to 1 if you have a working fcntl O_NONBLOCK function. */
/* #undef HAVE_FCNTL_O_NONBLOCK */

/* Define to 1 if you have the freeaddrinfo function. */
#define HAVE_FREEADDRINFO 1

/* Define to 1 if you have a working getaddrinfo function. */
#define HAVE_GETADDRINFO 1

/* Define to 1 if the getaddrinfo function is threadsafe. */
#define HAVE_GETADDRINFO_THREADSAFE 1

/* Define to 1 if you have the getenv function. */
#define HAVE_GETENV 1

/* Define to 1 if you have the gethostname function. */
#define HAVE_GETHOSTNAME 1

/* Define to 1 if you have the getnameinfo function. */
#define HAVE_GETNAMEINFO 1

/* Define to 1 if you have the getrandom function. */
/* #undef HAVE_GETRANDOM */

/* Define to 1 if you have the getservbyport_r function. */
/* #undef HAVE_GETSERVBYPORT_R */

/* Define to 1 if you have the getservbyname_r function. */
/* #undef HAVE_GETSERVBYNAME_R */

/* Define to 1 if you have the `gettimeofday' function. */
#define HAVE_GETTIMEOFDAY 1

/* Define to 1 if you have the `if_indextoname' function. */
#define HAVE_IF_INDEXTONAME 1

/* Define to 1 if you have the `if_nametoindex' function. */
#define HAVE_IF_NAMETOINDEX 1

/* Define to 1 if you have the `GetBestRoute2' function. */
#define HAVE_GETBESTROUTE2 1

/* Define to 1 if you have the `ConvertInterfaceIndexToLuid' function. */
#define HAVE_CONVERTINTERFACEINDEXTOLUID 1

/* Define to 1 if you have the `ConvertInterfaceLuidToNameA' function. */
#define HAVE_CONVERTINTERFACELUIDTONAMEA 1

/* Define to 1 if you have the `NotifyIpInterfaceChange' function. */
#define HAVE_NOTIFYIPINTERFACECHANGE 1

/* Define to 1 if you have the `RegisterWaitForSingleObject' function. */
#define HAVE_REGISTERWAITFORSINGLEOBJECT 1

/* Define to 1 if you have a IPv6 capable working inet_net_pton function. */
/* #undef HAVE_INET_NET_PTON */

/* Define to 1 if you have a IPv6 capable working inet_ntop function. */
/* #undef HAVE_INET_NTOP */

/* Define to 1 if you have a IPv6 capable working inet_pton function. */
/* #undef HAVE_INET_PTON */

/* Define to 1 if you have the <inttypes.h> header file. */
#define HAVE_INTTYPES_H 1

/* Define to 1 if you have the ioctl function. */
/* #undef HAVE_IOCTL */

/* Define to 1 if you have the ioctlsocket function. */
#define HAVE_IOCTLSOCKET 1

/* Define to 1 if you have the IoctlSocket camel case function. */
/* #undef HAVE_IOCTLSOCKET_CAMEL */

/* Define to 1 if you have a working IoctlSocket camel case FIONBIO function.
   */
/* #undef HAVE_IOCTLSOCKET_CAMEL_FIONBIO */

/* Define to 1 if you have a working ioctlsocket FIONBIO function. */
#define HAVE_IOCTLSOCKET_FIONBIO 1

/* Define to 1 if you have a working ioctl FIONBIO function. */
/* #undef HAVE_IOCTL_FIONBIO */

/* Define to 1 if you have a working ioctl SIOCGIFADDR function. */
/* #undef HAVE_IOCTL_SIOCGIFADDR */

/* Define to 1 if you have the `resolve' library (-lresolve). */
/* #undef HAVE_LIBRESOLV */

/* Define to 1 if you have iphlpapi.h */
#define HAVE_IPHLPAPI_H 1

/* Define to 1 if you have netioapi.h */
#define HAVE_NETIOAPI_H 1

/* Define to 1 if you have the <limits.h> header file. */
#define HAVE_LIMITS_H 1

/* Define to 1 if the compiler supports the 'long long' data type. */
#define HAVE_LONGLONG 1

/* Define to 1 if you have the malloc.h header file. */
#define HAVE_MALLOC_H 1

/* Define to 1 if you have the memory.h header file. */
#define HAVE_MEMORY_H 1

/* Define to 1 if you have the AvailabilityMacros.h header file. */
/* #undef HAVE_AVAILABILITYMACROS_H */

/* Define to 1 if you have the MSG_NOSIGNAL flag. */
/* #undef HAVE_MSG_NOSIGNAL */

/* Define to 1 if you have the <netdb.h> header file. */
/* #undef HAVE_NETDB_H */

/* Define to 1 if you have the <netinet/in.h> header file. */
/* #undef HAVE_NETINET_IN_H */

/* Define to 1 if you have the <netinet6/in6.h> header file. */
/* #undef HAVE_NETINET6_IN6_H */

/* Define to 1 if you have the <netinet/tcp.h> header file. */
/* #undef HAVE_NETINET_TCP_H */

/* Define to 1 if you have the <net/if.h> header file. */
/* #undef HAVE_NET_IF_H */

/* Define to 1 if you have PF_INET6. */
#define HAVE_PF_INET6 1

/* Define to 1 if you have the recv function. */
#define HAVE_RECV 1

/* Define to 1 if you have the recvfrom function. */
#define HAVE_RECVFROM 1

/* Define to 1 if you have the send function. */
#define HAVE_SEND 1

/* Define to 1 if you have the sendto function. */
#define HAVE_SENDTO 1

/* Define to 1 if you have the setsockopt function. */
#define HAVE_SETSOCKOPT 1

/* Define to 1 if you have a working setsockopt SO_NONBLOCK function. */
/* #undef HAVE_SETSOCKOPT_SO_NONBLOCK */

/* Define to 1 if you have the <signal.h> header file. */
#define HAVE_SIGNAL_H 1

/* Define to 1 if you have the strnlen function. */
#define HAVE_STRNLEN 1

/* Define to 1 if your struct sockaddr_in6 has sin6_scope_id. */
#define HAVE_STRUCT_SOCKADDR_IN6_SIN6_SCOPE_ID 1

/* Define to 1 if you have the socket function. */
#define HAVE_SOCKET 1

/* Define to 1 if you have the <socket.h> header file. */
/* #undef HAVE_SOCKET_H */

/* Define to 1 if you have the <stdbool.h> header file. */
#define HAVE_STDBOOL_H 1

/* Define to 1 if you have the <stdint.h> header file. */
#define HAVE_STDINT_H 1

/* Define to 1 if you have the <stdlib.h> header file. */
#define HAVE_STDLIB_H 1

/* Define to 1 if you have the strcasecmp function. */
#define HAVE_STRCASECMP 1

/* Define to 1 if you have the strcmpi function. */
#define HAVE_STRCMPI 1

/* Define to 1 if you have the strdup function. */
#define HAVE_STRDUP 1

/* Define to 1 if you have the stricmp function. */
#define HAVE_STRICMP 1

/* Define to 1 if you have the <strings.h> header file. */
#define HAVE_STRINGS_H 1

/* Define to 1 if you have the <string.h> header file. */
#define HAVE_STRING_H 1

/* Define to 1 if you have the strncasecmp function. */
#define HAVE_STRNCASECMP 1

/* Define to 1 if you have the strncmpi function. */
/* #undef HAVE_STRNCMPI */

/* Define to 1 if you have the strnicmp function. */
#define HAVE_STRNICMP 1

/* Define to 1 if you have the <stropts.h> header file. */
/* #undef HAVE_STROPTS_H */

/* Define to 1 if you have struct addrinfo. */
#define HAVE_STRUCT_ADDRINFO 1

/* Define to 1 if you have struct in6_addr. */
#define HAVE_STRUCT_IN6_ADDR 1

/* Define to 1 if you have struct sockaddr_in6. */
#define HAVE_STRUCT_SOCKADDR_IN6 1

/* if struct sockaddr_storage is defined */
#define HAVE_STRUCT_SOCKADDR_STORAGE 1

/* Define to 1 if you have the timeval struct. */
#define HAVE_STRUCT_TIMEVAL 1

/* Define to 1 if you have the <sys/ioctl.h> header file. */
/* #undef HAVE_SYS_IOCTL_H */

/* Define to 1 if you have the <sys/param.h> header file. */
#define HAVE_SYS_PARAM_H 1

/* Define to 1 if you have the <sys/random.h> header file. */
/* #undef HAVE_SYS_RANDOM_H */

/* Define to 1 if you have the <sys/event.h> header file. */
/* #undef HAVE_SYS_EVENT_H */

/* Define to 1 if you have the <sys/epoll.h> header file. */
/* #undef HAVE_SYS_EPOLL_H */

/* Define to 1 if you have the <sys/select.h> header file. */
/* #undef HAVE_SYS_SELECT_H */

/* Define to 1 if you have the <sys/socket.h> header file. */
/* #undef HAVE_SYS_SOCKET_H */

/* Define to 1 if you have the <sys/stat.h> header file. */
#define HAVE_SYS_STAT_H 1

/* Define to 1 if you have the <sys/time.h> header file. */
#define HAVE_SYS_TIME_H 1

/* Define to 1 if you have the <sys/types.h> header file. */
#define HAVE_SYS_TYPES_H 1

/* Define to 1 if you have the <sys/uio.h> header file. */
/* #undef HAVE_SYS_UIO_H */

/* Define to 1 if you have the <time.h> header file. */
#define HAVE_TIME_H 1

/* Define to 1 if you have the <ifaddrs.h> header file. */
/* #undef HAVE_IFADDRS_H */

/* Define to 1 if you have the <unistd.h> header file. */
#define HAVE_UNISTD_H 1

/* Define to 1 if you have the windows.h header file. */
#define HAVE_WINDOWS_H 1

/* Define to 1 if you have the winsock2.h header file. */
#define HAVE_WINSOCK2_H 1

/* Define to 1 if you have the winsock.h header file. */
#define HAVE_WINSOCK_H 1

/* Define to 1 if you have the mswsock.h header file. */
#define HAVE_MSWSOCK_H 1

/* Define to 1 if you have the winternl.h header file. */
#define HAVE_WINTERNL_H 1

/* Define to 1 if you have the ntstatus.h header file. */
#define HAVE_NTSTATUS_H 1

/* Define to 1 if you have the ntdef.h header file. */
#define HAVE_NTDEF_H 1

/* Define to 1 if you have the writev function. */
/* #undef HAVE_WRITEV */

/* Define to 1 if you have the ws2tcpip.h header file. */
#define HAVE_WS2TCPIP_H 1

/* Define to 1 if you have the __system_property_get function */
/* #undef HAVE___SYSTEM_PROPERTY_GET */

/* Define if have arc4random_buf() */
/* #undef HAVE_ARC4RANDOM_BUF */

/* Define if have getifaddrs() */
/* #undef HAVE_GETIFADDRS */

/* Define if have stat() */
#define HAVE_STAT 1

/* a suitable file/device to read random data from */
#define CARES_RANDOM_FILE "/dev/urandom"

/* Define to the type qualifier pointed by arg 5 for recvfrom. */
#define RECVFROM_QUAL_ARG5 

/* Define to the type of arg 1 for recvfrom. */
#define RECVFROM_TYPE_ARG1 SOCKET

/* Define to the type pointed by arg 2 for recvfrom. */
#define RECVFROM_TYPE_ARG2 char *

/* Define to 1 if the type pointed by arg 2 for recvfrom is void. */
#define RECVFROM_TYPE_ARG2_IS_VOID 0

/* Define to the type of arg 3 for recvfrom. */
#define RECVFROM_TYPE_ARG3 int

/* Define to the type of arg 4 for recvfrom. */
#define RECVFROM_TYPE_ARG4 int

/* Define to the type pointed by arg 5 for recvfrom. */
#define RECVFROM_TYPE_ARG5 struct sockaddr *

/* Define to 1 if the type pointed by arg 5 for recvfrom is void. */
#define RECVFROM_TYPE_ARG5_IS_VOID 0

/* Define to the type pointed by arg 6 for recvfrom. */
#define RECVFROM_TYPE_ARG6 socklen_t *

/* Define to 1 if the type pointed by arg 6 for recvfrom is void. */
#define RECVFROM_TYPE_ARG6_IS_VOID 0

/* Define to the function return type for recvfrom. */
#define RECVFROM_TYPE_RETV int

/* Define to the type of arg 1 for recv. */
#define RECV_TYPE_ARG1 SOCKET

/* Define to the type of arg 2 for recv. */
#define RECV_TYPE_ARG2 char *

/* Define to the type of arg 3 for recv. */
#define RECV_TYPE_ARG3 int

/* Define to the type of arg 4 for recv. */
#define RECV_TYPE_ARG4 int

/* Define to the function return type for recv. */
#define RECV_TYPE_RETV int

/* Define to the type of arg 1 for send. */
#define SEND_TYPE_ARG1 SOCKET

/* Define to the type of arg 2 for send. */
#define SEND_TYPE_ARG2 const void *

/* Define to the type of arg 3 for send. */
#define SEND_TYPE_ARG3 int

/* Define to the type of arg 4 for send. */
#define SEND_TYPE_ARG4 int

/* Define to the function return type for send. */
#define SEND_TYPE_RETV int

/* Define to disable non-blocking sockets. */
#undef USE_BLOCKING_SOCKETS

/* Define to avoid automatic inclusion of winsock.h */
#undef WIN32_LEAN_AND_MEAN

/* Define to 1 if you have the pthread.h header file. */
/* #undef HAVE_PTHREAD_H */

/* Define to 1 if you have the pthread_np.h header file. */
/* #undef HAVE_PTHREAD_NP_H */

/* Define to 1 if threads are enabled */
#define CARES_THREADS 1

/* Define to 1 if pthread_init() exists */
/* #undef HAVE_PTHREAD_INIT */

