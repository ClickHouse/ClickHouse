//
// SocketDefs.h
//
// $Id: //poco/1.4/Net/include/Poco/Net/SocketDefs.h#6 $
//
// Library: Net
// Package: NetCore
// Module:  SocketDefs
//
// Include platform-specific header files for sockets.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_SocketDefs_INCLUDED
#define Net_SocketDefs_INCLUDED


#define POCO_ENOERR 0


#if defined(POCO_OS_FAMILY_WINDOWS)
	#include "Poco/UnWindows.h"
	#include <winsock2.h>
	#include <ws2tcpip.h>
	#define POCO_INVALID_SOCKET  INVALID_SOCKET
	#define poco_socket_t        SOCKET
	#define poco_socklen_t       int
	#define poco_ioctl_request_t int
	#define poco_closesocket(s)  closesocket(s)
	#define POCO_EINTR           WSAEINTR
	#define POCO_EACCES          WSAEACCES
	#define POCO_EFAULT          WSAEFAULT
	#define POCO_EINVAL          WSAEINVAL
	#define POCO_EMFILE          WSAEMFILE
	#define POCO_EAGAIN          WSAEWOULDBLOCK
	#define POCO_EWOULDBLOCK     WSAEWOULDBLOCK
	#define POCO_EINPROGRESS     WSAEINPROGRESS
	#define POCO_EALREADY        WSAEALREADY
	#define POCO_ENOTSOCK        WSAENOTSOCK
	#define POCO_EDESTADDRREQ    WSAEDESTADDRREQ
	#define POCO_EMSGSIZE        WSAEMSGSIZE
	#define POCO_EPROTOTYPE      WSAEPROTOTYPE
	#define POCO_ENOPROTOOPT     WSAENOPROTOOPT
	#define POCO_EPROTONOSUPPORT WSAEPROTONOSUPPORT
	#define POCO_ESOCKTNOSUPPORT WSAESOCKTNOSUPPORT
	#define POCO_ENOTSUP         WSAEOPNOTSUPP
	#define POCO_EPFNOSUPPORT    WSAEPFNOSUPPORT
	#define POCO_EAFNOSUPPORT    WSAEAFNOSUPPORT
	#define POCO_EADDRINUSE      WSAEADDRINUSE
	#define POCO_EADDRNOTAVAIL   WSAEADDRNOTAVAIL
	#define POCO_ENETDOWN        WSAENETDOWN
	#define POCO_ENETUNREACH     WSAENETUNREACH
	#define POCO_ENETRESET       WSAENETRESET
	#define POCO_ECONNABORTED    WSAECONNABORTED
	#define POCO_ECONNRESET      WSAECONNRESET
	#define POCO_ENOBUFS         WSAENOBUFS
	#define POCO_EISCONN         WSAEISCONN
	#define POCO_ENOTCONN        WSAENOTCONN
	#define POCO_ESHUTDOWN       WSAESHUTDOWN
	#define POCO_ETIMEDOUT       WSAETIMEDOUT
	#define POCO_ECONNREFUSED    WSAECONNREFUSED
	#define POCO_EHOSTDOWN       WSAEHOSTDOWN
	#define POCO_EHOSTUNREACH    WSAEHOSTUNREACH
	#define POCO_ESYSNOTREADY    WSASYSNOTREADY
	#define POCO_ENOTINIT        WSANOTINITIALISED
	#define POCO_HOST_NOT_FOUND  WSAHOST_NOT_FOUND
	#define POCO_TRY_AGAIN       WSATRY_AGAIN
	#define POCO_NO_RECOVERY     WSANO_RECOVERY
	#define POCO_NO_DATA         WSANO_DATA
	#ifndef ADDRESS_FAMILY
		#define ADDRESS_FAMILY USHORT
	#endif
#elif defined(POCO_VXWORKS)
	#include <hostLib.h>
	#include <ifLib.h>
	#include <inetLib.h>
	#include <ioLib.h>
	#include <resolvLib.h>
	#include <types.h>
	#include <socket.h>
	#include <netinet/tcp.h>
	#define POCO_INVALID_SOCKET  -1
	#define poco_socket_t        int
	#define poco_socklen_t       int
	#define poco_ioctl_request_t int
	#define poco_closesocket(s)  ::close(s)
	#define POCO_EINTR           EINTR
	#define POCO_EACCES          EACCES
	#define POCO_EFAULT          EFAULT
	#define POCO_EINVAL          EINVAL
	#define POCO_EMFILE          EMFILE
	#define POCO_EAGAIN          EAGAIN
	#define POCO_EWOULDBLOCK     EWOULDBLOCK
	#define POCO_EINPROGRESS     EINPROGRESS
	#define POCO_EALREADY        EALREADY
	#define POCO_ENOTSOCK        ENOTSOCK
	#define POCO_EDESTADDRREQ    EDESTADDRREQ
	#define POCO_EMSGSIZE        EMSGSIZE
	#define POCO_EPROTOTYPE      EPROTOTYPE
	#define POCO_ENOPROTOOPT     ENOPROTOOPT
	#define POCO_EPROTONOSUPPORT EPROTONOSUPPORT
	#define POCO_ESOCKTNOSUPPORT ESOCKTNOSUPPORT
	#define POCO_ENOTSUP         ENOTSUP
	#define POCO_EPFNOSUPPORT    EPFNOSUPPORT
	#define POCO_EAFNOSUPPORT    EAFNOSUPPORT
	#define POCO_EADDRINUSE      EADDRINUSE
	#define POCO_EADDRNOTAVAIL   EADDRNOTAVAIL
	#define POCO_ENETDOWN        ENETDOWN
	#define POCO_ENETUNREACH     ENETUNREACH
	#define POCO_ENETRESET       ENETRESET
	#define POCO_ECONNABORTED    ECONNABORTED
	#define POCO_ECONNRESET      ECONNRESET
	#define POCO_ENOBUFS         ENOBUFS
	#define POCO_EISCONN         EISCONN
	#define POCO_ENOTCONN        ENOTCONN
	#define POCO_ESHUTDOWN       ESHUTDOWN
	#define POCO_ETIMEDOUT       ETIMEDOUT
	#define POCO_ECONNREFUSED    ECONNREFUSED
	#define POCO_EHOSTDOWN       EHOSTDOWN
	#define POCO_EHOSTUNREACH    EHOSTUNREACH
	#define POCO_ESYSNOTREADY    -4
	#define POCO_ENOTINIT        -5
	#define POCO_HOST_NOT_FOUND  HOST_NOT_FOUND
	#define POCO_TRY_AGAIN       TRY_AGAIN
	#define POCO_NO_RECOVERY     NO_RECOVERY
	#define POCO_NO_DATA         NO_DATA
#elif defined(POCO_OS_FAMILY_UNIX) || defined(POCO_OS_FAMILY_VMS)
	#include <unistd.h>
	#include <errno.h>
	#include <sys/types.h>
	#include <sys/socket.h>
	#include <fcntl.h>
	#if POCO_OS != POCO_OS_HPUX
		#include <sys/select.h>
	#endif
	#include <sys/ioctl.h>
	#if defined(POCO_OS_FAMILY_VMS)
		#include <inet.h>
	#else
		#include <arpa/inet.h>
	#endif
	#include <netinet/in.h>
	#include <netinet/tcp.h>
	#include <netdb.h>
	#if defined(POCO_OS_FAMILY_UNIX)
		#if (POCO_OS == POCO_OS_LINUX)
			// Net/src/NetworkInterface.cpp changed #include <linux/if.h> to #include <net/if.h>
			// no more conflict, can use #include <net/if.h> here
			#include <net/if.h>
		#elif (POCO_OS == POCO_OS_HPUX)
			extern "C"
			{
				#include <net/if.h>
			}
		#else
			#include <net/if.h>
		#endif
	#endif
	#if (POCO_OS == POCO_OS_SOLARIS) || (POCO_OS == POCO_OS_MAC_OS_X)
		#include <sys/sockio.h>
		#include <sys/filio.h>
	#endif
	#define POCO_INVALID_SOCKET  -1
	#define poco_socket_t        int
	#define poco_socklen_t       socklen_t
	#define poco_fcntl_request_t int
	#if defined(POCO_OS_FAMILY_BSD)
		#define poco_ioctl_request_t unsigned long
	#else
		#define poco_ioctl_request_t int
	#endif
	#define poco_closesocket(s)  ::close(s)
	#define POCO_EINTR           EINTR
	#define POCO_EACCES          EACCES
	#define POCO_EFAULT          EFAULT
	#define POCO_EINVAL          EINVAL
	#define POCO_EMFILE          EMFILE
	#define POCO_EAGAIN          EAGAIN
	#define POCO_EWOULDBLOCK     EWOULDBLOCK
	#define POCO_EINPROGRESS     EINPROGRESS
	#define POCO_EALREADY        EALREADY
	#define POCO_ENOTSOCK        ENOTSOCK
	#define POCO_EDESTADDRREQ    EDESTADDRREQ
	#define POCO_EMSGSIZE        EMSGSIZE
	#define POCO_EPROTOTYPE      EPROTOTYPE
	#define POCO_ENOPROTOOPT     ENOPROTOOPT
	#define POCO_EPROTONOSUPPORT EPROTONOSUPPORT
	#if defined(ESOCKTNOSUPPORT)
		#define POCO_ESOCKTNOSUPPORT ESOCKTNOSUPPORT
	#else
		#define POCO_ESOCKTNOSUPPORT -1
	#endif
	#define POCO_ENOTSUP         ENOTSUP
	#define POCO_EPFNOSUPPORT    EPFNOSUPPORT
	#define POCO_EAFNOSUPPORT    EAFNOSUPPORT
	#define POCO_EADDRINUSE      EADDRINUSE
	#define POCO_EADDRNOTAVAIL   EADDRNOTAVAIL
	#define POCO_ENETDOWN        ENETDOWN
	#define POCO_ENETUNREACH     ENETUNREACH
	#define POCO_ENETRESET       ENETRESET
	#define POCO_ECONNABORTED    ECONNABORTED
	#define POCO_ECONNRESET      ECONNRESET
	#define POCO_ENOBUFS         ENOBUFS
	#define POCO_EISCONN         EISCONN
	#define POCO_ENOTCONN        ENOTCONN
	#if defined(ESHUTDOWN)
		#define POCO_ESHUTDOWN   ESHUTDOWN
	#else
		#define POCO_ESHUTDOWN   -2
	#endif
	#define POCO_ETIMEDOUT       ETIMEDOUT
	#define POCO_ECONNREFUSED    ECONNREFUSED
	#if defined(EHOSTDOWN)
		#define POCO_EHOSTDOWN   EHOSTDOWN
	#else
		#define POCO_EHOSTDOWN   -3
	#endif
	#define POCO_EHOSTUNREACH    EHOSTUNREACH
	#define POCO_ESYSNOTREADY    -4
	#define POCO_ENOTINIT        -5
	#define POCO_HOST_NOT_FOUND  HOST_NOT_FOUND
	#define POCO_TRY_AGAIN       TRY_AGAIN
	#define POCO_NO_RECOVERY     NO_RECOVERY
	#define POCO_NO_DATA         NO_DATA
#endif


#if defined(POCO_OS_FAMILY_BSD) || (POCO_OS == POCO_OS_TRU64) || (POCO_OS == POCO_OS_AIX) || (POCO_OS == POCO_OS_IRIX) || (POCO_OS == POCO_OS_QNX) || (POCO_OS == POCO_OS_VXWORKS)
	#define POCO_HAVE_SALEN 1
#endif


#if POCO_OS != POCO_OS_VXWORKS && !defined(POCO_NET_NO_ADDRINFO)
	#define POCO_HAVE_ADDRINFO 1
#endif


#if (POCO_OS == POCO_OS_HPUX) || (POCO_OS == POCO_OS_SOLARIS) || (POCO_OS == POCO_OS_WINDOWS_CE) || (POCO_OS == POCO_OS_CYGWIN)
	#define POCO_BROKEN_TIMEOUTS 1
#endif


#if defined(POCO_HAVE_ADDRINFO)
	#ifndef AI_PASSIVE
		#define AI_PASSIVE 0
	#endif
	#ifndef AI_CANONNAME
		#define AI_CANONNAME 0
	#endif
	#ifndef AI_NUMERICHOST
		#define AI_NUMERICHOST 0
	#endif
	#ifndef AI_NUMERICSERV
		#define AI_NUMERICSERV 0
	#endif
	#ifndef AI_ALL
		#define AI_ALL 0
	#endif
	#ifndef AI_ADDRCONFIG
		#define AI_ADDRCONFIG 0
	#endif
	#ifndef AI_V4MAPPED
		#define AI_V4MAPPED 0
	#endif
#endif


#if defined(POCO_HAVE_SALEN)
	#define poco_set_sa_len(pSA, len)  (pSA)->sa_len   = (len)
	#define poco_set_sin_len(pSA)      (pSA)->sin_len  = sizeof(struct sockaddr_in)
	#if defined(POCO_HAVE_IPv6)
		#define poco_set_sin6_len(pSA) (pSA)->sin6_len = sizeof(struct sockaddr_in6)
	#endif
#else
	#define poco_set_sa_len(pSA, len) (void) 0
	#define poco_set_sin_len(pSA)     (void) 0
	#define poco_set_sin6_len(pSA)    (void) 0
#endif


#ifndef INADDR_NONE
	#define INADDR_NONE 0xffffffff
#endif

#ifndef INADDR_ANY
	#define INADDR_ANY 0x00000000
#endif

#ifndef INADDR_BROADCAST
	#define INADDR_BROADCAST 0xffffffff
#endif

#ifndef INADDR_LOOPBACK
	#define INADDR_LOOPBACK 0x7f000001
#endif

#ifndef INADDR_UNSPEC_GROUP
	#define INADDR_UNSPEC_GROUP 0xe0000000
#endif

#ifndef INADDR_ALLHOSTS_GROUP
	#define INADDR_ALLHOSTS_GROUP 0xe0000001
#endif

#ifndef INADDR_ALLRTRS_GROUP
	#define INADDR_ALLRTRS_GROUP 0xe0000002
#endif

#ifndef INADDR_MAX_LOCAL_GROUP
	#define INADDR_MAX_LOCAL_GROUP 0xe00000ff
#endif

#if defined(POCO_ARCH_BIG_ENDIAN)
	#define poco_ntoh_16(x) (x)
	#define poco_ntoh_32(x) (x)
#else
	#define poco_ntoh_16(x) \
		((((x) >> 8) & 0x00ff) | (((x) << 8) & 0xff00))
	#define poco_ntoh_32(x) \
		((((x) >> 24) & 0x000000ff) | (((x) >> 8) & 0x0000ff00) | (((x) << 8) & 0x00ff0000) | (((x) << 24) & 0xff000000))
#endif
#define poco_hton_16(x) poco_ntoh_16(x)
#define poco_hton_32(x) poco_ntoh_32(x)


#if !defined(s6_addr16)
	#if defined(POCO_OS_FAMILY_WINDOWS)
		#define s6_addr16 u.Word
	#else
		#define s6_addr16 __u6_addr.__u6_addr16
	#endif
#endif


#if !defined(s6_addr32)
	#if defined(POCO_OS_FAMILY_UNIX)
		#if (POCO_OS == POCO_OS_SOLARIS)
			#define s6_addr32 _S6_un._S6_u32
		#else
			#define s6_addr32 __u6_addr.__u6_addr32
		#endif
	#endif
#endif


#endif // Net_SocketDefs_INCLUDED
