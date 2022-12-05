#define CURL_CA_BUNDLE "/etc/ssl/certs/ca-certificates.crt"
#define CURL_DISABLE_FTP
#define CURL_DISABLE_TFTP
#define CURL_DISABLE_LDAP
#define CURL_EXTERN_SYMBOL __attribute__ ((__visibility__ ("default")))

#define SIZEOF_SHORT 2
#define SIZEOF_INT   4
#define SIZEOF_LONG  8
#define SIZEOF_CURL_OFF_T 8
#define SIZEOF_SIZE_T 8

#define HAVE_ALARM
#define HAVE_FCNTL_O_NONBLOCK
#define HAVE_GETADDRINFO
#define HAVE_LONGLONG
#define HAVE_POLL_FINE
#define HAVE_SIGACTION
#define HAVE_SIGNAL
#define HAVE_SIGSETJMP
#define HAVE_SOCKET
#define HAVE_STRUCT_TIMEVAL

#define HAVE_RECV
#define RECV_TYPE_ARG1 int
#define RECV_TYPE_ARG2 void*
#define RECV_TYPE_ARG3 size_t
#define RECV_TYPE_ARG4 int
#define RECV_TYPE_RETV ssize_t

#define HAVE_SEND
#define SEND_TYPE_ARG1 int
#define SEND_TYPE_ARG2 void*
#define SEND_QUAL_ARG2 const
#define SEND_TYPE_ARG3 size_t
#define SEND_TYPE_ARG4 int
#define SEND_TYPE_RETV ssize_t

#define HAVE_ARPA_INET_H
#define HAVE_ERRNO_H
#define HAVE_FCNTL_H
#define HAVE_NETDB_H
#define HAVE_NETINET_IN_H
#define HAVE_SETJMP_H
#define HAVE_SYS_STAT_H
#define HAVE_UNISTD_H

#define ENABLE_IPV6
#define USE_OPENSSL
#define USE_THREADS_POSIX
