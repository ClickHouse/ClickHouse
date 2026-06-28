#define CURL_CA_BUNDLE "/etc/ssl/certs/ca-certificates.crt"

/* curl is only used as a plain HTTP(S) client, optionally through a proxy. */
#define HTTP_ONLY
#define CURL_DISABLE_ALTSVC
#define CURL_DISABLE_AWS
#define CURL_DISABLE_COOKIES
#define CURL_DISABLE_DOH
#define CURL_DISABLE_FORM_API
#define CURL_DISABLE_GETOPTIONS
#define CURL_DISABLE_HEADERS_API
#define CURL_DISABLE_HSTS
#define CURL_DISABLE_MIME
#define CURL_DISABLE_NETRC
#define CURL_DISABLE_PROGRESS_METER

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
#define HAVE_SELECT
#define HAVE_SIGACTION
#define HAVE_SIGNAL
#define HAVE_SIGSETJMP
#define HAVE_SOCKET
#define HAVE_STRUCT_TIMEVAL
#define HAVE_POLL

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
#define HAVE_GETSOCKNAME
#define HAVE_FCNTL_H
#define HAVE_NETDB_H
#define HAVE_NETINET_IN_H
#define HAVE_SELECT_H
#define HAVE_SETJMP_H
#define HAVE_SETJMP_H
#define HAVE_STDINT_H
#define HAVE_UNISTD_H
#define HAVE_POLL_H
#define HAVE_PTHREAD_H

#define ENABLE_IPV6
#define USE_OPENSSL
#define HAVE_THREADS_POSIX
#define USE_ARES
#define USE_RESOLV_ARES

#ifdef __illumos__
#define HAVE_POSIX_STRERROR_R 1
#define HAVE_STRERROR_R 1
#endif
