#ifndef	_NETDB_H
#define	_NETDB_H

#ifdef __cplusplus
extern "C" {
#endif

#include <features.h>
#include <netinet/in.h>

#if defined(_GNU_SOURCE) || defined(_BSD_SOURCE)
#define __NEED_size_t
#include <bits/alltypes.h>
#endif

struct addrinfo {
	int ai_flags;
	int ai_family;
	int ai_socktype;
	int ai_protocol;
	socklen_t ai_addrlen;
	struct sockaddr *ai_addr;
	char *ai_canonname;
	struct addrinfo *ai_next;
};

#define AI_PASSIVE      0x01
#define AI_CANONNAME    0x02
#define AI_NUMERICHOST  0x04
#define AI_V4MAPPED     0x08
#define AI_ALL          0x10
#define AI_ADDRCONFIG   0x20
#define AI_NUMERICSERV  0x400


#define NI_NUMERICHOST  0x01
#define NI_NUMERICSERV  0x02
#define NI_NOFQDN       0x04
#define NI_NAMEREQD     0x08
#define NI_DGRAM        0x10
#define NI_NUMERICSCOPE 0x100

#define EAI_BADFLAGS   -1
#define EAI_NONAME     -2
#define EAI_AGAIN      -3
#define EAI_FAIL       -4
#define EAI_FAMILY     -6
#define EAI_SOCKTYPE   -7
#define EAI_SERVICE    -8
#define EAI_MEMORY     -10
#define EAI_SYSTEM     -11
#define EAI_OVERFLOW   -12

int getaddrinfo (const char *__restrict, const char *__restrict, const struct addrinfo *__restrict, struct addrinfo **__restrict);
void freeaddrinfo (struct addrinfo *);
int getnameinfo (const struct sockaddr *__restrict, socklen_t, char *__restrict, socklen_t, char *__restrict, socklen_t, int);
const char *gai_strerror(int);


/* Legacy functions follow (marked OBsolete in SUS) */

struct netent {
	char *n_name;
	char **n_aliases;
	int n_addrtype;
	uint32_t n_net;
};

struct hostent {
	char *h_name;
	char **h_aliases;
	int h_addrtype;
	int h_length;
	char **h_addr_list;
};
#define h_addr h_addr_list[0]

struct servent {
	char *s_name;
	char **s_aliases;
	int s_port;
	char *s_proto;
};

struct protoent {
	char *p_name;
	char **p_aliases;
	int p_proto;
};

void sethostent (int);
void endhostent (void);
struct hostent *gethostent (void);

void setnetent (int);
void endnetent (void);
struct netent *getnetent (void);
struct netent *getnetbyaddr (uint32_t, int);
struct netent *getnetbyname (const char *);

void setservent (int);
void endservent (void);
struct servent *getservent (void);
struct servent *getservbyname (const char *, const char *);
struct servent *getservbyport (int, const char *);

void setprotoent (int);
void endprotoent (void);
struct protoent *getprotoent (void);
struct protoent *getprotobyname (const char *);
struct protoent *getprotobynumber (int);

#if defined(_GNU_SOURCE) || defined(_BSD_SOURCE) || defined(_POSIX_SOURCE) \
 || (defined(_POSIX_C_SOURCE) && _POSIX_C_SOURCE+0 < 200809L) \
 || (defined(_XOPEN_SOURCE) && _XOPEN_SOURCE+0 < 700)
struct hostent *gethostbyname (const char *);
struct hostent *gethostbyaddr (const void *, socklen_t, int);
#ifdef __GNUC__
__attribute__((const))
#endif
int *__h_errno_location(void);
#define h_errno (*__h_errno_location())
#define HOST_NOT_FOUND 1
#define TRY_AGAIN      2
#define NO_RECOVERY    3
#define NO_DATA        4
#define NO_ADDRESS     NO_DATA
#endif

#if defined(_GNU_SOURCE) || defined(_BSD_SOURCE)
void herror(const char *);
const char *hstrerror(int);
int gethostbyname_r(const char *, struct hostent *, char *, size_t, struct hostent **, int *);
int gethostbyname2_r(const char *, int, struct hostent *, char *, size_t, struct hostent **, int *);
struct hostent *gethostbyname2(const char *, int);
int gethostbyaddr_r(const void *, socklen_t, int, struct hostent *, char *, size_t, struct hostent **, int *);
int getservbyport_r(int, const char *, struct servent *, char *, size_t, struct servent **);
int getservbyname_r(const char *, const char *, struct servent *, char *, size_t, struct servent **);
#define EAI_NODATA     -5
#define EAI_ADDRFAMILY -9
#define EAI_INPROGRESS -100
#define EAI_CANCELED   -101
#define EAI_NOTCANCELED -102
#define EAI_ALLDONE    -103
#define EAI_INTR       -104
#define EAI_IDN_ENCODE -105
#define NI_MAXHOST 255
#define NI_MAXSERV 32
#endif


#ifdef __cplusplus
}
#endif

#endif
