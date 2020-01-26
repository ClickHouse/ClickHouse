#pragma once

/**
 * No MSG_NOSIGNAL on OS X.
 * Avoid SIGPIPE by using sockopt call.
 */
#ifdef MSG_NOSIGNAL
# define AMQP_CPP_MSG_NOSIGNAL MSG_NOSIGNAL
#else
# define AMQP_CPP_MSG_NOSIGNAL 0
# ifdef SO_NOSIGPIPE
#  define AMQP_CPP_USE_SO_NOSIGPIPE
# else
#  error "Cannot block SIGPIPE!"
# endif
#endif

#ifdef AMQP_CPP_USE_SO_NOSIGPIPE
/**
 * Ignore SIGPIPE when there is no MSG_NOSIGNAL.
 */
inline void set_sockopt_nosigpipe(int socket)
{
    int optval = 1;
    setsockopt(socket, SOL_SOCKET, SO_NOSIGPIPE, &optval, sizeof(optval));
}
#endif
