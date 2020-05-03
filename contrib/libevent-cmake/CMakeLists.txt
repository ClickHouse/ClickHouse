set(LIBEVENT_DIR ${ClickHouse_SOURCE_DIR}/contrib/libevent)

set(HEADERS
    ${LIBEVENT_DIR}/include/event2/buffer.h
    ${LIBEVENT_DIR}/include/event2/bufferevent.h
    ${LIBEVENT_DIR}/include/event2/bufferevent_compat.h
    ${LIBEVENT_DIR}/include/event2/bufferevent_struct.h
    ${LIBEVENT_DIR}/include/event2/buffer_compat.h
    ${LIBEVENT_DIR}/include/event2/dns.h
    ${LIBEVENT_DIR}/include/event2/dns_compat.h
    ${LIBEVENT_DIR}/include/event2/dns_struct.h
    ${LIBEVENT_DIR}/include/event2/event.h
    ${LIBEVENT_DIR}/include/event2/event_compat.h
    ${LIBEVENT_DIR}/include/event2/event_struct.h
    ${LIBEVENT_DIR}/include/event2/watch.h
    ${LIBEVENT_DIR}/include/event2/http.h
    ${LIBEVENT_DIR}/include/event2/http_compat.h
    ${LIBEVENT_DIR}/include/event2/http_struct.h
    ${LIBEVENT_DIR}/include/event2/keyvalq_struct.h
    ${LIBEVENT_DIR}/include/event2/listener.h
    ${LIBEVENT_DIR}/include/event2/rpc.h
    ${LIBEVENT_DIR}/include/event2/rpc_compat.h
    ${LIBEVENT_DIR}/include/event2/rpc_struct.h
    ${LIBEVENT_DIR}/include/event2/tag.h
    ${LIBEVENT_DIR}/include/event2/tag_compat.h
    ${LIBEVENT_DIR}/include/event2/thread.h
    ${LIBEVENT_DIR}/include/event2/util.h
    ${LIBEVENT_DIR}/include/event2/visibility.h
    )

set(SRC_CORE
    ${LIBEVENT_DIR}/buffer.c
    ${LIBEVENT_DIR}/bufferevent.c
    ${LIBEVENT_DIR}/bufferevent_filter.c
    ${LIBEVENT_DIR}/bufferevent_pair.c
    ${LIBEVENT_DIR}/bufferevent_ratelim.c
    ${LIBEVENT_DIR}/bufferevent_sock.c
    ${LIBEVENT_DIR}/event.c
    ${LIBEVENT_DIR}/evmap.c
    ${LIBEVENT_DIR}/evthread.c
    ${LIBEVENT_DIR}/evutil.c
    ${LIBEVENT_DIR}/evutil_rand.c
    ${LIBEVENT_DIR}/evutil_time.c
    ${LIBEVENT_DIR}/watch.c
    ${LIBEVENT_DIR}/listener.c
    ${LIBEVENT_DIR}/log.c
    ${LIBEVENT_DIR}/signal.c
    ${LIBEVENT_DIR}/strlcpy.c)

set(HEADERS_PRIVATE
    ${LIBEVENT_DIR}/bufferevent-internal.h
    ${LIBEVENT_DIR}/changelist-internal.h
    ${LIBEVENT_DIR}/defer-internal.h
    ${LIBEVENT_DIR}/epolltable-internal.h
    ${LIBEVENT_DIR}/evbuffer-internal.h
    ${LIBEVENT_DIR}/event-internal.h
    ${LIBEVENT_DIR}/evmap-internal.h
    ${LIBEVENT_DIR}/evrpc-internal.h
    ${LIBEVENT_DIR}/evsignal-internal.h
    ${LIBEVENT_DIR}/evthread-internal.h
    ${LIBEVENT_DIR}/ht-internal.h
    ${LIBEVENT_DIR}/http-internal.h
    ${LIBEVENT_DIR}/iocp-internal.h
    ${LIBEVENT_DIR}/ipv6-internal.h
    ${LIBEVENT_DIR}/log-internal.h
    ${LIBEVENT_DIR}/minheap-internal.h
    ${LIBEVENT_DIR}/mm-internal.h
    ${LIBEVENT_DIR}/ratelim-internal.h
    ${LIBEVENT_DIR}/strlcpy-internal.h
    ${LIBEVENT_DIR}/util-internal.h
    ${LIBEVENT_DIR}/evconfig-private.h
    ${LIBEVENT_DIR}/compat/sys/queue.h

    ${LIBEVENT_DIR}/select.c
    ${LIBEVENT_DIR}/poll.c
    ${LIBEVENT_DIR}/kqueue.c
    ${LIBEVENT_DIR}/devpoll.c
    ${LIBEVENT_DIR}/epoll.c
    ${LIBEVENT_DIR}/evport.c
    )

if(EVENT__HAVE_SELECT)
    list(APPEND SRC_CORE ${LIBEVENT_DIR}/select.c)
endif()

if(EVENT__HAVE_POLL)
    list(APPEND SRC_CORE ${LIBEVENT_DIR}/poll.c)
endif()

if(EVENT__HAVE_KQUEUE)
    list(APPEND SRC_CORE ${LIBEVENT_DIR}/kqueue.c)
endif()

if(EVENT__HAVE_DEVPOLL)
    list(APPEND SRC_CORE ${LIBEVENT_DIR}/devpoll.c)
endif()

if(EVENT__HAVE_EPOLL)
    list(APPEND SRC_CORE ${LIBEVENT_DIR}/epoll.c)
endif()

if(EVENT__HAVE_EVENT_PORTS)
    list(APPEND SRC_CORE ${LIBEVENT_DIR}/evport.c)
endif()


configure_file(
    ${LIBEVENT_DIR}/evconfig-private.h.cmake
    ${LIBEVENT_DIR}/include/evconfig-private.h)

set(BACKENDS "")

if (EVENT__HAVE_EPOLL)
    list(APPEND BACKENDS EPOLL)
endif()

if (EVENT__HAVE_SELECT)
    list(APPEND BACKENDS SELECT)
endif()

if (EVENT__HAVE_POLL)
    list(APPEND BACKENDS POLL)
endif()

if (EVENT__HAVE_KQUEUE)
    list(APPEND BACKENDS KQUEUE)
endif()

if (EVENT__HAVE_EVENT_PORTS)
    list(APPEND BACKENDS EVPORT)
endif()

if (EVENT__HAVE_DEVPOLL)
    list(APPEND BACKENDS DEVPOLL)
endif()

if (WIN32)
    list(APPEND BACKENDS WIN32)
endif()

add_library(LIBEVENT ${HEADERS} ${HEADERS_PRIVATE} ${SRC_CORE})

target_include_directories(LIBEVENT PRIVATE ${LIBEVENT_DIR})
target_include_directories(LIBEVENT PUBLIC ${LIBEVENT_DIR}/include)
target_include_directories(LIBEVENT PUBLIC ${LIBEVENT_DIR}/include/event2)


