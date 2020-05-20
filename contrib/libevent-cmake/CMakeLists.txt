set(LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/libevent)

set(SRCS
    ${LIBRARY_DIR}/buffer.c
    ${LIBRARY_DIR}/bufferevent_filter.c
    ${LIBRARY_DIR}/bufferevent_pair.c
    ${LIBRARY_DIR}/bufferevent_ratelim.c
    ${LIBRARY_DIR}/bufferevent_sock.c
    ${LIBRARY_DIR}/bufferevent.c
    ${LIBRARY_DIR}/event.c
    ${LIBRARY_DIR}/evmap.c
    ${LIBRARY_DIR}/evthread.c
    ${LIBRARY_DIR}/evutil_rand.c
    ${LIBRARY_DIR}/evutil_time.c
    ${LIBRARY_DIR}/evutil.c
    ${LIBRARY_DIR}/listener.c
    ${LIBRARY_DIR}/log.c
    ${LIBRARY_DIR}/signal.c
    ${LIBRARY_DIR}/strlcpy.c
    ${LIBRARY_DIR}/watch.c
)

if (OS_LINUX)
    list (APPEND SRCS
        ${LIBRARY_DIR}/epoll.c
        ${LIBRARY_DIR}/poll.c
        ${LIBRARY_DIR}/select.c
    )

elseif (OS_DARWIN)
    list (APPEND SRCS ${LIBRARY_DIR}/kqueue.c)
endif ()

add_library(libevent ${SRCS})

target_compile_options (libevent PUBLIC -Wno-reserved-id-macro)

if (OS_LINUX)
    target_include_directories (libevent PUBLIC linux)
endif ()

target_include_directories (libevent PUBLIC ${LIBRARY_DIR}/include)
