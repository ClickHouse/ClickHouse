set (LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/udt)

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
	add_definitions(-D_DEBUG)
endif()


SET(LIBRARIES
	pthread
	m
	${ARCAN_SHMIF_LIBRARY}
)

add_definitions(
	-Wall
	-D__UNIX
	-DPOSIX_C_SOURCE
	-DGNU_SOURCE
	-Wno-unused-function
	-std=c++11
)

set(SOURCES
    ${LIBRARY_DIR}/src/api.cpp
    ${LIBRARY_DIR}/src/buffer.cpp
    ${LIBRARY_DIR}/src/cache.cpp
    ${LIBRARY_DIR}/src/ccc.cpp
    ${LIBRARY_DIR}/src/channel.cpp
    ${LIBRARY_DIR}/src/core.cpp
    ${LIBRARY_DIR}/src/epoll.cpp
    ${LIBRARY_DIR}/src/list.cpp
    ${LIBRARY_DIR}/src/md5.cpp
    ${LIBRARY_DIR}/src/packet.cpp
    ${LIBRARY_DIR}/src/queue.cpp
    ${LIBRARY_DIR}/src/udtCommon.cpp
    ${LIBRARY_DIR}/src/window.cpp
)

add_library(udt ${SOURCES})

target_include_directories(udt PUBLIC ${LIBRARY_DIR}/src)

add_library (ch_contrib::udt ALIAS udt)

