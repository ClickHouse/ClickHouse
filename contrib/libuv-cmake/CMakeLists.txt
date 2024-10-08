# This file is a modified version of contrib/libuv/CMakeLists.txt

set (SOURCE_DIR "${PROJECT_SOURCE_DIR}/contrib/libuv")
set (BINARY_DIR "${PROJECT_BINARY_DIR}/contrib/libuv")

set(uv_sources
    src/fs-poll.c
    src/idna.c
    src/inet.c
    src/random.c
    src/strscpy.c
    src/strtok.c
    src/thread-common.c
    src/threadpool.c
    src/timer.c
    src/uv-common.c
    src/uv-data-getter-setters.c
    src/version.c
    src/unix/async.c
    src/unix/core.c
    src/unix/dl.c
    src/unix/fs.c
    src/unix/getaddrinfo.c
    src/unix/getnameinfo.c
    src/unix/loop-watcher.c
    src/unix/loop.c
    src/unix/pipe.c
    src/unix/poll.c
    src/unix/process.c
    src/unix/random-devurandom.c
    src/unix/signal.c
    src/unix/stream.c
    src/unix/tcp.c
    src/unix/thread.c
    src/unix/tty.c
    src/unix/udp.c)

if(APPLE OR CMAKE_SYSTEM_NAME MATCHES "Android|Linux|OS/390")
  list(APPEND uv_sources src/unix/proctitle.c)
endif()

if(CMAKE_SYSTEM_NAME MATCHES "DragonFly|FreeBSD")
  list(APPEND uv_sources src/unix/freebsd.c)
endif()

if(CMAKE_SYSTEM_NAME MATCHES "DragonFly|FreeBSD|NetBSD|OpenBSD")
  list(APPEND uv_sources src/unix/posix-hrtime.c src/unix/bsd-proctitle.c)
endif()

if(APPLE OR CMAKE_SYSTEM_NAME MATCHES "DragonFly|FreeBSD|NetBSD|OpenBSD")
  list(APPEND uv_sources src/unix/bsd-ifaddrs.c src/unix/kqueue.c)
endif()

if(CMAKE_SYSTEM_NAME MATCHES "FreeBSD")
  list(APPEND uv_sources src/unix/random-getrandom.c)
endif()

if(APPLE OR CMAKE_SYSTEM_NAME STREQUAL "OpenBSD")
  list(APPEND uv_sources src/unix/random-getentropy.c)
endif()

if(APPLE)
  list(APPEND uv_defines _DARWIN_UNLIMITED_SELECT=1 _DARWIN_USE_64_BIT_INODE=1)
  list(APPEND uv_sources
       src/unix/darwin-proctitle.c
       src/unix/darwin.c
       src/unix/fsevents.c)
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  list(APPEND uv_defines _GNU_SOURCE _POSIX_C_SOURCE=200112)
  list(APPEND uv_libraries rt)
  list(APPEND uv_sources
       src/unix/linux.c
       src/unix/procfs-exepath.c
       src/unix/random-getrandom.c
       src/unix/random-sysctl-linux.c)
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "NetBSD")
  list(APPEND uv_sources src/unix/netbsd.c)
  list(APPEND uv_libraries kvm)
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "OpenBSD")
  list(APPEND uv_sources src/unix/openbsd.c)
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "OS/390")
  list(APPEND uv_defines PATH_MAX=255)
  list(APPEND uv_defines _AE_BIMODAL)
  list(APPEND uv_defines _ALL_SOURCE)
  list(APPEND uv_defines _LARGE_TIME_API)
  list(APPEND uv_defines _OPEN_MSGQ_EXT)
  list(APPEND uv_defines _OPEN_SYS_FILE_EXT)
  list(APPEND uv_defines _OPEN_SYS_IF_EXT)
  list(APPEND uv_defines _OPEN_SYS_SOCK_EXT3)
  list(APPEND uv_defines _OPEN_SYS_SOCK_IPV6)
  list(APPEND uv_defines _UNIX03_SOURCE)
  list(APPEND uv_defines _UNIX03_THREADS)
  list(APPEND uv_defines _UNIX03_WITHDRAWN)
  list(APPEND uv_defines _XOPEN_SOURCE_EXTENDED)
  list(APPEND uv_sources
       src/unix/pthread-fixes.c
       src/unix/pthread-barrier.c
       src/unix/os390.c
       src/unix/os390-proctitle.c
       src/unix/os390-syscalls.c)
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "SunOS")
  list(APPEND uv_defines __EXTENSIONS__ _XOPEN_SOURCE=500)
  list(APPEND uv_libraries kstat nsl sendfile socket)
  list(APPEND uv_sources src/unix/no-proctitle.c src/unix/sunos.c)
endif()

set(uv_sources_tmp "")
foreach(file ${uv_sources})
  list(APPEND uv_sources_tmp "${SOURCE_DIR}/${file}")
endforeach(file)
set(uv_sources "${uv_sources_tmp}")

list(APPEND uv_defines CLICKHOUSE_GLIBC_COMPATIBILITY)

add_library(_uv ${uv_sources})
add_library(ch_contrib::uv ALIAS _uv)

target_compile_definitions(_uv PRIVATE ${uv_defines})
target_include_directories(_uv SYSTEM PUBLIC ${SOURCE_DIR}/include PRIVATE ${SOURCE_DIR}/src)
target_link_libraries(_uv ${uv_libraries})

if(UNIX)
  # Now for some gibbering horrors from beyond the stars...
  foreach(x ${uv_libraries})
    set(LIBS "${LIBS} -l${x}")
  endforeach(x)
  file(STRINGS ${SOURCE_DIR}/configure.ac configure_ac REGEX ^AC_INIT)
  string(REGEX MATCH [0-9]+[.][0-9]+[.][0-9]+ PACKAGE_VERSION "${configure_ac}")
  string(REGEX MATCH ^[0-9]+ UV_VERSION_MAJOR "${PACKAGE_VERSION}")
  # The version in the filename is mirroring the behaviour of autotools.
  set_target_properties(_uv PROPERTIES VERSION ${UV_VERSION_MAJOR}.0.0
                        SOVERSION ${UV_VERSION_MAJOR})
endif()
