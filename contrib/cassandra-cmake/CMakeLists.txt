# Need to use C++17 since the compilation is not possible with C++20 currently.
set (CMAKE_CXX_STANDARD 17)

set(CASS_ROOT_DIR ${CMAKE_SOURCE_DIR}/contrib/cassandra)
set(CASS_SRC_DIR "${CASS_ROOT_DIR}/src")
set(CASS_INCLUDE_DIR "${CASS_ROOT_DIR}/include")

# Ensure functions/modules are available
list(APPEND CMAKE_MODULE_PATH ${CASS_ROOT_DIR}/cmake)

set(CASS_BUILD_SHARED 1)
set(CASS_BUILD_STATIC 1)
set(CASS_USE_KERBEROS 0)
set(CASS_USE_LIBSSH2 0)
set(CASS_USE_OPENSSL 1)
set(CASS_USE_STD_ATOMIC 1)
set(CASS_USE_ZLIB 1)
set(CASS_USE_TIMERFD 0)


file(GLOB SOURCES ${CASS_SRC_DIR}/*.cpp)

if(APPLE)
  list(REMOVE_ITEM SOURCES ${CASS_SRC_DIR}/get_time-unix.cpp ${CASS_SRC_DIR}/get_time-win.cpp)
elseif(UNIX)
  list(REMOVE_ITEM SOURCES ${CASS_SRC_DIR}/get_time-mac.cpp ${CASS_SRC_DIR}/get_time-win.cpp)
elseif(WIN32)
  list(REMOVE_ITEM SOURCES ${CASS_SRC_DIR}/get_time-mac.cpp ${CASS_SRC_DIR}/get_time-unix.cpp)
endif()

if(CASS_USE_OPENSSL)
  list(APPEND INCLUDE_DIRS ${CASS_SRC_DIR}/ssl)
  list(APPEND SOURCES ${CASS_SRC_DIR}/ssl/ssl_openssl_impl.cpp ${CASS_SRC_DIR}/ssl/ring_buffer_bio.cpp)
else()
  list(APPEND SOURCES ${CASS_SRC_DIR}/ssl/ssl_no_impl.cpp)
endif()

if(CASS_USE_KERBEROS)
  list(APPEND INCLUDE_DIRS ${CASS_SRC_DIR}/gssapi)
  list(APPEND SOURCES ${CASS_SRC_DIR}/gssapi/dse_auth_gssapi.cpp ${CASS_SRC_DIR}/gssapi/dse_auth_gssapi.hpp)
endif()

list(APPEND SOURCES ${CASS_SRC_DIR}/atomic/atomic_std.hpp)


add_library(curl_hostcheck OBJECT ${CASS_SRC_DIR}/third_party/curl/hostcheck.cpp)
add_library(hdr_histogram OBJECT ${CASS_SRC_DIR}/third_party/hdr_histogram/hdr_histogram.cpp)
add_library(http-parser OBJECT ${CASS_SRC_DIR}/third_party/http-parser/http_parser.c)
add_library(minizip OBJECT
    ${CASS_SRC_DIR}/third_party/minizip/ioapi.c
    ${CASS_SRC_DIR}/third_party/minizip/zip.c
    ${CASS_SRC_DIR}/third_party/minizip/unzip.c)

target_link_libraries(minizip zlib)
target_compile_definitions(minizip PRIVATE "-Dz_crc_t=unsigned long")

list(APPEND INCLUDE_DIRS
  ${CASS_SRC_DIR}/third_party/curl
  ${CASS_SRC_DIR}/third_party/hdr_histogram
  ${CASS_SRC_DIR}/third_party/http-parser
  ${CASS_SRC_DIR}/third_party/minizip
  ${CASS_SRC_DIR}/third_party/mt19937_64
  ${CASS_SRC_DIR}/third_party/rapidjson/rapidjson
  ${CASS_SRC_DIR}/third_party/sparsehash/src)

list(APPEND INCLUDE_DIRS ${CASS_INCLUDE_DIR} ${CASS_SRC_DIR})

set(HASH_FUN_H "functional")
set(HASH_NAME hash)
set(HASH_NAMESPACE "std")
set(HAVE_INTTYPES_H 1)
set(HAVE_STDINT_H 1)
set(HAVE_SYS_TYPES_H 1)
set(HAVE_MEMCPY 1)
set(HAVE_LONG_LONG 1)
set(HAVE_UINT16_T 1)

configure_file("${CASS_SRC_DIR}/third_party/sparsehash/config.h.cmake" "${CMAKE_CURRENT_BINARY_DIR}/sparsehash/internal/sparseconfig.h")



# Determine random availability
if(CMAKE_SYSTEM_NAME MATCHES "Linux")
  check_symbol_exists(GRND_NONBLOCK "linux/random.h" HAVE_GETRANDOM)
  if(CASS_USE_TIMERFD)
    check_symbol_exists(timerfd_create "sys/timerfd.h" HAVE_TIMERFD)
  endif()
else()
  check_symbol_exists(arc4random_buf "stdlib.h" HAVE_ARC4RANDOM)
endif()

# Determine if sigpipe is available
check_symbol_exists(SO_NOSIGPIPE "sys/socket.h;sys/types.h" HAVE_NOSIGPIPE)
check_symbol_exists(sigtimedwait "signal.h" HAVE_SIGTIMEDWAIT)
if (NOT WIN32 AND NOT HAVE_NOSIGPIPE AND NOT HAVE_SIGTIMEDWAIT)
  message(WARNING "Unable to handle SIGPIPE on your platform")
endif()

set (HAVE_BUILTIN_BSWAP32 1)
set (HAVE_BUILTIN_BSWAP64 1)

set(HAVE_BOOST_ATOMIC 0)
set(HAVE_STD_ATOMIC 1)

set(HAVE_KERBEROS ${CASS_USE_KERBEROS})
set(HAVE_OPENSSL ${CASS_USE_OPENSSL})
set(HAVE_ZLIB ${CASS_USE_ZLIB})

# Generate the driver_config.hpp file
configure_file(
  ${CASS_ROOT_DIR}/driver_config.hpp.in
  ${CMAKE_CURRENT_BINARY_DIR}/driver_config.hpp)


add_library(cassandra
    ${SOURCES}
    $<TARGET_OBJECTS:curl_hostcheck>
    $<TARGET_OBJECTS:hdr_histogram>
    $<TARGET_OBJECTS:http-parser>
    $<TARGET_OBJECTS:minizip>)

target_link_libraries(cassandra zlib)
add_library(cassandra_static ALIAS cassandra)
target_include_directories(cassandra PRIVATE ${INCLUDE_DIRS} ${CMAKE_CURRENT_BINARY_DIR})
target_compile_definitions(cassandra PRIVATE CASS_BUILDING)

target_link_libraries(cassandra uv)

if(CASS_USE_OPENSSL)
    target_link_libraries(cassandra ssl)
endif()
