option(ENABLE_CASSANDRA "Enable Cassandra" ${ENABLE_LIBRARIES})

if (NOT ENABLE_CASSANDRA)
    message(STATUS "Not using cassandra")
    return()
endif()

# can be removed once libuv build on MacOS with GCC is possible
if (NOT TARGET ch_contrib::uv)
    message(STATUS "Not using cassandra because libuv is disabled")
    return()
endif()

if (APPLE)
    set(CMAKE_MACOSX_RPATH ON)
endif()

# Need to use C++17 since the compilation is not possible with C++20 currently.
set (CMAKE_CXX_STANDARD 17)

set(CASS_ROOT_DIR ${PROJECT_SOURCE_DIR}/contrib/cassandra)
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


add_library(_curl_hostcheck OBJECT ${CASS_SRC_DIR}/third_party/curl/hostcheck.cpp)
add_library(_hdr_histogram OBJECT ${CASS_SRC_DIR}/third_party/hdr_histogram/hdr_histogram.cpp)
add_library(_http-parser OBJECT ${CASS_SRC_DIR}/third_party/http-parser/http_parser.c)

list(APPEND INCLUDE_DIRS
  ${CASS_SRC_DIR}/third_party/curl
  ${CASS_SRC_DIR}/third_party/hdr_histogram
  ${CASS_SRC_DIR}/third_party/http-parser
  ${CASS_SRC_DIR}/third_party/mt19937_64
  ${CASS_SRC_DIR}/third_party/rapidjson/rapidjson)

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

# Determine random availability
if (OS_LINUX)
  #set (HAVE_GETRANDOM 1) - not on every Linux kernel
elseif (OS_FREEBSD OR OS_DARWIN)
  set (HAVE_ARC4RANDOM 1)
endif ()

# Determine if sigpipe is available
if (OS_LINUX)
    set (HAVE_SIGTIMEDWAIT 1)
else (OS_FREEBSD OR OS_DARWIN)
    set (HAVE_NOSIGPIPE 1)
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

add_library(_cassandra
    ${SOURCES}
    $<TARGET_OBJECTS:_curl_hostcheck>
    $<TARGET_OBJECTS:_hdr_histogram>
    $<TARGET_OBJECTS:_http-parser>)

target_link_libraries(_cassandra ch_contrib::zlib ch_contrib::minizip ch_contrib::sparsehash)
target_include_directories(_cassandra PRIVATE ${CMAKE_CURRENT_BINARY_DIR} ${INCLUDE_DIRS})
target_include_directories(_cassandra SYSTEM BEFORE PUBLIC ${CASS_INCLUDE_DIR})
target_compile_definitions(_cassandra PRIVATE CASS_BUILDING)
target_compile_definitions(_cassandra PRIVATE -DSPARSEHASH_HASH=std::hash -Dsparsehash=google)

target_link_libraries(_cassandra ch_contrib::uv)

if(CASS_USE_OPENSSL)
    target_link_libraries(_cassandra OpenSSL::SSL)
endif()

add_library(ch_contrib::cassandra ALIAS _cassandra)
