set (ENABLE_LIBURING_DEFAULT ${ENABLE_LIBRARIES})

if (NOT OS_LINUX)
    set (ENABLE_LIBURING_DEFAULT OFF)
endif ()

option (ENABLE_LIBURING "Enable liburing" ${ENABLE_LIBURING_DEFAULT})

if (NOT ENABLE_LIBURING)
    message(STATUS "Not using liburing")
    return()
endif()

set (LIBURING_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/liburing/src/include")
set (LIBURING_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/liburing/src")

set (SRCS
    "${LIBURING_SOURCE_DIR}/queue.c"
    "${LIBURING_SOURCE_DIR}/register.c"
    "${LIBURING_SOURCE_DIR}/setup.c"
    "${LIBURING_SOURCE_DIR}/syscall.c"
)

set (LIBURING_COMPAT_INCLUDE_DIR "${ClickHouse_BINARY_DIR}/contrib/liburing/src/include-compat")
set (LIBURING_COMPAT_HEADER "${LIBURING_COMPAT_INCLUDE_DIR}/liburing/compat.h")

include(CheckCSourceCompiles)

# The below checks should run against the bundled contrib/libc-headers
set (CMAKE_REQUIRED_INCLUDES ${CMAKE_C_STANDARD_INCLUDE_DIRECTORIES})

check_c_source_compiles ("
#include <linux/fs.h>
int main(int argc, char **argv)
{
  __kernel_rwf_t x;
  x = 0;
  return x;
}" LIBURING_CONFIG_HAS_KERNEL_RWF_T)

check_c_source_compiles ("
#include <linux/time.h>
#include <linux/time_types.h>
int main(int argc, char **argv)
{
  struct __kernel_timespec ts;
  ts.tv_sec = 0;
  ts.tv_nsec = 1;
  return 0;
}" LIBURING_CONFIG_HAS_KERNEL_TIMESPEC)

check_c_source_compiles ("
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
int main(int argc, char **argv)
{
  struct open_how how;
  how.flags = 0;
  how.mode = 0;
  how.resolve = 0;
  return 0;
}" LIBURING_CONFIG_HAS_OPEN_HOW)

check_c_source_compiles ("
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <linux/stat.h>
int main(int argc, char **argv)
{
  struct statx x;
  return memset(&x, 0, sizeof(x)) != NULL;
}" LIBURING_CONFIG_HAS_STATX)

check_c_source_compiles ("
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <linux/stat.h>
int main(int argc, char **argv)
{
  struct statx x;
  return memset(&x, 0, sizeof(x)) != NULL;
}" LIBURING_CONFIG_HAS_GLIBC_STATX)

unset (CMAKE_REQUIRED_INCLUDES)

add_compile_definitions(LIBURING_INTERNAL)
configure_file (compat.h.in ${LIBURING_COMPAT_HEADER})

add_library (_liburing ${SRCS})
add_library (ch_contrib::liburing ALIAS _liburing)

target_include_directories (_liburing PUBLIC ${LIBURING_COMPAT_INCLUDE_DIR} "${LIBURING_SOURCE_DIR}/include")
