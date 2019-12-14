#***************************************************************************
#                                  _   _ ____  _
#  Project                     ___| | | |  _ \| |
#                             / __| | | | |_) | |
#                            | (__| |_| |  _ <| |___
#                             \___|\___/|_| \_\_____|
#
# Copyright (C) 1998 - 2019, Daniel Stenberg, <daniel@haxx.se>, et al.
#
# This software is licensed as described in the file COPYING, which
# you should have received as part of this distribution. The terms
# are also available at https://curl.haxx.se/docs/copyright.html.
#
# You may opt to use, copy, modify, merge, publish, distribute and/or sell
# copies of the Software, and permit persons to whom the Software is
# furnished to do so, under the terms of the COPYING file.
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTY OF ANY
# KIND, either express or implied.
#
###########################################################################

# NOTE:
# This file is shrinked and reworked version of original curl CMakeLists.txt
# Original file link https://github.com/curl/curl/blob/3b8bbbbd1609c638a3d3d0acb148a33dedb67be3/CMakeLists.txt
# If you need to update curl building you can find patch file in this directory
# and apply it to fresh original CMakeLists.txt file.
cmake_minimum_required(VERSION 3.0 FATAL_ERROR)

SET(CURL_SOURCE_DIR ${ClickHouse_SOURCE_DIR}/contrib/curl)
SET(CURL_LIBRARY_DIR ${CURL_SOURCE_DIR}/lib)
set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/CMake;${CMAKE_MODULE_PATH}")
# Disable status messages when perform checks
set(CMAKE_REQUIRED_QUIET)

include(Macros)
include(CMakeDependentOption)
include(CheckCCompilerFlag)

file(READ ${CURL_SOURCE_DIR}/include/curl/curlver.h CURL_VERSION_H_CONTENTS)
string(REGEX MATCH "#define LIBCURL_VERSION \"[^\"]*"
        CURL_VERSION ${CURL_VERSION_H_CONTENTS})
string(REGEX REPLACE "[^\"]+\"" "" CURL_VERSION ${CURL_VERSION})
string(REGEX MATCH "#define LIBCURL_VERSION_NUM 0x[0-9a-fA-F]+"
        CURL_VERSION_NUM ${CURL_VERSION_H_CONTENTS})
string(REGEX REPLACE "[^0]+0x" "" CURL_VERSION_NUM ${CURL_VERSION_NUM})

message(STATUS "Use curl version=[${CURL_VERSION}]")
set(OPERATING_SYSTEM "${CMAKE_SYSTEM_NAME}")
set(OS "\"${CMAKE_SYSTEM_NAME}\"")

option(PICKY_COMPILER "Enable picky compiler options" ON)
option(ENABLE_THREADED_RESOLVER "Set to ON to enable threaded DNS lookup" ON)

if(CMAKE_COMPILER_IS_GNUCC OR CMAKE_COMPILER_IS_CLANG)
    if(PICKY_COMPILER)
        foreach(_CCOPT -pedantic -Wall -W -Wpointer-arith -Wwrite-strings -Wunused -Wshadow -Winline -Wnested-externs -Wmissing-declarations -Wmissing-prototypes -Wno-long-long -Wfloat-equal -Wno-multichar -Wsign-compare -Wundef -Wno-format-nonliteral -Wendif-labels -Wstrict-prototypes -Wdeclaration-after-statement -Wstrict-aliasing=3 -Wcast-align -Wtype-limits -Wold-style-declaration -Wmissing-parameter-type -Wempty-body -Wclobbered -Wignored-qualifiers -Wconversion -Wno-sign-conversion -Wvla -Wdouble-promotion -Wno-system-headers -Wno-pedantic-ms-format)
            # surprisingly, CHECK_C_COMPILER_FLAG needs a new variable to store each new
            # test result in.
            check_c_compiler_flag(${_CCOPT} OPT${_CCOPT})
            if(OPT${_CCOPT})
                set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${_CCOPT}")
            endif()
        endforeach()
    endif()
endif()

# For debug libs and exes, add "-d" postfix
if(NOT DEFINED CMAKE_DEBUG_POSTFIX)
    set(CMAKE_DEBUG_POSTFIX "-d")
endif()

# initialize CURL_LIBS
set(CURL_LIBS "")

include(CurlSymbolHiding)

# Http only
set(CURL_DISABLE_FTP ON)
set(CURL_DISABLE_LDAP ON)
set(CURL_DISABLE_LDAPS ON)
set(CURL_DISABLE_TELNET ON)
set(CURL_DISABLE_DICT ON)
set(CURL_DISABLE_FILE ON)
set(CURL_DISABLE_TFTP ON)
set(CURL_DISABLE_RTSP ON)
set(CURL_DISABLE_POP3 ON)
set(CURL_DISABLE_IMAP ON)
set(CURL_DISABLE_SMTP ON)
set(CURL_DISABLE_GOPHER ON)

option(CURL_DISABLE_COOKIES "to disable cookies support" OFF)
mark_as_advanced(CURL_DISABLE_COOKIES)

option(CURL_DISABLE_CRYPTO_AUTH "to disable cryptographic authentication" OFF)
mark_as_advanced(CURL_DISABLE_CRYPTO_AUTH)

option(CURL_DISABLE_VERBOSE_STRINGS "to disable verbose strings" OFF)
mark_as_advanced(CURL_DISABLE_VERBOSE_STRINGS)

option(ENABLE_IPV6 "Define if you want to enable IPv6 support" ON)
mark_as_advanced(ENABLE_IPV6)

if(ENABLE_IPV6 AND NOT WIN32)
    include(CheckStructHasMember)
    check_struct_has_member("struct sockaddr_in6" sin6_addr "netinet/in.h"
            HAVE_SOCKADDR_IN6_SIN6_ADDR)
    check_struct_has_member("struct sockaddr_in6" sin6_scope_id "netinet/in.h"
            HAVE_SOCKADDR_IN6_SIN6_SCOPE_ID)
    if(NOT HAVE_SOCKADDR_IN6_SIN6_ADDR)
        message(WARNING "struct sockaddr_in6 not available, disabling IPv6 support")
        # Force the feature off as this name is used as guard macro...
        set(ENABLE_IPV6 OFF
                CACHE BOOL "Define if you want to enable IPv6 support" FORCE)
    endif()
endif()

curl_nroff_check()

# We need ansi c-flags, especially on HP
set(CMAKE_C_FLAGS "${CMAKE_ANSI_CFLAGS} ${CMAKE_C_FLAGS}")
set(CMAKE_REQUIRED_FLAGS ${CMAKE_ANSI_CFLAGS})

# Include all the necessary files for macros
include(CheckFunctionExists)
include(CheckIncludeFile)
include(CheckIncludeFiles)
include(CheckLibraryExists)
include(CheckSymbolExists)
include(CheckTypeSize)
include(CheckCSourceCompiles)

if(ENABLE_THREADED_RESOLVER)
    find_package(Threads REQUIRED)
    if(WIN32)
        set(USE_THREADS_WIN32 ON)
    else()
        set(USE_THREADS_POSIX ${CMAKE_USE_PTHREADS_INIT})
        set(HAVE_PTHREAD_H ${CMAKE_USE_PTHREADS_INIT})
    endif()
    set(CURL_LIBS ${CURL_LIBS} ${CMAKE_THREAD_LIBS_INIT})
endif()

# Check for all needed libraries
check_library_exists_concat("${CMAKE_DL_LIBS}" dlopen HAVE_LIBDL)
check_library_exists_concat("socket" connect      HAVE_LIBSOCKET)
check_library_exists("c" gethostbyname "" NOT_NEED_LIBNSL)

check_function_exists(gethostname HAVE_GETHOSTNAME)

# From cmake/find/ssl.cmake
if (OPENSSL_FOUND)
    set(SSL_ENABLED ON)
    set(USE_OPENSSL ON)

    list(APPEND CURL_LIBS ${OPENSSL_LIBRARIES})
    check_include_file("openssl/crypto.h" HAVE_OPENSSL_CRYPTO_H)
    check_include_file("openssl/err.h"    HAVE_OPENSSL_ERR_H)
    check_include_file("openssl/pem.h"    HAVE_OPENSSL_PEM_H)
    check_include_file("openssl/rsa.h"    HAVE_OPENSSL_RSA_H)
    check_include_file("openssl/ssl.h"    HAVE_OPENSSL_SSL_H)
    check_include_file("openssl/x509.h"   HAVE_OPENSSL_X509_H)
    check_include_file("openssl/rand.h"   HAVE_OPENSSL_RAND_H)
    check_symbol_exists(RAND_status "${CURL_INCLUDES}" HAVE_RAND_STATUS)
    check_symbol_exists(RAND_screen "${CURL_INCLUDES}" HAVE_RAND_SCREEN)
    check_symbol_exists(RAND_egd    "${CURL_INCLUDES}" HAVE_RAND_EGD)
endif()

# Check for idn
check_library_exists_concat("idn2" idn2_lookup_ul HAVE_LIBIDN2)

# Check for symbol dlopen (same as HAVE_LIBDL)
check_library_exists("${CURL_LIBS}" dlopen "" HAVE_DLOPEN)

# From /cmake/find/zlib.cmake
if (ZLIB_FOUND)
    set(HAVE_ZLIB_H ON)
    set(HAVE_LIBZ ON)
    set(USE_ZLIB ON)

    list(APPEND CURL_LIBS ${ZLIB_LIBRARIES})
endif()

option(ENABLE_UNIX_SOCKETS "Define if you want Unix domain sockets support" ON)
if(ENABLE_UNIX_SOCKETS)
    include(CheckStructHasMember)
    check_struct_has_member("struct sockaddr_un" sun_path "sys/un.h" USE_UNIX_SOCKETS)
else()
    unset(USE_UNIX_SOCKETS CACHE)
endif()

# CA handling
# Explicitly set to most common case
if (OPENSSL_FOUND)
    set(CURL_CA_BUNDLE "/etc/ssl/certs/ca-certificates.crt")
    set(CURL_CA_BUNDLE_SET TRUE CACHE BOOL "Path to the CA bundle has been set")
    set(CURL_CA_PATH "/etc/ssl/certs")
    set(CURL_CA_PATH_SET TRUE CACHE BOOL "Path to the CA bundle has been set")
endif()

check_include_file_concat("stdio.h"          HAVE_STDIO_H)
check_include_file_concat("inttypes.h"       HAVE_INTTYPES_H)
check_include_file_concat("sys/filio.h"      HAVE_SYS_FILIO_H)
check_include_file_concat("sys/ioctl.h"      HAVE_SYS_IOCTL_H)
check_include_file_concat("sys/param.h"      HAVE_SYS_PARAM_H)
check_include_file_concat("sys/poll.h"       HAVE_SYS_POLL_H)
check_include_file_concat("sys/resource.h"   HAVE_SYS_RESOURCE_H)
check_include_file_concat("sys/select.h"     HAVE_SYS_SELECT_H)
check_include_file_concat("sys/socket.h"     HAVE_SYS_SOCKET_H)
check_include_file_concat("sys/sockio.h"     HAVE_SYS_SOCKIO_H)
check_include_file_concat("sys/stat.h"       HAVE_SYS_STAT_H)
check_include_file_concat("sys/time.h"       HAVE_SYS_TIME_H)
check_include_file_concat("sys/types.h"      HAVE_SYS_TYPES_H)
check_include_file_concat("sys/uio.h"        HAVE_SYS_UIO_H)
check_include_file_concat("sys/un.h"         HAVE_SYS_UN_H)
check_include_file_concat("sys/utime.h"      HAVE_SYS_UTIME_H)
check_include_file_concat("sys/xattr.h"      HAVE_SYS_XATTR_H)
check_include_file_concat("alloca.h"         HAVE_ALLOCA_H)
check_include_file_concat("arpa/inet.h"      HAVE_ARPA_INET_H)
check_include_file_concat("arpa/tftp.h"      HAVE_ARPA_TFTP_H)
check_include_file_concat("assert.h"         HAVE_ASSERT_H)
check_include_file_concat("crypto.h"         HAVE_CRYPTO_H)
check_include_file_concat("des.h"            HAVE_DES_H)
check_include_file_concat("err.h"            HAVE_ERR_H)
check_include_file_concat("errno.h"          HAVE_ERRNO_H)
check_include_file_concat("fcntl.h"          HAVE_FCNTL_H)
check_include_file_concat("idn2.h"           HAVE_IDN2_H)
check_include_file_concat("ifaddrs.h"        HAVE_IFADDRS_H)
check_include_file_concat("io.h"             HAVE_IO_H)
check_include_file_concat("krb.h"            HAVE_KRB_H)
check_include_file_concat("libgen.h"         HAVE_LIBGEN_H)
check_include_file_concat("locale.h"         HAVE_LOCALE_H)
check_include_file_concat("net/if.h"         HAVE_NET_IF_H)
check_include_file_concat("netdb.h"          HAVE_NETDB_H)
check_include_file_concat("netinet/in.h"     HAVE_NETINET_IN_H)
check_include_file_concat("netinet/tcp.h"    HAVE_NETINET_TCP_H)

check_include_file_concat("pem.h"            HAVE_PEM_H)
check_include_file_concat("poll.h"           HAVE_POLL_H)
check_include_file_concat("pwd.h"            HAVE_PWD_H)
check_include_file_concat("rsa.h"            HAVE_RSA_H)
check_include_file_concat("setjmp.h"         HAVE_SETJMP_H)
check_include_file_concat("sgtty.h"          HAVE_SGTTY_H)
check_include_file_concat("signal.h"         HAVE_SIGNAL_H)
check_include_file_concat("ssl.h"            HAVE_SSL_H)
check_include_file_concat("stdbool.h"        HAVE_STDBOOL_H)
check_include_file_concat("stdint.h"         HAVE_STDINT_H)
check_include_file_concat("stdio.h"          HAVE_STDIO_H)
check_include_file_concat("stdlib.h"         HAVE_STDLIB_H)
check_include_file_concat("string.h"         HAVE_STRING_H)
check_include_file_concat("strings.h"        HAVE_STRINGS_H)
check_include_file_concat("stropts.h"        HAVE_STROPTS_H)
check_include_file_concat("termio.h"         HAVE_TERMIO_H)
check_include_file_concat("termios.h"        HAVE_TERMIOS_H)
check_include_file_concat("time.h"           HAVE_TIME_H)
check_include_file_concat("unistd.h"         HAVE_UNISTD_H)
check_include_file_concat("utime.h"          HAVE_UTIME_H)
check_include_file_concat("x509.h"           HAVE_X509_H)

check_include_file_concat("process.h"        HAVE_PROCESS_H)
check_include_file_concat("stddef.h"         HAVE_STDDEF_H)
check_include_file_concat("dlfcn.h"          HAVE_DLFCN_H)
check_include_file_concat("malloc.h"         HAVE_MALLOC_H)
check_include_file_concat("memory.h"         HAVE_MEMORY_H)
check_include_file_concat("netinet/if_ether.h" HAVE_NETINET_IF_ETHER_H)
check_include_file_concat("stdint.h"        HAVE_STDINT_H)
check_include_file_concat("sockio.h"        HAVE_SOCKIO_H)
check_include_file_concat("sys/utsname.h"   HAVE_SYS_UTSNAME_H)

check_type_size(size_t  SIZEOF_SIZE_T)
check_type_size(ssize_t  SIZEOF_SSIZE_T)
check_type_size("long long"  SIZEOF_LONG_LONG)
check_type_size("long"  SIZEOF_LONG)
check_type_size("short"  SIZEOF_SHORT)
check_type_size("int"  SIZEOF_INT)
check_type_size("__int64"  SIZEOF___INT64)
check_type_size("long double"  SIZEOF_LONG_DOUBLE)
check_type_size("time_t"  SIZEOF_TIME_T)
if(NOT HAVE_SIZEOF_SSIZE_T)
    if(SIZEOF_LONG EQUAL SIZEOF_SIZE_T)
        set(ssize_t long)
    endif()
    if(NOT ssize_t AND SIZEOF___INT64 EQUAL SIZEOF_SIZE_T)
        set(ssize_t __int64)
    endif()
endif()
# off_t is sized later, after the HAVE_FILE_OFFSET_BITS test

if(HAVE_SIZEOF_LONG_LONG)
    set(HAVE_LONGLONG 1)
    set(HAVE_LL 1)
endif()

find_file(RANDOM_FILE urandom /dev)
mark_as_advanced(RANDOM_FILE)

# Check for some functions that are used
if(HAVE_LIBWS2_32)
    set(CMAKE_REQUIRED_LIBRARIES ws2_32)
elseif(HAVE_LIBSOCKET)
    set(CMAKE_REQUIRED_LIBRARIES socket)
endif()

check_symbol_exists(basename      "${CURL_INCLUDES}" HAVE_BASENAME)
check_symbol_exists(socket        "${CURL_INCLUDES}" HAVE_SOCKET)
check_symbol_exists(select        "${CURL_INCLUDES}" HAVE_SELECT)
check_symbol_exists(poll          "${CURL_INCLUDES}" HAVE_POLL)
check_symbol_exists(strdup        "${CURL_INCLUDES}" HAVE_STRDUP)
check_symbol_exists(strstr        "${CURL_INCLUDES}" HAVE_STRSTR)
check_symbol_exists(strtok_r      "${CURL_INCLUDES}" HAVE_STRTOK_R)
check_symbol_exists(strftime      "${CURL_INCLUDES}" HAVE_STRFTIME)
check_symbol_exists(uname         "${CURL_INCLUDES}" HAVE_UNAME)
check_symbol_exists(strcasecmp    "${CURL_INCLUDES}" HAVE_STRCASECMP)
check_symbol_exists(stricmp       "${CURL_INCLUDES}" HAVE_STRICMP)
check_symbol_exists(strcmpi       "${CURL_INCLUDES}" HAVE_STRCMPI)
check_symbol_exists(strncmpi      "${CURL_INCLUDES}" HAVE_STRNCMPI)
check_symbol_exists(alarm         "${CURL_INCLUDES}" HAVE_ALARM)
if(NOT HAVE_STRNCMPI)
    set(HAVE_STRCMPI)
endif()
check_symbol_exists(gethostbyaddr "${CURL_INCLUDES}" HAVE_GETHOSTBYADDR)
check_symbol_exists(gethostbyaddr_r "${CURL_INCLUDES}" HAVE_GETHOSTBYADDR_R)
check_symbol_exists(gettimeofday  "${CURL_INCLUDES}" HAVE_GETTIMEOFDAY)
check_symbol_exists(inet_addr     "${CURL_INCLUDES}" HAVE_INET_ADDR)
check_symbol_exists(inet_ntoa     "${CURL_INCLUDES}" HAVE_INET_NTOA)
check_symbol_exists(inet_ntoa_r   "${CURL_INCLUDES}" HAVE_INET_NTOA_R)
check_symbol_exists(tcsetattr     "${CURL_INCLUDES}" HAVE_TCSETATTR)
check_symbol_exists(tcgetattr     "${CURL_INCLUDES}" HAVE_TCGETATTR)
check_symbol_exists(perror        "${CURL_INCLUDES}" HAVE_PERROR)
check_symbol_exists(closesocket   "${CURL_INCLUDES}" HAVE_CLOSESOCKET)
check_symbol_exists(setvbuf       "${CURL_INCLUDES}" HAVE_SETVBUF)
check_symbol_exists(sigsetjmp     "${CURL_INCLUDES}" HAVE_SIGSETJMP)
check_symbol_exists(getpass_r     "${CURL_INCLUDES}" HAVE_GETPASS_R)
check_symbol_exists(strlcat       "${CURL_INCLUDES}" HAVE_STRLCAT)
check_symbol_exists(getpwuid      "${CURL_INCLUDES}" HAVE_GETPWUID)
check_symbol_exists(getpwuid_r    "${CURL_INCLUDES}" HAVE_GETPWUID_R)
check_symbol_exists(geteuid       "${CURL_INCLUDES}" HAVE_GETEUID)
check_symbol_exists(usleep        "${CURL_INCLUDES}" HAVE_USLEEP)
check_symbol_exists(utime         "${CURL_INCLUDES}" HAVE_UTIME)
check_symbol_exists(gmtime_r      "${CURL_INCLUDES}" HAVE_GMTIME_R)
check_symbol_exists(localtime_r   "${CURL_INCLUDES}" HAVE_LOCALTIME_R)

check_symbol_exists(gethostbyname   "${CURL_INCLUDES}" HAVE_GETHOSTBYNAME)
check_symbol_exists(gethostbyname_r "${CURL_INCLUDES}" HAVE_GETHOSTBYNAME_R)

check_symbol_exists(signal        "${CURL_INCLUDES}" HAVE_SIGNAL_FUNC)
check_symbol_exists(SIGALRM       "${CURL_INCLUDES}" HAVE_SIGNAL_MACRO)
if(HAVE_SIGNAL_FUNC AND HAVE_SIGNAL_MACRO)
    set(HAVE_SIGNAL 1)
endif()
check_symbol_exists(uname          "${CURL_INCLUDES}" HAVE_UNAME)
check_symbol_exists(strtoll        "${CURL_INCLUDES}" HAVE_STRTOLL)
check_symbol_exists(_strtoi64      "${CURL_INCLUDES}" HAVE__STRTOI64)
check_symbol_exists(strerror_r     "${CURL_INCLUDES}" HAVE_STRERROR_R)
check_symbol_exists(siginterrupt   "${CURL_INCLUDES}" HAVE_SIGINTERRUPT)
check_symbol_exists(perror         "${CURL_INCLUDES}" HAVE_PERROR)
check_symbol_exists(fork           "${CURL_INCLUDES}" HAVE_FORK)
check_symbol_exists(getaddrinfo    "${CURL_INCLUDES}" HAVE_GETADDRINFO)
check_symbol_exists(freeaddrinfo   "${CURL_INCLUDES}" HAVE_FREEADDRINFO)
check_symbol_exists(freeifaddrs    "${CURL_INCLUDES}" HAVE_FREEIFADDRS)
check_symbol_exists(pipe           "${CURL_INCLUDES}" HAVE_PIPE)
check_symbol_exists(ftruncate      "${CURL_INCLUDES}" HAVE_FTRUNCATE)
check_symbol_exists(getprotobyname "${CURL_INCLUDES}" HAVE_GETPROTOBYNAME)
check_symbol_exists(getpeername    "${CURL_INCLUDES}" HAVE_GETPEERNAME)
check_symbol_exists(getsockname    "${CURL_INCLUDES}" HAVE_GETSOCKNAME)
check_symbol_exists(if_nametoindex "${CURL_INCLUDES}" HAVE_IF_NAMETOINDEX)
check_symbol_exists(getrlimit      "${CURL_INCLUDES}" HAVE_GETRLIMIT)
check_symbol_exists(setlocale      "${CURL_INCLUDES}" HAVE_SETLOCALE)
check_symbol_exists(setmode        "${CURL_INCLUDES}" HAVE_SETMODE)
check_symbol_exists(setrlimit      "${CURL_INCLUDES}" HAVE_SETRLIMIT)
check_symbol_exists(fcntl          "${CURL_INCLUDES}" HAVE_FCNTL)
check_symbol_exists(ioctl          "${CURL_INCLUDES}" HAVE_IOCTL)
check_symbol_exists(setsockopt     "${CURL_INCLUDES}" HAVE_SETSOCKOPT)
check_function_exists(mach_absolute_time HAVE_MACH_ABSOLUTE_TIME)

check_symbol_exists(fsetxattr "${CURL_INCLUDES}" HAVE_FSETXATTR)
if(HAVE_FSETXATTR)
    foreach(CURL_TEST HAVE_FSETXATTR_5 HAVE_FSETXATTR_6)
        curl_internal_test(${CURL_TEST})
    endforeach()
endif()

# sigaction and sigsetjmp are special. Use special mechanism for
# detecting those, but only if previous attempt failed.
if(HAVE_SIGNAL_H)
    check_symbol_exists(sigaction "signal.h" HAVE_SIGACTION)
endif()

if(NOT HAVE_SIGSETJMP)
    if(HAVE_SETJMP_H)
        check_symbol_exists(sigsetjmp "setjmp.h" HAVE_MACRO_SIGSETJMP)
        if(HAVE_MACRO_SIGSETJMP)
            set(HAVE_SIGSETJMP 1)
        endif()
    endif()
endif()

# If there is no stricmp(), do not allow LDAP to parse URLs
if(NOT HAVE_STRICMP)
    set(HAVE_LDAP_URL_PARSE 1)
endif()

# Do curl specific tests
foreach(CURL_TEST
        HAVE_FCNTL_O_NONBLOCK
        HAVE_IOCTLSOCKET
        HAVE_IOCTLSOCKET_CAMEL
        HAVE_IOCTLSOCKET_CAMEL_FIONBIO
        HAVE_IOCTLSOCKET_FIONBIO
        HAVE_IOCTL_FIONBIO
        HAVE_IOCTL_SIOCGIFADDR
        HAVE_SETSOCKOPT_SO_NONBLOCK
        HAVE_SOCKADDR_IN6_SIN6_SCOPE_ID
        TIME_WITH_SYS_TIME
        HAVE_O_NONBLOCK
        HAVE_GETHOSTBYADDR_R_5
        HAVE_GETHOSTBYADDR_R_7
        HAVE_GETHOSTBYADDR_R_8
        HAVE_GETHOSTBYADDR_R_5_REENTRANT
        HAVE_GETHOSTBYADDR_R_7_REENTRANT
        HAVE_GETHOSTBYADDR_R_8_REENTRANT
        HAVE_GETHOSTBYNAME_R_3
        HAVE_GETHOSTBYNAME_R_5
        HAVE_GETHOSTBYNAME_R_6
        HAVE_GETHOSTBYNAME_R_3_REENTRANT
        HAVE_GETHOSTBYNAME_R_5_REENTRANT
        HAVE_GETHOSTBYNAME_R_6_REENTRANT
        HAVE_IN_ADDR_T
        HAVE_BOOL_T
        STDC_HEADERS
        RETSIGTYPE_TEST
        HAVE_INET_NTOA_R_DECL
        HAVE_INET_NTOA_R_DECL_REENTRANT
        HAVE_GETADDRINFO
        HAVE_FILE_OFFSET_BITS
        HAVE_VARIADIC_MACROS_C99
        HAVE_VARIADIC_MACROS_GCC
        )
    curl_internal_test(${CURL_TEST})
endforeach()

if(HAVE_FILE_OFFSET_BITS)
    set(_FILE_OFFSET_BITS 64)
    set(CMAKE_REQUIRED_FLAGS "-D_FILE_OFFSET_BITS=64")
endif()
check_type_size("off_t"  SIZEOF_OFF_T)

# include this header to get the type
set(CMAKE_REQUIRED_INCLUDES "${CURL_SOURCE_DIR}/include")
set(CMAKE_EXTRA_INCLUDE_FILES "curl/system.h")
check_type_size("curl_off_t"  SIZEOF_CURL_OFF_T)
set(CMAKE_EXTRA_INCLUDE_FILES "")

foreach(CURL_TEST
        HAVE_GLIBC_STRERROR_R
        HAVE_POSIX_STRERROR_R
        )
    curl_internal_test(${CURL_TEST})
endforeach()

# Check for reentrant
foreach(CURL_TEST
        HAVE_GETHOSTBYADDR_R_5
        HAVE_GETHOSTBYADDR_R_7
        HAVE_GETHOSTBYADDR_R_8
        HAVE_GETHOSTBYNAME_R_3
        HAVE_GETHOSTBYNAME_R_5
        HAVE_GETHOSTBYNAME_R_6
        HAVE_INET_NTOA_R_DECL_REENTRANT)
    if(NOT ${CURL_TEST})
        if(${CURL_TEST}_REENTRANT)
            set(NEED_REENTRANT 1)
        endif()
    endif()
endforeach()

if(NEED_REENTRANT)
    foreach(CURL_TEST
            HAVE_GETHOSTBYADDR_R_5
            HAVE_GETHOSTBYADDR_R_7
            HAVE_GETHOSTBYADDR_R_8
            HAVE_GETHOSTBYNAME_R_3
            HAVE_GETHOSTBYNAME_R_5
            HAVE_GETHOSTBYNAME_R_6)
        set(${CURL_TEST} 0)
        if(${CURL_TEST}_REENTRANT)
            set(${CURL_TEST} 1)
        endif()
    endforeach()
endif()

if(HAVE_INET_NTOA_R_DECL_REENTRANT)
    set(HAVE_INET_NTOA_R_DECL 1)
    set(NEED_REENTRANT 1)
endif()

# Check clock_gettime(CLOCK_MONOTONIC, x) support
curl_internal_test(HAVE_CLOCK_GETTIME_MONOTONIC)

# Check compiler support of __builtin_available()
curl_internal_test(HAVE_BUILTIN_AVAILABLE)

# Some other minor tests

if(NOT HAVE_IN_ADDR_T)
    set(in_addr_t "unsigned long")
endif()

# Check for nonblocking
set(HAVE_DISABLED_NONBLOCKING 1)
if(HAVE_FIONBIO OR
        HAVE_IOCTLSOCKET OR
        HAVE_IOCTLSOCKET_CASE OR
        HAVE_O_NONBLOCK)
    set(HAVE_DISABLED_NONBLOCKING)
endif()

set(CURL_PULL_SYS_TYPES_H ${HAVE_SYS_TYPES_H})
set(CURL_PULL_SYS_SOCKET_H ${HAVE_SYS_SOCKET_H})
set(CURL_PULL_SYS_POLL_H ${HAVE_SYS_POLL_H})
set(CURL_PULL_STDINT_H ${HAVE_STDINT_H})
set(CURL_PULL_INTTYPES_H ${HAVE_INTTYPES_H})

include(CMake/OtherTests.cmake)

SET(LIB_VAUTH_CFILES
        "${CURL_LIBRARY_DIR}/vauth/vauth.c" "${CURL_LIBRARY_DIR}/vauth/cleartext.c" "${CURL_LIBRARY_DIR}/vauth/cram.c"
        "${CURL_LIBRARY_DIR}/vauth/digest.c" "${CURL_LIBRARY_DIR}/vauth/digest_sspi.c" "${CURL_LIBRARY_DIR}/vauth/krb5_gssapi.c"
        "${CURL_LIBRARY_DIR}/vauth/krb5_sspi.c" "${CURL_LIBRARY_DIR}/vauth/ntlm.c" "${CURL_LIBRARY_DIR}/vauth/ntlm_sspi.c" "${CURL_LIBRARY_DIR}/vauth/oauth2.c"
        "${CURL_LIBRARY_DIR}/vauth/spnego_gssapi.c" "${CURL_LIBRARY_DIR}/vauth/spnego_sspi.c")

SET(LIB_VAUTH_HFILES "${CURL_LIBRARY_DIR}/vauth/vauth.h" "${CURL_LIBRARY_DIR}/vauth/digest.h" "${CURL_LIBRARY_DIR}/vauth/ntlm.h")

SET(LIB_VTLS_CFILES "${CURL_LIBRARY_DIR}/vtls/openssl.c" "${CURL_LIBRARY_DIR}/vtls/gtls.c" "${CURL_LIBRARY_DIR}/vtls/vtls.c" "${CURL_LIBRARY_DIR}/vtls/nss.c"
        "${CURL_LIBRARY_DIR}/vtls/polarssl.c" "${CURL_LIBRARY_DIR}/vtls/polarssl_threadlock.c"
        "${CURL_LIBRARY_DIR}/vtls/wolfssl.c" "${CURL_LIBRARY_DIR}/vtls/schannel.c" "${CURL_LIBRARY_DIR}/vtls/schannel_verify.c"
        "${CURL_LIBRARY_DIR}/vtls/sectransp.c" "${CURL_LIBRARY_DIR}/vtls/gskit.c" "${CURL_LIBRARY_DIR}/vtls/mbedtls.c" "${CURL_LIBRARY_DIR}/vtls/mesalink.c"
        "${CURL_LIBRARY_DIR}/vtls/bearssl.c")

SET(LIB_VTLS_HFILES "${CURL_LIBRARY_DIR}/vtls/openssl.h" "${CURL_LIBRARY_DIR}/vtls/vtls.h" "${CURL_LIBRARY_DIR}/vtls/gtls.h"
        "${CURL_LIBRARY_DIR}/vtls/nssg.h" "${CURL_LIBRARY_DIR}/vtls/polarssl.h" "${CURL_LIBRARY_DIR}/vtls/polarssl_threadlock.h"
        "${CURL_LIBRARY_DIR}/vtls/wolfssl.h" "${CURL_LIBRARY_DIR}/vtls/schannel.h" "${CURL_LIBRARY_DIR}/vtls/sectransp.h" "${CURL_LIBRARY_DIR}/vtls/gskit.h"
        "${CURL_LIBRARY_DIR}/vtls/mbedtls.h" "${CURL_LIBRARY_DIR}/vtls/mesalink.h" "${CURL_LIBRARY_DIR}/vtls/bearssl.h")

SET(LIB_VQUIC_CFILES "${CURL_LIBRARY_DIR}/vquic/ngtcp2.c" "${CURL_LIBRARY_DIR}/vquic/quiche.c")

SET(LIB_VQUIC_HFILES "${CURL_LIBRARY_DIR}/vquic/ngtcp2.h" "${CURL_LIBRARY_DIR}/vquic/quiche.h")

SET(LIB_VSSH_CFILES "${CURL_LIBRARY_DIR}/vssh/libssh2.c" "${CURL_LIBRARY_DIR}/vssh/libssh.c")

SET(LIB_VSSH_HFILES "${CURL_LIBRARY_DIR}/vssh/ssh.h")

SET(LIB_CFILES "${CURL_LIBRARY_DIR}/file.c"
        "${CURL_LIBRARY_DIR}/timeval.c" "${CURL_LIBRARY_DIR}/base64.c" "${CURL_LIBRARY_DIR}/hostip.c" "${CURL_LIBRARY_DIR}/progress.c" "${CURL_LIBRARY_DIR}/formdata.c"
        "${CURL_LIBRARY_DIR}/cookie.c" "${CURL_LIBRARY_DIR}/http.c" "${CURL_LIBRARY_DIR}/sendf.c" "${CURL_LIBRARY_DIR}/url.c" "${CURL_LIBRARY_DIR}/dict.c" "${CURL_LIBRARY_DIR}/if2ip.c" "${CURL_LIBRARY_DIR}/speedcheck.c"
        "${CURL_LIBRARY_DIR}/ldap.c" "${CURL_LIBRARY_DIR}/version.c" "${CURL_LIBRARY_DIR}/getenv.c" "${CURL_LIBRARY_DIR}/escape.c" "${CURL_LIBRARY_DIR}/mprintf.c" "${CURL_LIBRARY_DIR}/telnet.c" "${CURL_LIBRARY_DIR}/netrc.c"
        "${CURL_LIBRARY_DIR}/getinfo.c" "${CURL_LIBRARY_DIR}/transfer.c" "${CURL_LIBRARY_DIR}/strcase.c" "${CURL_LIBRARY_DIR}/easy.c" "${CURL_LIBRARY_DIR}/security.c" "${CURL_LIBRARY_DIR}/curl_fnmatch.c"
        "${CURL_LIBRARY_DIR}/fileinfo.c"  "${CURL_LIBRARY_DIR}/wildcard.c" "${CURL_LIBRARY_DIR}/krb5.c" "${CURL_LIBRARY_DIR}/memdebug.c" "${CURL_LIBRARY_DIR}/http_chunks.c"
        "${CURL_LIBRARY_DIR}/strtok.c" "${CURL_LIBRARY_DIR}/connect.c" "${CURL_LIBRARY_DIR}/llist.c" "${CURL_LIBRARY_DIR}/hash.c" "${CURL_LIBRARY_DIR}/multi.c" "${CURL_LIBRARY_DIR}/content_encoding.c" "${CURL_LIBRARY_DIR}/share.c"
        "${CURL_LIBRARY_DIR}/http_digest.c" "${CURL_LIBRARY_DIR}/md4.c" "${CURL_LIBRARY_DIR}/md5.c" "${CURL_LIBRARY_DIR}/http_negotiate.c" "${CURL_LIBRARY_DIR}/inet_pton.c" "${CURL_LIBRARY_DIR}/strtoofft.c"
        "${CURL_LIBRARY_DIR}/strerror.c" "${CURL_LIBRARY_DIR}/amigaos.c" "${CURL_LIBRARY_DIR}/hostasyn.c" "${CURL_LIBRARY_DIR}/hostip4.c" "${CURL_LIBRARY_DIR}/hostip6.c" "${CURL_LIBRARY_DIR}/hostsyn.c"
        "${CURL_LIBRARY_DIR}/inet_ntop.c" "${CURL_LIBRARY_DIR}/parsedate.c" "${CURL_LIBRARY_DIR}/select.c"  "${CURL_LIBRARY_DIR}/splay.c" "${CURL_LIBRARY_DIR}/strdup.c" "${CURL_LIBRARY_DIR}/socks.c"
        "${CURL_LIBRARY_DIR}/curl_addrinfo.c" "${CURL_LIBRARY_DIR}/socks_gssapi.c" "${CURL_LIBRARY_DIR}/socks_sspi.c"
        "${CURL_LIBRARY_DIR}/curl_sspi.c" "${CURL_LIBRARY_DIR}/slist.c" "${CURL_LIBRARY_DIR}/nonblock.c" "${CURL_LIBRARY_DIR}/curl_memrchr.c" "${CURL_LIBRARY_DIR}/imap.c" "${CURL_LIBRARY_DIR}/pop3.c" "${CURL_LIBRARY_DIR}/smtp.c"
        "${CURL_LIBRARY_DIR}/pingpong.c" "${CURL_LIBRARY_DIR}/rtsp.c" "${CURL_LIBRARY_DIR}/curl_threads.c" "${CURL_LIBRARY_DIR}/warnless.c" "${CURL_LIBRARY_DIR}/hmac.c" "${CURL_LIBRARY_DIR}/curl_rtmp.c"
        "${CURL_LIBRARY_DIR}/openldap.c" "${CURL_LIBRARY_DIR}/curl_gethostname.c" "${CURL_LIBRARY_DIR}/gopher.c" "${CURL_LIBRARY_DIR}/idn_win32.c"
        "${CURL_LIBRARY_DIR}/http_proxy.c" "${CURL_LIBRARY_DIR}/non-ascii.c" "${CURL_LIBRARY_DIR}/asyn-ares.c" "${CURL_LIBRARY_DIR}/asyn-thread.c" "${CURL_LIBRARY_DIR}/curl_gssapi.c"
        "${CURL_LIBRARY_DIR}/http_ntlm.c" "${CURL_LIBRARY_DIR}/curl_ntlm_wb.c" "${CURL_LIBRARY_DIR}/curl_ntlm_core.c" "${CURL_LIBRARY_DIR}/curl_sasl.c" "${CURL_LIBRARY_DIR}/rand.c"
        "${CURL_LIBRARY_DIR}/curl_multibyte.c" "${CURL_LIBRARY_DIR}/hostcheck.c" "${CURL_LIBRARY_DIR}/conncache.c" "${CURL_LIBRARY_DIR}/dotdot.c"
        "${CURL_LIBRARY_DIR}/x509asn1.c" "${CURL_LIBRARY_DIR}/http2.c" "${CURL_LIBRARY_DIR}/smb.c" "${CURL_LIBRARY_DIR}/curl_endian.c" "${CURL_LIBRARY_DIR}/curl_des.c" "${CURL_LIBRARY_DIR}/system_win32.c"
        "${CURL_LIBRARY_DIR}/mime.c" "${CURL_LIBRARY_DIR}/sha256.c" "${CURL_LIBRARY_DIR}/setopt.c" "${CURL_LIBRARY_DIR}/curl_path.c" "${CURL_LIBRARY_DIR}/curl_ctype.c" "${CURL_LIBRARY_DIR}/curl_range.c" "${CURL_LIBRARY_DIR}/psl.c"
        "${CURL_LIBRARY_DIR}/doh.c" "${CURL_LIBRARY_DIR}/urlapi.c" "${CURL_LIBRARY_DIR}/curl_get_line.c" "${CURL_LIBRARY_DIR}/altsvc.c" "${CURL_LIBRARY_DIR}/socketpair.c")

SET(LIB_HFILES "${CURL_LIBRARY_DIR}/arpa_telnet.h" "${CURL_LIBRARY_DIR}/netrc.h" "${CURL_LIBRARY_DIR}/file.h" "${CURL_LIBRARY_DIR}/timeval.h" "${CURL_LIBRARY_DIR}/hostip.h" "${CURL_LIBRARY_DIR}/progress.h"
        "${CURL_LIBRARY_DIR}/formdata.h" "${CURL_LIBRARY_DIR}/cookie.h" "${CURL_LIBRARY_DIR}/http.h" "${CURL_LIBRARY_DIR}/sendf.h" "${CURL_LIBRARY_DIR}/url.h" "${CURL_LIBRARY_DIR}/dict.h" "${CURL_LIBRARY_DIR}/if2ip.h"
        "${CURL_LIBRARY_DIR}/speedcheck.h" "${CURL_LIBRARY_DIR}/urldata.h" "${CURL_LIBRARY_DIR}/curl_ldap.h" "${CURL_LIBRARY_DIR}/escape.h" "${CURL_LIBRARY_DIR}/telnet.h" "${CURL_LIBRARY_DIR}/getinfo.h"
        "${CURL_LIBRARY_DIR}/strcase.h" "${CURL_LIBRARY_DIR}/curl_sec.h" "${CURL_LIBRARY_DIR}/memdebug.h" "${CURL_LIBRARY_DIR}/http_chunks.h" "${CURL_LIBRARY_DIR}/curl_fnmatch.h"
        "${CURL_LIBRARY_DIR}/wildcard.h" "${CURL_LIBRARY_DIR}/fileinfo.h"  "${CURL_LIBRARY_DIR}/strtok.h" "${CURL_LIBRARY_DIR}/connect.h" "${CURL_LIBRARY_DIR}/llist.h"
        "${CURL_LIBRARY_DIR}/hash.h" "${CURL_LIBRARY_DIR}/content_encoding.h" "${CURL_LIBRARY_DIR}/share.h" "${CURL_LIBRARY_DIR}/curl_md4.h" "${CURL_LIBRARY_DIR}/curl_md5.h" "${CURL_LIBRARY_DIR}/http_digest.h"
        "${CURL_LIBRARY_DIR}/http_negotiate.h" "${CURL_LIBRARY_DIR}/inet_pton.h" "${CURL_LIBRARY_DIR}/amigaos.h" "${CURL_LIBRARY_DIR}/strtoofft.h" "${CURL_LIBRARY_DIR}/strerror.h"
        "${CURL_LIBRARY_DIR}/inet_ntop.h" "${CURL_LIBRARY_DIR}/curlx.h" "${CURL_LIBRARY_DIR}/curl_memory.h" "${CURL_LIBRARY_DIR}/curl_setup.h" "${CURL_LIBRARY_DIR}/transfer.h" "${CURL_LIBRARY_DIR}/select.h"
        "${CURL_LIBRARY_DIR}/easyif.h" "${CURL_LIBRARY_DIR}/multiif.h" "${CURL_LIBRARY_DIR}/parsedate.h" "${CURL_LIBRARY_DIR}/sockaddr.h" "${CURL_LIBRARY_DIR}/splay.h" "${CURL_LIBRARY_DIR}/strdup.h"
        "${CURL_LIBRARY_DIR}/socks.h" "${CURL_LIBRARY_DIR}/curl_base64.h" "${CURL_LIBRARY_DIR}/curl_addrinfo.h" "${CURL_LIBRARY_DIR}/curl_sspi.h"
        "${CURL_LIBRARY_DIR}/slist.h" "${CURL_LIBRARY_DIR}/nonblock.h" "${CURL_LIBRARY_DIR}/curl_memrchr.h" "${CURL_LIBRARY_DIR}/imap.h" "${CURL_LIBRARY_DIR}/pop3.h" "${CURL_LIBRARY_DIR}/smtp.h" "${CURL_LIBRARY_DIR}/pingpong.h"
        "${CURL_LIBRARY_DIR}/rtsp.h" "${CURL_LIBRARY_DIR}/curl_threads.h" "${CURL_LIBRARY_DIR}/warnless.h" "${CURL_LIBRARY_DIR}/curl_hmac.h" "${CURL_LIBRARY_DIR}/curl_rtmp.h"
        "${CURL_LIBRARY_DIR}/curl_gethostname.h" "${CURL_LIBRARY_DIR}/gopher.h" "${CURL_LIBRARY_DIR}/http_proxy.h" "${CURL_LIBRARY_DIR}/non-ascii.h" "${CURL_LIBRARY_DIR}/asyn.h"
        "${CURL_LIBRARY_DIR}/http_ntlm.h" "${CURL_LIBRARY_DIR}/curl_gssapi.h" "${CURL_LIBRARY_DIR}/curl_ntlm_wb.h" "${CURL_LIBRARY_DIR}/curl_ntlm_core.h"
        "${CURL_LIBRARY_DIR}/curl_sasl.h" "${CURL_LIBRARY_DIR}/curl_multibyte.h" "${CURL_LIBRARY_DIR}/hostcheck.h" "${CURL_LIBRARY_DIR}/conncache.h"
        "${CURL_LIBRARY_DIR}/multihandle.h" "${CURL_LIBRARY_DIR}/setup-vms.h" "${CURL_LIBRARY_DIR}/dotdot.h"
        "${CURL_LIBRARY_DIR}/x509asn1.h" "${CURL_LIBRARY_DIR}/http2.h" "${CURL_LIBRARY_DIR}/sigpipe.h" "${CURL_LIBRARY_DIR}/smb.h" "${CURL_LIBRARY_DIR}/curl_endian.h" "${CURL_LIBRARY_DIR}/curl_des.h"
        "${CURL_LIBRARY_DIR}/curl_printf.h" "${CURL_LIBRARY_DIR}/system_win32.h" "${CURL_LIBRARY_DIR}/rand.h" "${CURL_LIBRARY_DIR}/mime.h" "${CURL_LIBRARY_DIR}/curl_sha256.h" "${CURL_LIBRARY_DIR}/setopt.h"
        "${CURL_LIBRARY_DIR}/curl_path.h" "${CURL_LIBRARY_DIR}/curl_ctype.h" "${CURL_LIBRARY_DIR}/curl_range.h" "${CURL_LIBRARY_DIR}/psl.h" "${CURL_LIBRARY_DIR}/doh.h" "${CURL_LIBRARY_DIR}/urlapi-int.h"
        "${CURL_LIBRARY_DIR}/curl_get_line.h" "${CURL_LIBRARY_DIR}/altsvc.h" "${CURL_LIBRARY_DIR}/quic.h" "${CURL_LIBRARY_DIR}/socketpair.h")

SET(LIB_RCFILES "${CURL_LIBRARY_DIR}/libcurl.rc")

SET(CSOURCES ${LIB_CFILES} ${LIB_VAUTH_CFILES} ${LIB_VTLS_CFILES}
        ${LIB_VQUIC_CFILES} ${LIB_VSSH_CFILES})
SET(HHEADERS ${LIB_HFILES} ${LIB_VAUTH_HFILES} ${LIB_VTLS_HFILES}
        ${LIB_VQUIC_HFILES} ${LIB_VSSH_HFILES})

configure_file(${CURL_SOURCE_DIR}/lib/curl_config.h.cmake
        ${CMAKE_CURRENT_BINARY_DIR}/curl/curl_config.h)

list(APPEND HHEADERS
        ${CMAKE_CURRENT_BINARY_DIR}/curl/curl_config.h
        )

add_library(libcurl ${HHEADERS} ${CSOURCES})

if(NOT BUILD_SHARED_LIBS)
    set_target_properties(libcurl PROPERTIES INTERFACE_COMPILE_DEFINITIONS CURL_STATICLIB)
endif()

if(HIDES_CURL_PRIVATE_SYMBOLS)
    set_property(TARGET libcurl APPEND PROPERTY COMPILE_DEFINITIONS "CURL_HIDDEN_SYMBOLS")
    set_property(TARGET libcurl APPEND PROPERTY COMPILE_FLAGS ${CURL_CFLAG_SYMBOLS_HIDE})
endif()

if(OPENSSL_FOUND)
    target_include_directories(libcurl PUBLIC ${OPENSSL_INCLUDE_DIR})
    message("-- Including openssl ${OPENSSL_INCLUDE_DIR} to curl")
endif()

if(ZLIB_FOUND)
    target_include_directories(libcurl PUBLIC ${ZLIB_INCLUDE_DIRS}})
    message("-- Including zlib ${ZLIB_INCLUDE_DIRS} to curl")
endif()

target_compile_definitions(libcurl PUBLIC -DHAVE_CONFIG_H)
target_compile_definitions(libcurl PUBLIC -DBUILDING_LIBCURL)
target_include_directories(libcurl PUBLIC "${CURL_SOURCE_DIR}/include" "${CURL_LIBRARY_DIR}" "${CMAKE_CURRENT_BINARY_DIR}/curl")

target_link_libraries(libcurl ${CURL_LIBS})
