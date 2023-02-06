set(LIBNAME "Foundation")
set(POCO_LIBNAME "Poco${LIBNAME}")

# Sources
file(GLOB SRCS_G "src/*.cpp")
POCO_SOURCES_AUTO( SRCS ${SRCS_G})

# Headers
file(GLOB_RECURSE HDRS_G "include/*.h" )
POCO_HEADERS_AUTO( SRCS ${HDRS_G})

# Platform Specific
POCO_SOURCES_AUTO_PLAT( SRCS UNIX src/SyslogChannel.cpp )
POCO_HEADERS_AUTO( SRCS include/Poco/SyslogChannel.h )

# For Windows CE we need to disable these
if(WINCE)
POCO_SOURCES_AUTO_PLAT( SRCS OFF
	src/WindowsConsoleChannel.cpp
	src/EventLogChannel.cpp
	)
else()
POCO_SOURCES_AUTO_PLAT( SRCS WIN32
	src/WindowsConsoleChannel.cpp
	src/EventLogChannel.cpp
	)
endif()

# Version Resource
if(MSVC AND NOT POCO_STATIC)
    source_group("Resources" FILES ${CMAKE_SOURCE_DIR}/DLLVersion.rc)
    list(APPEND SRCS ${CMAKE_SOURCE_DIR}/DLLVersion.rc)
endif()

# Messages
POCO_MESSAGES( SRCS Logging src/pocomsg.mc)

# If POCO_UNBUNDLED is enabled we try to find the required packages
# The configuration will fail if the packages are not found
option(POCO_UNBUNDLED_PCRE
  "Set to OFF|ON (default is OFF) to control linking pcre as external" ${POCO_UNBUNDLED})

find_library(LZ4_LIBRARY NAMES lz4)
set(SYSLIBS ${SYSLIBS} ${LZ4_LIBRARY})

if (POCO_UNBUNDLED_PCRE)
	find_package(PCRE REQUIRED)
	set(SYSLIBS ${SYSLIBS} ${PCRE_LIBRARIES})
	include_directories(${PCRE_INCLUDE_DIRS})

	add_definitions(-DPOCO_UNBUNDLED_PCRE)

	#HACK: Unicode.cpp requires functions from these files. The can't be taken from the library
	POCO_SOURCES( SRCS RegExp
		src/pcre_ucd.c
		src/pcre_tables.c
	)

else()
	# pcre
	POCO_SOURCES( SRCS pcre
		src/pcre_config.c
		src/pcre_byte_order.c
		src/pcre_chartables.c
		src/pcre_compile.c
		src/pcre_exec.c
		src/pcre_fullinfo.c
		src/pcre_globals.c
		src/pcre_maketables.c
		src/pcre_newline.c
		src/pcre_ord2utf8.c
		src/pcre_study.c
		src/pcre_tables.c
		src/pcre_dfa_exec.c
		src/pcre_get.c
		src/pcre_jit_compile.c
		src/pcre_refcount.c
		src/pcre_string_utils.c
		src/pcre_version.c
		src/pcre_ucd.c
		src/pcre_valid_utf8.c
		src/pcre_xclass.c
	)
endif (POCO_UNBUNDLED_PCRE)

option(POCO_UNBUNDLED_ZLIB
  "Set to OFF|ON (default is OFF) to control linking zlib as external" ${POCO_UNBUNDLED})

if (POCO_UNBUNDLED_ZLIB)
	find_package(ZLIB)
	set(SYSLIBS ${SYSLIBS} ${ZLIB_LIBRARIES})
else()

	# zlib
	POCO_HEADERS( SRCS zlib
		include/Poco/zconf.h
		include/Poco/zlib.h
		)

	POCO_SOURCES( SRCS zlib
		src/adler32.c
		src/compress.c
		src/crc32.c
		src/deflate.c
		src/infback.c
		src/inffast.c
		src/inflate.c
		src/inftrees.c
		src/trees.c
		src/zutil.c
	)
endif (POCO_UNBUNDLED_ZLIB)

if(WIN32)
	set(SYSLIBS ${SYSLIBS} iphlpapi)
endif(WIN32)
if (${CMAKE_CXX_COMPILER_ID} MATCHES "SunPro")
	set_target_properties( "${LIBNAME}" PROPERTIES LINK_FLAGS "-library=stlport4")
endif (${CMAKE_CXX_COMPILER_ID} MATCHES "SunPro")

if(ANDROID)
	set(SYSLIBS ${SYSLIBS} log)
endif(ANDROID)

# TODO: Why is this here?
add_definitions( -DPCRE_STATIC)

# For SetAffinity
if(UNIX AND NOT APPLE)
	INCLUDE (CheckFunctionExists)
	INCLUDE (CheckCXXSourceCompiles)
	INCLUDE (CheckLibraryExists)
	CHECK_LIBRARY_EXISTS(pthread pthread_setaffinity_np "pthread.h" HAVE_PTHREAD_SETAFFINITY_NP)
	#set(CMAKE_EXTRA_INCLUDE_FILES pthread.h)
	#CHECK_FUNCTION_EXISTS(pthread_setaffinity_np HAVE_PTHREAD_SETAFFINITY_NP)
	if(NOT HAVE_PTHREAD_SETAFFINITY_NP)
		message(STATUS "Platform has not PTHREAD_SETAFFINITY_NP")
	else(HAVE_PTHREAD_SETAFFINITY_NP)
		add_definitions(-DHAVE_PTHREAD_SETAFFINITY_NP)
		CHECK_CXX_SOURCE_COMPILES("
		#include <sched.h>
		int main() {
			cpu_set_t cpumask;
			sched_setaffinity( 0, sizeof(cpumask), &cpumask );
			return 0;
		}" HAVE_THREE_PARAM_SCHED_SETAFFINITY)

		if(HAVE_THREE_PARAM_SCHED_SETAFFINITY)
			message(STATUS "Platform has THREE PARAM at PTHREAD_SETAFFINITY_NP")
			add_definitions(-DHAVE_THREE_PARAM_SCHED_SETAFFINITY)
		else(HAVE_THREE_PARAM_SCHED_SETAFFINITY)
			CHECK_CXX_SOURCE_COMPILES("
			#include <sched.h>
			int main() {
				cpu_set_t cpumask;
				sched_setaffinity( 0, &cpumask );
				return 0;
			}" HAVE_TWO_PARAM_SCHED_SETAFFINITY)

			if(HAVE_TWO_PARAM_SCHED_SETAFFINITY)
				message(STATUS "Platform has TWO PARAM at PTHREAD_SETAFFINITY_NP")
				add_definitions(-DHAVE_TWO_PARAM_SCHED_SETAFFINITY)
			endif(HAVE_TWO_PARAM_SCHED_SETAFFINITY)

		endif(HAVE_THREE_PARAM_SCHED_SETAFFINITY)
	endif(NOT HAVE_PTHREAD_SETAFFINITY_NP)
endif(UNIX AND NOT APPLE)

add_library( "${LIBNAME}" ${LIB_MODE} ${SRCS})
add_library( "${POCO_LIBNAME}" ALIAS "${LIBNAME}")
set_target_properties( "${LIBNAME}"
	PROPERTIES
	VERSION ${SHARED_LIBRARY_VERSION} SOVERSION ${SHARED_LIBRARY_VERSION}
	OUTPUT_NAME ${POCO_LIBNAME}
	DEFINE_SYMBOL Foundation_EXPORTS
	)

target_link_libraries( "${LIBNAME}" ${SYSLIBS})

# Add -lrt because it is required to use clock_getres on glibc before 2.17.
# (we have a "Compatibility check" in CI that checks older glibc.)
if(NOT APPLE)
    target_link_libraries( "${LIBNAME}" "-lrt")
endif()

target_include_directories( "${LIBNAME}"
	SYSTEM PUBLIC
		$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
		$<INSTALL_INTERFACE:include>
	PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
	)
target_compile_definitions("${LIBNAME}" PUBLIC ${LIB_MODE_DEFINITIONS})

if (POCO_UNBUNDLED_ZLIB)
    target_include_directories("${LIBNAME}" PUBLIC ${ZLIB_INCLUDE_DIRS})
    target_compile_definitions("${LIBNAME}" PUBLIC POCO_UNBUNDLED_ZLIB)
endif ()

POCO_INSTALL("${LIBNAME}")
POCO_GENERATE_PACKAGE("${LIBNAME}")

if (ENABLE_TESTS)
	add_subdirectory( samples )
	add_subdirectory( testsuite )
endif ()

