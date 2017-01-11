#
# On ubuntu 16.10 static link libanl.a fails with -fPIC error
#

include (CheckCXXSourceRuns)
find_package (Threads)

if (USE_STATIC_LIBRARIES)
	set (ANL_LIB_NAME "libanl.a")
else ()
	set(ANL_LIB_NAME "anl")
endif ()

find_library (ANL_LIB NAMES ${ANL_LIB_NAME})

# better use Threads::Threads but incompatible with cmake < 3
set (CMAKE_REQUIRED_LIBRARIES ${CMAKE_REQUIRED_LIBRARIES} ${ANL_LIB} ${CMAKE_THREAD_LIBS_INIT})

check_cxx_source_runs("
	#include <netdb.h>
	int main() {
		getaddrinfo_a(GAI_NOWAIT, nullptr, 0, nullptr);
		return 0;
	}
" HAVE_GETADDRINFO_A)

#message(STATUS "test_anl: USE_STATIC_LIBRARIES=${USE_STATIC_LIBRARIES} ANL_LIB_NAME=${ANL_LIB_NAME} ANL_LIB=${ANL_LIB} HAVE_GETADDRINFO_A=${HAVE_GETADDRINFO_A}")

if (HAVE_GETADDRINFO_A)
	add_definitions (-DHAVE_GETADDRINFO_A=1)
endif ()
