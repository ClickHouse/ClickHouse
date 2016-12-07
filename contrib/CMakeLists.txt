set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-old-style-cast")

add_subdirectory (libcityhash)
add_subdirectory (liblz4)
add_subdirectory (libdouble-conversion)
add_subdirectory (libzstd)
add_subdirectory (libfarmhash)
add_subdirectory (libmetrohash)
add_subdirectory (libpoco)
add_subdirectory (libre2)
add_subdirectory (libzookeeper)

if (ENABLE_LIBTCMALLOC)
	add_subdirectory (libtcmalloc)
endif ()

if (NOT AARCH64)
	add_subdirectory (libcpuid)
endif ()
