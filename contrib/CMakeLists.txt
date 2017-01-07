set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-old-style-cast")

if (USE_INTERNAL_BOOST_LIBRARY)
	add_subdirectory (libboost)
endif ()

add_subdirectory (libpoco)
add_subdirectory (liblz4)
add_subdirectory (libzstd)
add_subdirectory (libre2)
add_subdirectory (libdouble-conversion)
add_subdirectory (libzookeeper)
add_subdirectory (libcityhash)
add_subdirectory (libfarmhash)
add_subdirectory (libmetrohash)

if (ENABLE_LIBTCMALLOC)
	add_subdirectory (libtcmalloc)
endif ()

if (NOT AARCH64)
	add_subdirectory (libcpuid)
endif ()
