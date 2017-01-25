set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-old-style-cast")

if (USE_INTERNAL_BOOST_LIBRARY)
	add_subdirectory (libboost)
endif ()

if (USE_INTERNAL_POCO_LIBRARY)
	add_subdirectory (libpoco)
endif ()

add_subdirectory (liblz4)
add_subdirectory (libzstd)
add_subdirectory (libre2)
add_subdirectory (libdouble-conversion)
add_subdirectory (libzookeeper)
add_subdirectory (libcityhash)
add_subdirectory (libfarmhash)
add_subdirectory (libmetrohash)
add_subdirectory (libzlib-ng)
add_subdirectory (libcctz)

if (ENABLE_LIBTCMALLOC AND USE_INTERNAL_GPERFTOOLS_LIBRARY)
	add_subdirectory (libtcmalloc)
endif ()

if (NOT AARCH64)
	add_subdirectory (libcpuid)
endif ()
