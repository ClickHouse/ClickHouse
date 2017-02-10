if (ENABLE_LIBTCMALLOC)
	#contrib/libtcmalloc doesnt build debug version, try find in system
	if (DEBUG_LIBTCMALLOC OR NOT USE_INTERNAL_GPERFTOOLS_LIBRARY)
		find_package (Gperftools REQUIRED)
		include_directories (${GPERFTOOLS_INCLUDE_DIR})
	endif ()

	if (NOT (GPERFTOOLS_INCLUDE_DIR AND GPERFTOOLS_TCMALLOC_MINIMAL))
		set (USE_INTERNAL_GPERFTOOLS_LIBRARY 1)
		set (GPERFTOOLS_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/libtcmalloc/include")
		set (GPERFTOOLS_TCMALLOC_MINIMAL tcmalloc_minimal_internal)
		include_directories (BEFORE ${GPERFTOOLS_INCLUDE_DIR})
	endif ()

	set (USE_TCMALLOC 1)

	message (STATUS "Using tcmalloc=${USE_TCMALLOC}: ${GPERFTOOLS_INCLUDE_DIR} : ${GPERFTOOLS_TCMALLOC_MINIMAL}")
endif ()
