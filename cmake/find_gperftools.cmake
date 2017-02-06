if (ENABLE_LIBTCMALLOC)
	if (UNBUNDLED OR NOT USE_INTERNAL_GPERFTOOLS_LIBRARY)
		find_package (Gperftools REQUIRED)
		include_directories (${GPERFTOOLS_INCLUDE_DIR})

		if (USE_STATIC_LIBRARIES AND CMAKE_SYSTEM MATCHES "FreeBSD")
		#if (USE_STATIC_LIBRARIES)
			find_library (UNWIND_LIBRARY unwind)
			find_library (LZMA_LIBRARY lzma)
			list (APPEND GPERFTOOLS_TCMALLOC ${UNWIND_LIBRARY} ${LZMA_LIBRARY})
		endif ()
	endif ()

	if (NOT (GPERFTOOLS_INCLUDE_DIR AND GPERFTOOLS_TCMALLOC))
		set(GPERFTOOLS_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/libtcmalloc/include/")
		set(GPERFTOOLS_TCMALLOC tcmalloc_minimal_internal)
		include_directories (BEFORE ${GPERFTOOLS_INCLUDE_DIR})
	endif ()

	set (USE_TCMALLOC 1)

	message(STATUS "Using tcmalloc=${USE_TCMALLOC}: ${GPERFTOOLS_INCLUDE_DIR} : ${GPERFTOOLS_TCMALLOC}")
endif ()
