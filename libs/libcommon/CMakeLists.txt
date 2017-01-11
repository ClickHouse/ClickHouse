SET(REVISIONFILE ${CMAKE_CURRENT_BINARY_DIR}/src/revision.h)

ADD_CUSTOM_COMMAND(
    OUTPUT ${REVISIONFILE}
    COMMAND bash -f -v -x ${CMAKE_CURRENT_SOURCE_DIR}/src/create_revision.sh ${REVISIONFILE} ${USE_DBMS_TCP_PROTOCOL_VERSION_AS_CLICKHOUSE_REVISION}
)

set_source_files_properties(
	src/ClickHouseRevision.cpp PROPERTIES OBJECT_DEPENDS ${REVISIONFILE}
)

include_directories (${CMAKE_CURRENT_BINARY_DIR}/src/)

if (APPLE)
	if ("${CMAKE_SYSTEM_NAME}" STREQUAL "Darwin" AND NOT "${CMAKE_SYSTEM_VERSION}" VERSION_LESS "16.1.0")
		set (APPLE_SIERRA_OR_NEWER 1)
	else ()
		set (APPLE_SIERRA_OR_NEWER 0)
	endif ()
	configure_file (${CMAKE_CURRENT_SOURCE_DIR}/include/common/apple_version.h.in ${CMAKE_CURRENT_BINARY_DIR}/include/common/apple_version.h)
	add_library (apple_rt
		src/apple_rt.cpp
		include/common/apple_rt.h
	)
endif()

add_library (common
	src/DateLUT.cpp
	src/DateLUTImpl.cpp
	src/exp10.cpp
	src/ClickHouseRevision.cpp
	src/JSON.cpp

	include/common/ApplicationServerExt.h
	include/common/Common.h
	include/common/DateLUT.h
	include/common/DateLUTImpl.h
	include/common/LocalDate.h
	include/common/LocalDateTime.h
	include/common/ErrorHandlers.h
	include/common/exp10.h
	include/common/likely.h
	include/common/logger_useful.h
	include/common/MultiVersion.h
	include/common/ClickHouseRevision.h
	include/common/singleton.h
	include/common/strong_typedef.h
	include/common/JSON.h

	include/ext/bit_cast.hpp
	include/ext/collection_cast.hpp
	include/ext/enumerate.hpp
	include/ext/function_traits.hpp
	include/ext/identity.hpp
	include/ext/map.hpp
	include/ext/range.hpp
	include/ext/scope_guard.hpp
	include/ext/size.hpp
	include/ext/unlock_guard.hpp

	${REVISIONFILE}
)

# When testing for memory leaks, disable libtcmalloc.
if (ENABLE_LIBTCMALLOC)
	if (DEBUG_LIBTCMALLOC)
		message (STATUS "Link libtcmalloc_minimal_debug for testing")
		set (MALLOC_LIBRARIES libtcmalloc_minimal_debug.a)
	else ()
		message (STATUS "Link libtcmalloc_minimal")
		set (MALLOC_LIBRARIES tcmalloc_minimal_internal)
	endif ()
else ()
	message (STATUS "Disabling libtcmalloc for valgrind better analysis")
endif ()

include (${ClickHouse_SOURCE_DIR}/cmake/find_glib.cmake)

find_package (Threads)

target_link_libraries (
	common
	pocoext
	${MALLOC_LIBRARIES}
	${CMAKE_THREAD_LIBS_INIT}
	${GLIB_LIBS}
	${ICU_LIBS}
	${RT_LIBRARIES})

if (ENABLE_TESTS)
	add_subdirectory (src/tests)
endif (ENABLE_TESTS)
