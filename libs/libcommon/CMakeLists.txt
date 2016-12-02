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
	src/ClickHouseRevision.cpp
	src/JSON.cpp

	include/common/ApplicationServerExt.h
	include/common/Common.h
	include/common/DateLUT.h
	include/common/DateLUTImpl.h
	include/common/LocalDate.h
	include/common/LocalDateTime.h
	include/common/ErrorHandlers.h
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

# TESTIRT-3687 DISABLE_LIBTCMALLOC - when testing for memory leaks, disable libtcmalloc
IF(DISABLE_LIBTCMALLOC)
	message(STATUS "Disabling libtcmalloc for valgrind better analysis")
ELSE(DISABLE_LIBTCMALLOC)
	IF(DEBUG_LIBTCMALLOC)
		find_library(LIBTCMALLOC_DEBUG libtcmalloc_minimal_debug.a tcmalloc_minimal_debug) # debug version of tcmalloc from package
		message(STATUS "Link libtcmalloc_minimal_debug for testing from ${LIBTCMALLOC_DEBUG}")
		SET(MALLOC_LIBRARIES ${LIBTCMALLOC_DEBUG})
	ELSE(DEBUG_LIBTCMALLOC)
		message(STATUS "Link libtcmalloc_minimal")
		SET(MALLOC_LIBRARIES tcmalloc_minimal_internal)
	ENDIF(DEBUG_LIBTCMALLOC)
ENDIF(DISABLE_LIBTCMALLOC)

if (APPLE)
	SET(RT_LIBRARIES "apple_rt")
else()
	SET(RT_LIBRARIES "librt.a")
endif()

set (GLIB_HINTS "/usr/local/opt/glib/lib")
find_library (GLIB_LIB libglib-2.0.a HINTS ${GLIB_HINTS})
if (APPLE)
	set (INTL_HINTS "/usr/local/opt/gettext/lib")
	find_library (INTL_LIB libintl.a HINTS ${INTL_HINTS})
	find_library (CORE_FOUNDATION_LIB CoreFoundation)
	find_library (CARBON_LIB Carbon)
	set (GLIB_LIBS ${GLIB_LIB} ${INTL_LIB} libiconv.a ${CORE_FOUNDATION_LIB} ${CARBON_LIB})
else ()
	set (GLIB_LIBS ${GLIB_LIB})
endif ()

target_link_libraries (
	common
	pocoext
	${MALLOC_LIBRARIES}
	pthread
	${GLIB_LIBS}
	${ICU_LIBS}
	${RT_LIBRARIES})

IF(TESTS)
	add_subdirectory (src/tests)
ENDIF(TESTS)
