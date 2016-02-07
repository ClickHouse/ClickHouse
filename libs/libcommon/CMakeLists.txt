SET(REVISIONFILE ${CMAKE_CURRENT_BINARY_DIR}/src/revision.h)

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/src/create_revision.sh.cmake ${CMAKE_CURRENT_BINARY_DIR}/src/create_revision.sh)
ADD_CUSTOM_COMMAND(
    OUTPUT ${REVISIONFILE}
    COMMAND bash -f ${CMAKE_CURRENT_BINARY_DIR}/src/create_revision.sh
)

set_source_files_properties(
	src/Revision.cpp PROPERTIES OBJECT_DEPENDS ${REVISIONFILE}
)

include_directories (${CMAKE_CURRENT_BINARY_DIR}/src/)

add_library (common
	src/DateLUT.cpp
	src/DateLUTImpl.cpp
	src/Revision.cpp
	src/JSON.cpp

	include/common/ApplicationExt.h
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
	include/common/Revision.h
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
IF($ENV{DISABLE_LIBTCMALLOC})
	message(STATUS "Disabling libtcmalloc for valgrind better analysis")
	target_link_libraries (common pocoext
	    pthread libglib-2.0.a libicui18n.a libicuuc.a libicudata.a librt.a)
ELSE($ENV{DISABLE_LIBTCMALLOC})
	IF($ENV{DEBUG_LIBTCMALLOC})
		message(STATUS "Link libtcmalloc_minimal_debug for testing")
		target_link_libraries (common pocoext
			libtcmalloc_minimal_debug.a
			pthread libglib-2.0.a libicui18n.a libicuuc.a libicudata.a librt.a)
	ELSE($ENV{DEBUG_LIBTCMALLOC})
		message(STATUS "Link libtcmalloc_minimal")
		target_link_libraries (common pocoext
			libtcmalloc_minimal.a
			pthread libglib-2.0.a libicui18n.a libicuuc.a libicudata.a librt.a)
	ENDIF($ENV{DEBUG_LIBTCMALLOC})
ENDIF($ENV{DISABLE_LIBTCMALLOC})

IF(TESTS)
	add_subdirectory (src/tests)
ENDIF(TESTS)
