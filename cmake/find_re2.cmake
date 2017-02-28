#TODO: option (USE_INTERNAL_RE2_LIBRARY "Set to FALSE to use system re2 library instead of bundled" ${NOT_UNBUNDLED})
set (USE_INTERNAL_RE2_LIBRARY ON)

if (NOT USE_INTERNAL_RE2_LIBRARY)
	# TODO! re2_st
	find_library (RE2_LIBRARY re2)
	find_path (RE2_INCLUDE_DIR NAMES re2/re2.h PATHS ${RE2_INCLUDE_PATHS})
endif ()

if (RE2_LIBRARY AND RE2_INCLUDE_DIR)
	include_directories (${RE2_INCLUDE_DIR})
else ()
	set (USE_INTERNAL_RE2_LIBRARY 1)
	set (RE2_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/libre2")
	set (RE2_ST_INCLUDE_DIR "${ClickHouse_BINARY_DIR}/contrib/libre2")
	include_directories (BEFORE ${RE2_INCLUDE_DIR})
	include_directories (BEFORE ${RE2_ST_INCLUDE_DIR})
	set (RE2_LIBRARY re2)
	set (RE2_ST_LIBRARY re2_st)
endif ()

message (STATUS "Using re2: ${RE2_INCLUDE_DIR} : ${RE2_LIBRARY}; ${RE2_ST_INCLUDE_DIR} : ${RE2_ST_LIBRARY}")
