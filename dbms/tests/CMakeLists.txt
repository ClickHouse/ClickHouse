if (NOT ENABLE_TESTS)
	return()
endif()

message (STATUS "Tests are enabled")

# Run tests with "make check"
if (TARGET check)
	message (STATUS "Target check already exists")
else ()
	include (${ClickHouse_SOURCE_DIR}/cmake/add_check.cmake)
endif ()

add_subdirectory (unit_tests)