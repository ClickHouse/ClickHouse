find_library(READLINE_LIB
	NAMES libreadline.a libreadline.so
	HINTS "/usr/local/opt/readline/lib")

find_library(LIBEDIT_LIB
	NAMES libedit.a libedit.so)

if(READLINE_LIB)
	include_directories("/usr/local/opt/readline/include")
	add_definitions(-D USE_READLINE)
	set(LINE_EDITING_LIBS ${READLINE_LIB} libtermcap.a)
	message(STATUS "Using line editing libraries: ${LINE_EDITING_LIBS}")
elseif(LIBEDIT_LIB)
	add_definitions(-D USE_LIBEDIT)
	set(LINE_EDITING_LIBS ${LIBEDIT_LIB} libcurses.a libtermcap.a)
	message(STATUS "Using line editing libraries: ${LINE_EDITING_LIBS}")
else()
	message(STATUS "Not using any library for line editing.")
endif()

add_library(clickhouse-client Client.cpp)
target_link_libraries (clickhouse-client dbms ${LINE_EDITING_LIBS} ${BOOST_PROGRAM_OPTIONS_LIB})
INSTALL(FILES config.xml DESTINATION /etc/clickhouse-client COMPONENT clickhouse-client)

add_library(clickhouse-benchmark Benchmark.cpp)
target_link_libraries (clickhouse-benchmark dbms ${BOOST_PROGRAM_OPTIONS_LIB})

IF(TESTS)
	add_subdirectory (tests)
ENDIF(TESTS)
