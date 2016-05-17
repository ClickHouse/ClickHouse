find_library(READLINE_LIB
	NAMES libreadline.a libreadline.so)

find_library(LIBEDIT_LIB
	NAMES libedit.a libedit.so)

if(READLINE_LIB)
	add_definitions(-D USE_READLINE)
	set(LINE_EDITING_LIBS ${READLINE_LIB} libcurses.a libtermcap.a)
elseif(LIBEDIT_LIB)
	add_definitions(-D USE_LIBEDIT)
	set(LINE_EDITING_LIBS ${LIBEDIT_LIB} libcurses.a libtermcap.a)
endif()

add_executable(clickhouse-client Client.cpp)
add_executable(clickhouse-benchmark Benchmark.cpp)

target_link_libraries (clickhouse-client dbms ${LINE_EDITING_LIBS} libboost_program_options.a)
target_link_libraries (clickhouse-benchmark dbms libboost_program_options.a)

INSTALL(TARGETS clickhouse-client RUNTIME DESTINATION bin COMPONENT clickhouse-client)
INSTALL(FILES config.xml DESTINATION /etc/clickhouse-client  COMPONENT clickhouse-client)

INSTALL(TARGETS clickhouse-benchmark RUNTIME DESTINATION bin  COMPONENT clickhouse-benchmark)

IF(TESTS)
	add_subdirectory (tests)
ENDIF(TESTS)
