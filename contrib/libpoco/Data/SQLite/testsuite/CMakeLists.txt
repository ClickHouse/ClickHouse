set(TESTUNIT "${LIBNAME}-testrunner")

# Sources
file(GLOB SRCS_G "src/*.cpp")
POCO_SOURCES_AUTO( TEST_SRCS ${SRCS_G})

# Headers
file(GLOB_RECURSE HDRS_G "src/*.h" )
POCO_HEADERS_AUTO( TEST_SRCS ${HDRS_G})

POCO_SOURCES_AUTO_PLAT( TEST_SRCS OFF
    src/WinDriver.cpp
)

POCO_SOURCES_AUTO_PLAT( TEST_SRCS WINCE
    src/WinCEDriver.cpp
)

add_executable( ${TESTUNIT} ${TEST_SRCS} )
add_test(NAME ${LIBNAME} WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR} COMMAND ${TESTUNIT} -all)
target_link_libraries( ${TESTUNIT} PocoDataSQLite PocoData PocoFoundation CppUnit )
