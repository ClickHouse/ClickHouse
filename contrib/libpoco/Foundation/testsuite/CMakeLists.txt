set(TESTUNIT "${LIBNAME}-testrunner")

# Sources
file(GLOB SRCS_G "src/*.cpp")
file(GLOB SRCS_G_REMOVE 
    src/TestApp.cpp
    src/TestApp_WINCE.cpp
    src/TestLibrary.cpp
    src/TestPlugin.cpp
)
list(REMOVE_ITEM SRCS_G ${SRCS_G_REMOVE})
POCO_SOURCES_AUTO( TEST_SRCS ${SRCS_G})

# Headers
file(GLOB_RECURSE HDRS_G "src/*.h" )
POCO_HEADERS_AUTO( TEST_SRCS ${HDRS_G})

# WinDriver depends on WinTestRunner which depends on MFC, and we don't want that
POCO_SOURCES_AUTO_PLAT( TEST_SRCS OFF
    src/WinDriver.cpp
)

POCO_SOURCES_AUTO_PLAT( TEST_SRCS WINCE
    src/WinCEDriver.cpp
)

add_executable( ${TESTUNIT} ${TEST_SRCS} )
add_test(NAME ${LIBNAME} WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND ${TESTUNIT} -all)
set_tests_properties(${LIBNAME} PROPERTIES ENVIRONMENT "LD_LIBRARY_PATH=.") # The SharedLibaryTest has to look for shared libraries in the working directory

target_link_libraries( ${TESTUNIT} PocoFoundation CppUnit )
if(UNIX)
    target_link_libraries( ${TESTUNIT} pthread)
endif(UNIX)

# The test is run in the runtime directory. So the test data is copied there too
add_custom_command(TARGET ${TESTUNIT} POST_BUILD
                   COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/data ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/data )

# TestApp
if(WINCE)
add_executable( TestApp src/TestApp_WINCE.cpp )
set_target_properties(TestApp PROPERTIES LINK_FLAGS "/ENTRY:wmainCRTStartup")
else()
add_executable( TestApp src/TestApp.cpp )
endif()
# The test is run in the runtime directory. So the TestApp is built there too because it is used by the tests
set_target_properties( TestApp PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} )
target_link_libraries( TestApp PocoFoundation )

if(NOT POCO_STATIC)
# TestLibrary
add_library( TestLibrary SHARED src/TestLibrary.cpp src/TestPlugin.cpp src/TestPlugin.h )
set_target_properties( TestLibrary PROPERTIES PREFIX "" DEBUG_POSTFIX "") # The test requires the library named TestLibrary. By default it is prefixed with lib.
# The test is run in the runtime directory. So the TestLibrary is built there too because it is used by the tests
set_target_properties( TestLibrary PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} )
target_link_libraries( TestLibrary PocoFoundation )
endif()

