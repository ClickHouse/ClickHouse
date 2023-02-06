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
if(ANDROID)
    add_test(NAME ${LIBNAME} WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} 
            COMMAND ${CMAKE_COMMAND} -DANDROID_NDK=${ANDROID_NDK} "-DTEST_FILES=${CMAKE_CURRENT_SOURCE_DIR}/any.pem;${CMAKE_CURRENT_SOURCE_DIR}/rootcert.pem;${CMAKE_CURRENT_SOURCE_DIR}/testrunner.xml" -DLIBRARY_DIR=${CMAKE_BINARY_DIR}/lib -DUNITTEST=${CMAKE_BINARY_DIR}/bin/${TESTUNIT} -DTEST_PARAMETER=-all -P ${CMAKE_SOURCE_DIR}/cmake/ExecuteOnAndroid.cmake)
    add_custom_command(TARGET ${TESTUNIT} POST_BUILD
            COMMAND ${CMAKE_COMMAND} -DANDROID_NDK=${ANDROID_NDK} -DFILES="${CMAKE_CURRENT_SOURCE_DIR}/any.pem" -P ${CMAKE_SOURCE_DIR}/cmake/PushOnAndroid.cmake
            COMMAND ${CMAKE_COMMAND} -DANDROID_NDK=${ANDROID_NDK} -DFILES="${CMAKE_CURRENT_SOURCE_DIR}/rootcert.pem" -P ${CMAKE_SOURCE_DIR}/cmake/PushOnAndroid.cmake
            COMMAND ${CMAKE_COMMAND} -DANDROID_NDK=${ANDROID_NDK} -DFILES="${CMAKE_CURRENT_SOURCE_DIR}/testrunner.xml" -P ${CMAKE_SOURCE_DIR}/cmake/PushOnAndroid.cmake)
else()
    add_test(NAME ${LIBNAME} WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY} COMMAND ${TESTUNIT} -all)
    # The test is run in the build directory. So the test data is copied there too
    add_custom_command(TARGET ${TESTUNIT} POST_BUILD
                   COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/any.pem ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
                   COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/rootcert.pem ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
                   COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/testrunner.xml ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${TESTUNIT}.xml
                   )
endif()
target_link_libraries( ${TESTUNIT} PocoNetSSL PocoCrypto PocoNet PocoUtil PocoXML PocoFoundation CppUnit)

