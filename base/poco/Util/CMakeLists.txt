set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -w")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")

file (GLOB SRCS src/*.cpp)

add_library (_poco_util ${SRCS})
add_library (Poco::Util ALIAS _poco_util)

target_include_directories (_poco_util SYSTEM PUBLIC "include")
target_link_libraries (_poco_util PUBLIC Poco::JSON Poco::XML)
