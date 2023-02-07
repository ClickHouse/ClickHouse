set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -w")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")

file (GLOB SRCS src/*.cpp)

add_library (_poco_mongodb ${SRCS})
add_library (Poco::MongoDB ALIAS _poco_mongodb)

target_include_directories (_poco_mongodb SYSTEM PUBLIC "include")
target_link_libraries (_poco_mongodb PUBLIC Poco::Net)
