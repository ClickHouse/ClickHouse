set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -w")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")

file (GLOB SRCS src/*.cpp)

add_library (_poco_data ${SRCS})
add_library (Poco::Data ALIAS _poco_data)

target_include_directories (_poco_data SYSTEM PUBLIC "include")
target_link_libraries (_poco_data PUBLIC Poco::Foundation)
