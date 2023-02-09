file (GLOB SRCS src/*.cpp)

add_library (_poco_redis ${SRCS})
add_library (Poco::Redis ALIAS _poco_redis)

target_compile_options (_poco_redis PRIVATE -Wno-shadow)
target_include_directories (_poco_redis SYSTEM PUBLIC "include")
target_link_libraries (_poco_redis PUBLIC Poco::Net)
