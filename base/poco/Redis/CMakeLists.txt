set (SRCS
    src/Array.cpp
    src/AsyncReader.cpp
    src/Client.cpp
    src/Command.cpp
    src/Error.cpp
    src/Exception.cpp
    src/RedisEventArgs.cpp
    src/RedisStream.cpp
    src/Type.cpp
)

add_library (_poco_redis ${SRCS})
add_library (Poco::Redis ALIAS _poco_redis)

target_compile_options (_poco_redis PRIVATE -Wno-shadow)
target_include_directories (_poco_redis SYSTEM PUBLIC "include")
target_link_libraries (_poco_redis PUBLIC Poco::Net)
