file (GLOB SRCS src/*.cpp)

add_library (_poco_redis ${SRCS})
add_library (Poco::Redis ALIAS _poco_redis)

# TODO: remove these warning exclusions
target_compile_options (_poco_redis
    PRIVATE
        -Wno-deprecated-dynamic-exception-spec
        -Wno-shadow
        -Wno-shorten-64-to-32
        -Wno-sign-compare
        -Wno-zero-as-null-pointer-constant
)
target_include_directories (_poco_redis SYSTEM PUBLIC "include")
target_link_libraries (_poco_redis PUBLIC Poco::Net)
