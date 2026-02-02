file (GLOB SRCS src/*.cpp)

add_library (_poco_mongodb ${SRCS})
add_library (Poco::MongoDB ALIAS _poco_mongodb)

# TODO: remove these warning exclusions
target_compile_options (_poco_mongodb
    PRIVATE
        -Wno-old-style-cast
        -Wno-unused-parameter
        -Wno-zero-as-null-pointer-constant
)

target_include_directories (_poco_mongodb SYSTEM PUBLIC "include")
target_link_libraries (_poco_mongodb PUBLIC Poco::Net)

