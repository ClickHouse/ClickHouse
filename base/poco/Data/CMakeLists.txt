file (GLOB SRCS src/*.cpp)

add_library (_poco_data ${SRCS})
add_library (Poco::Data ALIAS _poco_data)

# TODO: remove these warning exclusions
target_compile_options (_poco_data
    PRIVATE
        -Wno-comma
        -Wno-covered-switch-default
        -Wno-deprecated-dynamic-exception-spec
        -Wno-extra-semi-stmt
        -Wno-old-style-cast
        -Wno-shorten-64-to-32
        -Wno-sign-compare
        -Wno-unused-parameter
        -Wno-zero-as-null-pointer-constant
)
target_include_directories (_poco_data SYSTEM PUBLIC "include")
target_link_libraries (_poco_data PUBLIC Poco::Foundation)
