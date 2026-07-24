file (GLOB SRCS src/*.cpp)

add_library (_poco_util ${SRCS})
add_library (Poco::Util ALIAS _poco_util)

# TODO: remove these warning exclusions
target_compile_options (_poco_util
    PRIVATE
        -Wno-deprecated-dynamic-exception-spec
        -Wno-extra-semi-stmt
        -Wno-newline-eof
        -Wno-old-style-cast
        -Wno-shadow
        -Wno-sign-compare
        -Wno-suggest-destructor-override
        -Wno-suggest-override
        -Wno-unused-parameter
        -Wno-zero-as-null-pointer-constant
)
target_include_directories (_poco_util SYSTEM PUBLIC "include")
target_link_libraries (_poco_util PUBLIC Poco::JSON Poco::XML Poco::Net)
