# Poco::JSON (pdjson)

set (SRCS_PDJSON
    src/pdjson.c
)

add_library (_poco_json_pdjson ${SRCS_PDJSON})
add_library (Poco::JSON::Pdjson ALIAS _poco_json_pdjson)

# TODO: remove these warning exclusions
target_compile_options (_poco_json_pdjson
    PRIVATE
        -Wno-cast-qual
        -Wno-declaration-after-statement
        -Wno-extra-semi-stmt
        -Wno-implicit-fallthrough
        -Wno-shorten-64-to-32
        -Wno-unreachable-code-return
)

# Poco::JSON

file (GLOB SRCS src/*.cpp)

add_library (_poco_json ${SRCS})
add_library (Poco::JSON ALIAS _poco_json)

# TODO: remove these warning exclusions
target_compile_options (_poco_json
    PRIVATE
        -Wno-deprecated-dynamic-exception-spec
        -Wno-sign-compare
        -Wno-suggest-destructor-override
        -Wno-suggest-override
        -Wno-unused-parameter
        -Wno-zero-as-null-pointer-constant
)

target_include_directories (_poco_json SYSTEM PUBLIC "include")
target_link_libraries (_poco_json PUBLIC Poco::Foundation Poco::JSON::Pdjson)
