# Poco::JSON (pdjson)

set (SRCS_PDJSON
    src/pdjson.c
)

add_library (_poco_json_pdjson ${SRCS_PDJSON})
add_library (Poco::JSON::Pdjson ALIAS _poco_json_pdjson)

# Poco::JSON

file (GLOB SRCS src/*.cpp)

add_library (_poco_json ${SRCS})
add_library (Poco::JSON ALIAS _poco_json)

target_include_directories (_poco_json SYSTEM PUBLIC "include")
target_link_libraries (_poco_json PUBLIC Poco::Foundation Poco::JSON::Pdjson)
