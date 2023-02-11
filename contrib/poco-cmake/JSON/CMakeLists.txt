# Poco::JSON (pdjson)

set (SRCS_PDJSON
    "${LIBRARY_DIR}/JSON/src/pdjson.c"
)

add_library (_poco_json_pdjson ${SRCS_PDJSON})
add_library (Poco::JSON::Pdjson ALIAS _poco_json_pdjson)

# Poco::JSON

set (SRCS
    "${LIBRARY_DIR}/JSON/src/Array.cpp"
    "${LIBRARY_DIR}/JSON/src/Handler.cpp"
    "${LIBRARY_DIR}/JSON/src/JSONException.cpp"
    "${LIBRARY_DIR}/JSON/src/Object.cpp"
    "${LIBRARY_DIR}/JSON/src/ParseHandler.cpp"
    "${LIBRARY_DIR}/JSON/src/Parser.cpp"
    "${LIBRARY_DIR}/JSON/src/ParserImpl.cpp"
    "${LIBRARY_DIR}/JSON/src/PrintHandler.cpp"
    "${LIBRARY_DIR}/JSON/src/Query.cpp"
    "${LIBRARY_DIR}/JSON/src/Stringifier.cpp"
    "${LIBRARY_DIR}/JSON/src/Template.cpp"
    "${LIBRARY_DIR}/JSON/src/TemplateCache.cpp"
)

add_library (_poco_json ${SRCS})
add_library (Poco::JSON ALIAS _poco_json)

target_include_directories (_poco_json SYSTEM PUBLIC "${LIBRARY_DIR}/JSON/include")
target_link_libraries (_poco_json PUBLIC Poco::Foundation Poco::JSON::Pdjson)
