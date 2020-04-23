if (USE_INTERNAL_POCO_LIBRARY)
    # Poco::JSON (pdjson)

    set (SRCS_PDJSON
        ${LIBRARY_DIR}/JSON/src/pdjson.c
    )

    add_library (poco_json_pdjson ${SRCS_PDJSON})

    # Poco::JSON

    set (SRCS
        ${LIBRARY_DIR}/JSON/src/Array.cpp
        ${LIBRARY_DIR}/JSON/src/Handler.cpp
        ${LIBRARY_DIR}/JSON/src/JSONException.cpp
        ${LIBRARY_DIR}/JSON/src/Object.cpp
        ${LIBRARY_DIR}/JSON/src/ParseHandler.cpp
        ${LIBRARY_DIR}/JSON/src/Parser.cpp
        ${LIBRARY_DIR}/JSON/src/ParserImpl.cpp
        ${LIBRARY_DIR}/JSON/src/PrintHandler.cpp
        ${LIBRARY_DIR}/JSON/src/Query.cpp
        ${LIBRARY_DIR}/JSON/src/Stringifier.cpp
        ${LIBRARY_DIR}/JSON/src/Template.cpp
        ${LIBRARY_DIR}/JSON/src/TemplateCache.cpp
    )

    add_library (poco_json ${SRCS})

    target_include_directories (poco_json PUBLIC ${LIBRARY_DIR}/JSON/include)
    target_link_libraries (poco_json PUBLIC poco_foundation poco_json_pdjson)
endif ()
