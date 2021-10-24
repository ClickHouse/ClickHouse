option(ENABLE_NLP "Enable NLP functions support" ${ENABLE_LIBRARIES})

if (NOT ENABLE_NLP)

    message (STATUS "NLP functions disabled")
    return()
endif()

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/libstemmer_c/Makefile")
    message (WARNING "submodule contrib/libstemmer_c is missing. to fix try run: \n git submodule update --init")
    message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find internal libstemmer_c library, NLP functions will be disabled")
    set (USE_NLP 0)
    return()
endif ()

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/wordnet-blast/CMakeLists.txt")
    message (WARNING "submodule contrib/wordnet-blast is missing. to fix try run: \n git submodule update --init")
    message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find internal wordnet-blast library, NLP functions will be disabled")
    set (USE_NLP 0)
    return()
endif ()

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/lemmagen-c/README.md")
    message (WARNING "submodule contrib/lemmagen-c is missing. to fix try run: \n git submodule update --init")
    message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find internal lemmagen-c library, NLP functions will be disabled")
    set (USE_NLP 0)
    return()
endif ()

set (USE_NLP 1)

message (STATUS "Using Libraries for NLP functions: contrib/wordnet-blast, contrib/libstemmer_c, contrib/lemmagen-c")
