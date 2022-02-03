set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/wordnet-blast")

set(SRCS
    "${LIBRARY_DIR}/wnb/core/info_helper.cc"
    "${LIBRARY_DIR}/wnb/core/load_wordnet.cc"
    "${LIBRARY_DIR}/wnb/core/wordnet.cc"
)

add_library(wnb ${SRCS})

target_link_libraries(wnb PRIVATE boost::headers_only boost::graph)

target_include_directories(wnb PUBLIC "${LIBRARY_DIR}")