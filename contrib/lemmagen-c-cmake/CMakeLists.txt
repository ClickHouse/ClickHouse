set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/lemmagen-c")
set(LEMMAGEN_INCLUDE_DIR "${LIBRARY_DIR}/include")

set(SRCS
    "${LIBRARY_DIR}/src/RdrLemmatizer.cpp"
)

add_library(_lemmagen STATIC ${SRCS})
target_include_directories(_lemmagen SYSTEM PUBLIC "${LEMMAGEN_INCLUDE_DIR}")
add_library(ch_contrib::lemmagen ALIAS _lemmagen)
