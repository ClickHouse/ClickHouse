set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/fsst")
set (SRCS
    "${LIBRARY_DIR}/libfsst.cpp"
    "${LIBRARY_DIR}/fsst_avx512.cpp"
    "${LIBRARY_DIR}/libfsst12.cpp"
)

add_library (fsst ${SRCS})
add_library (ch_contrib::fsst ALIAS fsst) 

target_include_directories(fsst PUBLIC "${LIBRARY_DIR}")
