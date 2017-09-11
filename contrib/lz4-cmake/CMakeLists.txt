SET(LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/lz4/lib)

add_library (lz4
    ${LIBRARY_DIR}/lz4.c
    ${LIBRARY_DIR}/lz4hc.c

    ${LIBRARY_DIR}/lz4.h
    ${LIBRARY_DIR}/lz4hc.h
    ${LIBRARY_DIR}/lz4opt.h)

target_include_directories(lz4 PUBLIC ${LIBRARY_DIR})
