SET(LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/lz4/lib)

add_library (lz4
    ${LIBRARY_DIR}/lz4.c
    ${LIBRARY_DIR}/lz4hc.c
    ${LIBRARY_DIR}/lz4frame.c
    ${LIBRARY_DIR}/lz4frame.h
    ${LIBRARY_DIR}/xxhash.c
    ${LIBRARY_DIR}/xxhash.h

    ${LIBRARY_DIR}/lz4.h
    ${LIBRARY_DIR}/lz4hc.h)

target_compile_definitions(lz4 PUBLIC LZ4_DISABLE_DEPRECATE_WARNINGS=1)

target_include_directories(lz4 PUBLIC ${LIBRARY_DIR})
