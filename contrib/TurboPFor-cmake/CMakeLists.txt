set(TURBOPFOR_LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/TurboPFor)

set (SRCS
    "${TURBOPFOR_LIBRARY_DIR}/lib/bitpack.c"
    "${TURBOPFOR_LIBRARY_DIR}/lib/bitunpack.c"
    "${TURBOPFOR_LIBRARY_DIR}/lib/bitutil.c"
    "${TURBOPFOR_LIBRARY_DIR}/lib/eliasfano.c"
    "${TURBOPFOR_LIBRARY_DIR}/lib/fp.c"
    "${TURBOPFOR_LIBRARY_DIR}/lib/icapp.c"
    "${TURBOPFOR_LIBRARY_DIR}/lib/vbit.c"
    "${TURBOPFOR_LIBRARY_DIR}/lib/vint.c"
    "${TURBOPFOR_LIBRARY_DIR}/lib/v8.c"
    "${TURBOPFOR_LIBRARY_DIR}/lib/v8pack.c"
    "${TURBOPFOR_LIBRARY_DIR}/lib/vp4c.c"
    "${TURBOPFOR_LIBRARY_DIR}/lib/vp4d.c"
    "${TURBOPFOR_LIBRARY_DIR}/lib/vsimple.c"
)

add_library(_turbopfor ${SRCS})

target_include_directories(_turbopfor SYSTEM BEFORE PUBLIC "${TURBOPFOR_LIBRARY_DIR}/include/")
target_include_directories(_turbopfor SYSTEM BEFORE PUBLIC "${TURBOPFOR_LIBRARY_DIR}/lib/")

add_library(ch_contrib::turbopfor ALIAS _turbopfor)
