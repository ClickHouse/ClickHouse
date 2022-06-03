set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/udns")

add_library(_udns
        "${LIBRARY_DIR}/udns_dn.c"
        "${LIBRARY_DIR}/udns_dntosp.c"
        "${LIBRARY_DIR}/udns_parse.c"
        "${LIBRARY_DIR}/udns_resolver.c"
        "${LIBRARY_DIR}/udns_init.c"
        "${LIBRARY_DIR}/udns_misc.c"
        "${LIBRARY_DIR}/udns_XtoX.c"
        "${LIBRARY_DIR}/udns_rr_a.c"
        "${LIBRARY_DIR}/udns_rr_ptr.c"
        "${LIBRARY_DIR}/udns_rr_mx.c"
        "${LIBRARY_DIR}/udns_rr_txt.c"
        "${LIBRARY_DIR}/udns_bl.c"
        "${LIBRARY_DIR}/udns_rr_srv.c"
        "${LIBRARY_DIR}/udns_rr_naptr.c"
        "${LIBRARY_DIR}/udns_codes.c"
        "${LIBRARY_DIR}/udns_jran.c"
        )

target_compile_definitions(_udns PRIVATE -DHAVE_CONFIG_H)

target_include_directories(_udns SYSTEM BEFORE PUBLIC ${LIBRARY_DIR})

add_library(ch_contrib::udns ALIAS _udns)