set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/udns")

#add_library(udns
#        udns_dn.c
#        udns_dntosp.c
#        udns_parse.c
#        udns_resolver.c
#        udns_init.c
#        udns_misc.c
#        udns_XtoX.c
#        udns_rr_a.c
#        udns_rr_ptr.c
#        udns_rr_mx.c
#        udns_rr_txt.c
#        udns_bl.c
#        udns_rr_srv.c
#        udns_rr_naptr.c
#        udns_codes.c
#        udns_jran.c
#        )

#add_library(_udns
#        "${LIBRARY_DIR}/udns_dn.c"
#        "${LIBRARY_DIR}/dnsget.c" "${LIBRARY_DIR}/ex-rdns.c" "${LIBRARY_DIR}/getopt.c" "${LIBRARY_DIR}/inet_XtoX.c"
#        "${LIBRARY_DIR}/rblcheck.c" "${LIBRARY_DIR}/udns_bl.c" "${LIBRARY_DIR}/udns_dntosp.c"
#        "${LIBRARY_DIR}/udns_init.c" "${LIBRARY_DIR}/udns_jran.c" "${LIBRARY_DIR}/udns_misc.c" "${LIBRARY_DIR}/udns_parse.c"
#        "${LIBRARY_DIR}/udns_resolver.c" "${LIBRARY_DIR}/udns_rr_a.c" "${LIBRARY_DIR}/udns_rr_mx.c"
#        "${LIBRARY_DIR}/udns_rr_naptr.c" "${LIBRARY_DIR}/udns_rr_ptr.c" "${LIBRARY_DIR}/udns_rr_srv.c" "${LIBRARY_DIR}/udns_rr_txt.c"
#        "${LIBRARY_DIR}/udns_XtoX.c" )

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