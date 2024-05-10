set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/libpg_query")

set (SRCS
    ${LIBRARY_DIR}/src/pg_query.c
    ${LIBRARY_DIR}/src/pg_query_deparse.c
    ${LIBRARY_DIR}/src/pg_query_fingerprint.c
    ${LIBRARY_DIR}/src/pg_query_json_plpgsql.c
    ${LIBRARY_DIR}/src/pg_query_normalize.c
    ${LIBRARY_DIR}/src/pg_query_outfuncs_json.c
    ${LIBRARY_DIR}/src/pg_query_outfuncs_protobuf.c
    ${LIBRARY_DIR}/src/pg_query_parse.c
    ${LIBRARY_DIR}/src/pg_query_parse_plpgsql.c
    ${LIBRARY_DIR}/src/pg_query_readfuncs_protobuf.c
    ${LIBRARY_DIR}/src/pg_query_scan.c
    ${LIBRARY_DIR}/src/pg_query_split.c
    ${LIBRARY_DIR}/src/postgres_deparse.c
    ${LIBRARY_DIR}/src/postgres/src_backend_catalog_namespace.c
    ${LIBRARY_DIR}/src/postgres/src_backend_catalog_pg_proc.c
    ${LIBRARY_DIR}/src/postgres/src_backend_commands_define.c
    ${LIBRARY_DIR}/src/postgres/src_backend_nodes_bitmapset.c
    ${LIBRARY_DIR}/src/postgres/src_backend_nodes_copyfuncs.c
    ${LIBRARY_DIR}/src/postgres/src_backend_nodes_equalfuncs.c
    ${LIBRARY_DIR}/src/postgres/src_backend_nodes_extensible.c
    ${LIBRARY_DIR}/src/postgres/src_backend_nodes_list.c
    ${LIBRARY_DIR}/src/postgres/src_backend_nodes_makefuncs.c
    ${LIBRARY_DIR}/src/postgres/src_backend_nodes_nodeFuncs.c
    ${LIBRARY_DIR}/src/postgres/src_backend_nodes_nodes.c
    ${LIBRARY_DIR}/src/postgres/src_backend_nodes_value.c
    ${LIBRARY_DIR}/src/postgres/src_backend_parser_gram.c
    ${LIBRARY_DIR}/src/postgres/src_backend_parser_parser.c
    ${LIBRARY_DIR}/src/postgres/src_backend_parser_scan.c
    ${LIBRARY_DIR}/src/postgres/src_backend_parser_scansup.c
    ${LIBRARY_DIR}/src/postgres/src_backend_storage_ipc_ipc.c
    ${LIBRARY_DIR}/src/postgres/src_backend_tcop_postgres.c
    ${LIBRARY_DIR}/src/postgres/src_backend_utils_activity_pgstat_database.c
    ${LIBRARY_DIR}/src/postgres/src_backend_utils_adt_datum.c
    ${LIBRARY_DIR}/src/postgres/src_backend_utils_adt_expandeddatum.c
    ${LIBRARY_DIR}/src/postgres/src_backend_utils_adt_format_type.c
    ${LIBRARY_DIR}/src/postgres/src_backend_utils_adt_numutils.c
    ${LIBRARY_DIR}/src/postgres/src_backend_utils_adt_ruleutils.c
    ${LIBRARY_DIR}/src/postgres/src_backend_utils_error_assert.c
    ${LIBRARY_DIR}/src/postgres/src_backend_utils_error_elog.c
    ${LIBRARY_DIR}/src/postgres/src_backend_utils_fmgr_fmgr.c
    ${LIBRARY_DIR}/src/postgres/src_backend_utils_init_globals.c
    ${LIBRARY_DIR}/src/postgres/src_backend_utils_mb_mbutils.c
    ${LIBRARY_DIR}/src/postgres/src_backend_utils_misc_guc_tables.c
    ${LIBRARY_DIR}/src/postgres/src_backend_utils_mmgr_alignedalloc.c
    ${LIBRARY_DIR}/src/postgres/src_backend_utils_mmgr_aset.c
    ${LIBRARY_DIR}/src/postgres/src_backend_utils_mmgr_generation.c
    ${LIBRARY_DIR}/src/postgres/src_backend_utils_mmgr_mcxt.c
    ${LIBRARY_DIR}/src/postgres/src_backend_utils_mmgr_slab.c
    ${LIBRARY_DIR}/src/postgres/src_common_encnames.c
    ${LIBRARY_DIR}/src/postgres/src_common_hashfn.c
    ${LIBRARY_DIR}/src/postgres/src_common_keywords.c
    ${LIBRARY_DIR}/src/postgres/src_common_kwlookup.c
    ${LIBRARY_DIR}/src/postgres/src_common_psprintf.c
    ${LIBRARY_DIR}/src/postgres/src_common_stringinfo.c
    ${LIBRARY_DIR}/src/postgres/src_common_wchar.c
    ${LIBRARY_DIR}/src/postgres/src_pl_plpgsql_src_pl_comp.c
    ${LIBRARY_DIR}/src/postgres/src_pl_plpgsql_src_pl_funcs.c
    ${LIBRARY_DIR}/src/postgres/src_pl_plpgsql_src_pl_gram.c
    ${LIBRARY_DIR}/src/postgres/src_pl_plpgsql_src_pl_handler.c
    ${LIBRARY_DIR}/src/postgres/src_pl_plpgsql_src_pl_scanner.c
    ${LIBRARY_DIR}/src/postgres/src_port_pg_bitutils.c
    ${LIBRARY_DIR}/src/postgres/src_port_pgstrcasecmp.c
    ${LIBRARY_DIR}/src/postgres/src_port_qsort.c
    ${LIBRARY_DIR}/src/postgres/src_port_snprintf.c
    ${LIBRARY_DIR}/src/postgres/src_port_strerror.c
    ${LIBRARY_DIR}/src/postgres/src_port_strlcpy.c
    ${LIBRARY_DIR}/vendor/protobuf-c/protobuf-c.c
    ${LIBRARY_DIR}/vendor/xxhash/xxhash.c
    ${LIBRARY_DIR}/protobuf/pg_query.pb-c.c
)

add_library(_libpg_query ${SRCS})
target_include_directories(_libpg_query SYSTEM PUBLIC "${LIBRARY_DIR}")
target_include_directories(_libpg_query SYSTEM PUBLIC "${LIBRARY_DIR}/src/include")
target_include_directories(_libpg_query SYSTEM PUBLIC "${LIBRARY_DIR}/src/postgres/include")
target_include_directories(_libpg_query SYSTEM PUBLIC "${LIBRARY_DIR}/vendor")
add_library(ch_contrib::libpg_query ALIAS _libpg_query)
