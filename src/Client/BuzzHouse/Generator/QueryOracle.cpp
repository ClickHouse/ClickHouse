#include <Client/BuzzHouse/Generator/QueryOracle.h>

#include <cstdio>

namespace BuzzHouse
{

/*
Correctness query oracle
*/
/*
SELECT COUNT(*) FROM <FROM_CLAUSE> WHERE <PRED>;
or
SELECT COUNT(*) FROM <FROM_CLAUSE> WHERE <PRED1> GROUP BY <GROUP_BY CLAUSE> HAVING <PRED2>;
*/
int QueryOracle::generateCorrectnessTestFirstQuery(RandomGenerator & rg, StatementGenerator & gen, SQLQuery & sq1)
{
    const std::filesystem::path & qfile = fc.db_file_path / "query.data";
    TopSelect * ts = sq1.mutable_inner_query()->mutable_select();
    SelectIntoFile * sif = ts->mutable_intofile();
    SelectStatementCore * ssc = ts->mutable_sel()->mutable_select_core();
    const uint32_t combination = 0; //TODO fix this rg.nextLargeNumber() % 3; /* 0 WHERE, 1 HAVING, 2 WHERE + HAVING */

    gen.setAllowNotDetermistic(false);
    gen.enforceFinal(true);
    gen.levels[gen.current_level] = QueryLevel(gen.current_level);
    gen.generateFromStatement(rg, std::numeric_limits<uint32_t>::max(), ssc->mutable_from());

    const bool prev_allow_aggregates = gen.levels[gen.current_level].allow_aggregates;
    const bool prev_allow_window_funcs = gen.levels[gen.current_level].allow_window_funcs;
    gen.levels[gen.current_level].allow_aggregates = gen.levels[gen.current_level].allow_window_funcs = false;
    if (combination != 1)
    {
        BinaryExpr * bexpr = ssc->mutable_where()->mutable_expr()->mutable_expr()->mutable_comp_expr()->mutable_binary_expr();

        bexpr->set_op(BinaryOperator::BINOP_EQ);
        bexpr->mutable_rhs()->mutable_lit_val()->set_special_val(SpecialVal::VAL_TRUE);
        gen.generateWherePredicate(rg, bexpr->mutable_lhs());
    }
    if (combination != 0)
    {
        gen.generateGroupBy(rg, 1, true, true, ssc->mutable_groupby());
    }
    gen.levels[gen.current_level].allow_aggregates = prev_allow_aggregates;
    gen.levels[gen.current_level].allow_window_funcs = prev_allow_window_funcs;

    ssc->add_result_columns()->mutable_eca()->mutable_expr()->mutable_comp_expr()->mutable_func_call()->mutable_func()->set_catalog_func(
        FUNCcount);
    gen.levels.erase(gen.current_level);
    gen.setAllowNotDetermistic(true);
    gen.enforceFinal(false);

    ts->set_format(OutFormat::OUT_CSV);
    sif->set_path(qfile.generic_string());
    sif->set_step(SelectIntoFile_SelectIntoFileStep::SelectIntoFile_SelectIntoFileStep_TRUNCATE);
    return 0;
}

/*
SELECT ifNull(SUM(PRED),0) FROM <FROM_CLAUSE>;
or
SELECT ifNull(SUM(PRED2),0) FROM <FROM_CLAUSE> WHERE <PRED1> GROUP BY <GROUP_BY CLAUSE>;
*/
int QueryOracle::generateCorrectnessTestSecondQuery(SQLQuery & sq1, SQLQuery & sq2)
{
    const std::filesystem::path & qfile = fc.db_file_path / "query.data";
    TopSelect * ts = sq2.mutable_inner_query()->mutable_select();
    SelectIntoFile * sif = ts->mutable_intofile();
    SelectStatementCore & ssc1 = const_cast<SelectStatementCore &>(sq1.inner_query().select().sel().select_core());
    SelectStatementCore * ssc2 = ts->mutable_sel()->mutable_select_core();
    SQLFuncCall * sfc1 = ssc2->add_result_columns()->mutable_eca()->mutable_expr()->mutable_comp_expr()->mutable_func_call();
    SQLFuncCall * sfc2 = sfc1->add_args()->mutable_expr()->mutable_comp_expr()->mutable_func_call();

    sfc1->mutable_func()->set_catalog_func(FUNCifNull);
    sfc1->add_args()->mutable_expr()->mutable_lit_val()->set_special_val(SpecialVal::VAL_ZERO);
    sfc2->mutable_func()->set_catalog_func(FUNCsum);

    ssc2->set_allocated_from(ssc1.release_from());
    if (ssc1.has_groupby())
    {
        GroupByStatement & gbs = const_cast<GroupByStatement &>(ssc1.groupby());

        sfc2->add_args()->set_allocated_expr(gbs.release_having_expr());
        ssc2->set_allocated_groupby(ssc1.release_groupby());
        ssc2->set_allocated_where(ssc1.release_where());
    }
    else
    {
        ExprComparisonHighProbability & expr = const_cast<ExprComparisonHighProbability &>(ssc1.where().expr());

        sfc2->add_args()->set_allocated_expr(expr.release_expr());
    }
    ts->set_format(OutFormat::OUT_CSV);
    sif->set_path(qfile.generic_string());
    sif->set_step(SelectIntoFile_SelectIntoFileStep::SelectIntoFile_SelectIntoFileStep_TRUNCATE);
    return 0;
}

/*
Dump and read table oracle
*/
int QueryOracle::dumpTableContent(RandomGenerator & rg, StatementGenerator & gen, const SQLTable & t, SQLQuery & sq1)
{
    bool first = true;
    const std::filesystem::path & qfile = fc.db_file_path / "query.data";
    TopSelect * ts = sq1.mutable_inner_query()->mutable_select();
    SelectIntoFile * sif = ts->mutable_intofile();
    SelectStatementCore * sel = ts->mutable_sel()->mutable_select_core();
    JoinedTable * jt = sel->mutable_from()->mutable_tos()->mutable_join_clause()->mutable_tos()->mutable_joined_table();
    OrderByList * obs = sel->mutable_orderby()->mutable_olist();
    ExprSchemaTable * est = jt->mutable_est();

    if (t.db)
    {
        est->mutable_database()->set_database("d" + std::to_string(t.db->dname));
    }
    est->mutable_table()->set_table("t" + std::to_string(t.tname));
    jt->set_final(t.supportsFinal());
    gen.flatTableColumnPath(0, t, [](const SQLColumn & c) { return c.canBeInserted(); });
    for (const auto & entry : gen.entries)
    {
        ExprOrderingTerm * eot = first ? obs->mutable_ord_term() : obs->add_extra_ord_terms();

        gen.columnPathRef(entry, sel->add_result_columns()->mutable_etc()->mutable_col()->mutable_path());
        gen.columnPathRef(entry, eot->mutable_expr()->mutable_comp_expr()->mutable_expr_stc()->mutable_col()->mutable_path());
        if (rg.nextBool())
        {
            eot->set_asc_desc(rg.nextBool() ? AscDesc::ASC : AscDesc::DESC);
        }
        if (rg.nextBool())
        {
            eot->set_nulls_order(
                rg.nextBool() ? ExprOrderingTerm_NullsOrder::ExprOrderingTerm_NullsOrder_FIRST
                              : ExprOrderingTerm_NullsOrder::ExprOrderingTerm_NullsOrder_LAST);
        }
        first = false;
    }
    gen.entries.clear();
    ts->set_format(OutFormat::OUT_CSV);
    sif->set_path(qfile.generic_string());
    sif->set_step(SelectIntoFile_SelectIntoFileStep::SelectIntoFile_SelectIntoFileStep_TRUNCATE);
    return 0;
}

static const std::map<OutFormat, InFormat> out_in{
    {OutFormat::OUT_CSV, InFormat::IN_CSV},
    {OutFormat::OUT_CSVWithNames, InFormat::IN_CSVWithNames},
    {OutFormat::OUT_CSVWithNamesAndTypes, InFormat::IN_CSVWithNamesAndTypes},
    {OutFormat::OUT_Values, InFormat::IN_Values},
    {OutFormat::OUT_JSON, InFormat::IN_JSON},
    {OutFormat::OUT_JSONColumns, InFormat::IN_JSONColumns},
    {OutFormat::OUT_JSONColumnsWithMetadata, InFormat::IN_JSONColumnsWithMetadata},
    {OutFormat::OUT_JSONCompact, InFormat::IN_JSONCompact},
    {OutFormat::OUT_JSONCompactColumns, InFormat::IN_JSONCompactColumns},
    {OutFormat::OUT_JSONEachRow, InFormat::IN_JSONEachRow},
    {OutFormat::OUT_JSONStringsEachRow, InFormat::IN_JSONStringsEachRow},
    {OutFormat::OUT_JSONCompactEachRow, InFormat::IN_JSONCompactEachRow},
    {OutFormat::OUT_JSONCompactEachRowWithNames, InFormat::IN_JSONCompactEachRowWithNames},
    {OutFormat::OUT_JSONCompactEachRowWithNamesAndTypes, InFormat::IN_JSONCompactEachRowWithNamesAndTypes},
    {OutFormat::OUT_JSONCompactStringsEachRow, InFormat::IN_JSONCompactStringsEachRow},
    {OutFormat::OUT_JSONCompactStringsEachRowWithNames, InFormat::IN_JSONCompactStringsEachRowWithNames},
    {OutFormat::OUT_JSONCompactStringsEachRowWithNamesAndTypes, InFormat::IN_JSONCompactStringsEachRowWithNamesAndTypes},
    {OutFormat::OUT_JSONObjectEachRow, InFormat::IN_JSONObjectEachRow},
    {OutFormat::OUT_BSONEachRow, InFormat::IN_BSONEachRow},
    {OutFormat::OUT_TSKV, InFormat::IN_TSKV},
    {OutFormat::OUT_Protobuf, InFormat::IN_Protobuf},
    {OutFormat::OUT_ProtobufSingle, InFormat::IN_ProtobufSingle},
    {OutFormat::OUT_Avro, InFormat::IN_Avro},
    {OutFormat::OUT_Parquet, InFormat::IN_Parquet},
    {OutFormat::OUT_Arrow, InFormat::IN_Arrow},
    {OutFormat::OUT_ArrowStream, InFormat::IN_ArrowStream},
    {OutFormat::OUT_ORC, InFormat::IN_ORC},
    {OutFormat::OUT_RowBinary, InFormat::IN_RowBinary},
    {OutFormat::OUT_RowBinaryWithNames, InFormat::IN_RowBinaryWithNames},
    {OutFormat::OUT_RowBinaryWithNamesAndTypes, InFormat::IN_RowBinaryWithNamesAndTypes},
    {OutFormat::OUT_Native, InFormat::IN_Native},
    {OutFormat::OUT_MsgPack, InFormat::IN_MsgPack}};

int QueryOracle::generateExportQuery(RandomGenerator & rg, StatementGenerator & gen, const SQLTable & t, SQLQuery & sq2)
{
    bool first = true;
    Insert * ins = sq2.mutable_inner_query()->mutable_insert();
    FileFunc * ff = ins->mutable_tfunction()->mutable_file();
    SelectStatementCore * sel = ins->mutable_insert_select()->mutable_select()->mutable_select_core();
    const std::filesystem::path & nfile = fc.db_file_path / "table.data";
    OutFormat outf = rg.pickKeyRandomlyFromMap(out_in);

    if (std::filesystem::exists(nfile))
    {
        (void)std::remove(nfile.generic_string().c_str()); //remove the file
    }
    ff->set_path(nfile.generic_string());

    buf.resize(0);
    gen.flatTableColumnPath(0, t, [](const SQLColumn & c) { return c.canBeInserted(); });
    for (const auto & entry : gen.entries)
    {
        SQLType * tp = entry.getBottomType();

        if (!first)
        {
            buf += ", ";
        }
        buf += entry.getBottomName();
        buf += " ";
        tp->typeName(buf, true);
        if (entry.nullable.has_value())
        {
            buf += entry.nullable.value() ? "" : " NOT";
            buf += " NULL";
        }
        gen.columnPathRef(entry, sel->add_result_columns()->mutable_etc()->mutable_col()->mutable_path());
        /* ArrowStream doesn't support UUID */
        if (outf == OutFormat::OUT_ArrowStream && dynamic_cast<UUIDType *>(tp))
        {
            outf = OutFormat::OUT_CSV;
        }
        first = false;
    }
    gen.entries.clear();
    ff->set_outformat(outf);
    ff->set_structure(buf);
    if (rg.nextSmallNumber() < 4)
    {
        ff->set_fcomp(static_cast<FileCompression>((rg.nextRandomUInt32() % static_cast<uint32_t>(FileCompression_MAX)) + 1));
    }

    //Set the table on select
    JoinedTable * jt = sel->mutable_from()->mutable_tos()->mutable_join_clause()->mutable_tos()->mutable_joined_table();
    ExprSchemaTable * est = jt->mutable_est();

    if (t.db)
    {
        est->mutable_database()->set_database("d" + std::to_string(t.db->dname));
    }
    est->mutable_table()->set_table("t" + std::to_string(t.tname));
    jt->set_final(t.supportsFinal());
    return 0;
}

int QueryOracle::generateClearQuery(const SQLTable & t, SQLQuery & sq3)
{
    Truncate * trunc = sq3.mutable_inner_query()->mutable_trunc();
    ExprSchemaTable * est = trunc->mutable_est();

    if (t.db)
    {
        est->mutable_database()->set_database("d" + std::to_string(t.db->dname));
    }
    est->mutable_table()->set_table("t" + std::to_string(t.tname));
    return 0;
}

int QueryOracle::generateImportQuery(StatementGenerator & gen, const SQLTable & t, const SQLQuery & sq2, SQLQuery & sq4)
{
    Insert * ins = sq4.mutable_inner_query()->mutable_insert();
    InsertFromFile * iff = ins->mutable_insert_file();
    const FileFunc & ff = sq2.inner_query().insert().tfunction().file();
    ExprSchemaTable * est = ins->mutable_est();

    if (t.db)
    {
        est->mutable_database()->set_database("d" + std::to_string(t.db->dname));
    }
    est->mutable_table()->set_table("t" + std::to_string(t.tname));
    gen.flatTableColumnPath(0, t, [](const SQLColumn & c) { return c.canBeInserted(); });
    for (const auto & entry : gen.entries)
    {
        gen.columnPathRef(entry, ins->add_cols());
    }
    gen.entries.clear();
    iff->set_path(ff.path());
    iff->set_format(out_in.at(ff.outformat()));
    if (ff.has_fcomp())
    {
        iff->set_fcomp(ff.fcomp());
    }
    if (iff->format() == IN_CSV)
    {
        SettingValues * vals = iff->mutable_settings();
        SetValue * sv = vals->mutable_set_value();

        sv->set_property("input_format_csv_detect_header");
        sv->set_value("0");
    }
    return 0;
}

/*
Run query with different settings oracle
*/
static const std::vector<TestSetting> test_settings{
    TestSetting("aggregate_functions_null_for_empty", {"0", "1"}),
    TestSetting("aggregation_in_order_max_block_bytes", {"0", "1"}),
    TestSetting("allow_aggregate_partitions_independently", {"0", "1"}),
    TestSetting("allow_experimental_parallel_reading_from_replicas", {"0", "1"}),
    TestSetting("allow_introspection_functions", {"0", "1"}),
    TestSetting("allow_reorder_prewhere_conditions", {"0", "1"}),
    TestSetting("analyze_index_with_space_filling_curves", {"0", "1"}),
    TestSetting("analyzer_compatibility_join_using_top_level_identifier", {"0", "1"}),
    TestSetting("any_join_distinct_right_table_keys", {"0", "1"}),
    TestSetting("async_insert", {"0", "1"}),
    TestSetting("async_query_sending_for_remote", {"0", "1"}),
    TestSetting("async_socket_for_remote", {"0", "1"}),
    TestSetting("check_query_single_value_result", {"0", "1"}),
    TestSetting("cloud_mode", {"0", "1"}),
    TestSetting("collect_hash_table_stats_during_aggregation", {"0", "1"}),
    TestSetting("collect_hash_table_stats_during_joins", {"0", "1"}),
    TestSetting("compile_aggregate_expressions", {"0", "1"}),
    TestSetting("compile_expressions", {"0", "1"}),
    TestSetting("compile_sort_description", {"0", "1"}),
    TestSetting("count_distinct_optimization", {"0", "1"}),
    TestSetting("cross_join_min_bytes_to_compress", {"0", "1"}),
    TestSetting("cross_join_min_rows_to_compress", {"0", "1"}),
    TestSetting("describe_include_subcolumns", {"0", "1"}),
    TestSetting("distributed_aggregation_memory_efficient", {"0", "1"}),
    TestSetting("enable_filesystem_cache", {"0", "1"}),
    TestSetting("enable_memory_bound_merging_of_aggregation_results", {"0", "1"}),
    TestSetting("enable_multiple_prewhere_read_steps", {"0", "1"}),
    TestSetting("enable_named_columns_in_function_tuple", {"0", "1"}),
    TestSetting("enable_optimize_predicate_expression", {"0", "1"}),
    TestSetting("enable_optimize_predicate_expression_to_final_subquery", {"0", "1"}),
    TestSetting("enable_parallel_replicas", {"0", "1"}),
    TestSetting("enable_parsing_to_custom_serialization", {"0", "1"}),
    TestSetting("enable_reads_from_query_cache", {"0", "1"}),
    TestSetting("enable_scalar_subquery_optimization", {"0", "1"}),
    TestSetting("enable_sharing_sets_for_mutations", {"0", "1"}),
    TestSetting("enable_software_prefetch_in_aggregation", {"0", "1"}),
    TestSetting("enable_unaligned_array_join", {"0", "1"}),
    TestSetting("enable_vertical_final", {"0", "1"}),
    TestSetting("enable_writes_to_query_cache", {"0", "1"}),
    TestSetting("exact_rows_before_limit", {"0", "1"}),
    TestSetting("filesystem_cache_prefer_bigger_buffer_size", {"0", "1"}),
    TestSetting("flatten_nested", {"0", "1"}),
    TestSetting("force_aggregate_partitions_independently", {"0", "1"}),
    TestSetting("force_optimize_projection", {"0", "1"}),
    TestSetting("fsync_metadata", {"0", "1"}),
    TestSetting("group_by_two_level_threshold", {"0", "1"}),
    TestSetting("group_by_two_level_threshold_bytes", {"0", "1"}),
    TestSetting("http_wait_end_of_query", {"0", "1"}),
    TestSetting("input_format_import_nested_json", {"0", "1"}),
    TestSetting("input_format_orc_filter_push_down", {"0", "1"}),
    TestSetting("input_format_parallel_parsing", {"0", "1"}),
    TestSetting("input_format_parquet_bloom_filter_push_down", {"0", "1"}),
    TestSetting("input_format_parquet_filter_push_down", {"0", "1"}),
    TestSetting(
        "join_algorithm",
        {"'default'",
         "'grace_hash'",
         "'direct, hash'",
         "'hash'",
         "'parallel_hash'",
         "'partial_merge'",
         "'direct'",
         "'auto'",
         "'full_sorting_merge'",
         "'prefer_partial_merge'"}),
    TestSetting("join_any_take_last_row", {"0", "1"}),
    TestSetting("local_filesystem_read_method", {"'read'", "'pread'", "'mmap'", "'pread_threadpool'", "'io_uring'"}),
    TestSetting("local_filesystem_read_prefetch", {"0", "1"}),
    TestSetting("log_queries", {"0", "1"}),
    TestSetting("log_query_threads", {"0", "1"}),
    TestSetting("low_cardinality_use_single_dictionary_for_part", {"0", "1"}),
    TestSetting("max_bytes_before_external_group_by", {"0", "1", "100000000"}),
    TestSetting("max_bytes_before_external_sort", {"0", "1", "100000000"}),
    TestSetting("max_bytes_before_remerge_sort", {"0", "1"}),
    TestSetting("max_final_threads", {"0", "1"}),
    TestSetting("max_threads", {"1", std::to_string(std::thread::hardware_concurrency())}),
    TestSetting("min_chunk_bytes_for_parallel_parsing", {"0", "1"}),
    TestSetting("min_external_table_block_size_bytes", {"0", "1", "100000000"}),
    TestSetting("move_all_conditions_to_prewhere", {"0", "1"}),
    TestSetting("move_primary_key_columns_to_end_of_prewhere", {"0", "1"}),
    TestSetting("optimize_aggregation_in_order", {"0", "1"}),
    TestSetting("optimize_aggregators_of_group_by_keys", {"0", "1"}),
    TestSetting("optimize_append_index", {"0", "1"}),
    TestSetting("optimize_arithmetic_operations_in_aggregate_functions", {"0", "1"}),
    TestSetting("optimize_count_from_files", {"0", "1"}),
    TestSetting("optimize_distinct_in_order", {"0", "1"}),
    TestSetting("optimize_distributed_group_by_sharding_key", {"0", "1"}),
    TestSetting("optimize_extract_common_expressions", {"0", "1"}),
    TestSetting("optimize_functions_to_subcolumns", {"0", "1"}),
    TestSetting("optimize_group_by_constant_keys", {"0", "1"}),
    TestSetting("optimize_group_by_function_keys", {"0", "1"}),
    TestSetting("optimize_if_chain_to_multiif", {"0", "1"}),
    TestSetting("optimize_if_transform_strings_to_enum", {"0", "1"}),
    TestSetting("optimize_injective_functions_in_group_by", {"0", "1"}),
    TestSetting("optimize_injective_functions_inside_uniq", {"0", "1"}),
    TestSetting("optimize_move_to_prewhere", {"0", "1"}),
    TestSetting("optimize_move_to_prewhere_if_final", {"0", "1"}),
    TestSetting("optimize_multiif_to_if", {"0", "1"}),
    TestSetting("optimize_normalize_count_variants", {"0", "1"}),
    TestSetting("optimize_on_insert", {"0", "1"}),
    TestSetting("optimize_or_like_chain", {"0", "1"}),
    TestSetting("optimize_read_in_order", {"0", "1"}),
    TestSetting("optimize_read_in_window_order", {"0", "1"}),
    TestSetting("optimize_redundant_functions_in_order_by", {"0", "1"}),
    TestSetting("optimize_rewrite_aggregate_function_with_if", {"0", "1"}),
    TestSetting("optimize_rewrite_array_exists_to_has", {"0", "1"}),
    TestSetting("optimize_rewrite_sum_if_to_count_if", {"0", "1"}),
    TestSetting("optimize_skip_merged_partitions", {"0", "1"}),
    TestSetting("optimize_skip_unused_shards", {"0", "1"}),
    TestSetting("optimize_skip_unused_shards_rewrite_in", {"0", "1"}),
    TestSetting("optimize_sorting_by_input_stream_properties", {"0", "1"}),
    TestSetting("optimize_substitute_columns", {"0", "1"}),
    TestSetting("optimize_syntax_fuse_functions", {"0", "1"}),
    TestSetting("optimize_time_filter_with_preimage", {"0", "1"}),
    TestSetting("optimize_trivial_approximate_count_query", {"0", "1"}),
    TestSetting("optimize_trivial_count_query", {"0", "1"}),
    TestSetting("optimize_trivial_insert_select", {"0", "1"}),
    TestSetting("optimize_uniq_to_count", {"0", "1"}),
    TestSetting("optimize_use_implicit_projections", {"0", "1"}),
    TestSetting("optimize_use_projections", {"0", "1"}),
    TestSetting("optimize_using_constraints", {"0", "1"}),
    TestSetting("output_format_parallel_formatting", {"0", "1"}),
    TestSetting("output_format_pretty_highlight_digit_groups", {"0", "1"}),
    TestSetting("output_format_pretty_row_numbers", {"0", "1"}),
    TestSetting("output_format_write_statistics", {"0", "1"}),
    TestSetting("page_cache_inject_eviction", {"0", "1"}),
    TestSetting("parallel_replica_offset", {"0", "1", "2", "3", "4"}),
    TestSetting("parallel_replicas_allow_in_with_subquery", {"0", "1"}),
    TestSetting("parallel_replicas_count", {"0", "1", "2", "3", "4"}),
    TestSetting("parallel_replicas_for_non_replicated_merge_tree", {"0", "1"}),
    TestSetting("parallel_replicas_index_analysis_only_on_coordinator", {"0", "1"}),
    TestSetting("parallel_replicas_local_plan", {"0", "1"}),
    TestSetting("parallel_replicas_mode", {"sampling_key", "read_tasks", "custom_key_range", "custom_key_sampling", "auto"}),
    TestSetting("parallel_replicas_prefer_local_join", {"0", "1"}),
    TestSetting("parallel_view_processing", {"0", "1"}),
    TestSetting("parallelize_output_from_storages", {"0", "1"}),
    TestSetting("partial_merge_join_optimizations", {"0", "1"}),
    TestSetting("precise_float_parsing", {"0", "1"}),
    TestSetting("prefer_external_sort_block_bytes", {"0", "1", "100000000"}),
    TestSetting("prefer_global_in_and_join", {"0", "1"}),
    TestSetting("prefer_localhost_replica", {"0", "1"}),
    TestSetting("query_plan_aggregation_in_order", {"0", "1"}),
    TestSetting("query_plan_convert_outer_join_to_inner_join", {"0", "1"}),
    TestSetting("query_plan_enable_multithreading_after_window_functions", {"0", "1"}),
    TestSetting("query_plan_enable_optimizations", {"0", "1"}),
    TestSetting("query_plan_execute_functions_after_sorting", {"0", "1"}),
    TestSetting("query_plan_filter_push_down", {"0", "1"}),
    TestSetting("query_plan_join_swap_table", {"'false'", "'true'", "'auto'"}),
    TestSetting("query_plan_lift_up_array_join", {"0", "1"}),
    TestSetting("query_plan_lift_up_union", {"0", "1"}),
    TestSetting("query_plan_merge_expressions", {"0", "1"}),
    TestSetting("query_plan_merge_filters", {"0", "1"}),
    TestSetting("query_plan_optimize_prewhere", {"0", "1"}),
    TestSetting("query_plan_push_down_limit", {"0", "1"}),
    TestSetting("query_plan_read_in_order", {"0", "1"}),
    TestSetting("query_plan_remove_redundant_distinct", {"0", "1"}),
    TestSetting("query_plan_remove_redundant_sorting", {"0", "1"}),
    TestSetting("query_plan_reuse_storage_ordering_for_window_functions", {"0", "1"}),
    TestSetting("query_plan_split_filter", {"0", "1"}),
    TestSetting("read_from_filesystem_cache_if_exists_otherwise_bypass_cache", {"0", "1"}),
    TestSetting("read_from_page_cache_if_exists_otherwise_bypass_cache", {"0", "1"}),
    TestSetting("read_in_order_use_buffering", {"0", "1"}),
    TestSetting("read_in_order_use_virtual_row", {"0", "1"}),
    TestSetting("read_through_distributed_cache", {"0", "1"}),
    TestSetting("regexp_dict_allow_hyperscan", {"0", "1"}),
    TestSetting("regexp_dict_flag_case_insensitive", {"0", "1"}),
    TestSetting("regexp_dict_flag_dotall", {"0", "1"}),
    TestSetting("remote_filesystem_read_prefetch", {"0", "1"}),
    TestSetting("rewrite_count_distinct_if_with_count_distinct_implementation", {"0", "1"}),
    TestSetting("rows_before_aggregation", {"0", "1"}),
    TestSetting("single_join_prefer_left_table", {"0", "1"}),
    TestSetting("split_intersecting_parts_ranges_into_layers_final", {"0", "1"}),
    TestSetting("split_parts_ranges_into_intersecting_and_non_intersecting_final", {"0", "1"}),
    TestSetting("splitby_max_substrings_includes_remaining_string", {"0", "1"}),
    TestSetting("stream_like_engine_allow_direct_select", {"0", "1"}),
    TestSetting("throw_on_error_from_cache_on_write_operations", {"0", "1"}),
    TestSetting("transform_null_in", {"0", "1"}),
    TestSetting("update_insert_deduplication_token_in_dependent_materialized_views", {"0", "1"}),
    TestSetting("use_async_executor_for_materialized_views", {"0", "1"}),
    TestSetting("use_cache_for_count_from_files", {"0", "1"}),
    TestSetting("use_concurrency_control", {"0", "1"}),
    TestSetting("use_index_for_in_with_subqueries", {"0", "1"}),
    TestSetting("use_local_cache_for_remote_storage", {"0", "1"}),
    TestSetting("use_page_cache_for_disks_without_file_cache", {"0", "1"}),
    TestSetting(
        "use_query_cache",
        {"0, set_overflow_mode = 'break', group_by_overflow_mode = 'break', join_overflow_mode = 'break'",
         "1, set_overflow_mode = 'throw', group_by_overflow_mode = 'throw', join_overflow_mode = 'throw'"}),
    TestSetting("use_skip_indexes", {"0", "1"}),
    TestSetting("use_skip_indexes_if_final", {"0", "1"}),
    TestSetting("use_structure_from_insertion_table_in_table_functions", {"0", "1"}),
    TestSetting("use_uncompressed_cache", {"0", "1"}),
    TestSetting("use_variant_as_common_type", {"0", "1"}),
    TestSetting("use_with_fill_by_sorting_prefix", {"0", "1"})};

int QueryOracle::generateFirstSetting(RandomGenerator & rg, SQLQuery & sq1)
{
    const uint32_t nsets = rg.nextBool() ? 1 : ((rg.nextSmallNumber() % 3) + 1);
    SettingValues * sv = sq1.mutable_inner_query()->mutable_setting_values();

    nsettings.clear();
    for (uint32_t i = 0; i < nsets; i++)
    {
        const TestSetting & ts = rg.pickRandomlyFromVector(test_settings);
        SetValue * setv = i == 0 ? sv->mutable_set_value() : sv->add_other_values();

        setv->set_property(ts.tsetting);
        if (ts.options.size() == 2)
        {
            if (rg.nextBool())
            {
                setv->set_value(*ts.options.begin());
                nsettings.push_back(*std::next(ts.options.begin(), 1));
            }
            else
            {
                setv->set_value(*std::next(ts.options.begin(), 1));
                nsettings.push_back(*(ts.options.begin()));
            }
        }
        else
        {
            setv->set_value(rg.pickRandomlyFromSet(ts.options));
            nsettings.push_back(rg.pickRandomlyFromSet(ts.options));
        }
    }
    return 0;
}

int QueryOracle::generateSecondSetting(const SQLQuery & sq1, SQLQuery & sq3)
{
    const SettingValues & osv = sq1.inner_query().setting_values();
    SettingValues * sv = sq3.mutable_inner_query()->mutable_setting_values();

    for (size_t i = 0; i < nsettings.size(); i++)
    {
        const SetValue & osetv = i == 0 ? osv.set_value() : osv.other_values(static_cast<int>(i - 1));
        SetValue * setv = i == 0 ? sv->mutable_set_value() : sv->add_other_values();

        setv->set_property(osetv.property());
        setv->set_value(nsettings[i]);
    }
    return 0;
}

int QueryOracle::generateOracleSelectQuery(RandomGenerator & rg, const bool peer_query, StatementGenerator & gen, SQLQuery & sq2)
{
    const std::filesystem::path & qfile = fc.db_file_path / "query.data";
    TopSelect * ts = sq2.mutable_inner_query()->mutable_select();
    SelectIntoFile * sif = ts->mutable_intofile();
    const bool global_aggregate = rg.nextSmallNumber() < 4;

    gen.setAllowNotDetermistic(false);
    gen.enforceFinal(true);
    gen.generatingPeerQuery(peer_query);
    gen.generateTopSelect(rg, global_aggregate, std::numeric_limits<uint32_t>::max(), ts);
    gen.setAllowNotDetermistic(true);
    gen.enforceFinal(false);
    gen.generatingPeerQuery(false);

    if (!global_aggregate)
    {
        //if not global aggregate, use ORDER BY clause
        Select * osel = ts->release_sel();
        SelectStatementCore * nsel = ts->mutable_sel()->mutable_select_core();
        nsel->mutable_from()->mutable_tos()->mutable_join_clause()->mutable_tos()->mutable_joined_derived_query()->set_allocated_select(
            osel);
        nsel->mutable_orderby()->set_oall(true);
    }
    ts->set_format(OutFormat::OUT_CSV);
    sif->set_path(qfile.generic_string());
    sif->set_step(SelectIntoFile_SelectIntoFileStep::SelectIntoFile_SelectIntoFileStep_TRUNCATE);
    return 0;
}

void QueryOracle::findTablesWithPeersAndReplace(RandomGenerator & rg, google::protobuf::Message & mes, StatementGenerator & gen)
{
    checkStackSize();

    if (mes.GetTypeName() == "BuzzHouse.Select")
    {
        auto & sel = static_cast<Select &>(mes);

        if (sel.has_select_core())
        {
            findTablesWithPeersAndReplace(rg, const_cast<SelectStatementCore &>(sel.select_core()), gen);
        }
        else if (sel.has_set_query())
        {
            findTablesWithPeersAndReplace(rg, const_cast<SetQuery &>(sel.set_query()), gen);
        }
        if (sel.has_ctes())
        {
            findTablesWithPeersAndReplace(rg, const_cast<Select &>(sel.ctes().cte().query()), gen);
            for (int i = 0; i < sel.ctes().other_ctes_size(); i++)
            {
                findTablesWithPeersAndReplace(rg, const_cast<Select &>(sel.ctes().other_ctes(i).query()), gen);
            }
        }
    }
    else if (mes.GetTypeName() == "BuzzHouse.SetQuery")
    {
        auto & setq = static_cast<SetQuery &>(mes);

        findTablesWithPeersAndReplace(rg, const_cast<Select &>(setq.sel1()), gen);
        findTablesWithPeersAndReplace(rg, const_cast<Select &>(setq.sel2()), gen);
    }
    else if (mes.GetTypeName() == "BuzzHouse.SelectStatementCore")
    {
        auto & ssc = static_cast<SelectStatementCore &>(mes);

        if (ssc.has_from())
        {
            findTablesWithPeersAndReplace(rg, const_cast<JoinedQuery &>(ssc.from().tos()), gen);
        }
    }
    else if (mes.GetTypeName() == "BuzzHouse.JoinedQuery")
    {
        auto & jquery = static_cast<JoinedQuery &>(mes);

        for (int i = 0; i < jquery.tos_list_size(); i++)
        {
            findTablesWithPeersAndReplace(rg, const_cast<TableOrSubquery &>(jquery.tos_list(i)), gen);
        }
        findTablesWithPeersAndReplace(rg, const_cast<JoinClause &>(jquery.join_clause()), gen);
    }
    else if (mes.GetTypeName() == "BuzzHouse.JoinClause")
    {
        auto & jclause = static_cast<JoinClause &>(mes);

        for (int i = 0; i < jclause.clauses_size(); i++)
        {
            if (jclause.clauses(i).has_core())
            {
                findTablesWithPeersAndReplace(rg, const_cast<TableOrSubquery &>(jclause.clauses(i).core().tos()), gen);
            }
        }
        findTablesWithPeersAndReplace(rg, const_cast<TableOrSubquery &>(jclause.tos()), gen);
    }
    else if (mes.GetTypeName() == "BuzzHouse.TableOrSubquery")
    {
        auto & tos = static_cast<TableOrSubquery &>(mes);

        if (tos.has_joined_table())
        {
            const ExprSchemaTable & est = tos.joined_table().est();

            if ((!est.has_database() || est.database().database() != "system") && est.table().table().at(0) == 't')
            {
                const uint32_t tname = static_cast<uint32_t>(std::stoul(tos.joined_table().est().table().table().substr(1)));

                if (gen.tables.find(tname) != gen.tables.end())
                {
                    const SQLTable & t = gen.tables.at(tname);

                    if (t.hasDatabasePeer())
                    {
                        buf.resize(0);
                        buf += tos.joined_table().table_alias().table();
                        tos.clear_joined_table();
                        JoinedTableFunction * jtf = tos.mutable_joined_table_function();
                        gen.setTableRemote<false>(rg, t, jtf->mutable_tfunc());
                        jtf->mutable_table_alias()->set_table(buf);
                        found_tables.insert(tname);
                    }
                }
            }
        }
        else if (tos.has_joined_derived_query())
        {
            findTablesWithPeersAndReplace(rg, const_cast<Select &>(tos.joined_derived_query().select()), gen);
        }
        else if (tos.has_joined_query())
        {
            findTablesWithPeersAndReplace(rg, const_cast<JoinedQuery &>(tos.joined_query()), gen);
        }
    }
}

int QueryOracle::truncatePeerTables(const StatementGenerator & gen) const
{
    for (const auto & entry : found_tables)
    {
        //first truncate tables
        gen.connections.truncatePeerTableOnRemote(gen.tables.at(entry));
    }
    return 0;
}

int QueryOracle::optimizePeerTables(const StatementGenerator & gen) const
{
    for (const auto & entry : found_tables)
    {
        //lastly optimize tables
        gen.connections.optimizePeerTableOnRemote(gen.tables.at(entry));
    }
    return 0;
}

int QueryOracle::replaceQueryWithTablePeers(
    RandomGenerator & rg, const SQLQuery & sq1, StatementGenerator & gen, std::vector<SQLQuery> & peer_queries, SQLQuery & sq2)
{
    found_tables.clear();
    peer_queries.clear();

    sq2.CopyFrom(sq1);
    findTablesWithPeersAndReplace(rg, const_cast<Select &>(sq2.inner_query().select().sel()), gen);
    for (const auto & entry : found_tables)
    {
        SQLQuery next;
        const SQLTable & t = gen.tables.at(entry);
        Insert * ins = next.mutable_inner_query()->mutable_insert();
        SelectStatementCore * sel = ins->mutable_insert_select()->mutable_select()->mutable_select_core();

        //then insert
        gen.setTableRemote<false>(rg, t, ins->mutable_tfunction());
        JoinedTable * jt = sel->mutable_from()->mutable_tos()->mutable_join_clause()->mutable_tos()->mutable_joined_table();
        ExprSchemaTable * est = jt->mutable_est();
        if (t.db)
        {
            est->mutable_database()->set_database("d" + std::to_string(t.db->dname));
        }
        est->mutable_table()->set_table("t" + std::to_string(t.tname));
        jt->set_final(t.supportsFinal());
        gen.flatTableColumnPath(0, t, [](const SQLColumn & c) { return c.canBeInserted(); });
        for (const auto & colRef : gen.entries)
        {
            gen.columnPathRef(colRef, ins->add_cols());
            gen.columnPathRef(colRef, sel->add_result_columns()->mutable_etc()->mutable_col()->mutable_path());
        }
        gen.entries.clear();
        peer_queries.push_back(std::move(next));
    }
    return 0;
}

int QueryOracle::processOracleQueryResult(const bool first, const bool success, const std::string & oracle_name)
{
    bool & res = first ? first_success : second_sucess;

    if (success)
    {
        const std::filesystem::path & qfile = fc.db_file_path / "query.data";

        md5_hash.hashFile(qfile.generic_string(), first ? first_digest : second_digest);
    }
    res = success;
    if (!first && first_success && second_sucess
        && !std::equal(std::begin(first_digest), std::end(first_digest), std::begin(second_digest)))
    {
        throw std::runtime_error(oracle_name + " oracle failed");
    }
    return 0;
}

}
