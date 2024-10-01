#include "query_oracle.h"

#include <cstdio>

namespace chfuzz {

/*
Correctness query oracle
*/
/*
SELECT COUNT(*) FROM <FROM_CLAUSE> WHERE <PRED>;
or
SELECT COUNT(*) FROM <FROM_CLAUSE> WHERE <PRED1> GROUP BY <GROUP_BY CLAUSE> HAVING <PRED2>;
*/
int QueryOracle::GenerateCorrectnessTestFirstQuery(RandomGenerator &rg, StatementGenerator &gen, sql_query_grammar::SQLQuery &sq1) {
	const std::filesystem::path &qfile = fc.db_file_path / "query.data";
	sql_query_grammar::TopSelect *ts = sq1.mutable_inner_query()->mutable_select();
	sql_query_grammar::SelectIntoFile *sif = ts->mutable_intofile();
	sql_query_grammar::SelectStatementCore *ssc = ts->mutable_sel()->mutable_select_core();
	const uint32_t combination = 0;//TODO fix this rg.NextLargeNumber() % 3; /* 0 WHERE, 1 HAVING, 2 WHERE + HAVING */

	gen.levels[gen.current_level] = QueryLevel(gen.current_level);
	gen.GenerateFromStatement(rg, std::numeric_limits<uint32_t>::max(), ssc->mutable_from());

	const bool prev_allow_aggregates = gen.levels[gen.current_level].allow_aggregates,
			   prev_allow_window_funcs = gen.levels[gen.current_level].allow_window_funcs;
	gen.levels[gen.current_level].allow_aggregates = gen.levels[gen.current_level].allow_window_funcs = false;
	if (combination != 1) {
		gen.GenerateWherePredicate(rg, ssc->mutable_where()->mutable_expr()->mutable_expr());
	}
	if (combination != 0) {
		gen.GenerateGroupBy(rg, 1, true, true, ssc->mutable_groupby());
	}
	gen.levels[gen.current_level].allow_aggregates = prev_allow_aggregates;
	gen.levels[gen.current_level].allow_window_funcs = prev_allow_window_funcs;

	ssc->add_result_columns()->mutable_eca()->mutable_expr()->mutable_comp_expr()->mutable_func_call()->set_func(sql_query_grammar::FUNCcount);
	gen.levels.erase(gen.current_level);
	ts->set_format(sql_query_grammar::OutFormat::OUT_RawBLOB);
	sif->set_path(qfile.generic_string());
	sif->set_step(sql_query_grammar::SelectIntoFile_SelectIntoFileStep::SelectIntoFile_SelectIntoFileStep_TRUNCATE);
	return 0;
}

/*
SELECT ifNull(SUM(PRED),0) FROM <FROM_CLAUSE>;
or
SELECT ifNull(SUM(PRED2),0) FROM <FROM_CLAUSE> WHERE <PRED1> GROUP BY <GROUP_BY CLAUSE>;
*/
int QueryOracle::GenerateCorrectnessTestSecondQuery(sql_query_grammar::SQLQuery &sq1, sql_query_grammar::SQLQuery &sq2) {
	const std::filesystem::path &qfile = fc.db_file_path / "query.data";
	sql_query_grammar::TopSelect *ts = sq2.mutable_inner_query()->mutable_select();
	sql_query_grammar::SelectIntoFile *sif = ts->mutable_intofile();
	sql_query_grammar::SelectStatementCore &ssc1 =
		const_cast<sql_query_grammar::SelectStatementCore &>(sq1.inner_query().select().sel().select_core());
	sql_query_grammar::SelectStatementCore *ssc2 = ts->mutable_sel()->mutable_select_core();
	sql_query_grammar::SQLFuncCall *sfc1 = ssc2->add_result_columns()->mutable_eca()->mutable_expr()->mutable_comp_expr()->mutable_func_call();
	sql_query_grammar::SQLFuncCall *sfc2 = sfc1->add_args()->mutable_expr()->mutable_comp_expr()->mutable_func_call();

	sfc1->set_func(sql_query_grammar::FUNCifNull);
	sfc1->add_args()->mutable_expr()->mutable_lit_val()->set_special_val(sql_query_grammar::SpecialVal::VAL_ZERO);
	sfc2->set_func(sql_query_grammar::FUNCsum);

	ssc2->set_allocated_from(ssc1.release_from());
	if (ssc1.has_groupby()) {
		sql_query_grammar::GroupByStatement &gbs = const_cast<sql_query_grammar::GroupByStatement &>(ssc1.groupby());

		sfc2->add_args()->set_allocated_expr(gbs.release_having_expr());
		ssc2->set_allocated_groupby(ssc1.release_groupby());
		ssc2->set_allocated_where(ssc1.release_where());
	} else {
		sql_query_grammar::ExprComparisonHighProbability &expr = const_cast<sql_query_grammar::ExprComparisonHighProbability &>(ssc1.where().expr());

		sfc2->add_args()->set_allocated_expr(expr.release_expr());
	}
	ts->set_format(sql_query_grammar::OutFormat::OUT_RawBLOB);
	sif->set_path(qfile.generic_string());
	sif->set_step(sql_query_grammar::SelectIntoFile_SelectIntoFileStep::SelectIntoFile_SelectIntoFileStep_TRUNCATE);
	return 0;
}

/*
Dump and read table oracle
*/
int QueryOracle::DumpTableContent(RandomGenerator &rg, const SQLTable &t, sql_query_grammar::SQLQuery &sq1) {
	const std::filesystem::path &qfile = fc.db_file_path / "query.data";
	sql_query_grammar::TopSelect *ts = sq1.mutable_inner_query()->mutable_select();
	sql_query_grammar::SelectIntoFile *sif = ts->mutable_intofile();
	sql_query_grammar::SelectStatementCore *sel = ts->mutable_sel()->mutable_select_core();
	sql_query_grammar::JoinedTable *jt = sel->mutable_from()->mutable_tos()->mutable_join_clause()->mutable_tos()->mutable_joined_table();

	jt->mutable_est()->mutable_table_name()->set_table("t" + std::to_string(t.tname));
	jt->set_final(t.SupportsFinal() && rg.NextSmallNumber() < 3);
	ts->set_format(sql_query_grammar::OutFormat::OUT_RawBLOB);
	sif->set_path(qfile.generic_string());
	sif->set_step(sql_query_grammar::SelectIntoFile_SelectIntoFileStep::SelectIntoFile_SelectIntoFileStep_TRUNCATE);
	return 0;
}

static const std::map<sql_query_grammar::OutFormat, sql_query_grammar::InFormat> out_in{
	{sql_query_grammar::OutFormat::OUT_TabSeparated, sql_query_grammar::InFormat::IN_TabSeparated},
	{sql_query_grammar::OutFormat::OUT_TabSeparatedRaw, sql_query_grammar::InFormat::IN_TabSeparatedRaw},
	{sql_query_grammar::OutFormat::OUT_TabSeparatedWithNames, sql_query_grammar::InFormat::IN_TabSeparatedWithNames},
	{sql_query_grammar::OutFormat::OUT_TabSeparatedWithNamesAndTypes, sql_query_grammar::InFormat::IN_TabSeparatedWithNamesAndTypes},
	{sql_query_grammar::OutFormat::OUT_TabSeparatedRawWithNames, sql_query_grammar::InFormat::IN_TabSeparatedRawWithNames},
	{sql_query_grammar::OutFormat::OUT_TabSeparatedRawWithNamesAndTypes, sql_query_grammar::InFormat::IN_TabSeparatedRawWithNamesAndTypes},
	{sql_query_grammar::OutFormat::OUT_Template, sql_query_grammar::InFormat::IN_Template},
	{sql_query_grammar::OutFormat::OUT_CSV, sql_query_grammar::InFormat::IN_CSV},
	{sql_query_grammar::OutFormat::OUT_CSVWithNames, sql_query_grammar::InFormat::IN_CSVWithNames},
	{sql_query_grammar::OutFormat::OUT_CSVWithNamesAndTypes, sql_query_grammar::InFormat::IN_CSVWithNamesAndTypes},
	{sql_query_grammar::OutFormat::OUT_CustomSeparated, sql_query_grammar::InFormat::IN_CustomSeparated},
	{sql_query_grammar::OutFormat::OUT_CustomSeparatedWithNames, sql_query_grammar::InFormat::IN_CustomSeparatedWithNames},
	{sql_query_grammar::OutFormat::OUT_CustomSeparatedWithNamesAndTypes, sql_query_grammar::InFormat::IN_CustomSeparatedWithNamesAndTypes},
	{sql_query_grammar::OutFormat::OUT_Values, sql_query_grammar::InFormat::IN_Values},
	{sql_query_grammar::OutFormat::OUT_JSON, sql_query_grammar::InFormat::IN_JSON},
	{sql_query_grammar::OutFormat::OUT_JSONStrings, sql_query_grammar::InFormat::IN_JSONStrings},
	{sql_query_grammar::OutFormat::OUT_JSONColumns, sql_query_grammar::InFormat::IN_JSONColumns},
	{sql_query_grammar::OutFormat::OUT_JSONColumnsWithMetadata, sql_query_grammar::InFormat::IN_JSONColumnsWithMetadata},
	{sql_query_grammar::OutFormat::OUT_JSONCompact, sql_query_grammar::InFormat::IN_JSONCompact},
	{sql_query_grammar::OutFormat::OUT_JSONCompactColumns, sql_query_grammar::InFormat::IN_JSONCompactColumns},
	{sql_query_grammar::OutFormat::OUT_JSONEachRow, sql_query_grammar::InFormat::IN_JSONEachRow},
	{sql_query_grammar::OutFormat::OUT_JSONStringsEachRow, sql_query_grammar::InFormat::IN_JSONStringsEachRow},
	{sql_query_grammar::OutFormat::OUT_JSONCompactEachRow, sql_query_grammar::InFormat::IN_JSONCompactEachRow},
	{sql_query_grammar::OutFormat::OUT_JSONCompactEachRowWithNames, sql_query_grammar::InFormat::IN_JSONCompactEachRowWithNames},
	{sql_query_grammar::OutFormat::OUT_JSONCompactEachRowWithNamesAndTypes, sql_query_grammar::InFormat::IN_JSONCompactEachRowWithNamesAndTypes},
	{sql_query_grammar::OutFormat::OUT_JSONCompactStringsEachRow, sql_query_grammar::InFormat::IN_JSONCompactStringsEachRow},
	{sql_query_grammar::OutFormat::OUT_JSONCompactStringsEachRowWithNames, sql_query_grammar::InFormat::IN_JSONCompactStringsEachRowWithNames},
	{sql_query_grammar::OutFormat::OUT_JSONCompactStringsEachRowWithNamesAndTypes, sql_query_grammar::InFormat::IN_JSONCompactStringsEachRowWithNamesAndTypes},
	{sql_query_grammar::OutFormat::OUT_JSONObjectEachRow, sql_query_grammar::InFormat::IN_JSONObjectEachRow},
	{sql_query_grammar::OutFormat::OUT_BSONEachRow, sql_query_grammar::InFormat::IN_BSONEachRow},
	{sql_query_grammar::OutFormat::OUT_TSKV, sql_query_grammar::InFormat::IN_TSKV},
	{sql_query_grammar::OutFormat::OUT_Protobuf, sql_query_grammar::InFormat::IN_Protobuf},
	{sql_query_grammar::OutFormat::OUT_ProtobufSingle, sql_query_grammar::InFormat::IN_ProtobufSingle},
	//{sql_query_grammar::OutFormat::OUT_ProtobufList, sql_query_grammar::InFormat::IN_ProtobufList},
	{sql_query_grammar::OutFormat::OUT_Avro, sql_query_grammar::InFormat::IN_Avro},
	{sql_query_grammar::OutFormat::OUT_Parquet, sql_query_grammar::InFormat::IN_Parquet},
	{sql_query_grammar::OutFormat::OUT_Arrow, sql_query_grammar::InFormat::IN_Arrow},
	{sql_query_grammar::OutFormat::OUT_ArrowStream, sql_query_grammar::InFormat::IN_ArrowStream},
	{sql_query_grammar::OutFormat::OUT_ORC, sql_query_grammar::InFormat::IN_ORC},
	{sql_query_grammar::OutFormat::OUT_Npy, sql_query_grammar::InFormat::IN_Npy},
	{sql_query_grammar::OutFormat::OUT_RowBinary, sql_query_grammar::InFormat::IN_RowBinary},
	{sql_query_grammar::OutFormat::OUT_RowBinaryWithNames, sql_query_grammar::InFormat::IN_RowBinaryWithNames},
	{sql_query_grammar::OutFormat::OUT_RowBinaryWithNamesAndTypes, sql_query_grammar::InFormat::IN_RowBinaryWithNamesAndTypes},
	{sql_query_grammar::OutFormat::OUT_Native, sql_query_grammar::InFormat::IN_Native},
	{sql_query_grammar::OutFormat::OUT_CapnProto, sql_query_grammar::InFormat::IN_CapnProto},
	{sql_query_grammar::OutFormat::OUT_LineAsString, sql_query_grammar::InFormat::IN_LineAsString},
	{sql_query_grammar::OutFormat::OUT_RawBLOB, sql_query_grammar::InFormat::IN_RawBLOB},
	{sql_query_grammar::OutFormat::OUT_MsgPack, sql_query_grammar::InFormat::IN_MsgPack}
};

int QueryOracle::GenerateExportQuery(RandomGenerator &rg, const SQLTable &t, sql_query_grammar::SQLQuery &sq2) {
	bool first = true;
	NestedType *ntp = nullptr;
	sql_query_grammar::Insert *ins = sq2.mutable_inner_query()->mutable_insert();
	sql_query_grammar::FileFunc *ff = ins->mutable_tfunction()->mutable_file();
	sql_query_grammar::SelectStatementCore *sel = ins->mutable_select()->mutable_select_core();
	const std::filesystem::path &nfile = fc.db_file_path / "table.data";

	if (std::filesystem::exists(nfile)) {
		std::remove(nfile.generic_string().c_str()); //remove the file
	}
	ff->set_path(nfile.generic_string());
	ff->set_outformat(rg.PickKeyRandomlyFromMap(out_in));

	buf.resize(0);
	for (const auto &col : t.cols) {
		if ((ntp = dynamic_cast<NestedType*>(col.second.tp))) {
			for (const auto &entry2 : ntp->subtypes) {
				const std::string &cname = "c" + std::to_string(col.first) + ".c" + std::to_string(entry2.cname);

				if (!first) {
					buf += ", ";
				}
				buf += cname;
				buf += " ";
				buf += entry2.subtype->TypeName(true);
				sel->add_result_columns()->mutable_etc()->mutable_col()->mutable_col()->set_column(std::move(cname));
				first = false;
			}
		} else {
			const std::string &cname = "c" + std::to_string(col.first);

			if (!first) {
				buf += ", ";
			}
			buf += cname;
			buf += " ";
			buf += col.second.tp->TypeName(true);
			if (col.second.nullable.has_value()) {
				buf += col.second.nullable.value() ? " NOT" : "";
				buf += " NULL";
			}
			sel->add_result_columns()->mutable_etc()->mutable_col()->mutable_col()->set_column(std::move(cname));
			first = false;
		}
	}
	ff->set_structure(buf);
	if (rg.NextSmallNumber() < 4) {
		ff->set_fcomp(static_cast<sql_query_grammar::FileCompression>((rg.NextRandomUInt32() % static_cast<uint32_t>(sql_query_grammar::FileCompression_MAX)) + 1));
	}

	//Set the table on select
	sql_query_grammar::JoinedTable *jt = sel->mutable_from()->mutable_tos()->mutable_join_clause()->mutable_tos()->mutable_joined_table();
	jt->mutable_est()->mutable_table_name()->set_table("t" + std::to_string(t.tname));
	return 0;
}

int QueryOracle::GenerateClearQuery(const SQLTable &t, sql_query_grammar::SQLQuery &sq3) {
	sql_query_grammar::Truncate *trunc = sq3.mutable_inner_query()->mutable_trunc();
	trunc->mutable_est()->mutable_table_name()->set_table("t" + std::to_string(t.tname));
	return 0;
}

int QueryOracle::GenerateImportQuery(const SQLTable &t, const sql_query_grammar::SQLQuery &sq2, sql_query_grammar::SQLQuery &sq4) {
	NestedType *ntp = nullptr;
	sql_query_grammar::Insert *ins = sq4.mutable_inner_query()->mutable_insert();
	sql_query_grammar::InsertIntoTable *iit = ins->mutable_itable();
	sql_query_grammar::InsertFromFile *iff = ins->mutable_ffile();
	const sql_query_grammar::FileFunc &ff = sq2.inner_query().insert().tfunction().file();

	iit->mutable_est()->mutable_table_name()->set_table("t" + std::to_string(t.tname));
	for (const auto &entry : t.cols) {
		if ((ntp = dynamic_cast<NestedType*>(entry.second.tp))) {
			for (const auto &entry2 : ntp->subtypes) {
				sql_query_grammar::ColumnPath *cp = iit->add_cols();

				cp->mutable_col()->set_column("c" + std::to_string(entry.first));
				cp->add_sub_cols()->set_column("c" + std::to_string(entry2.cname));
			}
		} else {
			sql_query_grammar::ColumnPath *cp = iit->add_cols();

			cp->mutable_col()->set_column("c" + std::to_string(entry.first));
		}
	}
	iff->set_path(ff.path());
	iff->set_format(out_in.at(ff.outformat()));
	if (ff.has_fcomp()) {
		iff->set_fcomp(ff.fcomp());
	}
	return 0;
}

/*
Run query with different settings oracle
*/
typedef struct TestSetting {
	const std::string tsetting;
	const std::set<std::string> options;

	TestSetting (const std::string &sett, const std::set<std::string> &noptions) :
		tsetting(sett), options(noptions) {}
} TestSetting;

static const std::vector<TestSetting> test_settings{
	TestSetting("aggregate_functions_null_for_empty", {"0", "1"}),
	TestSetting("aggregation_in_order_max_block_bytes", {"0", "1"}),
	TestSetting("any_join_distinct_right_table_keys", {"0", "1"}),
	TestSetting("async_insert", {"0", "1"}),
	TestSetting("cast_keep_nullable", {"0", "1"}),
	TestSetting("check_query_single_value_result", {"0", "1"}),
	TestSetting("compile_aggregate_expressions", {"0", "1"}),
	TestSetting("compile_sort_description", {"0", "1"}),
	TestSetting("cross_join_min_bytes_to_compress", {"0", "1"}),
	TestSetting("cross_join_min_rows_to_compress", {"0", "1"}),
	TestSetting("data_type_default_nullable", {"0", "1"}),
	TestSetting("deduplicate_blocks_in_dependent_materialized_views", {"0", "1"}),
	TestSetting("describe_include_subcolumns", {"0", "1"}),
	TestSetting("distributed_aggregation_memory_efficient", {"0", "1"}),
	TestSetting("enable_analyzer", {"0", "1"}),
	TestSetting("enable_memory_bound_merging_of_aggregation_results", {"0", "1"}),
	TestSetting("enable_multiple_prewhere_read_steps", {"0", "1"}),
	TestSetting("exact_rows_before_limit", {"0", "1"}),
	TestSetting("flatten_nested", {"0", "1"}),
	TestSetting("force_optimize_projection", {"0", "1"}),
	TestSetting("fsync_metadata", {"0", "1"}),
	TestSetting("group_by_two_level_threshold", {"0", "1"}),
	TestSetting("group_by_two_level_threshold_bytes", {"0", "1"}),
	TestSetting("group_by_use_nulls", {"0", "1"}),
	TestSetting("http_wait_end_of_query", {"0", "1"}),
	TestSetting("input_format_import_nested_json", {"0", "1"}),
	TestSetting("input_format_parallel_parsing", {"0", "1"}),
	TestSetting("insert_null_as_default", {"0", "1"}),
	TestSetting("join_algorithm", {"'default'", "'grace_hash'", "'hash'", "'parallel_hash'", "'partial_merge'",
								   "'direct'", "'auto'", "'full_sorting_merge'", "'prefer_partial_merge'"}),
	TestSetting("join_any_take_last_row", {"0", "1"}),
	TestSetting("join_use_nulls", {"0", "1"}),
	TestSetting("local_filesystem_read_method", {"'read'", "'pread'", "'mmap'", "'pread_threadpool'", "'io_uring'"}),
	TestSetting("local_filesystem_read_prefetch", {"0", "1"}),
	TestSetting("log_query_threads", {"0", "1"}),
	TestSetting("low_cardinality_use_single_dictionary_for_part", {"0", "1"}),
	TestSetting("max_bytes_before_external_group_by", {"0", "1"}),
	TestSetting("max_bytes_before_external_sort", {"0", "1"}),
	TestSetting("max_bytes_before_remerge_sort", {"0", "1"}),
	TestSetting("max_final_threads", {"0", "1"}),
	TestSetting("max_threads", {"1", std::to_string(std::thread::hardware_concurrency())}),
	TestSetting("min_chunk_bytes_for_parallel_parsing", {"0", "1"}),
	TestSetting("min_external_table_block_size_bytes", {"0", "1"}),
	TestSetting("optimize_aggregation_in_order", {"0", "1"}),
	TestSetting("optimize_append_index", {"0", "1"}),
	TestSetting("optimize_distinct_in_order", {"0", "1"}),
	TestSetting("optimize_functions_to_subcolumns", {"0", "1"}),
	TestSetting("optimize_if_chain_to_multiif", {"0", "1"}),
	TestSetting("optimize_if_transform_strings_to_enum", {"0", "1"}),
	TestSetting("optimize_move_to_prewhere_if_final", {"0", "1"}),
	TestSetting("optimize_or_like_chain", {"0", "1"}),
	TestSetting("optimize_read_in_order", {"0", "1"}),
	TestSetting("optimize_skip_merged_partitions", {"0", "1"}),
	TestSetting("optimize_sorting_by_input_stream_properties", {"0", "1"}),
	TestSetting("optimize_substitute_columns", {"0", "1"}),
	TestSetting("optimize_syntax_fuse_functions", {"0", "1"}),
	TestSetting("optimize_trivial_approximate_count_query", {"0", "1"}),
	TestSetting("output_format_parallel_formatting", {"0", "1"}),
	TestSetting("output_format_pretty_highlight_digit_groups", {"0", "1"}),
	TestSetting("output_format_pretty_row_numbers", {"0", "1"}),
	TestSetting("output_format_write_statistics", {"0", "1"}),
	TestSetting("page_cache_inject_eviction", {"0", "1"}),
	TestSetting("partial_merge_join_optimizations", {"0", "1"}),
	TestSetting("precise_float_parsing", {"0", "1"}),
	TestSetting("prefer_external_sort_block_bytes", {"0", "1"}),
	TestSetting("prefer_merge_sort_block_bytes", {"0", "1"}),
	TestSetting("prefer_localhost_replica", {"0", "1"}),
	TestSetting("query_plan_aggregation_in_order", {"0", "1"}),
	TestSetting("query_plan_enable_optimizations", {"0", "1"}),
	TestSetting("read_from_filesystem_cache_if_exists_otherwise_bypass_cache", {"0", "1"}),
	TestSetting("read_in_order_use_buffering", {"0", "1"}),
	TestSetting("remote_filesystem_read_prefetch", {"0", "1"}),
	TestSetting("rows_before_aggregation", {"0", "1"}),
	TestSetting("throw_on_error_from_cache_on_write_operations", {"0", "1"}),
	TestSetting("transform_null_in", {"0", "1"}),
	TestSetting("ttl_only_drop_parts", {"0", "1"}),
	TestSetting("update_insert_deduplication_token_in_dependent_materialized_views", {"0", "1"}),
	TestSetting("use_page_cache_for_disks_without_file_cache", {"0", "1"}),
	TestSetting("use_skip_indexes", {"0", "1"}),
	TestSetting("use_uncompressed_cache", {"0", "1"}),
	TestSetting("use_variant_as_common_type", {"0", "1"})
};

int QueryOracle::GenerateFirstSetting(RandomGenerator &rg, sql_query_grammar::SQLQuery &sq1) {
	const uint32_t nsets = rg.NextBool() ? 1 : ((rg.NextSmallNumber() % 3) + 1);
	sql_query_grammar::SettingValues *sv = sq1.mutable_inner_query()->mutable_setting_values();

	nsettings.clear();
	for (uint32_t i = 0 ; i < nsets; i++) {
		const TestSetting &ts = rg.PickRandomlyFromVector(test_settings);
		sql_query_grammar::SetValue *setv = i == 0 ? sv->mutable_set_value() : sv->add_other_values();

		setv->set_property(ts.tsetting);
		if (ts.options.size() == 2) {
			if (rg.NextBool()) {
				setv->set_value(*ts.options.begin());
				nsettings.push_back(*std::next(ts.options.begin(), 1));
			} else {
				setv->set_value(*std::next(ts.options.begin(), 1));
				nsettings.push_back(*(ts.options.begin()));
			}
		} else {
			setv->set_value(rg.PickRandomlyFromSet(ts.options));
			nsettings.push_back(rg.PickRandomlyFromSet(ts.options));
		}
	}
	return 0;
}

int QueryOracle::GenerateSecondSetting(const sql_query_grammar::SQLQuery &sq1, sql_query_grammar::SQLQuery &sq3) {
	const sql_query_grammar::SettingValues &osv = sq1.inner_query().setting_values();
	sql_query_grammar::SettingValues *sv = sq3.mutable_inner_query()->mutable_setting_values();

	for (size_t i = 0 ; i < nsettings.size(); i++) {
		const sql_query_grammar::SetValue &osetv = i == 0 ? osv.set_value() : osv.other_values(static_cast<int>(i - 1));
		sql_query_grammar::SetValue *setv = i == 0 ? sv->mutable_set_value() : sv->add_other_values();

		setv->set_property(osetv.property());
		setv->set_value(nsettings[i]);
	}
	return 0;
}

int QueryOracle::GenerateSettingQuery(RandomGenerator &rg, StatementGenerator &gen, sql_query_grammar::SQLQuery &sq2) {
	const std::filesystem::path &qfile = fc.db_file_path / "query.data";
	sql_query_grammar::TopSelect *ts = sq2.mutable_inner_query()->mutable_select();
	sql_query_grammar::SelectIntoFile *sif = ts->mutable_intofile();

	gen.GenerateTopSelect(rg, ts);
	ts->set_format(sql_query_grammar::OutFormat::OUT_RawBLOB);
	sif->set_path(qfile.generic_string());
	sif->set_step(sql_query_grammar::SelectIntoFile_SelectIntoFileStep::SelectIntoFile_SelectIntoFileStep_TRUNCATE);
	return 0;
}

int QueryOracle::ProcessOracleQueryResult(const bool first, const bool success, const std::string &oracle_name) {
	bool &res = first ? first_success : second_sucess;

	if (success) {
		const std::filesystem::path &qfile = fc.db_file_path / "query.data";

		md5_hash.hashFile(qfile.generic_string(), first ? first_digest : second_digest);
	}
	res = success;
	if (!first && first_success && second_sucess &&
		!std::equal(std::begin(first_digest), std::end(first_digest), std::begin(second_digest))) {
		throw std::runtime_error(oracle_name + " oracle failed");
	}
	return 0;
}

}
