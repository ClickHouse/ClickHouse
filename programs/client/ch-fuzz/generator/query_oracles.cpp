#include "statement_generator.h"

#include <filesystem>

namespace chfuzz {

/*
SELECT COUNT(*) FROM <FROM_CLAUSE> WHERE <PRED>;
or
SELECT COUNT(*) FROM <FROM_CLAUSE> WHERE <PRED1> GROUP BY <GROUP_BY CLAUSE> HAVING <PRED2>;
*/
int StatementGenerator::GenerateCorrectnessTestFirstQuery(RandomGenerator &rg, sql_query_grammar::SQLQuery &sq) {
	sql_query_grammar::SelectStatementCore *ssc = sq.mutable_inner_query()->mutable_select()->mutable_sel()->mutable_select_core();
	const uint32_t combination = rg.NextLargeNumber() % 3; /* 0 WHERE, 1 HAVING, 2 WHERE + HAVING */

	this->levels[this->current_level] = QueryLevel(this->current_level);
	GenerateFromStatement(rg, std::numeric_limits<uint32_t>::max(), ssc->mutable_from());

	const bool prev_allow_aggregates = this->levels[this->current_level].allow_aggregates,
			   prev_allow_window_funcs = this->levels[this->current_level].allow_window_funcs;
	this->levels[this->current_level].allow_aggregates = this->levels[this->current_level].allow_window_funcs = false;
	if (combination != 1) {
		GenerateWherePredicate(rg, ssc->mutable_where()->mutable_expr()->mutable_expr());
	}
	if (combination != 0) {
		GenerateGroupBy(rg, 1, true, true, ssc->mutable_groupby());
	}
	this->levels[this->current_level].allow_aggregates = prev_allow_aggregates;
	this->levels[this->current_level].allow_window_funcs = prev_allow_window_funcs;

	ssc->add_result_columns()->mutable_eca()->mutable_expr()->mutable_comp_expr()->mutable_func_call()->set_func(sql_query_grammar::FUNCcount);
	this->levels.erase(this->current_level);
	return 0;
}

/*
SELECT SUM(PRED) FROM <FROM_CLAUSE>;
or
SELECT SUM(PRED2) FROM <FROM_CLAUSE> WHERE <PRED1> GROUP BY <GROUP_BY CLAUSE>;
*/
int StatementGenerator::GenerateCorrectnessTestSecondQuery(sql_query_grammar::SQLQuery &sq1, sql_query_grammar::SQLQuery &sq2) {
	sql_query_grammar::SelectStatementCore &ssc1 =
		const_cast<sql_query_grammar::SelectStatementCore &>(sq1.inner_query().select().sel().select_core());
	sql_query_grammar::SelectStatementCore *ssc2 = sq2.mutable_inner_query()->mutable_select()->mutable_sel()->mutable_select_core();
	sql_query_grammar::SQLFuncCall *sfc = ssc2->add_result_columns()->mutable_eca()->mutable_expr()->mutable_comp_expr()->mutable_func_call();

	sfc->set_func(sql_query_grammar::FUNCsum);
	ssc2->set_allocated_from(ssc1.release_from());
	if (ssc1.has_groupby()) {
		sql_query_grammar::GroupByStatement &gbs = const_cast<sql_query_grammar::GroupByStatement &>(ssc1.groupby());

		sfc->add_args()->set_allocated_expr(gbs.release_having_expr());
		ssc2->set_allocated_groupby(ssc1.release_groupby());
		ssc2->set_allocated_where(ssc1.release_where());
	} else {
		sql_query_grammar::ExprComparisonHighProbability &expr = const_cast<sql_query_grammar::ExprComparisonHighProbability &>(ssc1.where().expr());

		sfc->add_args()->set_allocated_expr(expr.release_expr());
	}
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
	{sql_query_grammar::OutFormat::OUT_ProtobufList, sql_query_grammar::InFormat::IN_ProtobufList},
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

int StatementGenerator::GenerateExportQuery(RandomGenerator &rg, sql_query_grammar::SQLQuery &sq1) {
	bool first = true;
	NestedType *ntp = nullptr;
	const SQLTable &t = rg.PickValueRandomlyFromMap(this->tables);
	sql_query_grammar::Insert *ins = sq1.mutable_inner_query()->mutable_insert();
	sql_query_grammar::FileFunc *ff = ins->mutable_tfunction()->mutable_file();
	sql_query_grammar::SelectStatementCore *sel = ins->mutable_select()->mutable_select_core();
	auto outf = std::filesystem::temp_directory_path() / "table.data"; //TODO fix this

	if (std::filesystem::exists(outf)) {
		std::filesystem::resize_file(outf, 0); //truncate the file
	}
	ff->set_path(outf.generic_string());
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
	jt->set_final(rg.NextSmallNumber() < 3);
	return 0;
}

int StatementGenerator::GenerateClearQuery(sql_query_grammar::SQLQuery &sq1, sql_query_grammar::SQLQuery &sq2) {
	sql_query_grammar::Truncate *trunc = sq2.mutable_inner_query()->mutable_trunc();
	sql_query_grammar::JoinedTable &jt =
		const_cast<sql_query_grammar::JoinedTable &>(sq1.inner_query().insert().select().select_core().from().tos().join_clause().tos().joined_table());

	trunc->set_allocated_est(jt.release_est());
	return 0;
}

int StatementGenerator::GenerateImportQuery(sql_query_grammar::SQLQuery &sq1, sql_query_grammar::SQLQuery &sq2, sql_query_grammar::SQLQuery &sq3) {
	NestedType *ntp = nullptr;
	sql_query_grammar::Insert *ins = sq3.mutable_inner_query()->mutable_insert();
	sql_query_grammar::InsertIntoTable *iit = ins->mutable_itable();
	sql_query_grammar::InsertFromFile *iff = ins->mutable_ffile();
	sql_query_grammar::Truncate &trunc = const_cast<sql_query_grammar::Truncate &>(sq2.inner_query().trunc());
	const sql_query_grammar::FileFunc &ff = sq1.inner_query().insert().tfunction().file();
	const uint32_t tname = static_cast<uint32_t>(std::stoul(trunc.est().table_name().table().substr(1)));
	const SQLTable &t = this->tables[tname];

	iit->set_allocated_est(trunc.release_est());
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

}
