#include "statement_generator.h"

#include <cstdint>
#include <string>

namespace chfuzz {

int StatementGenerator::GenerateArrayJoin(RandomGenerator &rg, sql_query_grammar::ArrayJoin *aj) {
	SQLRelation rel("");
	std::string cname = "c" + std::to_string(this->levels[this->current_level].aliases_counter++);
	sql_query_grammar::ExprColAlias *eca = aj->mutable_constraint();
	sql_query_grammar::Expr *expr = eca->mutable_expr();

	aj->set_left(rg.NextBool());
	if (rg.NextSmallNumber() < 8) {
		const SQLRelation &rel1 = rg.PickRandomlyFromVector(this->levels[this->current_level].rels);
		const SQLRelationCol &col1 = rg.PickRandomlyFromVector(rel1.cols);
		sql_query_grammar::ExprSchemaTableColumn *estc = expr->mutable_comp_expr()->mutable_expr_stc();
		sql_query_grammar::ExprColumn *ecol = estc->mutable_col();

		if (rel1.name != "") {
			estc->mutable_table()->set_table(rel1.name);
		}
		ecol->mutable_col()->set_column(col1.name);
		AddFieldAccess(rg, expr, 16);
		AddColNestedAccess(rg, ecol, 31);
	} else {
		GenerateExpression(rg, expr);
	}
	rel.cols.push_back(SQLRelationCol("", cname));
	this->levels[this->current_level].rels.push_back(std::move(rel));
	eca->mutable_col_alias()->set_column(cname);
	return 0;
}

int StatementGenerator::GenerateDerivedTable(RandomGenerator &rg, SQLRelation &rel, const uint32_t allowed_clauses, sql_query_grammar::Select *sel) {
	std::map<uint32_t, QueryLevel> levels_backup;
	uint32_t ncols = std::min(this->max_width - this->width, (rg.NextMediumNumber() % UINT32_C(5)) + 1);

	for (const auto &entry : this->levels) {
		levels_backup[entry.first] = std::move(entry.second);
	}
	this->levels.clear();

	this->current_level++;
	this->levels[this->current_level] = QueryLevel(this->current_level);
	GenerateSelect(rg, false, ncols, allowed_clauses, sel);
	this->current_level--;

	for (const auto &entry : levels_backup) {
		this->levels[entry.first] = std::move(entry.second);
	}

	if (sel->has_select_core()) {
		const sql_query_grammar::SelectStatementCore &scc = sel->select_core();

		for (int i = 0; i < scc.result_columns_size(); i++) {
			rel.cols.push_back(SQLRelationCol(rel.name, scc.result_columns(i).eca().col_alias().column()));
		}
	} else if (sel->has_set_query()) {
		const sql_query_grammar::Select *aux = &sel->set_query().sel1();

		while (aux->has_set_query()) {
			aux = &aux->set_query().sel1();
		}
		if (aux->has_select_core()) {
			const sql_query_grammar::SelectStatementCore &scc = aux->select_core();
			for (int i = 0; i < scc.result_columns_size(); i++) {
				rel.cols.push_back(SQLRelationCol(rel.name, scc.result_columns(i).eca().col_alias().column()));
			}
		}
	}
	if (rel.cols.empty()) {
		rel.cols.push_back(SQLRelationCol(rel.name, "c0"));
	}
	return 0;
}

int StatementGenerator::GenerateFromElement(RandomGenerator &rg, const uint32_t allowed_clauses, sql_query_grammar::TableOrSubquery *tos) {
	std::string name;
	const uint32_t derived_table = 30 * static_cast<uint32_t>(this->depth < this->max_depth && this->width < this->max_width),
				   cte = 10 * static_cast<uint32_t>(!this->ctes.empty()),
				   table = 40 * static_cast<uint32_t>(!this->tables.empty()),
				   view = 20 * static_cast<uint32_t>(!this->views.empty()),
				   prob_space = derived_table + cte + table + view;

	name += "t";
	name += std::to_string(this->levels[this->current_level].rels.size());
	name += "d";
	name += std::to_string(this->current_level);
	if (prob_space) {
		std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
		const uint32_t nopt = next_dist(rg.gen);

		if (derived_table && nopt < (derived_table + 1)) {
			SQLRelation rel(name);
			sql_query_grammar::JoinedDerivedQuery *jdq = tos->mutable_jdq();

			GenerateDerivedTable(rg, rel, allowed_clauses, jdq->mutable_select());
			jdq->mutable_table_alias()->set_table(name);
			this->levels[this->current_level].rels.push_back(std::move(rel));
		} else if (cte && nopt < (derived_table + cte + 1)) {
			SQLRelation rel(name);
			sql_query_grammar::JoinedTable *jt = tos->mutable_jt();
			const auto &next_cte = rg.PickValueRandomlyFromMap(rg.PickValueRandomlyFromMap(this->ctes));

			jt->mutable_est()->mutable_table_name()->set_table(next_cte.name);
			for (const auto &entry : next_cte.cols) {
				rel.cols.push_back(entry);
			}
			jt->mutable_table_alias()->set_table(name);
			this->levels[this->current_level].rels.push_back(std::move(rel));
		} else if (table && nopt < (derived_table + cte + table + 1)) {
			sql_query_grammar::JoinedTable *jt = tos->mutable_jt();
			const SQLTable &t = rg.PickValueRandomlyFromMap(this->tables);

			jt->mutable_est()->mutable_table_name()->set_table("t" + std::to_string(t.tname));
			jt->mutable_table_alias()->set_table(name);
			jt->set_final(t.teng != sql_query_grammar::TableEngineValues::Memory &&
						  t.teng != sql_query_grammar::TableEngineValues::MergeTree &&
						  rg.NextSmallNumber() < 3);
			AddTableRelation(rg, true, name, t);
		} else if (view && nopt < (derived_table + cte + table + view + 1)) {
			SQLRelation rel(name);
			sql_query_grammar::JoinedTable *jt = tos->mutable_jt();
			const SQLView &v = rg.PickValueRandomlyFromMap(this->views);

			jt->mutable_est()->mutable_table_name()->set_table("v" + std::to_string(v.vname));
			jt->mutable_table_alias()->set_table(name);
			jt->set_final(!v.is_materialized && rg.NextSmallNumber() < 3);
			for (uint32_t i = 0 ; i < v.ncols; i++) {
				rel.cols.push_back(SQLRelationCol(name, "c" + std::to_string(i)));
			}
			this->levels[this->current_level].rels.push_back(std::move(rel));
		} else {
			assert(0);
		}
	} else {
		//fallback case
		SQLRelation rel(name);
		sql_query_grammar::JoinedDerivedQuery *jdq = tos->mutable_jdq();
		sql_query_grammar::Select *sel = jdq->mutable_select();
		sql_query_grammar::SelectStatementCore *ssc = sel->mutable_select_core();
		sql_query_grammar::ExprColAlias *eca = ssc->add_result_columns()->mutable_eca();

		GenerateLiteralValue(rg, eca->mutable_expr());
		eca->mutable_col_alias()->set_column("c0");
		rel.cols.push_back(SQLRelationCol(name, "c0"));
		jdq->mutable_table_alias()->set_table(name);
		this->levels[this->current_level].rels.push_back(std::move(rel));
	}
	return 0;
}

int StatementGenerator::AddJoinClause(RandomGenerator &rg, sql_query_grammar::BinaryExpr *bexpr) {
	const SQLRelation *rel1 = &rg.PickRandomlyFromVector(this->levels[this->current_level].rels),
					  *rel2 = &this->levels[this->current_level].rels.back();

	if (rel1->name == rel2->name) {
		rel1 = &this->levels[this->current_level].rels[this->levels[this->current_level].rels.size() - 2];
	}
	if (rg.NextSmallNumber() < 4) {
		//swap
		const SQLRelation *rel3 = rel1;
		rel1 = rel2;
		rel2 = rel3;
	}
	bexpr->set_op(rg.NextSmallNumber() < 9 ? sql_query_grammar::BinaryOperator::BINOP_EQ :
		static_cast<sql_query_grammar::BinaryOperator>((rg.NextRandomUInt32() % static_cast<uint32_t>(sql_query_grammar::BinaryOperator::BINOP_LEGR)) + 1));
	const SQLRelationCol &col1 = rg.PickRandomlyFromVector(rel1->cols),
						 &col2 = rg.PickRandomlyFromVector(rel2->cols);
	sql_query_grammar::Expr *expr1 = bexpr->mutable_lhs(), *expr2 = bexpr->mutable_rhs();
	sql_query_grammar::ExprSchemaTableColumn *estc1 = expr1->mutable_comp_expr()->mutable_expr_stc(),
											 *estc2 = expr2->mutable_comp_expr()->mutable_expr_stc();
	sql_query_grammar::ExprColumn *ecol1 = estc1->mutable_col(), *ecol2 = estc2->mutable_col();

	if (rel1->name != "") {
		estc1->mutable_table()->set_table(rel1->name);
	}
	if (rel2->name != "") {
		estc2->mutable_table()->set_table(rel2->name);
	}
	ecol1->mutable_col()->set_column(col1.name);
	ecol2->mutable_col()->set_column(col2.name);
	AddFieldAccess(rg, expr1, 16);
	AddFieldAccess(rg, expr2, 16);
	AddColNestedAccess(rg, ecol1, 31);
	AddColNestedAccess(rg, ecol2, 31);
	return 0;
}

int StatementGenerator::GenerateJoinConstraint(RandomGenerator &rg, const bool allow_using, sql_query_grammar::JoinConstraint *jc) {
	if (rg.NextSmallNumber() < 9) {
		bool generated = false;

		if (allow_using && rg.NextSmallNumber() < 3) {
			//using clause
			const SQLRelation &rel1 = rg.PickRandomlyFromVector(this->levels[this->current_level].rels),
							  &rel2 = this->levels[this->current_level].rels.back();
			std::vector<std::string> cols1, cols2, intersect;

			for (const auto &entry : rel1.cols) {
				cols1.push_back(entry.name);
			}
			for (const auto &entry : rel2.cols) {
				cols2.push_back(entry.name);
			}
			std::set_intersection(cols1.begin(), cols1.end(), cols2.begin(), cols2.end(), std::back_inserter(intersect));

			if (!intersect.empty()) {
				sql_query_grammar::ExprColumnList *ecl = jc->mutable_using_expr()->mutable_col_list();
				const uint32_t nclauses = std::min<uint32_t>(UINT32_C(3), (rg.NextRandomUInt32() % intersect.size()) + 1);

				std::shuffle(intersect.begin(), intersect.end(), rg.gen);
				for (uint32_t i = 0 ; i < nclauses; i++) {
					sql_query_grammar::ExprColumn *ec = i == 0 ? ecl->mutable_col() : ecl->add_extra_cols();

					ec->mutable_col()->set_column(intersect[i]);
				}
				generated = true;
			}
		}
		if (!generated) {
			//joining clause
			const uint32_t nclauses = std::min(this->max_width - this->width, rg.NextSmallNumber() % 3) + UINT32_C(1);
			sql_query_grammar::BinaryExpr *bexpr = jc->mutable_on_expr()->mutable_comp_expr()->mutable_binary_expr();

			for (uint32_t i = 0 ; i < nclauses; i++) {
				if (i == nclauses - 1) {
					AddJoinClause(rg, bexpr);
				} else {
					AddJoinClause(rg, bexpr->mutable_lhs()->mutable_comp_expr()->mutable_binary_expr());
					bexpr->set_op(rg.NextSmallNumber() < 9 ? sql_query_grammar::BinaryOperator::BINOP_AND : sql_query_grammar::BinaryOperator::BINOP_OR);
					bexpr = bexpr->mutable_rhs()->mutable_comp_expr()->mutable_binary_expr();
				}
			}
		}
	} else {
		//random clause
		const bool prev_allow_aggregates = this->levels[this->current_level].allow_aggregates,
				   prev_allow_window_funcs = this->levels[this->current_level].allow_window_funcs;

		this->levels[this->current_level].allow_aggregates = this->levels[this->current_level].allow_window_funcs = false;
		GenerateExpression(rg, jc->mutable_on_expr());
		this->levels[this->current_level].allow_aggregates = prev_allow_aggregates;
		this->levels[this->current_level].allow_window_funcs = prev_allow_window_funcs;
	}
	return 0;
}

int StatementGenerator::AddWhereSide(RandomGenerator &rg, const std::vector<GroupCol> &available_cols, sql_query_grammar::Expr *expr) {
	if (rg.NextSmallNumber() < 3) {
		RefColumn(rg, rg.PickRandomlyFromVector(available_cols), expr);
	} else {
		GenerateLiteralValue(rg, expr);
	}
	return 0;
}

int StatementGenerator::AddWhereFilter(RandomGenerator &rg, const std::vector<GroupCol> &available_cols, sql_query_grammar::Expr *expr) {
	const GroupCol &gcol = rg.PickRandomlyFromVector(available_cols);
	const uint32_t noption = rg.NextLargeNumber();

	if (noption < 761) {
		//binary expr
		sql_query_grammar::BinaryExpr *bexpr = expr->mutable_comp_expr()->mutable_binary_expr();
		sql_query_grammar::Expr *lexpr = bexpr->mutable_lhs(), *rexpr = bexpr->mutable_rhs();

		if (rg.NextSmallNumber() < 9) {
			RefColumn(rg, gcol, lexpr);
			AddWhereSide(rg, available_cols, rexpr);
		} else {
			AddWhereSide(rg, available_cols, lexpr);
			RefColumn(rg, gcol, rexpr);
		}
		bexpr->set_op(rg.NextSmallNumber() < 7 ? sql_query_grammar::BinaryOperator::BINOP_EQ :
			static_cast<sql_query_grammar::BinaryOperator>((rg.NextRandomUInt32() % static_cast<uint32_t>(sql_query_grammar::BinaryOperator::BINOP_LEGR)) + 1));
	} else if (noption < 901) {
		//between expr
		const uint32_t noption2 = rg.NextMediumNumber();
		sql_query_grammar::ExprBetween *bexpr = expr->mutable_comp_expr()->mutable_expr_between();
		sql_query_grammar::Expr *expr1 = bexpr->mutable_expr1(), *expr2 = bexpr->mutable_expr2(), *expr3 = bexpr->mutable_expr3();

		bexpr->set_not_(rg.NextBool());
		if (noption2 < 34) {
			RefColumn(rg, gcol, expr1);
			AddWhereSide(rg, available_cols, expr2);
			AddWhereSide(rg, available_cols, expr3);
		} else if (noption2 < 68) {
			AddWhereSide(rg, available_cols, expr1);
			RefColumn(rg, gcol, expr2);
			AddWhereSide(rg, available_cols, expr3);
		} else {
			AddWhereSide(rg, available_cols, expr1);
			AddWhereSide(rg, available_cols, expr2);
			RefColumn(rg, gcol, expr3);
		}
	} else if (noption < 971) {
		//is null expr
		sql_query_grammar::ExprNullTests *enull = expr->mutable_comp_expr()->mutable_expr_null_tests();

		enull->set_not_(rg.NextBool());
		RefColumn(rg, gcol, enull->mutable_expr());
	} else if (noption < 981) {
		//like expr
		sql_query_grammar::ExprLike *elike = expr->mutable_comp_expr()->mutable_expr_like();
		sql_query_grammar::Expr *expr2 = elike->mutable_expr2();

		elike->set_not_(rg.NextBool());
		elike->set_keyword(static_cast<sql_query_grammar::ExprLike_PossibleKeywords>((rg.NextRandomUInt32() % static_cast<uint32_t>(sql_query_grammar::ExprLike::PossibleKeywords_MAX)) + 1));
		RefColumn(rg, gcol, elike->mutable_expr1());
		if (rg.NextSmallNumber() < 5) {
			buf.resize(0);
			buf += "'";
			rg.NextString(buf, 100000);
			buf += "'";
			expr2->mutable_lit_val()->set_no_quote_str(buf);
		} else {
			AddWhereSide(rg, available_cols, expr2);
		}
	} else if (noption < 991) {
		//in expr
		const uint32_t nclauses = rg.NextSmallNumber();
		sql_query_grammar::ExprIn *ein = expr->mutable_comp_expr()->mutable_expr_in();
		sql_query_grammar::ExprList *elist = ein->mutable_exprs();

		ein->set_not_(rg.NextBool());
		ein->set_global(rg.NextBool());
		RefColumn(rg, gcol, ein->mutable_expr()->mutable_expr());
		for (uint32_t i = 0; i < nclauses; i++) {
			AddWhereSide(rg, available_cols, elist->mutable_expr());
		}
	} else {
		//any predicate
		GeneratePredicate(rg, expr);
	}
	return 0;
}

int StatementGenerator::GenerateWherePredicate(RandomGenerator &rg, sql_query_grammar::Expr *expr) {
	std::vector<GroupCol> available_cols;
	const uint32_t noption = rg.NextSmallNumber();

	if (this->levels[this->current_level].gcols.empty() && !this->levels[this->current_level].global_aggregate) {
		for (const auto &entry : this->levels[this->current_level].rels) {
			for (const auto &col : entry.cols) {
				available_cols.push_back(GroupCol(col, nullptr));
			}
		}
	} else if (!this->levels[this->current_level].gcols.empty()) {
		for (const auto &entry : this->levels[this->current_level].gcols) {
			available_cols.push_back(entry);
		}
	}

	this->depth++;
	if (!available_cols.empty() && noption < 8) {
		const uint32_t nclauses = std::max(std::min(this->max_width - this->width, (rg.NextSmallNumber() % 4) + 1), UINT32_C(1));

		for (uint32_t i = 0 ; i < nclauses; i++) {
			this->width++;
			if (i == nclauses - 1) {
				AddWhereFilter(rg, available_cols, expr);
			} else {
				sql_query_grammar::BinaryExpr *bexpr = expr->mutable_comp_expr()->mutable_binary_expr();

				AddWhereFilter(rg, available_cols, bexpr->mutable_lhs());
				bexpr->set_op(rg.NextSmallNumber() < 8 ? sql_query_grammar::BinaryOperator::BINOP_AND : sql_query_grammar::BinaryOperator::BINOP_OR);
				expr = bexpr->mutable_rhs();
			}
		}
		this->width -= nclauses;
	} else if (noption < 10) {
		//predicate
		GeneratePredicate(rg, expr);
	} else {
		//random clause
		GenerateExpression(rg, expr);
	}
	this->depth--;
	return 0;
}

int StatementGenerator::GenerateFromStatement(RandomGenerator &rg, const uint32_t allowed_clauses, sql_query_grammar::FromStatement *ft) {
	sql_query_grammar::JoinClause *jc = ft->mutable_tos()->mutable_join_clause();
	const uint32_t njoined = std::min(this->max_width - this->width, (rg.NextMediumNumber() % UINT32_C(4)) + 1);

	this->depth++;
	this->width++;
	GenerateFromElement(rg, allowed_clauses, jc->mutable_tos());
	for (uint32_t i = 1 ; i < njoined; i++) {
		sql_query_grammar::JoinClauseCore *jcc = jc->add_clauses();

		this->depth++;
		this->width++;
		if (rg.NextSmallNumber() < 3) {
			GenerateArrayJoin(rg, jcc->mutable_arr());
		} else {
			sql_query_grammar::JoinCore *core = jcc->mutable_core();
			sql_query_grammar::JoinCore_JoinType jt = static_cast<sql_query_grammar::JoinCore_JoinType>((rg.NextRandomUInt32() % static_cast<uint32_t>(sql_query_grammar::JoinCore::JoinType_MAX)) + 1);

			core->set_global(rg.NextSmallNumber() < 3);
			core->set_join_op(jt);
			if (rg.NextSmallNumber() < 4) {
				switch (jt) {
					case sql_query_grammar::JoinCore_JoinType::JoinCore_JoinType_LEFT:
					case sql_query_grammar::JoinCore_JoinType::JoinCore_JoinType_INNER:
						core->set_join_const(static_cast<sql_query_grammar::JoinCore_JoinConst>((rg.NextRandomUInt32() % static_cast<uint32_t>(sql_query_grammar::JoinCore::JoinConst_MAX)) + 1));
						break;
					case sql_query_grammar::JoinCore_JoinType::JoinCore_JoinType_RIGHT:
						core->set_join_const(static_cast<sql_query_grammar::JoinCore_JoinConst>((rg.NextRandomUInt32() % static_cast<uint32_t>(sql_query_grammar::JoinCore_JoinConst::JoinCore_JoinConst_ANTI)) + 1));
						break;
					case sql_query_grammar::JoinCore_JoinType::JoinCore_JoinType_FULL:
						core->set_join_const(sql_query_grammar::JoinCore_JoinConst::JoinCore_JoinConst_ALL);
						break;
					default:
						break;
				}
			}
			GenerateFromElement(rg, allowed_clauses, core->mutable_tos());
			GenerateJoinConstraint(rg, njoined == 1, core->mutable_join_constraint());
		}
	}
	this->width -= njoined;
	this->depth -= njoined;
	return 0;
}

int StatementGenerator::GenerateGroupBy(RandomGenerator &rg, const uint32_t ncols, const bool enforce_having,
										const bool allow_settings, sql_query_grammar::GroupByStatement *gbs) {
	std::vector<SQLRelationCol> available_cols;

	if (!this->levels[this->current_level].rels.empty()) {
		for (const auto &entry : this->levels[this->current_level].rels) {
			available_cols.insert(available_cols.end(), entry.cols.begin(), entry.cols.end());
		}
		std::shuffle(available_cols.begin(), available_cols.end(), rg.gen);
	}
	if (enforce_having && available_cols.empty()) {
		return 0;
	}
	this->depth++;
	if (enforce_having || rg.NextSmallNumber() < (available_cols.empty() ? 3 : 9)) {
		std::vector<GroupCol> gcols;
		sql_query_grammar::GroupByList *gbl = gbs->mutable_glist();
		sql_query_grammar::ExprList *elist = gbl->mutable_exprs();
		const uint32_t nclauses = std::min<uint32_t>(this->max_width - this->width,
			std::min<uint32_t>(UINT32_C(5), (rg.NextRandomUInt32() % (available_cols.empty() ? 5 : static_cast<uint32_t>(available_cols.size()))) + 1));
		const bool has_gs = !enforce_having && allow_settings && rg.NextSmallNumber() < 4,
				   has_totals = !enforce_having && allow_settings && rg.NextSmallNumber() < 4;

		for (uint32_t i = 0 ; i < nclauses; i++) {
			sql_query_grammar::Expr *expr = i == 0 ? elist->mutable_expr() : elist->add_extra_exprs();
			const uint32_t next_option = rg.NextSmallNumber();

			this->width++;
			if (!available_cols.empty() && (enforce_having || next_option < 9)) {
				const SQLRelationCol &rel_col = available_cols[i];
				sql_query_grammar::ExprSchemaTableColumn *estc = expr->mutable_comp_expr()->mutable_expr_stc();
				sql_query_grammar::ExprColumn *ecol = estc->mutable_col();

				if (rel_col.rel_name != "") {
					estc->mutable_table()->set_table(rel_col.rel_name);
				}
				ecol->mutable_col()->set_column(rel_col.name);
				AddFieldAccess(rg, expr, 16);
				AddColNestedAccess(rg, ecol, 31);
				gcols.push_back(GroupCol(rel_col, expr));
			} else if (next_option < 10) {
				sql_query_grammar::LiteralValue *lv = expr->mutable_lit_val();

				lv->mutable_int_lit()->set_uint_lit((rg.NextRandomUInt64() % ncols) + 1);
			} else {
				GenerateExpression(rg, expr);
			}
		}
		this->width -= nclauses;
		this->levels[this->current_level].gcols = std::move(gcols);

		if (has_gs) {
			gbl->set_gs(static_cast<sql_query_grammar::GroupByList_GroupingSets>((rg.NextRandomUInt32() % static_cast<uint32_t>(sql_query_grammar::GroupByList::GroupingSets_MAX)) + 1));
		}
		gbl->set_with_totals(has_totals);

		if (!has_gs && !has_totals && allow_settings && (enforce_having || rg.NextSmallNumber() < 5)) {
			const bool prev_allow_aggregates = this->levels[this->current_level].allow_aggregates;

			this->levels[this->current_level].allow_aggregates = true;
			GenerateWherePredicate(rg, gbs->mutable_having_expr());
			this->levels[this->current_level].allow_aggregates = prev_allow_aggregates;
		}
	} else {
		gbs->set_gall(true);
		this->levels[this->current_level].group_by_all = true;
	}
	this->depth--;
	return 1;
}

int StatementGenerator::GenerateOrderBy(RandomGenerator &rg, const uint32_t ncols, const bool allow_settings, sql_query_grammar::OrderByStatement *ob) {
	std::vector<GroupCol> available_cols;

	if (this->levels[this->current_level].group_by_all) {
		for (const auto &entry : this->levels[this->current_level].projections) {
			const std::string cname = "c" + std::to_string(entry);
			available_cols.push_back(GroupCol(SQLRelationCol("", std::move(cname)), nullptr));
		}
	} else if (this->levels[this->current_level].gcols.empty() && !this->levels[this->current_level].global_aggregate) {
		for (const auto &entry : this->levels[this->current_level].rels) {
			for (const auto &col : entry.cols) {
				available_cols.push_back(GroupCol(col, nullptr));
			}
		}
	} else if (!this->levels[this->current_level].gcols.empty()) {
		for (const auto &entry : this->levels[this->current_level].gcols) {
			available_cols.push_back(entry);
		}
	}
	if (!available_cols.empty()) {
		std::shuffle(available_cols.begin(), available_cols.end(), rg.gen);
	}
	const uint32_t nclauses = std::min<uint32_t>(this->max_width - this->width,
		std::min<uint32_t>(UINT32_C(5), (rg.NextRandomUInt32() % (available_cols.empty() ? 5 : available_cols.size())) + 1));

	for (uint32_t i = 0 ; i < nclauses; i++) {
		sql_query_grammar::ExprOrderingTerm *eot = i == 0 ? ob->mutable_ord_term() : ob->add_extra_ord_terms();
		sql_query_grammar::Expr *expr = eot->mutable_expr();
		const uint32_t next_option = rg.NextSmallNumber();

		this->width++;
		if (!available_cols.empty() && next_option < 9) {
			RefColumn(rg, available_cols[i], expr);
		} else if (ncols && next_option < 10) {
			sql_query_grammar::LiteralValue *lv = expr->mutable_lit_val();

			lv->mutable_int_lit()->set_uint_lit((rg.NextRandomUInt64() % ncols) + 1);
		} else {
			GenerateExpression(rg, expr);
		}
		if (allow_settings) {
			if (rg.NextSmallNumber() < 7) {
				eot->set_asc_desc(rg.NextBool() ? sql_query_grammar::ExprOrderingTerm_AscDesc::ExprOrderingTerm_AscDesc_ASC :
												  sql_query_grammar::ExprOrderingTerm_AscDesc::ExprOrderingTerm_AscDesc_DESC);
			}
			eot->set_with_fill(rg.NextSmallNumber() < 3);
		}
	}
	this->width -= nclauses;
	return 0;
}

int StatementGenerator::GenerateLimit(RandomGenerator &rg, const bool has_order_by, const bool has_distinct,
									  const uint32_t ncols, sql_query_grammar::LimitStatement *ls) {
	uint32_t nlimit = 0;
	const int next_option = rg.NextSmallNumber();

	this->depth++;
	if (next_option < 3) {
		nlimit = 0;
	} else if (next_option < 5) {
		nlimit = 1;
	} else if (next_option < 7) {
		nlimit = 2;
	} else if (next_option < 9) {
		nlimit = 10;
	} else {
		nlimit = rg.NextRandomUInt32();
	}
	ls->set_limit(nlimit);
	if (rg.NextSmallNumber() < 6) {
		uint32_t noffset = 0;
		const int next_option2 = rg.NextSmallNumber();

		if (next_option2 < 3) {
			noffset = 0;
		} else if (next_option2 < 5) {
			noffset = 1;
		} else if (next_option2 < 7) {
			noffset = 2;
		} else if (next_option2 < 9) {
			noffset = 10;
		} else {
			noffset = rg.NextRandomUInt32();
		}
		ls->set_offset(noffset);
	}
	ls->set_with_ties(has_order_by && !has_distinct && rg.NextSmallNumber() < 7);
	if (rg.NextSmallNumber() < 4) {
		const int next_option3 = rg.NextSmallNumber();
		sql_query_grammar::Expr *expr = ls->mutable_limit_by();

		if (this->depth >= this->max_depth || next_option3 < 9) {
			sql_query_grammar::LiteralValue *lv = expr->mutable_lit_val();

			lv->mutable_int_lit()->set_uint_lit((rg.NextRandomUInt64() % ncols) + 1);
		} else {
			GenerateExpression(rg, expr);
		}
	}
	this->depth--;
	return 0;
}

int StatementGenerator::GenerateSelect(RandomGenerator &rg, const bool top, const uint32_t ncols, const uint32_t allowed_clauses, sql_query_grammar::Select *sel) {
	int res = 0;

	if ((allowed_clauses & allow_cte) && this->depth < this->max_depth &&
		this->width < this->max_width && rg.NextMediumNumber() < 13) {
		const uint32_t nclauses = std::min<uint32_t>(this->max_width - this->width, (rg.NextRandomUInt32() % 3) + 1);

		this->depth++;
		for (uint32_t i = 0 ; i < nclauses; i++) {
			sql_query_grammar::CTEquery *cte = sel->add_ctes();
			std::string name;

			name += "cte";
			name += std::to_string(i);
			name += "d";
			name += std::to_string(this->current_level);
			SQLRelation rel(name);

			cte->mutable_table()->set_table(name);
			GenerateDerivedTable(rg, rel, allowed_clauses, cte->mutable_query());
			this->ctes[this->current_level][name] = std::move(rel);
			this->width++;
		}
		this->width -= nclauses;
		this->depth--;
	}

	if ((allowed_clauses & allow_set) && this->depth < this->max_depth &&
		this->max_width > this->width + 1 && rg.NextSmallNumber() < 3) {
		sql_query_grammar::SetQuery *setq = sel->mutable_set_query();

		setq->set_set_op(static_cast<sql_query_grammar::SetQuery_SetOp>((rg.NextRandomUInt32() % static_cast<uint32_t>(sql_query_grammar::SetQuery::SetOp_MAX)) + 1));
		setq->set_s_or_d(rg.NextBool() ? sql_query_grammar::AllOrDistinct::ALL : sql_query_grammar::AllOrDistinct::DISTINCT);

		this->depth++;
		this->current_level++;
		this->levels[this->current_level] = QueryLevel(this->current_level);
		res = std::max<int>(res, GenerateSelect(rg, false, ncols, allowed_clauses, setq->mutable_sel1()));
		this->width++;
		this->levels[this->current_level] = QueryLevel(this->current_level);
		res = std::max<int>(res, GenerateSelect(rg, false, ncols, allowed_clauses, setq->mutable_sel2()));
		this->current_level--;
		this->depth--;
		this->width--;
	} else {
		sql_query_grammar::SelectStatementCore *ssc = sel->mutable_select_core();

		if ((allowed_clauses & allow_distinct) && rg.NextSmallNumber() < 3) {
			ssc->set_s_or_d(rg.NextBool() ? sql_query_grammar::AllOrDistinct::ALL : sql_query_grammar::AllOrDistinct::DISTINCT);
		}
		if ((allowed_clauses & allow_from) && this->depth < this->max_depth &&
			this->width < this->max_width && rg.NextSmallNumber() < 10) {
			res = std::max<int>(res, GenerateFromStatement(rg, allowed_clauses, ssc->mutable_from()));
		}
		const bool prev_allow_aggregates = this->levels[this->current_level].allow_aggregates,
				   prev_allow_window_funcs = this->levels[this->current_level].allow_window_funcs;

		this->levels[this->current_level].allow_aggregates = this->levels[this->current_level].allow_window_funcs = false;
		if ((allowed_clauses & allow_prewhere) && this->depth < this->max_depth &&
			ssc->has_from() && rg.NextSmallNumber() < 5) {
			GenerateWherePredicate(rg, ssc->mutable_pre_where()->mutable_expr()->mutable_expr());
		}
		if ((allowed_clauses & allow_where) && this->depth < this->max_depth &&
			rg.NextSmallNumber() < 5) {
			GenerateWherePredicate(rg, ssc->mutable_where()->mutable_expr()->mutable_expr());
		}

		if ((allowed_clauses & allow_groupby) && this->depth < this->max_depth &&
			this->width < this->max_width && rg.NextSmallNumber() < 4) {
			GenerateGroupBy(rg, ncols, false, (allowed_clauses & allow_groupby_settings), ssc->mutable_groupby());
		} else {
			this->levels[this->current_level].global_aggregate = rg.NextSmallNumber() < 4;
		}
		this->levels[this->current_level].allow_aggregates = prev_allow_aggregates;
		this->levels[this->current_level].allow_window_funcs = prev_allow_window_funcs;

		this->depth++;
		for (uint32_t i = 0 ; i < ncols; i++) {
			sql_query_grammar::ExprColAlias *eca = ssc->add_result_columns()->mutable_eca();

			this->width++;
			GenerateExpression(rg, eca->mutable_expr());
			if (!top) {
				const uint32_t cname = this->levels[this->current_level].aliases_counter++;
				const std::string cname_str = "c" + std::to_string(cname);

				SQLRelation rel("");
				rel.cols.push_back(SQLRelationCol("", cname_str));
				this->levels[this->current_level].rels.push_back(std::move(rel));
				eca->mutable_col_alias()->set_column(cname_str);
				this->levels[this->current_level].projections.push_back(cname);
			}
		}
		this->depth--;
		this->width -= ncols;

		if ((allowed_clauses & allow_orderby) && this->depth < this->max_depth &&
			this->width < this->max_width && rg.NextSmallNumber() < 4) {
			this->depth++;
			GenerateOrderBy(rg, ncols, (allowed_clauses & allow_orderby_settings), ssc->mutable_orderby());
			this->depth--;
		}
		if ((allowed_clauses & allow_limit) && rg.NextSmallNumber() < 4) {
			GenerateLimit(rg, ssc->has_orderby(), ssc->s_or_d() == sql_query_grammar::AllOrDistinct::DISTINCT, ncols, ssc->mutable_limit());
		}
	}
	this->levels.erase(this->current_level);
	this->ctes.erase(this->current_level);
	return res;
}

int StatementGenerator::GenerateTopSelect(RandomGenerator &rg, sql_query_grammar::TopSelect *ts) {
	int res = 0;
	const uint32_t ncols = std::max(std::min(this->max_width - this->width, (rg.NextMediumNumber() % UINT32_C(5)) + 1), UINT32_C(1));

	assert(this->levels.empty());
	this->levels[this->current_level] = QueryLevel(this->current_level);
	if ((res = GenerateSelect(rg, true, ncols, std::numeric_limits<uint32_t>::max(), ts->mutable_sel()))) {
		return res;
	}
	if (rg.NextSmallNumber() < 3) {
		ts->set_format(static_cast<sql_query_grammar::OutFormat>((rg.NextRandomUInt32() % static_cast<uint32_t>(sql_query_grammar::OutFormat_MAX)) + 1));
	}
	return res;
}

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

}
