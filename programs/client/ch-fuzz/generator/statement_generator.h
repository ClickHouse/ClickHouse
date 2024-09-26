#pragma once

#include "random_generator.h"
#include "random_settings.h"
#include "sql_catalog.h"

namespace chfuzz {

class SQLRelationCol {
public:
	std::string rel_name, name;

	SQLRelationCol() {}

	SQLRelationCol(const std::string rname, const std::string n) : rel_name(rname), name(n) {}
};

class SQLRelation {
public:
	std::string name;
	std::vector<SQLRelationCol> cols;

	SQLRelation() {}

	SQLRelation(const std::string n) : name(n) {}
};

class GroupCol {
public:
	SQLRelationCol col;
	sql_query_grammar::Expr *gexpr = nullptr;

	GroupCol() {}
	GroupCol(SQLRelationCol c, sql_query_grammar::Expr *g) : col(c), gexpr(g) {}
};

class QueryLevel {
public:
	bool global_aggregate = false, inside_aggregate = false,
		 allow_aggregates = true, allow_window_funcs = true,
		 group_by_all = false;
	uint32_t level, aliases_counter = 0;
	std::vector<GroupCol> gcols;
	std::vector<SQLRelation> rels;
	std::vector<uint32_t> projections;

	QueryLevel() {}

	QueryLevel(const uint32_t n) : level(n) {}
};

const constexpr uint32_t allow_set = (1 << 0),
						 allow_cte = (1 << 1),
						 allow_distinct = (1 << 2),
						 allow_from = (1 << 3),
						 allow_prewhere = (1 << 4),
						 allow_where = (1 << 5),
						 allow_groupby = (1 << 6),
						 allow_groupby_settings = (1 << 7),
						 allow_orderby = (1 << 8),
						 allow_orderby_settings = (1 << 9),
						 allow_limit = (1 << 10);

typedef struct InsertEntry {
	bool is_sign = false;
	uint32_t cname1;
	std::optional<uint32_t> cname2;
	SQLType *tp;

	InsertEntry(const bool isgn, const uint32_t c1, std::optional<uint32_t> c2, SQLType *t) :
		is_sign(isgn), cname1(c1), cname2(c2), tp(t) {}
} InsertEntry;

class StatementGenerator {
private:
	std::string buf;
	bool in_transaction = false, inside_projection = false;
	uint32_t table_counter = 0, current_level = 0;
	std::map<uint32_t, SQLTable> staged_tables, tables;
	std::map<uint32_t, SQLView> staged_views, views;

	std::vector<uint32_t> ids;
	std::vector<InsertEntry> entries;
	uint32_t depth = 0, width = 0, max_depth = 3, max_width = 3, max_tables = 10, max_views = 5;

	std::map<uint32_t, std::map<std::string, SQLRelation>> ctes;
	std::map<uint32_t, QueryLevel> levels;

	void AddTableRelation(RandomGenerator &rg, const bool allow_internal_cols, const std::string &rel_name, const SQLTable &t);
	void AppendDecimal(RandomGenerator &rg, std::string &ret, const uint32_t left, const uint32_t right);

	void StrAppendBottomValue(RandomGenerator &rg, std::string &ret, SQLType* tp);
	void StrAppendMap(RandomGenerator &rg, std::string &ret, MapType *mt);
	void StrAppendArray(RandomGenerator &rg, std::string &ret, ArrayType *at);
	void StrAppendTuple(RandomGenerator &rg, std::string &ret, TupleType *at);
	void StrAppendVariant(RandomGenerator &rg, std::string &ret, VariantType *vtp);
	void StrAppendAnyValue(RandomGenerator &rg, std::string &ret, SQLType *tp);

	void StrBuildJSONArray(RandomGenerator &rg, const int jdepth, const int jwidth, std::string &ret);
	void StrBuildJSONElement(RandomGenerator &rg, std::string &ret);
	void StrBuildJSON(RandomGenerator &rg, const int jdepth, const int jwidth, std::string &ret);

	int GenerateNextStatistics(RandomGenerator &rg, sql_query_grammar::ColumnStatistics *cstats);
	int PickUpNextCols(RandomGenerator &rg, const SQLTable &t, sql_query_grammar::ColumnList *clist);
	int AddTableColumn(RandomGenerator &rg, SQLTable &t, const uint32_t cname, const bool staged, const bool modify,
					   const ColumnSpecial special, sql_query_grammar::ColumnDef *cd);
	int AddTableIndex(RandomGenerator &rg, SQLTable &t, const bool staged, sql_query_grammar::IndexDef *idef);
	int AddTableProjection(RandomGenerator &rg, SQLTable &t, const bool staged, sql_query_grammar::ProjectionDef *pdef);
	int AddTableConstraint(RandomGenerator &rg, SQLTable &t, const bool staged, sql_query_grammar::ConstraintDef *cdef);
	int GenerateTableKey(RandomGenerator &rg, sql_query_grammar::TableKey *tkey);
	int GenerateEngineDetails(RandomGenerator &rg, sql_query_grammar::TableEngine *te);
	int GenerateNextCreateView(RandomGenerator &rg, sql_query_grammar::CreateView *cv);
	int GenerateNextDrop(RandomGenerator &rg, sql_query_grammar::Drop *sq);
	int GenerateNextInsert(RandomGenerator &rg, sql_query_grammar::Insert *sq);
	int GenerateNextDelete(RandomGenerator &rg, sql_query_grammar::Delete *sq);
	int GenerateNextTruncate(RandomGenerator &rg, sql_query_grammar::Truncate *sq);
	int GenerateNextOptimizeTable(RandomGenerator &rg, sql_query_grammar::OptimizeTable *sq);
	int GenerateNextCheckTable(RandomGenerator &rg, sql_query_grammar::CheckTable *sq);
	int GenerateNextDescTable(RandomGenerator &rg, sql_query_grammar::DescTable *sq);
	int GenerateNextExchangeTables(RandomGenerator &rg, sql_query_grammar::ExchangeTables *sq);
	int GenerateUptDelWhere(RandomGenerator &rg, const SQLTable &t, sql_query_grammar::Expr *expr);
	int GenerateAlterTable(RandomGenerator &rg, sql_query_grammar::AlterTable *at);
	int GenerateSettingValues(RandomGenerator &rg, const std::map<std::string, std::function<void(RandomGenerator&,std::string&)>> &settings,
							  sql_query_grammar::SettingValues *vals);
	int GenerateSettingList(RandomGenerator &rg, const std::map<std::string, std::function<void(RandomGenerator&,std::string&)>> &settings,
							sql_query_grammar::SettingList *pl);

	int AddFieldAccess(RandomGenerator &rg, sql_query_grammar::Expr *expr, const uint32_t nested_prob);
	int AddColNestedAccess(RandomGenerator &rg, sql_query_grammar::ExprColumn *expr, const uint32_t nested_prob);
	int RefColumn(RandomGenerator &rg, const GroupCol &gcol, sql_query_grammar::Expr *expr);
	int GenerateSubquery(RandomGenerator &rg, sql_query_grammar::Select *sel);
	int GenerateColRef(RandomGenerator &rg, sql_query_grammar::Expr *expr);
	int GenerateLiteralValue(RandomGenerator &rg, sql_query_grammar::Expr *expr);
	int GeneratePredicate(RandomGenerator &rg, sql_query_grammar::Expr *expr);
	int GenerateFrameBound(RandomGenerator &rg, sql_query_grammar::Expr *expr);
	int GenerateExpression(RandomGenerator &rg, sql_query_grammar::Expr *expr);
	int GenerateFuncCall(RandomGenerator &rg, const bool allow_funcs, const bool allow_aggr, sql_query_grammar::SQLFuncCall *expr);

	int GenerateOrderBy(RandomGenerator &rg, const uint32_t ncols, const bool allow_settings, sql_query_grammar::OrderByStatement *ob);
	int GenerateLimit(RandomGenerator &rg, const bool has_order_by, const bool has_distinct, const uint32_t ncols, sql_query_grammar::LimitStatement *ls);
	int GenerateGroupBy(RandomGenerator &rg, const uint32_t ncols, const bool enforce_having, const bool allow_settings, sql_query_grammar::GroupByStatement *gb);
	int AddWhereSide(RandomGenerator &rg, const std::vector<GroupCol> &available_cols, sql_query_grammar::Expr *expr);
	int AddWhereFilter(RandomGenerator &rg, const std::vector<GroupCol> &available_cols, sql_query_grammar::Expr *expr);
	int GenerateWherePredicate(RandomGenerator &rg, sql_query_grammar::Expr *expr);
	int AddJoinClause(RandomGenerator &rg, sql_query_grammar::BinaryExpr *bexpr);
	int GenerateArrayJoin(RandomGenerator &rg, sql_query_grammar::ArrayJoin *aj);
	int GenerateFromElement(RandomGenerator &rg, const uint32_t allowed_clauses, sql_query_grammar::TableOrSubquery *tos);
	int GenerateJoinConstraint(RandomGenerator &rg, const bool allow_using, sql_query_grammar::JoinConstraint *jc);
	int GenerateDerivedTable(RandomGenerator &rg, SQLRelation &rel, const uint32_t allowed_clauses, sql_query_grammar::Select *sel);
	int GenerateFromStatement(RandomGenerator &rg, const uint32_t allowed_clauses, sql_query_grammar::FromStatement *ft);
	int GenerateSelect(RandomGenerator &rg, const bool top, const uint32_t ncols, const uint32_t allowed_clauses, sql_query_grammar::Select *sel);

	int GenerateTopSelect(RandomGenerator &rg, sql_query_grammar::TopSelect *sq);
	int GenerateNextExplain(RandomGenerator &rg, sql_query_grammar::ExplainQuery *sq);
	int GenerateNextQuery(RandomGenerator &rg, sql_query_grammar::SQLQueryInner *sq);

	SQLType* BottomType(RandomGenerator &rg, const uint32_t allowed_types, const bool low_card, sql_query_grammar::BottomTypeName *tp);
	SQLType* GenerateArraytype(RandomGenerator &rg, const uint32_t allowed_types);
	SQLType* GenerateArraytype(RandomGenerator &rg, const uint32_t allowed_types, uint32_t &col_counter, sql_query_grammar::TopTypeName *tp);

	SQLType* RandomNextType(RandomGenerator &rg, const uint32_t allowed_types);
	SQLType* RandomNextType(RandomGenerator &rg, const uint32_t allowed_types, uint32_t &col_counter, sql_query_grammar::TopTypeName *tp);
public:
	int GenerateNextCreateTable(RandomGenerator &rg, sql_query_grammar::CreateTable *sq);
	int GenerateNextStatement(RandomGenerator &rg, sql_query_grammar::SQLQuery &sq);

	int GenerateCorrectnessTestFirstQuery(RandomGenerator &rg, sql_query_grammar::SQLQuery &sq);
	int GenerateCorrectnessTestSecondQuery(sql_query_grammar::SQLQuery &sq1, sql_query_grammar::SQLQuery &sq2);
	int GenerateExportQuery(RandomGenerator &rg, sql_query_grammar::SQLQuery &sq1);
	int GenerateClearQuery(sql_query_grammar::SQLQuery &sq1, sql_query_grammar::SQLQuery &sq2);
	int GenerateImportQuery(sql_query_grammar::SQLQuery &sq1, sql_query_grammar::SQLQuery &sq2, sql_query_grammar::SQLQuery &sq3);

	void UpdateGenerator(const sql_query_grammar::SQLQuery &sq, const bool success);
	void FinishGenerator();
};

}
