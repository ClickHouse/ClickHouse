#pragma once

#include "ExternalIntegrations.h"
#include "RandomGenerator.h"
#include "RandomSettings.h"
#include "SQLCatalog.h"


namespace BuzzHouse
{

class QueryOracle;

class SQLRelationCol
{
public:
    std::string rel_name, name;
    std::optional<std::string> name2;

    SQLRelationCol() { }

    SQLRelationCol(const std::string rname, const std::string cname, std::optional<std::string> cname2)
        : rel_name(rname), name(cname), name2(cname2)
    {
    }
};

class SQLRelation
{
public:
    std::string name;
    std::vector<SQLRelationCol> cols;

    SQLRelation() { }

    SQLRelation(const std::string n) : name(n) { }
};

class GroupCol
{
public:
    SQLRelationCol col;
    Expr * gexpr = nullptr;

    GroupCol() { }
    GroupCol(SQLRelationCol c, Expr * g) : col(c), gexpr(g) { }
};

class QueryLevel
{
public:
    bool global_aggregate = false, inside_aggregate = false, allow_aggregates = true, allow_window_funcs = true, group_by_all = false;
    uint32_t level, aliases_counter = 0;
    std::vector<GroupCol> gcols;
    std::vector<SQLRelation> rels;
    std::vector<uint32_t> projections;

    QueryLevel() { }

    QueryLevel(const uint32_t n) : level(n) { }
};

const constexpr uint32_t allow_set = (1 << 0), allow_cte = (1 << 1), allow_distinct = (1 << 2), allow_from = (1 << 3),
                         allow_prewhere = (1 << 4), allow_where = (1 << 5), allow_groupby = (1 << 6), allow_groupby_settings = (1 << 7),
                         allow_orderby = (1 << 8), allow_orderby_settings = (1 << 9), allow_limit = (1 << 10);

class StatementGenerator
{
private:
    FuzzConfig & fc;
    ExternalIntegrations & connections;
    const bool supports_cloud_features;

    std::string buf;

    bool in_transaction = false, inside_projection = false, allow_not_deterministic = true, allow_in_expression_alias = true,
         allow_subqueries = true, enforce_final = false;
    uint32_t depth = 0, width = 0, database_counter = 0, table_counter = 0, zoo_path_counter = 0, function_counter = 0, current_level = 0;
    std::map<uint32_t, std::shared_ptr<SQLDatabase>> staged_databases, databases;
    std::map<uint32_t, SQLTable> staged_tables, tables;
    std::map<uint32_t, SQLView> staged_views, views;
    std::map<uint32_t, SQLFunction> staged_functions, functions;

    std::vector<std::string> enum_values
        = {"'-1'",
           "'0'",
           "'1'",
           "'10'",
           "'1000'",
           "'is'",
           "'was'",
           "'are'",
           "'be'",
           "'have'",
           "'had'",
           "'were'",
           "'can'",
           "'said'",
           "'use'",
           "','",
           "'😀'",
           "'😀😀😀😀'"
           "'名字'",
           "'兄弟姐妹'",
           "''",
           "'\\n'",
           "x'c328'",
           "x'e28228'",
           "x'ff'",
           "b'101'",
           "b'100'",
           "b'10001'",
           "' '",
           "'c0'",
           "'c1'",
           "'11'"};

    std::vector<uint32_t> ids;
    std::vector<InsertEntry> entries;
    std::vector<std::reference_wrapper<const SQLTable>> filtered_tables;
    std::vector<std::reference_wrapper<const SQLView>> filtered_views;
    std::vector<std::reference_wrapper<const std::shared_ptr<SQLDatabase>>> filtered_databases;
    std::vector<std::reference_wrapper<const SQLFunction>> filtered_functions;

    std::map<uint32_t, std::map<std::string, SQLRelation>> ctes;
    std::map<uint32_t, QueryLevel> levels;

    void SetAllowNotDetermistic(const bool value) { allow_not_deterministic = value; }
    void EnforceFinal(const bool value) { enforce_final = value; }

    template <typename T>
    const std::map<uint32_t, T> & GetNextCollection() const
    {
        if constexpr (std::is_same<T, SQLTable>::value)
        {
            return tables;
        }
        else if constexpr (std::is_same<T, SQLView>::value)
        {
            return views;
        }
        else if constexpr (std::is_same<T, SQLFunction>::value)
        {
            return functions;
        }
        else
        {
            return databases;
        }
    }

public:
    template <typename T>
    bool CollectionHas(const std::function<bool(const T &)> func) const
    {
        const auto & input = GetNextCollection<T>();

        for (const auto & entry : input)
        {
            if (func(entry.second))
            {
                return true;
            }
        }
        return false;
    }

private:
    template <typename T>
    uint32_t CollectionCount(const std::function<bool(const T &)> func) const
    {
        uint32_t res = 0;
        const auto & input = GetNextCollection<T>();

        for (const auto & entry : input)
        {
            res += func(entry.second) ? 1 : 0;
        }
        return res;
    }

    template <typename T>
    std::vector<std::reference_wrapper<const T>> & GetNextCollectionResult()
    {
        if constexpr (std::is_same<T, SQLTable>::value)
        {
            return filtered_tables;
        }
        else if constexpr (std::is_same<T, SQLView>::value)
        {
            return filtered_views;
        }
        else if constexpr (std::is_same<T, SQLFunction>::value)
        {
            return filtered_functions;
        }
        else
        {
            return filtered_databases;
        }
    }

public:
    template <typename T>
    std::vector<std::reference_wrapper<const T>> & FilterCollection(const std::function<bool(const T &)> func)
    {
        const auto & input = GetNextCollection<T>();
        auto & res = GetNextCollectionResult<T>();

        res.clear();
        for (const auto & entry : input)
        {
            if (func(entry.second))
            {
                res.push_back(std::ref<const T>(entry.second));
            }
        }
        return res;
    }

private:
    void InsertEntryRef(const InsertEntry & entry, Expr * expr);
    void InsertEntryRefCP(const InsertEntry & entry, ColumnPath * cp);
    void AddTableRelation(RandomGenerator & rg, const bool allow_internal_cols, const std::string & rel_name, const SQLTable & t);

    void StrAppendBottomValue(RandomGenerator & rg, std::string & ret, const SQLType * tp);
    void StrAppendMap(RandomGenerator & rg, std::string & ret, const MapType * mt);
    void StrAppendArray(RandomGenerator & rg, std::string & ret, const ArrayType * at);
    void StrAppendArray(RandomGenerator & rg, std::string & ret, const ArrayType * at, const uint32_t limit);
    void StrAppendTuple(RandomGenerator & rg, std::string & ret, const TupleType * at);
    void StrAppendVariant(RandomGenerator & rg, std::string & ret, const VariantType * vtp);
    void StrAppendAnyValueInternal(RandomGenerator & rg, std::string & ret, const SQLType * tp);
    void StrAppendAnyValue(RandomGenerator & rg, std::string & ret, const SQLType * tp);

    int GenerateNextStatistics(RandomGenerator & rg, ColumnStatistics * cstats);
    int PickUpNextCols(RandomGenerator & rg, const SQLTable & t, ColumnList * clist);
    int AddTableColumn(
        RandomGenerator & rg,
        SQLTable & t,
        const uint32_t cname,
        const bool staged,
        const bool modify,
        const bool is_pk,
        const ColumnSpecial special,
        ColumnDef * cd);
    int AddTableIndex(RandomGenerator & rg, SQLTable & t, const bool staged, IndexDef * idef);
    int AddTableProjection(RandomGenerator & rg, SQLTable & t, const bool staged, ProjectionDef * pdef);
    int AddTableConstraint(RandomGenerator & rg, SQLTable & t, const bool staged, ConstraintDef * cdef);
    int GenerateTableKey(RandomGenerator & rg, TableKey * tkey);
    int GenerateMergeTreeEngineDetails(RandomGenerator & rg, const TableEngineValues teng, const bool add_pkey, TableEngine * te);
    int GenerateEngineDetails(RandomGenerator & rg, SQLBase & b, const bool add_pkey, TableEngine * te);

    TableEngineValues GetNextTableEngine(RandomGenerator & rg, const bool use_external_integrations);

    int GenerateNextRefreshableView(RandomGenerator & rg, RefreshableView * cv);
    int GenerateNextCreateView(RandomGenerator & rg, CreateView * cv);
    int GenerateNextDrop(RandomGenerator & rg, Drop * sq);
    int GenerateNextInsert(RandomGenerator & rg, Insert * sq);
    int GenerateNextDelete(RandomGenerator & rg, LightDelete * sq);
    int GenerateNextTruncate(RandomGenerator & rg, Truncate * sq);
    int GenerateNextOptimizeTable(RandomGenerator & rg, OptimizeTable * sq);
    int GenerateNextCheckTable(RandomGenerator & rg, CheckTable * sq);
    int GenerateNextDescTable(RandomGenerator & rg, DescTable * sq);
    int GenerateNextExchangeTables(RandomGenerator & rg, ExchangeTables * sq);
    int GenerateUptDelWhere(RandomGenerator & rg, const SQLTable & t, Expr * expr);
    int GenerateAlterTable(RandomGenerator & rg, AlterTable * at);
    int GenerateSettingValues(
        RandomGenerator & rg,
        const std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> & settings,
        SettingValues * vals);
    int GenerateSettingValues(
        RandomGenerator & rg,
        const std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> & settings,
        const size_t nvalues,
        SettingValues * vals);
    int GenerateSettingList(
        RandomGenerator & rg,
        const std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> & settings,
        SettingList * pl);
    int GenerateAttach(RandomGenerator & rg, Attach * att);
    int GenerateDetach(RandomGenerator & rg, Detach * det);
    int GenerateNextCreateFunction(RandomGenerator & rg, CreateFunction * cf);

    int AddFieldAccess(RandomGenerator & rg, Expr * expr, const uint32_t nested_prob);
    int AddColNestedAccess(RandomGenerator & rg, ExprColumn * expr, const uint32_t nested_prob);
    int RefColumn(RandomGenerator & rg, const GroupCol & gcol, Expr * expr);
    int GenerateSubquery(RandomGenerator & rg, Select * sel);
    int GenerateColRef(RandomGenerator & rg, Expr * expr);
    int GenerateLiteralValue(RandomGenerator & rg, Expr * expr);
    int GeneratePredicate(RandomGenerator & rg, Expr * expr);
    int GenerateFrameBound(RandomGenerator & rg, Expr * expr);
    int GenerateExpression(RandomGenerator & rg, Expr * expr);
    int GenerateLambdaCall(RandomGenerator & rg, const uint32_t nparams, LambdaExpr * lexpr);
    int GenerateFuncCall(RandomGenerator & rg, const bool allow_funcs, const bool allow_aggr, SQLFuncCall * expr);

    int GenerateOrderBy(RandomGenerator & rg, const uint32_t ncols, const bool allow_settings, OrderByStatement * ob);

    int GenerateLimitExpr(RandomGenerator & rg, Expr * expr);
    int GenerateLimit(RandomGenerator & rg, const bool has_order_by, const uint32_t ncols, LimitStatement * ls);
    int GenerateOffset(RandomGenerator & rg, OffsetStatement * off);
    int GenerateGroupByExpr(
        RandomGenerator & rg,
        const bool enforce_having,
        const uint32_t offset,
        const uint32_t ncols,
        const std::vector<SQLRelationCol> & available_cols,
        std::vector<GroupCol> & gcols,
        Expr * expr);
    int GenerateGroupBy(
        RandomGenerator & rg, const uint32_t ncols, const bool enforce_having, const bool allow_settings, GroupByStatement * gb);
    int AddWhereSide(RandomGenerator & rg, const std::vector<GroupCol> & available_cols, Expr * expr);
    int AddWhereFilter(RandomGenerator & rg, const std::vector<GroupCol> & available_cols, Expr * expr);
    int GenerateWherePredicate(RandomGenerator & rg, Expr * expr);
    int AddJoinClause(RandomGenerator & rg, BinaryExpr * bexpr);
    int GenerateArrayJoin(RandomGenerator & rg, ArrayJoin * aj);
    int GenerateFromElement(RandomGenerator & rg, const uint32_t allowed_clauses, TableOrSubquery * tos);
    int GenerateJoinConstraint(RandomGenerator & rg, const bool allow_using, JoinConstraint * jc);
    int GenerateDerivedTable(RandomGenerator & rg, SQLRelation & rel, const uint32_t allowed_clauses, Select * sel);
    int GenerateFromStatement(RandomGenerator & rg, const uint32_t allowed_clauses, FromStatement * ft);
    int AddCTEs(RandomGenerator & rg, const uint32_t allowed_clauses, CTEs * qctes);
    int GenerateSelect(RandomGenerator & rg, const bool top, const uint32_t ncols, const uint32_t allowed_clauses, Select * sel);

    int GenerateTopSelect(RandomGenerator & rg, const uint32_t allowed_clauses, TopSelect * sq);
    int GenerateNextExplain(RandomGenerator & rg, ExplainQuery * sq);
    int GenerateNextQuery(RandomGenerator & rg, SQLQueryInner * sq);

    std::tuple<const SQLType *, Integers> RandomIntType(RandomGenerator & rg, const uint32_t allowed_types);
    std::tuple<const SQLType *, FloatingPoints> RandomFloatType(RandomGenerator & rg);
    std::tuple<const SQLType *, Dates> RandomDateType(RandomGenerator & rg, const uint32_t allowed_types);
    const SQLType * RandomDateTimeType(RandomGenerator & rg, const uint32_t allowed_types, DateTimeTp * dt);
    const SQLType * BottomType(RandomGenerator & rg, const uint32_t allowed_types, const bool low_card, BottomTypeName * tp);
    const SQLType * GenerateArraytype(RandomGenerator & rg, const uint32_t allowed_types);
    const SQLType * GenerateArraytype(RandomGenerator & rg, const uint32_t allowed_types, uint32_t & col_counter, TopTypeName * tp);

    const SQLType * RandomNextType(RandomGenerator & rg, const uint32_t allowed_types);
    const SQLType * RandomNextType(RandomGenerator & rg, const uint32_t allowed_types, uint32_t & col_counter, TopTypeName * tp);

    void DropDatabase(const uint32_t dname);

    template <bool AllowParts>
    int GenerateNextTablePartition(RandomGenerator & rg, const SQLTable & t, PartitionExpr * pexpr)
    {
        bool set_part = false;

        if (t.IsMergeTreeFamily())
        {
            const std::string dname = t.db ? ("d" + std::to_string(t.db->dname)) : "", tname = "t" + std::to_string(t.tname);
            const bool table_has_partitions = rg.NextSmallNumber() < 9 && fc.TableHasPartitions<false>(dname, tname);

            if (table_has_partitions)
            {
                if (AllowParts && rg.NextBool())
                {
                    fc.TableGetRandomPartitionOrPart<false, false>(dname, tname, buf);
                    pexpr->set_part(buf);
                }
                else
                {
                    fc.TableGetRandomPartitionOrPart<false, true>(dname, tname, buf);
                    pexpr->set_partition_id(buf);
                }
                set_part = true;
            }
        }
        if (!set_part)
        {
            pexpr->set_tuple(true);
        }
        return 0;
    }

public:
    const std::function<bool(const std::shared_ptr<SQLDatabase> &)> attached_databases
        = [](const std::shared_ptr<SQLDatabase> & d) { return d->attached == DetachStatus::ATTACHED; };
    const std::function<bool(const SQLTable &)> attached_tables
        = [](const SQLTable & t) { return (!t.db || t.db->attached == DetachStatus::ATTACHED) && t.attached == DetachStatus::ATTACHED; };
    const std::function<bool(const SQLView &)> attached_views
        = [](const SQLView & v) { return (!v.db || v.db->attached == DetachStatus::ATTACHED) && v.attached == DetachStatus::ATTACHED; };

    const std::function<bool(const SQLTable &)> attached_tables_for_oracle = [](const SQLTable & t)
    { return (!t.db || t.db->attached == DetachStatus::ATTACHED) && t.attached == DetachStatus::ATTACHED && !t.IsNotTruncableEngine(); };


    const std::function<bool(const std::shared_ptr<SQLDatabase> &)> detached_databases
        = [](const std::shared_ptr<SQLDatabase> & d) { return d->attached != DetachStatus::ATTACHED; };
    const std::function<bool(const SQLTable &)> detached_tables
        = [](const SQLTable & t) { return (t.db && t.db->attached != DetachStatus::ATTACHED) || t.attached != DetachStatus::ATTACHED; };
    const std::function<bool(const SQLView &)> detached_views
        = [](const SQLView & v) { return (v.db && v.db->attached != DetachStatus::ATTACHED) || v.attached != DetachStatus::ATTACHED; };

    StatementGenerator(FuzzConfig & fuzzc, ExternalIntegrations & conn, const bool scf)
        : fc(fuzzc), connections(conn), supports_cloud_features(scf)
    {
        buf.reserve(2048);
    }

    int GenerateNextCreateTable(RandomGenerator & rg, CreateTable * sq);
    int GenerateNextCreateDatabase(RandomGenerator & rg, CreateDatabase * cd);
    int GenerateNextStatement(RandomGenerator & rg, SQLQuery & sq);

    void UpdateGenerator(const SQLQuery & sq, ExternalIntegrations & ei, bool success);

    friend class QueryOracle;
};

}
