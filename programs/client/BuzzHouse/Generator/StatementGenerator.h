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

    SQLRelationCol() = default;

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

    SQLRelation() = default;

    explicit SQLRelation(const std::string n) : name(n) { }
};

class GroupCol
{
public:
    SQLRelationCol col;
    Expr * gexpr = nullptr;

    GroupCol() = default;
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

    QueryLevel() = default;

    explicit QueryLevel(const uint32_t n) : level(n) { }
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
        = {"'-1'",    "'0'",       "'1'",    "'10'",   "'1000'", "'is'",     "'was'",      "'are'",  "'be'",       "'have'", "'had'",
           "'were'",  "'can'",     "'said'", "'use'",  "','",    "'😀'",     "'😀😀😀😀'", "'名字'", "'兄弟姐妹'", "''",     "'\\n'",
           "x'c328'", "x'e28228'", "x'ff'",  "b'101'", "b'100'", "b'10001'", "' '",        "'c0'",   "'c1'",       "'11'"};
    std::vector<int8_t> enum8_ids
        = {-1,  -2,  -3,  -4,  -5,  -6,  -7,  -8,   -9,   -10,  -11, -12, -13, -14, -15, -16, -17, -18, -19, -20, -21,
           -22, -23, -24, -25, -26, -27, -28, -29,  -30,  -31,  -32, -33, -34, -35, -36, -37, -38, -39, -40, -41, -42,
           -43, -44, -45, -46, -47, -48, -49, -126, -127, -128, 0,   1,   2,   3,   4,   5,   6,   7,   8,   9,   10,
           11,  12,  13,  14,  15,  16,  17,  18,   19,   20,   21,  22,  23,  24,  25,  26,  27,  28,  29,  30,  31,
           32,  33,  34,  35,  36,  37,  38,  39,   40,   41,   42,  43,  44,  45,  46,  47,  48,  49,  125, 126, 127};
    std::vector<int16_t> enum16_ids
        = {-1,  -2,   -3,   -4,   -5,   -6,     -7,     -8,     -9,  -10, -11, -12, -13, -14, -15, -16,   -17,  -18, -19, -20, -21,
           -22, -23,  -24,  -25,  -26,  -27,    -28,    -29,    -30, -31, -32, -33, -34, -35, -36, -37,   -38,  -39, -40, -41, -42,
           -43, -126, -127, -128, -129, -32766, -32767, -32768, 0,   1,   2,   3,   4,   5,   6,   7,     8,    9,   10,  11,  12,
           13,  14,   15,   16,   17,   18,     19,     20,     21,  22,  23,  24,  25,  26,  27,  28,    29,   30,  31,  32,  33,
           34,  35,   36,   37,   38,   39,     40,     41,     42,  43,  44,  126, 127, 128, 129, 32766, 32767};

    std::vector<uint32_t> ids;
    std::vector<InsertEntry> entries;
    std::vector<std::reference_wrapper<const SQLTable>> filtered_tables;
    std::vector<std::reference_wrapper<const SQLView>> filtered_views;
    std::vector<std::reference_wrapper<const std::shared_ptr<SQLDatabase>>> filtered_databases;
    std::vector<std::reference_wrapper<const SQLFunction>> filtered_functions;

    std::map<uint32_t, std::map<std::string, SQLRelation>> ctes;
    std::map<uint32_t, QueryLevel> levels;

    void setAllowNotDetermistic(const bool value) { allow_not_deterministic = value; }
    void enforceFinal(const bool value) { enforce_final = value; }

    template <typename T>
    const std::map<uint32_t, T> & getNextCollection() const
    {
        if constexpr (std::is_same_v<T, SQLTable>)
        {
            return tables;
        }
        else if constexpr (std::is_same_v<T, SQLView>)
        {
            return views;
        }
        else if constexpr (std::is_same_v<T, SQLFunction>)
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
    bool collectionHas(const std::function<bool(const T &)> func) const
    {
        const auto & input = getNextCollection<T>();

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
    uint32_t collectionCount(const std::function<bool(const T &)> func) const
    {
        uint32_t res = 0;
        const auto & input = getNextCollection<T>();

        for (const auto & entry : input)
        {
            res += func(entry.second) ? 1 : 0;
        }
        return res;
    }

    template <typename T>
    std::vector<std::reference_wrapper<const T>> & getNextCollectionResult()
    {
        if constexpr (std::is_same_v<T, SQLTable>)
        {
            return filtered_tables;
        }
        else if constexpr (std::is_same_v<T, SQLView>)
        {
            return filtered_views;
        }
        else if constexpr (std::is_same_v<T, SQLFunction>)
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
    std::vector<std::reference_wrapper<const T>> & filterCollection(const std::function<bool(const T &)> func)
    {
        const auto & input = getNextCollection<T>();
        auto & res = getNextCollectionResult<T>();

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
    void insertEntryRef(const InsertEntry & entry, Expr * expr) const;
    void insertEntryRefCP(const InsertEntry & entry, ColumnPath * cp) const;
    void addTableRelation(RandomGenerator & rg, bool allow_internal_cols, const std::string & rel_name, const SQLTable & t);

    void strAppendBottomValue(RandomGenerator & rg, std::string & ret, const SQLType * tp);
    void strAppendMap(RandomGenerator & rg, std::string & ret, const MapType * mt);
    void strAppendArray(RandomGenerator & rg, std::string & ret, const ArrayType * at);
    void strAppendArray(RandomGenerator & rg, std::string & ret, const ArrayType * at, uint32_t limit);
    void strAppendTuple(RandomGenerator & rg, std::string & ret, const TupleType * at);
    void strAppendVariant(RandomGenerator & rg, std::string & ret, const VariantType * vtp);
    void strAppendAnyValueInternal(RandomGenerator & rg, std::string & ret, const SQLType * tp);
    void strAppendAnyValue(RandomGenerator & rg, std::string & ret, const SQLType * tp);

    int generateNextStatistics(RandomGenerator & rg, ColumnStatistics * cstats);
    int pickUpNextCols(RandomGenerator & rg, const SQLTable & t, ColumnList * clist);
    int addTableColumn(
        RandomGenerator & rg, SQLTable & t, uint32_t cname, bool staged, bool modify, bool is_pk, ColumnSpecial special, ColumnDef * cd);
    int addTableIndex(RandomGenerator & rg, SQLTable & t, bool staged, IndexDef * idef);
    int addTableProjection(RandomGenerator & rg, SQLTable & t, bool staged, ProjectionDef * pdef);
    int addTableConstraint(RandomGenerator & rg, SQLTable & t, bool staged, ConstraintDef * cdef);
    int generateTableKey(RandomGenerator & rg, TableEngineValues teng, TableKey * tkey);
    int generateMergeTreeEngineDetails(RandomGenerator & rg, TableEngineValues teng, bool add_pkey, TableEngine * te);
    int generateEngineDetails(RandomGenerator & rg, SQLBase & b, bool add_pkey, TableEngine * te);

    TableEngineValues getNextTableEngine(RandomGenerator & rg, bool use_external_integrations);
    PeerTableDatabase getNextPeerTableDatabase(RandomGenerator & rg, TableEngineValues teng);

    int generateNextRefreshableView(RandomGenerator & rg, RefreshableView * cv);
    int generateNextCreateView(RandomGenerator & rg, CreateView * cv);
    int generateNextDrop(RandomGenerator & rg, Drop * dp);
    int generateNextInsert(RandomGenerator & rg, Insert * ins);
    int generateNextDelete(RandomGenerator & rg, LightDelete * del);
    int generateNextTruncate(RandomGenerator & rg, Truncate * trunc);
    int generateNextOptimizeTable(RandomGenerator & rg, OptimizeTable * ot);
    int generateNextCheckTable(RandomGenerator & rg, CheckTable * ct);
    int generateNextDescTable(RandomGenerator & rg, DescTable * dt);
    int generateNextExchangeTables(RandomGenerator & rg, ExchangeTables * et);
    int generateUptDelWhere(RandomGenerator & rg, const SQLTable & t, Expr * expr);
    int generateAlterTable(RandomGenerator & rg, AlterTable * at);
    int generateSettingValues(
        RandomGenerator & rg,
        const std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> & settings,
        SettingValues * vals);
    int generateSettingValues(
        RandomGenerator & rg,
        const std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> & settings,
        size_t nvalues,
        SettingValues * vals);
    int generateSettingList(
        RandomGenerator & rg,
        const std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> & settings,
        SettingList * sl);
    int generateAttach(RandomGenerator & rg, Attach * att);
    int generateDetach(RandomGenerator & rg, Detach * det);
    int generateNextCreateFunction(RandomGenerator & rg, CreateFunction * cf);

    int addFieldAccess(RandomGenerator & rg, Expr * expr, uint32_t nested_prob);
    int addColNestedAccess(RandomGenerator & rg, ExprColumn * expr, uint32_t nested_prob);
    int refColumn(RandomGenerator & rg, const GroupCol & gcol, Expr * expr);
    int generateSubquery(RandomGenerator & rg, Select * sel);
    int generateColRef(RandomGenerator & rg, Expr * expr);
    int generateLiteralValue(RandomGenerator & rg, Expr * expr);
    int generatePredicate(RandomGenerator & rg, Expr * expr);
    int generateFrameBound(RandomGenerator & rg, Expr * expr);
    int generateExpression(RandomGenerator & rg, Expr * expr);
    int generateLambdaCall(RandomGenerator & rg, uint32_t nparams, LambdaExpr * lexpr);
    int generateFuncCall(RandomGenerator & rg, bool allow_funcs, bool allow_aggr, SQLFuncCall * func_call);

    int generateOrderBy(RandomGenerator & rg, uint32_t ncols, bool allow_settings, OrderByStatement * ob);

    int generateLimitExpr(RandomGenerator & rg, Expr * expr);
    int generateLimit(RandomGenerator & rg, bool has_order_by, uint32_t ncols, LimitStatement * ls);
    int generateOffset(RandomGenerator & rg, OffsetStatement * off);
    int generateGroupByExpr(
        RandomGenerator & rg,
        bool enforce_having,
        uint32_t offset,
        uint32_t ncols,
        const std::vector<SQLRelationCol> & available_cols,
        std::vector<GroupCol> & gcols,
        Expr * expr);
    int generateGroupBy(RandomGenerator & rg, uint32_t ncols, bool enforce_having, bool allow_settings, GroupByStatement * gb);
    int addWhereSide(RandomGenerator & rg, const std::vector<GroupCol> & available_cols, Expr * expr);
    int addWhereFilter(RandomGenerator & rg, const std::vector<GroupCol> & available_cols, Expr * expr);
    int generateWherePredicate(RandomGenerator & rg, Expr * expr);
    int addJoinClause(RandomGenerator & rg, BinaryExpr * bexpr);
    int generateArrayJoin(RandomGenerator & rg, ArrayJoin * aj);
    int generateFromElement(RandomGenerator & rg, uint32_t allowed_clauses, TableOrSubquery * tos);
    int generateJoinConstraint(RandomGenerator & rg, bool allow_using, JoinConstraint * jc);
    int setTableRemote(const SQLTable & t, TableFunction * tfunc) const;
    int generateDerivedTable(RandomGenerator & rg, SQLRelation & rel, uint32_t allowed_clauses, Select * sel);
    int generateFromStatement(RandomGenerator & rg, uint32_t allowed_clauses, FromStatement * ft);
    int addCTEs(RandomGenerator & rg, uint32_t allowed_clauses, CTEs * qctes);
    int generateSelect(RandomGenerator & rg, bool top, bool force_global_agg, uint32_t ncols, uint32_t allowed_clauses, Select * sel);

    int generateTopSelect(RandomGenerator & rg, bool force_global_agg, uint32_t allowed_clauses, TopSelect * ts);
    int generateNextExplain(RandomGenerator & rg, ExplainQuery * eq);
    int generateNextQuery(RandomGenerator & rg, SQLQueryInner * sq);

    std::tuple<const SQLType *, Integers> randomIntType(RandomGenerator & rg, uint32_t allowed_types);
    std::tuple<const SQLType *, FloatingPoints> randomFloatType(RandomGenerator & rg);
    std::tuple<const SQLType *, Dates> randomDateType(RandomGenerator & rg, uint32_t allowed_types);
    const SQLType * randomDateTimeType(RandomGenerator & rg, uint32_t allowed_types, DateTimeTp * dt);
    const SQLType * bottomType(RandomGenerator & rg, uint32_t allowed_types, bool low_card, BottomTypeName * tp);
    const SQLType * generateArraytype(RandomGenerator & rg, uint32_t allowed_types);
    const SQLType * generateArraytype(RandomGenerator & rg, uint32_t allowed_types, uint32_t & col_counter, TopTypeName * tp);

    const SQLType * randomNextType(RandomGenerator & rg, uint32_t allowed_types);
    const SQLType * randomNextType(RandomGenerator & rg, uint32_t allowed_types, uint32_t & col_counter, TopTypeName * tp);

    void dropTable(bool staged, uint32_t tname);
    void dropDatabase(uint32_t dname);

    template <bool AllowParts>
    int generateNextTablePartition(RandomGenerator & rg, const SQLTable & t, PartitionExpr * pexpr)
    {
        bool set_part = false;

        if (t.isMergeTreeFamily())
        {
            const std::string dname = t.db ? ("d" + std::to_string(t.db->dname)) : "", tname = "t" + std::to_string(t.tname);
            const bool table_has_partitions = rg.nextSmallNumber() < 9 && fc.tableHasPartitions<false>(dname, tname);

            if (table_has_partitions)
            {
                if (AllowParts && rg.nextBool())
                {
                    fc.tableGetRandomPartitionOrPart<false, false>(dname, tname, buf);
                    pexpr->set_part(buf);
                }
                else
                {
                    fc.tableGetRandomPartitionOrPart<false, true>(dname, tname, buf);
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
    { return (!t.db || t.db->attached == DetachStatus::ATTACHED) && t.attached == DetachStatus::ATTACHED && !t.isNotTruncableEngine(); };
    const std::function<bool(const SQLTable &)> attached_tables_for_oracle_with_peers = [](const SQLTable & t)
    {
        return (!t.db || t.db->attached == DetachStatus::ATTACHED) && t.attached == DetachStatus::ATTACHED && !t.isNotTruncableEngine()
            && t.hasDatabasePeer();
    };

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
        assert(enum8_ids.size() > enum_values.size() && enum16_ids.size() > enum_values.size());
    }

    int generateNextCreateTable(RandomGenerator & rg, CreateTable * ct);
    int generateNextCreateDatabase(RandomGenerator & rg, CreateDatabase * cd);
    int generateNextStatement(RandomGenerator & rg, SQLQuery & sq);

    void updateGenerator(const SQLQuery & sq, ExternalIntegrations & ei, bool success);

    friend class QueryOracle;
};

}
