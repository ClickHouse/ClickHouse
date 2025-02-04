#pragma once

#include <Client/BuzzHouse/Generator/ExternalIntegrations.h>
#include <Client/BuzzHouse/Generator/RandomGenerator.h>
#include <Client/BuzzHouse/Generator/RandomSettings.h>
#include <Client/BuzzHouse/Generator/SQLCatalog.h>

namespace BuzzHouse
{

class QueryOracle;

class SQLRelationCol
{
public:
    String rel_name;
    DB::Strings path;

    SQLRelationCol() = default;

    SQLRelationCol(const String rname, const DB::Strings names) : rel_name(rname), path(names) { }

    void AddRef(ColumnPath * cp) const
    {
        for (size_t i = 0; i < path.size(); i++)
        {
            Column * col = i == 0 ? cp->mutable_col() : cp->add_sub_cols();

            col->set_column(path[i]);
        }
    }
    void AddRef(ExprColumn * expr) const { AddRef(expr->mutable_path()); }
};

class SQLRelation
{
public:
    String name;
    std::vector<SQLRelationCol> cols;

    SQLRelation() = default;

    explicit SQLRelation(const String n) : name(n) { }
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
                         allow_prewhere = (1 << 4), allow_where = (1 << 5), allow_groupby = (1 << 6), allow_global_aggregate = (1 << 7),
                         allow_groupby_settings = (1 << 8), allow_orderby = (1 << 9), allow_orderby_settings = (1 << 10),
                         allow_limit = (1 << 11);

const constexpr uint32_t collect_generated = (1 << 0), flat_tuple = (1 << 1), flat_nested = (1 << 2), flat_json = (1 << 3),
                         skip_tuple_node = (1 << 4), skip_nested_node = (1 << 5), to_table_entries = (1 << 6), to_remote_entries = (1 << 7);

class StatementGenerator
{
public:
    const FuzzConfig & fc;
    uint32_t next_type_mask = std::numeric_limits<uint32_t>::max();

private:
    ExternalIntegrations & connections;
    const bool supports_cloud_features, replica_setup;

    PeerQuery peer_query = PeerQuery::None;
    bool in_transaction = false, inside_projection = false, allow_not_deterministic = true, allow_in_expression_alias = true,
         allow_subqueries = true, enforce_final = false, allow_engine_udf = true;
    uint32_t depth = 0, width = 0, database_counter = 0, table_counter = 0, zoo_path_counter = 0, function_counter = 0, current_level = 0;
    std::unordered_map<uint32_t, std::shared_ptr<SQLDatabase>> staged_databases, databases;
    std::unordered_map<uint32_t, SQLTable> staged_tables, tables;
    std::unordered_map<uint32_t, SQLView> staged_views, views;
    std::unordered_map<uint32_t, SQLFunction> staged_functions, functions;

    DB::Strings enum_values
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
    std::vector<ColumnPathChain> entries, table_entries, remote_entries;
    std::vector<std::reference_wrapper<const ColumnPathChain>> filtered_entries;
    std::vector<std::reference_wrapper<const SQLTable>> filtered_tables;
    std::vector<std::reference_wrapper<const SQLView>> filtered_views;
    std::vector<std::reference_wrapper<const std::shared_ptr<SQLDatabase>>> filtered_databases;
    std::vector<std::reference_wrapper<const SQLFunction>> filtered_functions;

    std::unordered_map<uint32_t, std::unordered_map<String, SQLRelation>> ctes;
    std::unordered_map<uint32_t, QueryLevel> levels;

    void setAllowNotDetermistic(const bool value) { allow_not_deterministic = value; }
    void enforceFinal(const bool value) { enforce_final = value; }
    void generatingPeerQuery(const PeerQuery value) { peer_query = value; }
    void setAllowEngineUDF(const bool value) { allow_engine_udf = value; }

    template <typename T>
    String setMergeTableParameter(RandomGenerator & rg, char initial);

    template <typename T>
    const std::unordered_map<uint32_t, T> & getNextCollection() const
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

    template <typename T>
    void setTableSystemStatement(RandomGenerator & rg, const std::function<bool(const T &)> & f, ExprSchemaTable * est)
    {
        const T & t = rg.pickRandomlyFromVector(filterCollection<T>(f));

        if (t.db)
        {
            est->mutable_database()->set_database("d" + std::to_string(t.db->dname));
        }
        est->mutable_table()->set_table("t" + std::to_string(t.tname));
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
                res.emplace_back(std::ref<const T>(entry.second));
            }
        }
        return res;
    }

private:
    void columnPathRef(const ColumnPathChain & entry, Expr * expr) const;
    void columnPathRef(const ColumnPathChain & entry, ColumnPath * cp) const;
    void addTableRelation(RandomGenerator & rg, bool allow_internal_cols, const String & rel_name, const SQLTable & t);
    String strAppendAnyValue(RandomGenerator & rg, SQLType * tp);
    void flatTableColumnPath(uint32_t flags, const SQLTable & t, std::function<bool(const SQLColumn & c)> col_filter);
    void generateStorage(RandomGenerator & rg, Storage * store) const;
    void generateNextCodecs(RandomGenerator & rg, CodecList * cl);
    void generateTTLExpression(RandomGenerator & rg, const std::optional<SQLTable> & t, Expr * ttl_expr);
    void generateNextTTL(RandomGenerator & rg, const std::optional<SQLTable> & t, const TableEngine * te, TTLExpr * ttl_expr);
    void generateNextStatistics(RandomGenerator & rg, ColumnStatistics * cstats);
    void pickUpNextCols(RandomGenerator & rg, const SQLTable & t, ColumnPathList * clist);
    void addTableColumn(
        RandomGenerator & rg, SQLTable & t, uint32_t cname, bool staged, bool modify, bool is_pk, ColumnSpecial special, ColumnDef * cd);
    void addTableIndex(RandomGenerator & rg, SQLTable & t, bool staged, IndexDef * idef);
    void addTableProjection(RandomGenerator & rg, SQLTable & t, bool staged, ProjectionDef * pdef);
    void addTableConstraint(RandomGenerator & rg, SQLTable & t, bool staged, ConstraintDef * cdef);
    void generateTableKey(RandomGenerator & rg, TableEngineValues teng, bool allow_asc_desc, TableKey * tkey);
    void
    generateMergeTreeEngineDetails(RandomGenerator & rg, TableEngineValues teng, PeerTableDatabase peer, bool add_pkey, TableEngine * te);
    void generateEngineDetails(RandomGenerator & rg, SQLBase & b, bool add_pkey, TableEngine * te);

    DatabaseEngineValues getNextDatabaseEngine(RandomGenerator & rg);
    TableEngineValues getNextTableEngine(RandomGenerator & rg, bool use_external_integrations);
    PeerTableDatabase getNextPeerTableDatabase(RandomGenerator & rg, TableEngineValues teng);

    void generateNextRefreshableView(RandomGenerator & rg, RefreshableView * cv);
    void generateNextCreateView(RandomGenerator & rg, CreateView * cv);
    void generateNextDrop(RandomGenerator & rg, Drop * dp);
    void generateNextInsert(RandomGenerator & rg, Insert * ins);
    void generateNextDelete(RandomGenerator & rg, LightDelete * del);
    void generateNextTruncate(RandomGenerator & rg, Truncate * trunc);
    void generateNextOptimizeTable(RandomGenerator & rg, OptimizeTable * ot);
    void generateNextCheckTable(RandomGenerator & rg, CheckTable * ct);
    void generateNextDescTable(RandomGenerator & rg, DescTable * dt);
    void generateNextExchangeTables(RandomGenerator & rg, ExchangeTables * et);
    void generateUptDelWhere(RandomGenerator & rg, const SQLTable & t, Expr * expr);
    void generateAlterTable(RandomGenerator & rg, AlterTable * at);
    void setRandomSetting(RandomGenerator & rg, const std::unordered_map<String, CHSetting> & settings, SetValue * set);
    void generateSettingValues(RandomGenerator & rg, const std::unordered_map<String, CHSetting> & settings, SettingValues * vals);
    void generateSettingValues(
        RandomGenerator & rg, const std::unordered_map<String, CHSetting> & settings, size_t nvalues, SettingValues * vals);
    void generateSettingList(RandomGenerator & rg, const std::unordered_map<String, CHSetting> & settings, SettingList * sl);
    void generateAttach(RandomGenerator & rg, Attach * att);
    void generateDetach(RandomGenerator & rg, Detach * det);
    void generateNextCreateFunction(RandomGenerator & rg, CreateFunction * cf);
    void generateNextSystemStatement(RandomGenerator & rg, SystemCommand * sc);

    void addFieldAccess(RandomGenerator & rg, Expr * expr, uint32_t nested_prob);
    void addColNestedAccess(RandomGenerator & rg, ExprColumn * expr, uint32_t nested_prob);
    void refColumn(RandomGenerator & rg, const GroupCol & gcol, Expr * expr);
    void generateSubquery(RandomGenerator & rg, Select * sel);
    void generateColRef(RandomGenerator & rg, Expr * expr);
    void generateLiteralValue(RandomGenerator & rg, Expr * expr);
    void generatePredicate(RandomGenerator & rg, Expr * expr);
    void generateFrameBound(RandomGenerator & rg, Expr * expr);
    void generateExpression(RandomGenerator & rg, Expr * expr);
    void generateLambdaCall(RandomGenerator & rg, uint32_t nparams, LambdaExpr * lexpr);
    void generateFuncCall(RandomGenerator & rg, bool allow_funcs, bool allow_aggr, SQLFuncCall * func_call);
    void generateTableFuncCall(RandomGenerator & rg, SQLTableFuncCall * tfunc_call);

    void generateOrderBy(RandomGenerator & rg, uint32_t ncols, bool allow_settings, OrderByStatement * ob);

    void generateLimitExpr(RandomGenerator & rg, Expr * expr);
    void generateLimit(RandomGenerator & rg, bool has_order_by, uint32_t ncols, LimitStatement * ls);
    void generateOffset(RandomGenerator & rg, OffsetStatement * off);
    void generateGroupByExpr(
        RandomGenerator & rg,
        bool enforce_having,
        uint32_t offset,
        uint32_t ncols,
        const std::vector<SQLRelationCol> & available_cols,
        std::vector<GroupCol> & gcols,
        Expr * expr);
    bool generateGroupBy(RandomGenerator & rg, uint32_t ncols, bool enforce_having, bool allow_settings, GroupByStatement * gb);
    void addWhereSide(RandomGenerator & rg, const std::vector<GroupCol> & available_cols, Expr * expr);
    void addWhereFilter(RandomGenerator & rg, const std::vector<GroupCol> & available_cols, Expr * expr);
    void generateWherePredicate(RandomGenerator & rg, Expr * expr);
    void addJoinClause(RandomGenerator & rg, BinaryExpr * bexpr);
    void generateArrayJoin(RandomGenerator & rg, ArrayJoin * aj);
    void setTableRemote(RandomGenerator & rg, bool table_engine, const SQLTable & t, TableFunction * tfunc);
    void generateFromElement(RandomGenerator & rg, uint32_t allowed_clauses, TableOrSubquery * tos);
    void generateJoinConstraint(RandomGenerator & rg, bool allow_using, JoinConstraint * jc);
    void generateDerivedTable(RandomGenerator & rg, SQLRelation & rel, uint32_t allowed_clauses, Select * sel);
    void generateFromStatement(RandomGenerator & rg, uint32_t allowed_clauses, FromStatement * ft);
    void addCTEs(RandomGenerator & rg, uint32_t allowed_clauses, CTEs * qctes);
    void generateSelect(RandomGenerator & rg, bool top, bool force_global_agg, uint32_t ncols, uint32_t allowed_clauses, Select * sel);

    void generateTopSelect(RandomGenerator & rg, bool force_global_agg, uint32_t allowed_clauses, TopSelect * ts);
    void generateNextExplain(RandomGenerator & rg, ExplainQuery * eq);
    void generateNextQuery(RandomGenerator & rg, SQLQueryInner * sq);

    std::tuple<SQLType *, Integers> randomIntType(RandomGenerator & rg, uint32_t allowed_types);
    std::tuple<SQLType *, FloatingPoints> randomFloatType(RandomGenerator & rg) const;
    std::tuple<SQLType *, Dates> randomDateType(RandomGenerator & rg, uint32_t allowed_types) const;
    SQLType * randomDateTimeType(RandomGenerator & rg, uint32_t allowed_types, DateTimeTp * dt) const;
    SQLType * bottomType(RandomGenerator & rg, uint32_t allowed_types, bool low_card, BottomTypeName * tp);

    void dropTable(bool staged, bool drop_peer, uint32_t tname);
    void dropDatabase(uint32_t dname);

    void generateNextTablePartition(RandomGenerator & rg, bool allow_parts, const SQLTable & t, PartitionExpr * pexpr);

public:
    SQLType * randomNextType(RandomGenerator & rg, uint32_t allowed_types, uint32_t & col_counter, TopTypeName * tp);

    const std::function<bool(const std::shared_ptr<SQLDatabase> &)> attached_databases
        = [](const std::shared_ptr<SQLDatabase> & d) { return d->attached == DetachStatus::ATTACHED; };
    const std::function<bool(const SQLTable &)> attached_tables
        = [](const SQLTable & t) { return (!t.db || t.db->attached == DetachStatus::ATTACHED) && t.attached == DetachStatus::ATTACHED; };
    const std::function<bool(const SQLView &)> attached_views
        = [](const SQLView & v) { return (!v.db || v.db->attached == DetachStatus::ATTACHED) && v.attached == DetachStatus::ATTACHED; };

    const std::function<bool(const SQLTable &)> attached_tables_for_dump_table_oracle = [](const SQLTable & t)
    {
        return (!t.db || t.db->attached == DetachStatus::ATTACHED) && t.attached == DetachStatus::ATTACHED && !t.isNotTruncableEngine()
            && t.teng != TableEngineValues::CollapsingMergeTree;
    };
    const std::function<bool(const SQLTable &)> attached_tables_for_table_peer_oracle = [](const SQLTable & t)
    {
        return (!t.db || t.db->attached == DetachStatus::ATTACHED) && t.attached == DetachStatus::ATTACHED && !t.isNotTruncableEngine()
            && t.hasDatabasePeer();
    };
    const std::function<bool(const SQLTable &)> attached_tables_for_clickhouse_table_peer_oracle = [](const SQLTable & t)
    {
        return (!t.db || t.db->attached == DetachStatus::ATTACHED) && t.attached == DetachStatus::ATTACHED && !t.isNotTruncableEngine()
            && t.hasClickHousePeer();
    };

    const std::function<bool(const std::shared_ptr<SQLDatabase> &)> detached_databases
        = [](const std::shared_ptr<SQLDatabase> & d) { return d->attached != DetachStatus::ATTACHED; };
    const std::function<bool(const SQLTable &)> detached_tables
        = [](const SQLTable & t) { return (t.db && t.db->attached != DetachStatus::ATTACHED) || t.attached != DetachStatus::ATTACHED; };
    const std::function<bool(const SQLView &)> detached_views
        = [](const SQLView & v) { return (v.db && v.db->attached != DetachStatus::ATTACHED) || v.attached != DetachStatus::ATTACHED; };

    StatementGenerator(FuzzConfig & fuzzc, ExternalIntegrations & conn, const bool scf, const bool hrs)
        : fc(fuzzc), connections(conn), supports_cloud_features(scf), replica_setup(hrs)
    {
        chassert(enum8_ids.size() > enum_values.size() && enum16_ids.size() > enum_values.size());
    }

    void generateNextCreateTable(RandomGenerator & rg, CreateTable * ct);
    void generateNextCreateDatabase(RandomGenerator & rg, CreateDatabase * cd);
    void generateNextStatement(RandomGenerator & rg, SQLQuery & sq);

    void updateGenerator(const SQLQuery & sq, ExternalIntegrations & ei, bool success);

    friend class QueryOracle;
};

}
