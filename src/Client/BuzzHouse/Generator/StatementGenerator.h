#pragma once

#include <Client/BuzzHouse/Generator/ExternalIntegrations.h>
#include <Client/BuzzHouse/Generator/RandomGenerator.h>
#include <Client/BuzzHouse/Generator/RandomSettings.h>
#include <Client/BuzzHouse/Generator/SQLCatalog.h>
#include <Client/BuzzHouse/Generator/SQLFuncs.h>

namespace BuzzHouse
{

class QueryOracle;

class SQLRelationCol
{
public:
    String rel_name;
    DB::Strings path;

    SQLRelationCol() = default;

    SQLRelationCol(const String rname, const DB::Strings names)
        : rel_name(rname)
        , path(names)
    {
    }

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

    explicit SQLRelation(const String n)
        : name(n)
    {
    }
};

class GroupCol
{
public:
    std::optional<SQLRelationCol> col;
    const Expr * gexpr = nullptr;

    GroupCol() = default;
    GroupCol(std::optional<SQLRelationCol> c, Expr * g)
        : col(c)
        , gexpr(g)
    {
    }
};

class QueryLevel
{
public:
    bool global_aggregate = false, inside_aggregate = false, allow_aggregates = true, allow_window_funcs = true, group_by_all = false;
    uint32_t level, cte_counter = 0, window_counter = 0;
    std::vector<GroupCol> gcols;
    std::vector<SQLRelation> rels;
    std::vector<String> projections;

    QueryLevel() = default;

    explicit QueryLevel(const uint32_t n)
        : level(n)
    {
    }
};

const constexpr uint32_t allow_set = (1 << 0), allow_cte = (1 << 1), allow_distinct = (1 << 2), allow_from = (1 << 3),
                         allow_prewhere = (1 << 4), allow_where = (1 << 5), allow_groupby = (1 << 6), allow_global_aggregate = (1 << 7),
                         allow_groupby_settings = (1 << 8), allow_orderby = (1 << 9), allow_orderby_settings = (1 << 10),
                         allow_limit = (1 << 11), allow_window_clause = (1 << 12), allow_qualify = (1 << 13);

const constexpr uint32_t collect_generated = (1 << 0), flat_tuple = (1 << 1), flat_nested = (1 << 2), flat_json = (1 << 3),
                         skip_tuple_node = (1 << 4), skip_nested_node = (1 << 5), to_table_entries = (1 << 6), to_remote_entries = (1 << 7);

class CatalogBackup
{
public:
    uint32_t backup_num = 0;
    bool everything = false;
    BackupRestore_BackupOutput outf;
    std::optional<OutFormat> out_format;
    DB::Strings out_params;
    std::unordered_map<uint32_t, std::shared_ptr<SQLDatabase>> databases;
    std::unordered_map<uint32_t, SQLTable> tables;
    std::unordered_map<uint32_t, SQLView> views;
    std::unordered_map<uint32_t, SQLDictionary> dictionaries;
    /// Backup a system table
    std::optional<String> system_table, partition_id;

    CatalogBackup() = default;
};

class StatementGenerator
{
public:
    static const std::unordered_map<OutFormat, InFormat> outIn;

    FuzzConfig & fc;
    uint32_t next_type_mask = std::numeric_limits<uint32_t>::max();

private:
    std::vector<TableEngineValues> likeEngs;
    ExternalIntegrations & connections;
    const bool supports_cloud_features, replica_setup;
    const size_t deterministic_funcs_limit, deterministic_aggrs_limit;

    PeerQuery peer_query = PeerQuery::None;
    bool in_transaction = false, inside_projection = false, allow_not_deterministic = true, allow_in_expression_alias = true,
         allow_subqueries = true, enforce_final = false, allow_engine_udf = true;
    uint32_t depth = 0, width = 0, database_counter = 0, table_counter = 0, zoo_path_counter = 0, function_counter = 0, current_level = 0,
             backup_counter = 0, cache_counter = 0, aliases_counter = 0;
    std::unordered_map<uint32_t, std::shared_ptr<SQLDatabase>> staged_databases, databases;
    std::unordered_map<uint32_t, SQLTable> staged_tables, tables;
    std::unordered_map<uint32_t, SQLView> staged_views, views;
    std::unordered_map<uint32_t, SQLDictionary> staged_dictionaries, dictionaries;
    std::unordered_map<uint32_t, SQLFunction> staged_functions, functions;
    std::unordered_map<uint32_t, CatalogBackup> backups;

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
    std::vector<CHFunction> one_arg_funcs;
    std::vector<ColumnPathChain> entries, table_entries, remote_entries;
    std::vector<std::reference_wrapper<const ColumnPathChain>> filtered_entries;
    std::vector<std::reference_wrapper<const SQLColumn>> filtered_columns;
    std::vector<std::reference_wrapper<const SQLTable>> filtered_tables;
    std::vector<std::reference_wrapper<const SQLView>> filtered_views;
    std::vector<std::reference_wrapper<const SQLDictionary>> filtered_dictionaries;
    std::vector<std::reference_wrapper<const std::shared_ptr<SQLDatabase>>> filtered_databases;
    std::vector<std::reference_wrapper<const SQLFunction>> filtered_functions;
    std::vector<std::reference_wrapper<const SQLRelation>> filtered_relations;

    std::unordered_map<uint32_t, std::unordered_map<String, SQLRelation>> ctes;
    std::unordered_map<uint32_t, QueryLevel> levels;

    void setAllowNotDetermistic(const bool value) { allow_not_deterministic = value; }
    void enforceFinal(const bool value) { enforce_final = value; }
    void generatingPeerQuery(const PeerQuery value) { peer_query = value; }
    void setAllowEngineUDF(const bool value) { allow_engine_udf = value; }
    void resetAliasCounter() { aliases_counter = 0; }

    template <typename T>
    String setMergeTableParameter(RandomGenerator & rg, const String & initial);

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
        else if constexpr (std::is_same_v<T, SQLDictionary>)
        {
            return dictionaries;
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
        else if constexpr (std::is_same_v<T, SQLDictionary>)
        {
            return filtered_dictionaries;
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
    std::optional<String> setTableSystemStatement(RandomGenerator & rg, const std::function<bool(const T &)> & f, ExprSchemaTable * est)
    {
        const T & t = rg.pickRandomly(filterCollection<T>(f));

        t.setName(est, false);
        return t.getCluster();
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
    String getNextAlias() { return "a" + std::to_string(aliases_counter++); }
    void columnPathRef(const ColumnPathChain & entry, Expr * expr) const;
    void columnPathRef(const ColumnPathChain & entry, ColumnPath * cp) const;
    String columnPathRef(const ColumnPathChain & entry) const;
    void
    colRefOrExpression(RandomGenerator & rg, const SQLRelation & rel, TableEngineValues teng, const ColumnPathChain & entry, Expr * expr);
    String nextComment(RandomGenerator & rg) const;
    SQLRelation createTableRelation(RandomGenerator & rg, bool allow_internal_cols, const String & rel_name, const SQLTable & t);
    void addTableRelation(RandomGenerator & rg, bool allow_internal_cols, const String & rel_name, const SQLTable & t);
    SQLRelation createViewRelation(const String & rel_name, size_t ncols);
    void addViewRelation(const String & rel_name, const SQLView & v);
    void addDictionaryRelation(const String & rel_name, const SQLDictionary & d);
    String strAppendAnyValue(RandomGenerator & rg, SQLType * tp);
    void flatTableColumnPath(
        uint32_t flags, const std::unordered_map<uint32_t, SQLColumn> & cols, std::function<bool(const SQLColumn & c)> col_filter);
    void flatColumnPath(uint32_t flags, const std::unordered_map<uint32_t, std::unique_ptr<SQLType>> & centries);
    void addRandomRelation(RandomGenerator & rg, std::optional<String> rel_name, uint32_t ncols, bool escape, Expr * expr);
    void generateStorage(RandomGenerator & rg, Storage * store) const;
    void generateNextCodecs(RandomGenerator & rg, CodecList * cl);
    void generateTTLExpression(RandomGenerator & rg, const std::optional<SQLTable> & t, Expr * ttl_expr);
    void generateNextTTL(RandomGenerator & rg, const std::optional<SQLTable> & t, const TableEngine * te, TTLExpr * ttl_expr);
    void generateNextStatistics(RandomGenerator & rg, ColumnStatistics * cstats);
    void pickUpNextCols(RandomGenerator & rg, const SQLTable & t, ColumnPathList * clist);
    void addTableColumnInternal(
        RandomGenerator & rg,
        SQLTable & t,
        uint32_t cname,
        bool modify,
        bool is_pk,
        ColumnSpecial special,
        uint32_t col_tp_mask,
        SQLColumn & col,
        ColumnDef * cd);
    void addTableColumn(
        RandomGenerator & rg, SQLTable & t, uint32_t cname, bool staged, bool modify, bool is_pk, ColumnSpecial special, ColumnDef * cd);
    void addTableIndex(RandomGenerator & rg, SQLTable & t, bool staged, IndexDef * idef);
    void addTableProjection(RandomGenerator & rg, SQLTable & t, bool staged, ProjectionDef * pdef);
    void addTableConstraint(RandomGenerator & rg, SQLTable & t, bool staged, ConstraintDef * cdef);
    void generateTableKey(RandomGenerator & rg, const SQLRelation & rel, TableEngineValues teng, bool allow_asc_desc, TableKey * tkey);
    void setClusterInfo(RandomGenerator & rg, SQLBase & b) const;
    void generateMergeTreeEngineDetails(RandomGenerator & rg, const SQLRelation & rel, const SQLBase & b, bool add_pkey, TableEngine * te);
    void generateEngineDetails(RandomGenerator & rg, const SQLRelation & rel, SQLBase & b, bool add_pkey, TableEngine * te);

    DatabaseEngineValues getNextDatabaseEngine(RandomGenerator & rg);
    void getNextTableEngine(RandomGenerator & rg, bool use_external_integrations, SQLBase & b);
    void getNextPeerTableDatabase(RandomGenerator & rg, SQLBase & b);

    void generateNextRefreshableView(RandomGenerator & rg, RefreshableView * cv);
    void generateNextCreateView(RandomGenerator & rg, CreateView * cv);
    void generateNextCreateDictionary(RandomGenerator & rg, CreateDictionary * cd);
    void generateNextDrop(RandomGenerator & rg, Drop * dp);
    void generateNextInsert(RandomGenerator & rg, bool in_parallel, Insert * ins);
    void generateNextUpdate(RandomGenerator & rg, const SQLTable & t, Update * upt);
    void generateNextDelete(RandomGenerator & rg, const SQLTable & t, Delete * del);
    template <typename T>
    void generateNextUpdateOrDelete(RandomGenerator & rg, T * st);
    void generateNextTruncate(RandomGenerator & rg, Truncate * trunc);
    void generateNextOptimizeTableInternal(RandomGenerator & rg, const SQLTable & t, bool strict, OptimizeTable * ot);
    void generateNextOptimizeTable(RandomGenerator & rg, OptimizeTable * ot);
    void generateNextCheckTable(RandomGenerator & rg, CheckTable * ct);
    void generateNextDescTable(RandomGenerator & rg, DescTable * dt);
    void generateNextRename(RandomGenerator & rg, Rename * ren);
    void generateNextExchange(RandomGenerator & rg, Exchange * exc);
    void generateNextKill(RandomGenerator & rg, Kill * kil);
    void generateUptDelWhere(RandomGenerator & rg, const SQLTable & t, Expr * expr);
    void generateAlter(RandomGenerator & rg, Alter * at);
    void setRandomSetting(RandomGenerator & rg, const std::unordered_map<String, CHSetting> & settings, SetValue * set);
    void generateSettingValues(RandomGenerator & rg, const std::unordered_map<String, CHSetting> & settings, SettingValues * vals);
    void generateSettingValues(
        RandomGenerator & rg, const std::unordered_map<String, CHSetting> & settings, size_t nvalues, SettingValues * vals);
    void generateSettingList(RandomGenerator & rg, const std::unordered_map<String, CHSetting> & settings, SettingList * sl);
    void generateAttach(RandomGenerator & rg, Attach * att);
    void generateDetach(RandomGenerator & rg, Detach * det);
    void generateNextCreateFunction(RandomGenerator & rg, CreateFunction * cf);
    void generateNextSystemStatement(RandomGenerator & rg, bool allow_table_statements, SystemCommand * sc);

    void addFieldAccess(RandomGenerator & rg, Expr * expr, uint32_t nested_prob);
    void addColNestedAccess(RandomGenerator & rg, ExprColumn * expr, uint32_t nested_prob);
    void addSargableColRef(RandomGenerator & rg, const SQLRelationCol & rel_col, Expr * expr);
    void refColumn(RandomGenerator & rg, const GroupCol & gcol, Expr * expr);
    void generateSubquery(RandomGenerator & rg, ExplainQuery * eq);
    void generateColRef(RandomGenerator & rg, Expr * expr);
    void generateLiteralValueInternal(RandomGenerator & rg, bool complex, Expr * expr);
    void generateLiteralValue(RandomGenerator & rg, bool complex, Expr * expr);
    void generatePredicate(RandomGenerator & rg, Expr * expr);
    void generateFrameBound(RandomGenerator & rg, Expr * expr);
    void generateWindowDefinition(RandomGenerator & rg, WindowDefn * wdef);
    void generateExpression(RandomGenerator & rg, Expr * expr);
    void generateLambdaCall(RandomGenerator & rg, uint32_t nparams, LambdaExpr * lexpr);
    void generateFuncCall(RandomGenerator & rg, bool allow_funcs, bool allow_aggr, SQLFuncCall * func_call);
    void generateTableFuncCall(RandomGenerator & rg, SQLTableFuncCall * tfunc_call);
    void prepareNextExplain(RandomGenerator & rg, ExplainQuery * eq);
    void generateOrderBy(RandomGenerator & rg, uint32_t ncols, bool allow_settings, bool is_window, OrderByStatement * ob);
    void generateLimitExpr(RandomGenerator & rg, Expr * expr);
    void generateLimit(RandomGenerator & rg, bool has_order_by, uint32_t ncols, LimitStatement * ls);
    void generateOffset(RandomGenerator & rg, bool has_order_by, OffsetStatement * off);
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
    void addJoinClause(RandomGenerator & rg, Expr * expr);
    void generateArrayJoin(RandomGenerator & rg, ArrayJoin * aj);
    void setTableRemote(RandomGenerator & rg, bool table_engine, bool use_cluster, const SQLTable & t, TableFunction * tfunc);
    bool joinedTableOrFunction(
        RandomGenerator & rg, const String & rel_name, uint32_t allowed_clauses, bool under_remote, TableOrFunction * tof);
    void generateFromElement(RandomGenerator & rg, uint32_t allowed_clauses, TableOrSubquery * tos);
    void generateJoinConstraint(RandomGenerator & rg, bool allow_using, JoinConstraint * jc);
    void generateDerivedTable(RandomGenerator & rg, SQLRelation & rel, uint32_t allowed_clauses, uint32_t ncols, Select * sel);
    /* Returns the number of from elements generated */
    uint32_t generateFromStatement(RandomGenerator & rg, uint32_t allowed_clauses, FromStatement * ft);
    void addCTEs(RandomGenerator & rg, uint32_t allowed_clauses, CTEs * qctes);
    void addWindowDefs(RandomGenerator & rg, SelectStatementCore * ssc);
    void generateSelect(RandomGenerator & rg, bool top, bool force_global_agg, uint32_t ncols, uint32_t allowed_clauses, Select * sel);

    void generateTopSelect(RandomGenerator & rg, bool force_global_agg, uint32_t allowed_clauses, TopSelect * ts);
    void generateNextExplain(RandomGenerator & rg, bool in_parallel, ExplainQuery * eq);
    void generateNextQuery(RandomGenerator & rg, bool in_parallel, SQLQueryInner * sq);

    std::tuple<SQLType *, Integers> randomIntType(RandomGenerator & rg, uint32_t allowed_types);
    std::tuple<SQLType *, FloatingPoints> randomFloatType(RandomGenerator & rg) const;
    std::tuple<SQLType *, Dates> randomDateType(RandomGenerator & rg, uint32_t allowed_types) const;
    SQLType * randomTimeType(RandomGenerator & rg, uint32_t allowed_types, TimeTp * dt) const;
    SQLType * randomDateTimeType(RandomGenerator & rg, uint32_t allowed_types, DateTimeTp * dt) const;
    SQLType * randomDecimalType(RandomGenerator & rg, uint32_t allowed_types, BottomTypeName * tp) const;
    SQLType * bottomType(RandomGenerator & rg, uint32_t allowed_types, bool low_card, BottomTypeName * tp);

    void dropTable(bool staged, bool drop_peer, uint32_t tname);
    void dropDatabase(uint32_t dname);

    void generateNextTablePartition(RandomGenerator & rg, bool allow_parts, const SQLTable & t, PartitionExpr * pexpr);

    void generateNextBackup(RandomGenerator & rg, BackupRestore * br);
    void generateNextRestore(RandomGenerator & rg, BackupRestore * br);
    void generateNextBackupOrRestore(RandomGenerator & rg, BackupRestore * br);
    void updateGeneratorFromSingleQuery(const SingleSQLQuery & sq, ExternalIntegrations & ei, bool success);

    template <typename T>
    void exchangeObjects(uint32_t tname1, uint32_t tname2);
    template <typename T>
    void renameObjects(uint32_t old_tname, uint32_t new_tname, const std::optional<uint32_t> & new_db);
    template <typename T>
    void attachOrDetachObject(uint32_t tname, DetachStatus status);

    static const constexpr auto funcDeterministicLambda = [](const SQLFunction & f) { return f.is_deterministic; };

    static const constexpr auto funcNotDeterministicIndexLambda = [](const CHFunction & f) { return f.fnum == SQLFunc::FUNCarrayShuffle; };

    static const constexpr auto aggrNotDeterministicIndexLambda = [](const CHAggregate & a) { return a.fnum == SQLFunc::FUNCany; };

public:
    SQLType * randomNextType(RandomGenerator & rg, uint32_t allowed_types, uint32_t & col_counter, TopTypeName * tp);

    const std::function<bool(const std::shared_ptr<SQLDatabase> &)> attached_databases
        = [](const std::shared_ptr<SQLDatabase> & d) { return d->isAttached(); };
    const std::function<bool(const SQLTable &)> attached_tables = [](const SQLTable & t) { return t.isAttached(); };
    const std::function<bool(const SQLView &)> attached_views = [](const SQLView & v) { return v.isAttached(); };
    const std::function<bool(const SQLDictionary &)> attached_dictionaries = [](const SQLDictionary & d) { return d.isAttached(); };

    const std::function<bool(const SQLTable &)> attached_tables_to_test_format
        = [](const SQLTable & t) { return t.isAttached() && t.teng != TableEngineValues::GenerateRandom; };
    const std::function<bool(const SQLTable &)> attached_tables_to_compare_content = [](const SQLTable & t)
    {
        return t.isAttached() && !t.isNotTruncableEngine() && t.teng != TableEngineValues::CollapsingMergeTree
            && t.teng != TableEngineValues::GenerateRandom;
    };
    const std::function<bool(const SQLTable &)> attached_tables_for_table_peer_oracle
        = [](const SQLTable & t) { return t.isAttached() && !t.isNotTruncableEngine() && t.is_deterministic; };
    const std::function<bool(const SQLTable &)> attached_tables_for_clickhouse_table_peer_oracle
        = [](const SQLTable & t) { return t.isAttached() && !t.isNotTruncableEngine() && t.hasClickHousePeer(); };

    const std::function<bool(const std::shared_ptr<SQLDatabase> &)> detached_databases
        = [](const std::shared_ptr<SQLDatabase> & d) { return d->isDettached(); };
    const std::function<bool(const SQLTable &)> detached_tables = [](const SQLTable & t) { return t.isDettached(); };
    const std::function<bool(const SQLView &)> detached_views = [](const SQLView & v) { return v.isDettached(); };
    const std::function<bool(const SQLDictionary &)> detached_dictionaries = [](const SQLDictionary & d) { return d.isDettached(); };

    template <typename T>
    std::function<bool(const T &)> hasTableOrView(const SQLBase & b) const
    {
        return [&b](const T & t) { return t.isAttached() && (t.is_deterministic || !b.is_deterministic); };
    }

    template <bool RequireMergeTree>
    auto getQueryTableLambda();

    StatementGenerator(FuzzConfig & fuzzc, ExternalIntegrations & conn, bool scf, bool rs);

    void generateNextCreateTable(RandomGenerator & rg, bool in_parallel, CreateTable * ct);
    void generateNextCreateDatabase(RandomGenerator & rg, CreateDatabase * cd);
    void generateNextStatement(RandomGenerator & rg, SQLQuery & sq);

    void updateGenerator(const SQLQuery & sq, ExternalIntegrations & ei, bool success);
    void setInTransaction(const bool value) { in_transaction = value; }

    friend class QueryOracle;
};

}
