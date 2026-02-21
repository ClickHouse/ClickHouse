#pragma once

#include <Client/BuzzHouse/Generator/ExternalIntegrations.h>
#include <Client/BuzzHouse/Generator/ProbabilityGenerator.h>
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

    SQLRelationCol(const String & rname, const DB::Strings & names)
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

    explicit SQLRelation(const String & n)
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
    bool global_aggregate = false;
    bool inside_aggregate = false;
    bool allow_aggregates = true;
    bool allow_window_funcs = true;
    bool group_by_all = false;
    uint32_t level;
    uint32_t cte_counter = 0;
    uint32_t window_counter = 0;
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
                         allow_limitby = (1 << 11), allow_limit = (1 << 12), allow_offset = (1 << 13), allow_window_clause = (1 << 14),
                         allow_qualify = (1 << 15);

const constexpr uint32_t collect_generated = (1 << 0), flat_tuple = (1 << 1), flat_nested = (1 << 2), flat_json = (1 << 3),
                         skip_tuple_node = (1 << 4), skip_nested_node = (1 << 5), to_table_entries = (1 << 6), to_remote_entries = (1 << 7);

class CatalogBackup
{
public:
    uint32_t backup_num = 0;
    bool everything = false;
    BackupRestore_BackupOutput outf;
    std::optional<OutFormat> out_format;
    BackupParams out_params;
    std::unordered_map<uint32_t, std::shared_ptr<SQLDatabase>> databases;
    std::unordered_map<uint32_t, SQLTable> tables;
    std::unordered_map<uint32_t, SQLView> views;
    std::unordered_map<uint32_t, SQLDictionary> dictionaries;
    /// Backup a system table
    std::optional<String> system_table_schema;
    std::optional<String> system_table_name;
    std::optional<String> partition_id;

    CatalogBackup() = default;
};

enum class TableFunctionUsage
{
    PeerTable = 1,
    EngineReplace = 2,
    RemoteCall = 3,
    ClusterCall = 4,
};

enum class TableRequirement
{
    NoRequirement = 0,
    RequireMergeTree = 1,
    RequireReplaceable = 2,
    RequireProjection = 3,
};

class StatementGenerator
{
public:
    static const std::unordered_map<JoinType, std::vector<JoinConst>> joinMappings;

    FuzzConfig & fc;
    uint64_t next_type_mask = std::numeric_limits<uint64_t>::max();

private:
    std::vector<TableEngineValues> likeEngsDeterministic;
    std::vector<TableEngineValues> likeEngsNotDeterministic;
    std::vector<TableEngineValues> likeEngsInfinite;
    std::unordered_map<SQLFunc, uint32_t> dictFuncs;
    ExternalIntegrations & connections;
    const bool supports_cloud_features;
    const size_t deterministic_funcs_limit;
    const size_t deterministic_aggrs_limit;
    PeerQuery peer_query = PeerQuery::None;

    bool in_transaction = false;
    bool inside_projection = false;
    bool allow_not_deterministic = true;
    bool allow_in_expression_alias = true;
    bool allow_subqueries = true;
    bool enforce_final = false;
    bool allow_engine_udf = true;

    uint32_t depth = 0;
    uint32_t width = 0;
    uint32_t database_counter = 0;
    uint32_t table_counter = 0;
    uint32_t function_counter = 0;
    uint32_t current_level = 0;
    uint32_t backup_counter = 0;
    uint32_t cache_counter = 0;
    uint32_t aliases_counter = 0;
    uint32_t id_counter = 0;

    std::unordered_map<uint32_t, std::shared_ptr<SQLDatabase>> staged_databases;
    std::unordered_map<uint32_t, std::shared_ptr<SQLDatabase>> databases;
    std::unordered_map<uint32_t, SQLTable> staged_tables;
    std::unordered_map<uint32_t, SQLTable> tables;
    std::unordered_map<uint32_t, SQLView> staged_views;
    std::unordered_map<uint32_t, SQLView> views;
    std::unordered_map<uint32_t, SQLDictionary> staged_dictionaries;
    std::unordered_map<uint32_t, SQLDictionary> dictionaries;
    std::unordered_map<uint32_t, SQLFunction> staged_functions;
    std::unordered_map<uint32_t, SQLFunction> functions;
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
    std::vector<ColumnPathChain> entries;
    std::vector<ColumnPathChain> table_entries;
    std::vector<ColumnPathChain> remote_entries;
    std::vector<std::reference_wrapper<const ColumnPathChain>> filtered_entries;
    std::vector<std::reference_wrapper<SQLTable>> filtered_tables;
    std::vector<std::reference_wrapper<SQLView>> filtered_views;
    std::vector<std::reference_wrapper<SQLDictionary>> filtered_dictionaries;
    std::vector<std::reference_wrapper<std::shared_ptr<SQLDatabase>>> filtered_databases;
    std::vector<std::reference_wrapper<SQLFunction>> filtered_functions;
    std::vector<std::reference_wrapper<const SQLRelation>> filtered_relations;

    std::unordered_map<uint32_t, std::unordered_map<String, SQLRelation>> ctes;
    std::unordered_map<uint32_t, QueryLevel> levels;

    void setAllowNotDetermistic(const bool value) { allow_not_deterministic = value; }
    void enforceFinal(const bool value) { enforce_final = value; }
    void generatingPeerQuery(const PeerQuery value) { peer_query = value; }
    void setAllowEngineUDF(const bool value) { allow_engine_udf = value; }
    void resetAliasCounter() { aliases_counter = 0; }

    enum class SQLOp
    {
        CreateTable = 0,
        CreateView,
        Drop,
        Insert,
        LightDelete,
        Truncate,
        OptimizeTable,
        CheckTable,
        DescTable,
        Exchange,
        Alter,
        SetValues,
        Attach,
        Detach,
        CreateDatabase,
        CreateFunction,
        SystemStmt,
        BackupOrRestore,
        CreateDictionary,
        Rename,
        LightUpdate,
        SelectQuery,
        Kill,
        ShowStatement
    };

    enum class LitOp
    {
        LitHugeInt = 0,
        LitUHugeInt,
        LitInt,
        LitUInt,
        LitTime,
        LitDate,
        LitDateTime,
        LitDecimal,
        LitRandStr,
        LitUUID,
        LitIPv4,
        LitIPv6,
        LitGeo,
        LitStr,
        LitSpecial,
        LitJSON,
        LitNULLVal,
        LitFraction
    };

    enum class ExpOp
    {
        Literal = 0,
        ColumnRef,
        Predicate,
        CastExpr,
        UnaryExpr,
        IntervalExpr,
        ColumnsExpr,
        CondExpr,
        CaseExpr,
        SubqueryExpr,
        BinaryExpr,
        ArrayTupleExpr,
        FuncExpr,
        WindowFuncExpr,
        TableStarExpr,
        LambdaExpr,
        ProjectionExpr,
        DictExpr,
        JoinExpr,
        StarExpr
    };

    enum class PredOp
    {
        UnaryExpr = 0,
        BinaryExpr,
        BetweenExpr,
        InExpr,
        AnyExpr,
        IsNullExpr,
        ExistsExpr,
        LikeExpr,
        SearchExpr,
        OtherExpr
    };

    enum class QueryOp
    {
        DerivatedTable = 0,
        CTE,
        Table,
        View,
        RemoteUDF,
        NumbersUDF,
        SystemTable,
        MergeUDF,
        ClusterUDF,
        MergeIndexUDF,
        LoopUDF,
        ValuesUDF,
        RandomDataUDF,
        Dictionary,
        URLEncodedTable,
        TableEngineUDF,
        MergeProjectionUDF,
        RandomTableUDF,
        MergeIndexAnalyzeUDF
    };

    ProbabilityGenerator SQLGen;
    ProbabilityGenerator litGen;
    ProbabilityGenerator expGen;
    ProbabilityGenerator predGen;
    ProbabilityGenerator queryGen;
    std::vector<bool> SQLMask;
    std::vector<bool> litMask;
    std::vector<bool> expMask;
    std::vector<bool> predMask;
    std::vector<bool> queryMask;

    template <typename T>
    String setMergeTableParameter(RandomGenerator & rg, const String & initial);

    template <typename T>
    std::unordered_map<uint32_t, T> & getNextCollection()
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
    bool collectionHas(const std::function<bool(const T &)> func)
    {
        auto & input = getNextCollection<T>();

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
    uint32_t collectionCount(const std::function<bool(const T &)> func)
    {
        uint32_t res = 0;
        auto & input = getNextCollection<T>();

        for (const auto & entry : input)
        {
            res += func(entry.second) ? 1 : 0;
        }
        return res;
    }

    template <typename T>
    std::vector<std::reference_wrapper<T>> & getNextCollectionResult()
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
    std::vector<std::reference_wrapper<T>> & filterCollection(std::function<bool(T &)> func)
    {
        auto & input = getNextCollection<T>();
        auto & res = getNextCollectionResult<T>();

        res.clear();
        for (auto & entry : input)
        {
            if (func(entry.second))
            {
                res.emplace_back(std::ref<T>(entry.second));
            }
        }
        return res;
    }

private:
    String getNextAlias(RandomGenerator & rg);
    uint32_t getIdentifierFromString(const String & tname) const;
    void columnPathRef(const ColumnPathChain & entry, Expr * expr) const;
    void columnPathRef(const ColumnPathChain & entry, ColumnPath * cp) const;
    void entryOrConstant(RandomGenerator & rg, const ColumnPathChain & entry, Expr * expr);
    void colRefOrExpression(RandomGenerator & rg, const SQLRelation & rel, const SQLBase & b, const ColumnPathChain & entry, Expr * expr);
    String nextComment(RandomGenerator & rg) const;
    SQLRelation createTableRelation(RandomGenerator & rg, bool allow_internal_cols, const String & rel_name, const SQLTable & t);
    void addTableRelation(RandomGenerator & rg, bool allow_internal_cols, const String & rel_name, const SQLTable & t);
    SQLRelation createViewRelation(const String & rel_name, const SQLView & v);
    void addViewRelation(const String & rel_name, const SQLView & v);
    void addDictionaryRelation(const String & rel_name, const SQLDictionary & d);
    String strAppendAnyValue(RandomGenerator & rg, bool allow_cast, SQLType * tp);
    void flatTableColumnPath(
        uint32_t flags, const std::unordered_map<uint32_t, SQLColumn> & cols, std::function<bool(const SQLColumn & c)> col_filter);
    void flatColumnPath(uint32_t flags, const std::unordered_map<uint32_t, std::unique_ptr<SQLType>> & centries);
    void addRandomRelation(RandomGenerator & rg, std::optional<String> rel_name, uint32_t ncols, Expr * expr);
    void generateStorage(RandomGenerator & rg, Storage * store) const;
    void generateNextCodecs(RandomGenerator & rg, CodecList * cl);
    void generateTableExpression(RandomGenerator & rg, std::optional<SQLRelation> & rel, bool use_global_agg, bool pred, Expr * expr);
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
        SQLColumn & col,
        ColumnDef * cd);
    void addTableColumn(
        RandomGenerator & rg, SQLTable & t, uint32_t cname, bool staged, bool modify, bool is_pk, ColumnSpecial special, ColumnDef * cd);
    void addTableIndex(RandomGenerator & rg, SQLTable & t, bool staged, bool projection, IndexDef * idef);
    void addTableProjection(RandomGenerator & rg, SQLTable & t, bool staged, ProjectionDef * pdef);
    void addTableConstraint(RandomGenerator & rg, SQLTable & t, bool staged, ConstraintDef * cdef);
    void generateTableKey(RandomGenerator & rg, const SQLRelation & rel, const SQLBase & b, bool allow_asc_desc, TableKey * tkey);
    void setClusterClause(RandomGenerator & rg, const std::optional<String> & cluster, Cluster * clu, bool force = false) const;
    void setClusterInfo(RandomGenerator & rg, SQLBase & b) const;
    template <typename T>
    void randomEngineParams(RandomGenerator & rg, std::optional<SQLRelation> & rel, T * te);
    void generateMergeTreeEngineDetails(RandomGenerator & rg, const SQLRelation & rel, SQLBase & b, bool add_pkey, TableEngine * te);
    void generateEngineDetails(RandomGenerator & rg, const SQLRelation & rel, SQLBase & b, bool add_pkey, TableEngine * te);

    DatabaseEngineValues getNextDatabaseEngine(RandomGenerator & rg, const SQLDatabase & d);
    void generateDatabaseEngineDetails(RandomGenerator & rg, SQLDatabase & d);
    void getNextTableEngine(RandomGenerator & rg, bool use_external_integrations, SQLBase & b);
    void setRandomShardKey(RandomGenerator & rg, const std::optional<SQLTable> & t, Expr * expr);
    void getNextPeerTableDatabase(RandomGenerator & rg, SQLBase & b);

    void generateNextRefreshableView(RandomGenerator & rg, RefreshableView * rv);
    void generateNextCreateView(RandomGenerator & rg, CreateView * cv);
    void generateNextCreateDictionary(RandomGenerator & rg, CreateDictionary * cd);
    void generateNextDrop(RandomGenerator & rg, Drop * dp);
    void generateInsertToTable(RandomGenerator & rg, const SQLTable & t, bool in_parallel, std::optional<uint64_t> rows, Insert * ins);
    void generateNextInsert(RandomGenerator & rg, bool in_parallel, Insert * ins);
    void generateNextUpdate(RandomGenerator & rg, const SQLTable & t, Update * upt);
    void generateNextDelete(RandomGenerator & rg, const SQLTable & t, Delete * del);
    template <typename T>
    void generateNextUpdateOrDeleteOnTable(RandomGenerator & rg, const SQLTable & t, T * st);
    template <typename T>
    void generateNextUpdateOrDelete(RandomGenerator & rg, T * st);
    void generateNextTruncate(RandomGenerator & rg, Truncate * trunc);
    void generateNextOptimizeTableInternal(RandomGenerator & rg, const SQLTable & t, bool strict, OptimizeTable * ot);
    void generateNextOptimizeTable(RandomGenerator & rg, OptimizeTable * ot);
    void generateNextCheckTable(RandomGenerator & rg, CheckTable * ct);
    bool tableOrFunctionRef(RandomGenerator & rg, const SQLTable & t, bool allow_remote_cluster, TableOrFunction * tof);
    void generateNextDescTable(RandomGenerator & rg, DescribeStatement * dt);
    void generateNextRename(RandomGenerator & rg, Rename * ren);
    void generateNextExchange(RandomGenerator & rg, Exchange * exc);
    void generateNextKill(RandomGenerator & rg, Kill * kil);
    void generateUptDelWhere(RandomGenerator & rg, const SQLTable & t, Expr * expr);
    std::optional<String>
    alterSingleTable(RandomGenerator & rg, SQLTable & t, uint32_t nalters, bool no_oracle, bool can_update, bool in_parallel, Alter * at);
    void generateAlter(RandomGenerator & rg, bool in_parallel, Alter * at);
    void generateHotTableSettingsValues(RandomGenerator & rg, bool create, SettingValues * vals);
    void generateSettingValues(RandomGenerator & rg, const std::unordered_map<String, CHSetting> & settings, SettingValues * vals);
    void generateSettingValues(
        RandomGenerator & rg, const std::unordered_map<String, CHSetting> & settings, size_t nvalues, SettingValues * vals);
    void generateHotTableSettingList(RandomGenerator & rg, SettingList * sl);
    void generateSettingList(RandomGenerator & rg, const std::unordered_map<String, CHSetting> & settings, SettingList * sl);
    void generateAttach(RandomGenerator & rg, Attach * att);
    void generateDetach(RandomGenerator & rg, Detach * det);
    void generateNextCreateFunction(RandomGenerator & rg, CreateFunction * cf);
    void generateNextSystemStatement(RandomGenerator & rg, bool allow_table_statements, SystemCommand * sc);
    void generateNextShowStatement(RandomGenerator & rg, ShowStatement * st);

    void generateLikeExpr(RandomGenerator & rg, Expr * expr);
    Expr * generatePartialSearchExpr(RandomGenerator & rg, Expr * expr) const;
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
    void generateLimitBy(RandomGenerator & rg, LimitByStatement * ls);
    void generateLimit(RandomGenerator & rg, bool has_order_by, LimitStatement * lim);
    void generateOffset(RandomGenerator & rg, bool has_order_by, OffsetStatement * off);
    void generateGroupByExpr(
        RandomGenerator & rg,
        bool enforce_having,
        uint32_t offset,
        uint32_t ncols,
        const std::vector<SQLRelationCol> & available_cols,
        std::vector<GroupCol> & gcols,
        Expr * expr);
    bool generateGroupBy(RandomGenerator & rg, uint32_t ncols, bool enforce_having, bool allow_settings, SelectStatementCore * ssc);
    void addWhereSide(RandomGenerator & rg, const std::vector<GroupCol> & available_cols, Expr * expr);
    void addWhereFilter(RandomGenerator & rg, const std::vector<GroupCol> & available_cols, Expr * expr);
    void generateWherePredicate(RandomGenerator & rg, Expr * expr);
    void addJoinClause(RandomGenerator & rg, Expr * expr);
    void generateArrayJoin(RandomGenerator & rg, ArrayJoin * aj);
    String getTableStructure(RandomGenerator & rg, const SQLTable & t, bool escape);
    void setTableFunction(RandomGenerator & rg, TableFunctionUsage usage, const SQLTable & t, TableFunction * tfunc);
    String getNextRandomServerAddresses(RandomGenerator & rg, bool secure);
    String getNextHTTPURL(RandomGenerator & rg, bool secure);
    bool joinedTableOrFunction(
        RandomGenerator & rg, const String & rel_name, uint32_t allowed_clauses, bool under_remote, TableOrFunction * tof);
    void generateFromElement(RandomGenerator & rg, uint32_t allowed_clauses, TableOrSubquery * tos);
    void generateJoinConstraint(RandomGenerator & rg, bool allow_using, JoinConstraint * jc);
    void generateDerivedTable(
        RandomGenerator & rg,
        SQLRelation & rel,
        uint32_t allowed_clauses,
        uint32_t ncols,
        bool backup,
        std::optional<String> recursive,
        Select * sel);
    /* Returns the number of from elements generated */
    uint32_t generateFromStatement(RandomGenerator & rg, uint32_t allowed_clauses, FromStatement * ft);
    void addCTEs(RandomGenerator & rg, uint32_t allowed_clauses, CTEs * qctes);
    void addWindowDefs(RandomGenerator & rg, SelectStatementCore * ssc);
    void generateSelect(
        RandomGenerator & rg,
        bool top,
        bool force_global_agg,
        uint32_t ncols,
        uint32_t allowed_clauses,
        std::optional<String> recursive,
        Select * sel);

    void generateTopSelect(RandomGenerator & rg, bool force_global_agg, uint32_t allowed_clauses, TopSelect * ts);
    void generateNextExplain(RandomGenerator & rg, bool in_parallel, ExplainQuery * eq);
    void generateNextQuery(RandomGenerator & rg, bool in_parallel, SQLQueryInner * sq);

    std::tuple<SQLType *, Integers> randomIntType(RandomGenerator & rg, uint64_t allowed_types);
    std::tuple<SQLType *, FloatingPoints> randomFloatType(RandomGenerator & rg, uint64_t allowed_types);
    std::tuple<SQLType *, Dates> randomDateType(RandomGenerator & rg, uint64_t allowed_types) const;
    SQLType * randomTimeType(RandomGenerator & rg, uint64_t allowed_types, TimeTp * dt) const;
    SQLType * randomDateTimeType(RandomGenerator & rg, uint64_t allowed_types, DateTimeTp * dt) const;
    SQLType * randomDecimalType(RandomGenerator & rg, uint64_t allowed_types, BottomTypeName * tp) const;
    SQLType * bottomType(RandomGenerator & rg, uint64_t allowed_types, bool low_card, BottomTypeName * tp);

    void dropTable(bool staged, bool drop_peer, uint32_t tname);
    void dropDatabase(uint32_t dname, bool all);

    void generateNextTablePartition(
        RandomGenerator & rg, bool allow_parts, bool detached, bool supports_all, const SQLTable & t, PartitionExpr * pexpr);

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

    template <typename T, typename U>
    void setObjectStoreParams(RandomGenerator & rg, const T & b, U * source)
    {
        uint32_t added_path = 0;
        uint32_t added_format = 0;
        uint32_t added_compression = 0;
        uint32_t added_partition_strategy = 0;
        uint32_t added_partition_columns_in_data_file = 0;
        uint32_t added_structure = 0;
        uint32_t added_storage_class_name = 0;
        const uint32_t toadd_path = 1;
        const uint32_t toadd_format = (b.file_format.has_value() && !this->allow_not_deterministic)
            || (this->allow_not_deterministic && rg.nextMediumNumber() < 91);
        const uint32_t toadd_compression
            = (b.file_comp.has_value() && !this->allow_not_deterministic) || (this->allow_not_deterministic && rg.nextMediumNumber() < 51);
        const uint32_t toadd_partition_strategy = (b.partition_strategy.has_value() && !this->allow_not_deterministic)
            || ((b.isS3Engine() || b.isAzureEngine()) && this->allow_not_deterministic && rg.nextMediumNumber() < 21);
        const uint32_t toadd_partition_columns_in_data_file
            = (b.partition_columns_in_data_file.has_value() && !this->allow_not_deterministic)
            || ((b.isS3Engine() || b.isAzureEngine()) && this->allow_not_deterministic && rg.nextMediumNumber() < 21);
        const uint32_t toadd_storage_class_name = (b.storage_class_name.has_value() && !this->allow_not_deterministic)
            || (b.isS3Engine() && this->allow_not_deterministic && rg.nextMediumNumber() < 21);
        const uint32_t toadd_structure = !std::is_same_v<U, TableEngine> && (!this->allow_not_deterministic || rg.nextMediumNumber() < 91);
        const uint32_t total_to_add = toadd_path + toadd_format + toadd_compression + toadd_partition_strategy
            + toadd_partition_columns_in_data_file + toadd_storage_class_name + toadd_structure;

        for (uint32_t i = 0; i < total_to_add; i++)
        {
            KeyValuePair * next = nullptr;
            const uint32_t add_path = 4 * static_cast<uint32_t>(added_path < toadd_path);
            const uint32_t add_format = 4 * static_cast<uint32_t>(added_format < toadd_format);
            const uint32_t add_compression = 4 * static_cast<uint32_t>(added_compression < toadd_compression);
            const uint32_t add_partition_strategy = 2 * static_cast<uint32_t>(added_partition_strategy < toadd_partition_strategy);
            const uint32_t add_partition_columns_in_data_file
                = 2 * static_cast<uint32_t>(added_partition_columns_in_data_file < toadd_partition_columns_in_data_file);
            const uint32_t add_storage_class_name = 2 * static_cast<uint32_t>(added_storage_class_name < toadd_storage_class_name);
            const uint32_t add_structure = 4 * static_cast<uint32_t>(added_structure < toadd_structure);
            const uint32_t prob_space = add_path + add_format + add_compression + add_partition_strategy
                + add_partition_columns_in_data_file + add_storage_class_name + add_structure;
            std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
            const uint32_t nopt = next_dist(rg.generator);

            if constexpr (std::is_same_v<U, TableEngine>)
            {
                next = source->add_params()->mutable_kvalue();
            }
            else
            {
                next = source->add_params();
            }
            if (add_path && nopt < (add_path + 1))
            {
                /// Path to the bucket
                next->set_key(
                    b.isOnS3() ? (b.getLakeCatalog() == LakeCatalog::None ? "filename" : "url") : (b.isOnAzure() ? "blob_path" : "path"));
                next->set_value(b.getTablePath(rg, fc, this->allow_not_deterministic));
                added_path++;
            }
            else if (add_format && nopt < (add_path + add_format + 1))
            {
                /// Format
                const InOutFormat next_format
                    = (b.file_format.has_value() && (!this->allow_not_deterministic || rg.nextMediumNumber() < 81))
                    ? b.file_format.value()
                    : rg.pickRandomly(rg.pickRandomly(inOutFormats));

                next->set_key("format");
                next->set_value(InOutFormat_Name(next_format).substr(6));
                added_format++;
            }
            else if (add_compression && nopt < (add_path + add_format + add_compression + 1))
            {
                /// Compression
                const String next_compression = (b.file_comp.has_value() && (!this->allow_not_deterministic || rg.nextMediumNumber() < 81))
                    ? b.file_comp.value()
                    : rg.pickRandomly(compressionMethods);

                next->set_key("compression");
                next->set_value(next_compression);
                added_compression++;
            }
            else if (add_partition_strategy && nopt < (add_path + add_format + add_compression + add_partition_strategy + 1))
            {
                /// Partition strategy
                const String next_ps = (b.partition_strategy.has_value() && (!this->allow_not_deterministic || rg.nextMediumNumber() < 81))
                    ? b.partition_strategy.value()
                    : (rg.nextBool() ? "wildcard" : "hive");

                next->set_key("partition_strategy");
                next->set_value(next_ps);
                added_partition_strategy++;
            }
            else if (
                add_partition_columns_in_data_file
                && nopt < (add_path + add_format + add_compression + add_partition_strategy + add_partition_columns_in_data_file + 1))
            {
                /// Partition columns in data file
                const String next_pcdf
                    = (b.partition_columns_in_data_file.has_value() && (!this->allow_not_deterministic || rg.nextMediumNumber() < 81))
                    ? b.partition_columns_in_data_file.value()
                    : (rg.nextBool() ? "1" : "0");

                next->set_key("partition_columns_in_data_file");
                next->set_value(next_pcdf);
                added_partition_columns_in_data_file++;
            }
            else if (
                add_storage_class_name
                && nopt
                    < (add_path + add_format + add_compression + add_partition_strategy + add_partition_columns_in_data_file
                       + add_storage_class_name + 1))
            {
                /// Storage class name in S3
                const String next_scn = (b.storage_class_name.has_value() && (!this->allow_not_deterministic || rg.nextMediumNumber() < 81))
                    ? b.storage_class_name.value()
                    : (rg.nextBool() ? "STANDARD" : "INTELLIGENT_TIERING");

                next->set_key("storage_class_name");
                next->set_value(next_scn);
                added_storage_class_name++;
            }
            else if (
                add_structure
                && nopt
                    < (add_path + add_format + add_compression + add_partition_strategy + add_partition_columns_in_data_file
                       + add_storage_class_name + add_structure + 1))
            {
                /// Structure for table function
                next->set_key("structure");
                if constexpr (std::is_same_v<U, TableEngine>)
                {
                    UNREACHABLE();
                }
                else
                {
                    next->set_value(getTableStructure(rg, b, true));
                }
                added_structure++;
            }
            else
            {
                UNREACHABLE();
            }
        }
    }

public:
    SQLType * randomNextType(RandomGenerator & rg, uint64_t allowed_types, uint32_t & col_counter, TopTypeName * tp);

    const std::function<bool(const std::shared_ptr<SQLDatabase> &)> attached_databases
        = [](const std::shared_ptr<SQLDatabase> & d) { return d->isAttached(); };
    const std::function<bool(const SQLTable &)> attached_tables = [](const SQLTable & t) { return t.isAttached(); };
    const std::function<bool(const SQLView &)> attached_views = [](const SQLView & v) { return v.isAttached(); };
    const std::function<bool(const SQLDictionary &)> attached_dictionaries = [](const SQLDictionary & d) { return d.isAttached(); };
    const std::function<bool(const SQLTable &)> has_mergeable_tables
        = [](const SQLTable & t) { return t.isAttached() && t.isMergeTreeFamily() && t.can_run_merges; };

    const std::function<bool(const SQLTable &)> attached_tables_to_test_format
        = [](const SQLTable & t) { return t.isAttached() && t.teng != TableEngineValues::GenerateRandom; };
    const std::function<bool(const SQLTable &)> attached_tables_to_compare_content = [](const SQLTable & t)
    {
        return t.isAttached() && !t.isNotTruncableEngine() && t.teng != TableEngineValues::CollapsingMergeTree
            && t.teng != TableEngineValues::VersionedCollapsingMergeTree && t.is_deterministic;
    };
    const std::function<bool(const SQLTable &)> attached_tables_for_table_peer_oracle
        = [](const SQLTable & t) { return t.isAttached() && !t.isNotTruncableEngine() && t.is_deterministic; };
    const std::function<bool(const SQLTable &)> attached_tables_for_clickhouse_table_peer_oracle
        = [](const SQLTable & t) { return t.isAttached() && !t.isNotTruncableEngine() && t.hasClickHousePeer(); };
    const std::function<bool(const SQLTable &)> attached_tables_for_external_call
        = [](const SQLTable & t) { return t.isAttached() && t.integration == IntegrationCall::Dolor; };

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

    template <TableRequirement req>
    auto getQueryTableLambda();

    StatementGenerator(RandomGenerator & rg, FuzzConfig & fuzzc, ExternalIntegrations & conn, bool supports_cloud_features_);

    void setBackupDestination(RandomGenerator & rg, BackupRestore * br);
    std::optional<String> backupOrRestoreObject(BackupRestoreObject * bro, SQLObject obj, const SQLBase & b);

    void generateNextCreateTable(RandomGenerator & rg, bool in_parallel, CreateTable * ct);
    void generateNextCreateDatabase(RandomGenerator & rg, CreateDatabase * cd);
    void generateNextStatement(RandomGenerator & rg, SQLQuery & sq);

    void updateGenerator(const SQLQuery & sq, ExternalIntegrations & ei, bool success);
    void setInTransaction(const bool value) { in_transaction = value; }
    bool getAllowNotDetermistic() const { return allow_not_deterministic; }

    friend class QueryOracle;
};

}
