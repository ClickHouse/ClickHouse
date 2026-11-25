#include <Client/BuzzHouse/Generator/StatementGenerator.h>
#include <Common/checkStackSize.h>
#include <fmt/ranges.h>

/// Rules that steer a query to where its distributed plan is built differently from the local one. The replicas get
/// the query tree rewritten and shipped as text, and the initiator then pairs objects of the two plans: sets by the
/// hash of their subquery, reads by table names. Each rule plants a shape that such pairing got wrong before.

namespace BuzzHouse
{

/// Writes `and(<old WHERE>, conjunct)` into the WHERE of `core`, or only `conjunct` when there is no WHERE
static void addConjunctToWhere(SelectStatementCore & core, Expr & conjunct)
{
    const bool had_where = core.has_where();
    Expr * where = core.mutable_where()->mutable_expr()->mutable_expr();

    if (!had_where)
    {
        where->Swap(&conjunct);
        return;
    }
    Expr old_where;

    old_where.Swap(where);
    SQLFuncCall * fcall = where->mutable_comp_expr()->mutable_func_call();

    fcall->mutable_func()->set_catalog_func("and");
    fcall->add_args()->mutable_expr()->Swap(&old_where);
    fcall->add_args()->mutable_expr()->Swap(&conjunct);
}

/// Finds an `IN (subquery)` in `message` without descending into nested queries, which have their own WHERE
static const ExprIn * findInSubquery(const google::protobuf::Message & message)
{
    const google::protobuf::Descriptor * desc = message.GetDescriptor();

    if (desc == ExprIn::descriptor())
    {
        const ExprIn & ein = static_cast<const ExprIn &>(message);

        return ein.in_type().has_sel() ? &ein : nullptr;
    }
    if (desc == Select::descriptor())
    {
        return nullptr;
    }
    const google::protobuf::Reflection * refl = message.GetReflection();

    for (int i = 0; i < desc->field_count(); i++)
    {
        const google::protobuf::FieldDescriptor * field = desc->field(i);

        if (field->cpp_type() != google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE)
        {
            continue;
        }
        if (field->is_repeated())
        {
            for (int j = 0; j < refl->FieldSize(message, field); j++)
            {
                if (const ExprIn * res = findInSubquery(refl->GetRepeatedMessage(message, field, j)))
                {
                    return res;
                }
            }
        }
        else if (refl->HasField(message, field))
        {
            if (const ExprIn * res = findInSubquery(refl->GetMessage(message, field)))
            {
                return res;
            }
        }
    }
    return nullptr;
}

void StatementGenerator::injectDistributedShapes(RandomGenerator & rg, google::protobuf::Message & message, const bool in_subquery)
{
    const google::protobuf::Descriptor * desc = message.GetDescriptor();
    const google::protobuf::Reflection * refl = message.GetReflection();
    const bool is_select_core = desc == SelectStatementCore::descriptor();

    checkStackSize();
    /// Nested queries first: a subquery copied below must be final, or its copies diverge and are no longer one set
    for (int i = 0; i < desc->field_count(); i++)
    {
        const google::protobuf::FieldDescriptor * field = desc->field(i);

        if (field->cpp_type() != google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE)
        {
            continue;
        }
        if (field->is_repeated())
        {
            for (int j = 0; j < refl->FieldSize(message, field); j++)
            {
                injectDistributedShapes(rg, *refl->MutableRepeatedMessage(&message, field, j), in_subquery || is_select_core);
            }
        }
        else if (refl->HasField(message, field))
        {
            injectDistributedShapes(rg, *refl->MutableMessage(&message, field), in_subquery || is_select_core);
        }
    }
    if (!is_select_core)
    {
        return;
    }
    SelectStatementCore & core = static_cast<SelectStatementCore &>(message);

    /// The same subquery twice in one query is one set, built once and looked up by every use
    if (core.has_where() && rg.nextBool())
    {
        if (const ExprIn * ein = findInSubquery(core.where()))
        {
            Expr copy;

            copy.mutable_comp_expr()->mutable_expr_in()->CopyFrom(*ein);
            addConjunctToWhere(core, copy);
        }
    }
    /// A constant longer than `optimize_const_name_size` is shipped to the replicas as a `__getScalar` call,
    /// which changes the hash of the subquery around it. `materialize` keeps the filter from folding away
    if (rg.nextMediumNumber() < (in_subquery ? 61 : 21))
    {
        static const std::vector<uint32_t> lengths = {1, 31, 32, 33, 255, 256, 257, 300, 1000};
        Expr conjunct;

        conjunct.mutable_lit_val()->set_no_quote_str(fmt::format("materialize(repeat('x', {})) != ''", rg.pickRandomly(lengths)));
        addConjunctToWhere(core, conjunct);
    }
}

std::vector<std::pair<String, String>> StatementGenerator::parallelReplicasSettings(RandomGenerator & rg)
{
    std::vector<std::pair<String, String>> res;
    const auto add = [&](const String & name, const String & value) { res.emplace_back(name, value); };
    /// The automatic mode needs the local plan, and decides on statistics from an earlier execution of the query
    const bool automatic = rg.nextBool();

    add("enable_parallel_replicas", automatic || rg.nextSmallNumber() < 8 ? "1" : "2");
    add("cluster_for_parallel_replicas", "'" + rg.pickRandomly(fc.parallel_replicas_clusters) + "'");
    add("max_parallel_replicas", std::to_string(rg.randomInt<uint32_t>(2, 3)));
    add("parallel_replicas_for_non_replicated_merge_tree", "1");
    add("parallel_replicas_local_plan", automatic || rg.nextSmallNumber() < 8 ? "1" : "0");
    add("parallel_replicas_min_number_of_rows_per_replica", "0");
    /// Settings the session may have randomized, each of which turns off what this query targets
    add("parallel_replicas_mode", "'read_tasks'");
    add("make_distributed_plan", "0");
    add("parallel_replicas_for_queries_with_multiple_tables", "1");
    /// Parallel replicas do not read with `FINAL`, and turn themselves off for it
    add("final", "0");
    add("parallel_replicas_allow_in_with_subquery", "1");
    /// Set either way: an inherited mode 2 only collects statistics, which keeps a query off the replicas
    add("automatic_parallel_replicas_mode", automatic ? "1" : "0");
    if (automatic)
    {
        /// Let the cost model switch on the small tables the fuzzer creates
        add("automatic_parallel_replicas_min_bytes_per_replica", "0");
        add("automatic_parallel_replicas_ignore_thresholds", "1");
        this->repeat_statement = true;
    }
    if (rg.nextSmallNumber() < 4)
    {
        add("optimize_const_name_size", std::to_string(rg.randomInt<int32_t>(0, 64)));
    }
    if (rg.nextSmallNumber() < 4)
    {
        add("query_plan_convert_join_to_in", "1");
    }
    /// Settings that change how the replicas plan, read and send their part of the query
    static const std::vector<std::pair<String, DB::Strings>> knobs = {
        {"parallel_replicas_plan_based", {"0", "1"}},
        {"parallel_replicas_filter_pushdown", {"0", "1"}},
        {"parallel_replicas_index_analysis_only_on_coordinator", {"0", "1"}},
        {"parallel_replicas_prefer_local_join", {"0", "1"}},
        {"parallel_replicas_support_projection", {"0", "1"}},
        {"parallel_replicas_allow_merge_tables", {"0", "1"}},
        {"parallel_replicas_allow_view_over_mergetree", {"0", "1"}},
        {"parallel_replicas_mark_segment_size", {"0", "1", "2", "16", "128", "16384"}},
        {"max_threads", {"1", "2", "3", "4", "16"}},
        {"max_block_size", {"1", "7", "64", "1024", "65409"}},
        {"merge_tree_min_rows_for_concurrent_read", {"1", "100", "20000"}},
        {"group_by_two_level_threshold", {"1", "100", "100000"}},
        {"group_by_two_level_threshold_bytes", {"1", "100000000"}},
        {"distributed_aggregation_memory_efficient", {"0", "1"}},
        {"max_bytes_before_external_group_by", {"0", "1", "1000000"}},
        {"max_bytes_before_external_sort", {"0", "1", "1000000"}},
        {"join_algorithm", {"'hash'", "'parallel_hash'", "'grace_hash'", "'full_sorting_merge'", "'partial_merge'", "'auto'"}},
        {"query_plan_join_swap_table", {"'auto'", "'true'", "'false'"}},
        {"optimize_read_in_order", {"0", "1"}},
        {"optimize_aggregation_in_order", {"0", "1"}},
        {"enable_join_runtime_filters", {"0", "1"}},
        {"use_query_condition_cache", {"0", "1"}},
        {"query_plan_optimize_lazy_materialization", {"0", "1"}},
        {"optimize_use_projections", {"0", "1"}},
        {"use_skip_indexes", {"0", "1"}},
        {"distributed_product_mode", {"'deny'", "'local'", "'global'", "'allow'"}},
        {"prefer_global_in_and_join", {"0", "1"}},
        {"transform_null_in", {"0", "1"}},
        {"join_use_nulls", {"0", "1"}},
        {"automatic_parallel_replicas_max_duplicated_read_ratio", {"0", "0.5", "1", "10"}},
        {"automatic_parallel_replicas_statistics_drift_ratio", {"0", "0.1", "10"}},
        {"automatic_parallel_replicas_remember_boundary", {"0", "1"}},
        {"automatic_parallel_replicas_undecided_retry_after_executions", {"0", "1", "5"}},
    };
    for (const auto & [name, values] : knobs)
    {
        if (rg.nextMediumNumber() < 16)
        {
            add(name, rg.pickRandomly(values));
        }
    }
    /// Without the local replica the automatic mode has no local branch to match with the single-node plan
    if (!automatic && rg.nextMediumNumber() < 16)
    {
        add("parallel_replicas_prefer_local_replica", rg.nextBool() ? "0" : "1");
    }
    return res;
}

void StatementGenerator::addParallelReplicasSettings(RandomGenerator & rg, SettingValues * svs)
{
    for (const auto & [name, value] : parallelReplicasSettings(rg))
    {
        SetValue * setv = svs->has_set_value() ? svs->add_other_values() : svs->mutable_set_value();

        setv->set_property(name);
        setv->set_value(value);
    }
}

/// Columns any query can read and hash, skipping nested and aggregate function states
static DB::Strings shapeColumns(const SQLTable & t)
{
    DB::Strings res;

    for (const auto & [_, col] : t.cols)
    {
        if (!col.tp)
        {
            continue;
        }
        switch (col.tp->getTypeClass())
        {
            case SQLTypeClass::BOOL:
            case SQLTypeClass::INT:
            case SQLTypeClass::FLOAT:
            case SQLTypeClass::DATE:
            case SQLTypeClass::TIME:
            case SQLTypeClass::DATETIME:
            case SQLTypeClass::DECIMAL:
            case SQLTypeClass::STRING:
            case SQLTypeClass::UUID:
            case SQLTypeClass::ENUM:
            case SQLTypeClass::IPV4:
            case SQLTypeClass::IPV6:
            case SQLTypeClass::NULLABLE:
            case SQLTypeClass::LOWCARDINALITY:
            case SQLTypeClass::ARRAY:
            case SQLTypeClass::TUPLE:
                res.emplace_back("`" + col.getColumnName() + "`");
                break;
            default:
                break;
        }
    }
    std::sort(res.begin(), res.end());
    return res;
}

namespace
{

struct ShapeTable
{
    String database;
    String table;
    DB::Strings cols;
    /// Known for the tables the shapes create themselves
    String sorting_key;

    [[nodiscard]] String name() const { return "`" + database + "`.`" + table + "`"; }
};

/// Builds the text of a shaped query. Every key is `cityHash64(<column>) % M`, so both sides of a comparison have one
/// type whatever the columns are, and nested queries are bounded by `max_depth`
class ShapeBuilder
{
public:
    ShapeBuilder(RandomGenerator & rg_, const std::vector<ShapeTable> & tables_, const DB::Strings & clusters_)
        : rg(rg_)
        , tables(tables_)
        , clusters(clusters_)
    {
    }

    String query()
    {
        const ShapeTable & t1 = table();
        /// A table read twice in one query: the reads must be told apart when the plans are paired
        const ShapeTable & t2 = rg.nextBool() ? t1 : table();
        const uint32_t m = modulus();
        bool virtuals = false;

        switch (rg.randomInt<uint32_t>(0, t1.sorting_key.empty() ? 15 : 17))
        {
            case 0: {
                /// Aggregation, finalized once on the initiator from the states of the replicas
                static const DB::Strings modifiers = {"", "", " WITH ROLLUP", " WITH CUBE", " WITH TOTALS"};
                const String from = source(t1, "", virtuals);
                String res = fmt::format(
                    "SELECT {} AS k, {}, {} FROM {}{} GROUP BY k{}",
                    key(column(t1, "", virtuals), m),
                    aggregate(t1, "", virtuals),
                    aggregate(t1, "", virtuals),
                    from,
                    where(t1, "", virtuals),
                    rg.pickRandomly(modifiers));
                if (rg.nextSmallNumber() < 4)
                {
                    res += fmt::format(" HAVING count() > {}", rg.randomInt<uint32_t>(0, 3));
                }
                return res + " ORDER BY k LIMIT 100";
            }
            case 1: {
                const String from = source(t1, "", virtuals);
                return fmt::format(
                    "SELECT {} FROM {} {} {}{}",
                    aggregate(t1, "", virtuals),
                    from,
                    rg.nextSmallNumber() < 4 && !virtuals ? "PREWHERE" : "WHERE",
                    predicate(t1, "", virtuals),
                    rg.nextSmallNumber() < 3 ? " AND " + predicate(t1, "", virtuals) : "");
            }
            case 2:
                return join(t1, t2, m);
            case 3: {
                const String inner = fmt::format("SELECT {} AS k FROM {}{}", key(column(t1, "", virtuals), m), source(t1, "", virtuals), where(t1, "", virtuals));
                return fmt::format("SELECT k, count() FROM ({}) GROUP BY k ORDER BY k", nest(inner));
            }
            case 4: {
                /// A scalar subquery is executed before the main query and shipped to the replicas as a constant
                bool v2 = false;
                const String from2 = source(t2, "", v2);
                const String scalar = fmt::format("SELECT min(cityHash64({})) FROM {}{}", column(t2, "", v2), from2, where(t2, "", v2));
                const String from1 = source(t1, "", virtuals);

                if (rg.nextBool())
                {
                    return fmt::format("WITH ({}) AS s SELECT count(), max(s) FROM {} WHERE cityHash64({}) > s", scalar, from1, column(t1, "", virtuals));
                }
                return fmt::format("SELECT count() FROM {} WHERE cityHash64({}) > ({})", from1, column(t1, "", virtuals), scalar);
            }
            case 5: {
                static const DB::Strings ops = {"UNION ALL", "UNION ALL", "UNION DISTINCT", "INTERSECT DISTINCT", "INTERSECT ALL", "EXCEPT DISTINCT", "EXCEPT ALL"};
                return fmt::format("SELECT k, count() FROM ({} {} {}) GROUP BY k ORDER BY k", keys(t1, m), rg.pickRandomly(ops), keys(t2, m));
            }
            case 6:
                /// One CTE read twice, by `IN` and by `JOIN`
                return fmt::format(
                    "WITH s AS ({}) SELECT count() FROM {} AS a INNER JOIN s ON {} = s.k WHERE {} IN (SELECT k FROM s)",
                    keys(t2, m),
                    t1.name(),
                    key(column(t1, "a", virtuals), m),
                    key(column(t1, "a", virtuals), m));
            case 7: {
                const String from = source(t1, "", virtuals);
                const String c1 = column(t1, "", virtuals);
                const String c2 = column(t1, "", virtuals);
                String res = fmt::format("SELECT {}, {} FROM {}{} ORDER BY ", c1, c2, from, where(t1, "", virtuals));

                if (rg.nextBool())
                {
                    return res + fmt::format("cityHash64({}) LIMIT {} BY {} LIMIT 100", c1, rg.randomInt<uint32_t>(1, 3), key(c2, m));
                }
                return res
                    + fmt::format(
                           "ALL LIMIT {}{}{}",
                           rg.randomInt<uint32_t>(0, 1000),
                           rg.nextSmallNumber() < 3 ? fmt::format(" OFFSET {}", rg.randomInt<uint32_t>(0, 100)) : "",
                           rg.nextSmallNumber() < 3 ? " WITH TIES" : "");
            }
            case 8: {
                /// Window functions run after the replicas send their rows
                static const DB::Strings windows = {"count()", "row_number()", "rank()", "sum(cityHash64(x))", "lagInFrame(cityHash64(x))"};
                const String from = source(t1, "", virtuals);
                return fmt::format(
                    "SELECT k, {} OVER (PARTITION BY k ORDER BY cityHash64(x)) AS w FROM (SELECT {} AS x, {} AS k FROM {}{}) ORDER BY k, w LIMIT 100",
                    rg.pickRandomly(windows),
                    column(t1, "", virtuals),
                    key(column(t1, "", virtuals), m),
                    from,
                    where(t1, "", virtuals));
            }
            case 9: {
                const String from = source(t1, "", virtuals);
                if (rg.nextBool())
                {
                    return fmt::format("SELECT DISTINCT {} AS k FROM {}{} ORDER BY k", key(column(t1, "", virtuals), m), from, where(t1, "", virtuals));
                }
                return fmt::format(
                    "SELECT count(DISTINCT {}), uniq({}), uniqExact({}) FROM {}{}",
                    key(column(t1, "", virtuals), m),
                    column(t1, "", virtuals),
                    column(t1, "", virtuals),
                    from,
                    where(t1, "", virtuals));
            }
            case 10: {
                const String from = source(t1, "", virtuals);
                const String k1 = key(column(t1, "", virtuals), m);
                const String k2 = key(column(t1, "", virtuals), modulus());
                return fmt::format(
                    "SELECT {} AS k1, {} AS k2, count() FROM {}{} GROUP BY GROUPING SETS ((k1), (k2), (k1, k2), ()) ORDER BY ALL",
                    k1,
                    k2,
                    from,
                    where(t1, "", virtuals));
            }
            case 11: {
                const String from = source(t1, "", virtuals);
                return fmt::format(
                    "SELECT a, count() FROM {} ARRAY JOIN range(ifNull({}, 0)) AS a{} GROUP BY a ORDER BY a",
                    from,
                    key(column(t1, "", virtuals), m),
                    where(t1, "", virtuals));
            }
            case 12: {
                /// Virtual columns describe the parts a replica reads, which differ from the parts of the initiator
                static const DB::Strings parts = {"_part", "_partition_id", "_part_index", "_part_uuid"};
                static const DB::Strings offsets = {"_part_offset", "_part_starting_offset", "_part_granule_offset", "_block_number"};
                return fmt::format(
                    "SELECT {}, count(), min({}), max({}) FROM {}{} GROUP BY ALL ORDER BY ALL",
                    rg.pickRandomly(parts),
                    rg.pickRandomly(offsets),
                    rg.pickRandomly(offsets),
                    t1.name(),
                    where(t1, "", true));
            }
            case 13:
                /// A correlated subquery
                return fmt::format(
                    "SELECT count() FROM {} AS a WHERE {}EXISTS (SELECT 1 FROM {} AS b WHERE {} = {})",
                    t1.name(),
                    rg.nextBool() ? "NOT " : "",
                    t2.name(),
                    key(column(t2, "b", virtuals), m),
                    key(column(t1, "a", virtuals), m));
            case 14: {
                /// Tuples of keys against a subquery of tuples
                const String from = source(t1, "", virtuals);
                const uint32_t m2 = modulus();
                bool v2 = false;
                const String from2 = source(t2, "", v2);
                return fmt::format(
                    "SELECT count() FROM {} WHERE ({}, {}) {}IN (SELECT {}, {} FROM {}{})",
                    from,
                    key(column(t1, "", virtuals), m),
                    key(column(t1, "", virtuals), m2),
                    rg.nextSmallNumber() < 3 ? "NOT " : "",
                    key(column(t2, "", v2), m),
                    key(column(t2, "", v2), m2),
                    from2,
                    where(t2, "", v2));
            }
            case 16:
            case 17: {
                /// Reading in the order of the sorting key, which the replicas announce to the coordinator
                if (t1.sorting_key.empty())
                {
                    return fmt::format("SELECT count() FROM {}", t1.name());
                }
                const String direction = rg.nextBool() ? " DESC" : "";
                if (rg.nextBool())
                {
                    return fmt::format(
                        "SELECT {0}, count() FROM {1}{2} GROUP BY {0} ORDER BY {0}{3} LIMIT {4}",
                        t1.sorting_key,
                        t1.name(),
                        where(t1, "", true),
                        direction,
                        rg.randomInt<uint32_t>(1, 1000));
                }
                return fmt::format(
                    "SELECT {0}, {1} FROM {2}{3} ORDER BY {0}{4} LIMIT {5}",
                    t1.sorting_key,
                    column(t1, "", true),
                    t1.name(),
                    where(t1, "", true),
                    direction,
                    rg.randomInt<uint32_t>(1, 1000));
            }
            default: {
                /// Combinators and states, merged from the states the replicas send
                static const DB::Strings aggregates
                    = {"countIf(cityHash64(x) % 2 = 0)",
                       "sumIf(cityHash64(x) % 100, cityHash64(x) % 3 = 0)",
                       "finalizeAggregation(uniqState(x))",
                       "length(groupArraySorted(5)(cityHash64(x)))",
                       "topK(3)(cityHash64(x) % 10)",
                       "argMax(cityHash64(x) % 100, cityHash64(x))",
                       "quantileExact(0.5)(cityHash64(x) % 1000)",
                       "groupBitXor(cityHash64(x))",
                       "uniqCombined(x)",
                       "anyLast(cityHash64(x) % 1)"};
                const String from = source(t1, "", virtuals);
                return fmt::format(
                    "SELECT k, {}, {} FROM (SELECT {} AS x, {} AS k FROM {}{}) GROUP BY k ORDER BY k",
                    rg.pickRandomly(aggregates),
                    rg.pickRandomly(aggregates),
                    column(t1, "", virtuals),
                    key(column(t1, "", virtuals), m),
                    from,
                    where(t1, "", virtuals));
            }
        }
    }

private:
    RandomGenerator & rg;
    const std::vector<ShapeTable> & tables;
    const DB::Strings & clusters;
    uint32_t depth = 0;
    static constexpr uint32_t max_depth = 3;

    const ShapeTable & table() { return rg.pickRandomly(tables); }

    uint32_t modulus()
    {
        static const std::vector<uint32_t> moduli = {1, 2, 3, 10, 100, 1000};
        return rg.pickRandomly(moduli);
    }

    String column(const ShapeTable & t, const String & alias, const bool virtuals)
    {
        static const DB::Strings virtual_columns = {"_part", "_part_offset", "_partition_id", "_part_index", "_part_starting_offset"};
        const String col = virtuals && rg.nextSmallNumber() < 3 ? rg.pickRandomly(virtual_columns) : rg.pickRandomly(t.cols);

        return alias.empty() ? col : alias + "." + col;
    }

    static String key(const String & col, const uint32_t m) { return fmt::format("cityHash64({}) % {}", col, m); }

    /// A constant longer than `optimize_const_name_size` is shipped to the replicas as a `__getScalar` call
    String longConstant()
    {
        static const std::vector<uint32_t> lengths = {1, 31, 32, 33, 255, 256, 257, 300, 1000};
        return fmt::format("materialize(repeat('x', {})) != ''", rg.pickRandomly(lengths));
    }

    String constantList(const uint32_t m)
    {
        static const std::vector<uint32_t> lengths = {1, 3, 10, 33, 100, 300};
        const uint32_t n = rg.pickRandomly(lengths);
        String res;

        for (uint32_t i = 0; i < n; i++)
        {
            res += (i ? ", " : "") + std::to_string(rg.randomInt<uint32_t>(0, m));
        }
        return res;
    }

    /// Where the rows come from: the table, or something reading it that the replicas have to see through
    String source(const ShapeTable & t, const String & alias, bool & virtuals)
    {
        const String as = alias.empty() ? "" : " AS " + alias;
        const uint32_t pick = rg.nextMediumNumber();

        virtuals = false;
        if (pick < 71 || depth >= max_depth)
        {
            virtuals = true;
            return t.name() + as;
        }
        if (pick < 81)
        {
            bool unused = false;
            depth++;
            String res = fmt::format("(SELECT {} FROM {}{}){}", fmt::join(t.cols, ", "), t.name(), where(t, "", unused), as);
            depth--;
            return res;
        }
        if (pick < 88)
        {
            virtuals = true;
            return fmt::format("merge('{}', '^{}$'){}", t.database, t.table, as);
        }
        if (pick < 94)
        {
            return fmt::format("view(SELECT {} FROM {}){}", fmt::join(t.cols, ", "), t.name(), as);
        }
        return fmt::format("cluster('{}', '{}', '{}'){}", rg.pickRandomly(clusters), t.database, t.table, as);
    }

    /// `SELECT <key> AS k FROM ...` of a table
    String keys(const ShapeTable & t, const uint32_t m)
    {
        bool virtuals = false;
        depth++;
        const String from = source(t, "", virtuals);
        String res = fmt::format("SELECT {} AS k FROM {}{}", key(column(t, "", virtuals), m), from, where(t, "", virtuals));
        if (rg.nextSmallNumber() < 3)
        {
            res += fmt::format(" ORDER BY k LIMIT {}", rg.randomInt<uint32_t>(1, 100));
        }
        depth--;
        return res;
    }

    String nest(const String & inner)
    {
        /// The same rows through more levels of queries
        return rg.nextSmallNumber() < 4 ? fmt::format("SELECT * FROM ({})", inner) : inner;
    }

    /// A subquery of the keys of a table, with long constants in it that change its hash on the replicas
    String inSubquery(const uint32_t m)
    {
        const ShapeTable & t = table();
        bool virtuals = false;

        depth++;
        const String from = source(t, "", virtuals);
        String res = fmt::format("SELECT {} FROM {}", key(column(t, "", virtuals), m), from);
        if (rg.nextMediumNumber() < 61)
        {
            res += " WHERE " + longConstant();
        }
        if (depth < max_depth && rg.nextSmallNumber() < 4)
        {
            res += (rg.nextMediumNumber() < 61 ? " AND " : " WHERE ") + predicate(t, "", virtuals);
        }
        depth--;
        return res;
    }

    String predicate(const ShapeTable & t, const String & alias, const bool virtuals)
    {
        const uint32_t m = modulus();

        switch (rg.randomInt<uint32_t>(0, depth < max_depth ? 10 : 5))
        {
            case 0:
                return fmt::format("{} < {}", key(column(t, alias, virtuals), m), rg.randomInt<uint32_t>(0, m));
            case 1:
                return longConstant();
            case 2:
                return fmt::format("NOT ({} = {})", key(column(t, alias, virtuals), m), rg.randomInt<uint32_t>(0, m));
            case 3:
                /// A long constant set, or a long array, is a long constant too
                return fmt::format("{} {}IN ({})", key(column(t, alias, virtuals), m), rg.nextSmallNumber() < 3 ? "NOT " : "", constantList(m));
            case 4:
                return fmt::format("has([{}], {})", constantList(m), key(column(t, alias, virtuals), m));
            case 5:
                return fmt::format("{} IS NOT NULL", column(t, alias, virtuals));
            case 6:
                return fmt::format(
                    "{} {}{}IN ({})",
                    key(column(t, alias, virtuals), m),
                    rg.nextSmallNumber() < 3 ? "GLOBAL " : "",
                    rg.nextSmallNumber() < 3 ? "NOT " : "",
                    inSubquery(m));
            case 7: {
                /// The same subquery twice is one set, built once and looked up by every use
                const String sub = inSubquery(m);
                return fmt::format(
                    "({} IN ({}) {} {} IN ({}))",
                    key(column(t, alias, virtuals), m),
                    sub,
                    rg.nextBool() ? "AND" : "OR",
                    key(column(t, alias, virtuals), m),
                    sub);
            }
            case 8:
                return fmt::format("indexHint({})", predicate(t, alias, virtuals));
            case 9:
                return fmt::format("({} OR {})", predicate(t, alias, virtuals), predicate(t, alias, virtuals));
            default:
                return fmt::format("cityHash64({}) > (SELECT min(cityHash64({})) FROM {})", column(t, alias, virtuals), rg.pickRandomly(t.cols), t.name());
        }
    }

    String where(const ShapeTable & t, const String & alias, const bool virtuals)
    {
        String res;
        const uint32_t npreds = rg.randomInt<uint32_t>(0, 2);

        for (uint32_t i = 0; i < npreds; i++)
        {
            res += (i == 0 ? " WHERE " : " AND ") + predicate(t, alias, virtuals);
        }
        return res;
    }

    String aggregate(const ShapeTable & t, const String & alias, const bool virtuals)
    {
        switch (rg.randomInt<uint32_t>(0, 4))
        {
            case 0:
                return "count()";
            case 1:
                return fmt::format("sum(cityHash64({}))", column(t, alias, virtuals));
            case 2:
                return fmt::format("uniqExact({})", column(t, alias, virtuals));
            case 3:
                return fmt::format("min(cityHash64({}))", column(t, alias, virtuals));
            default:
                return fmt::format("max({})", key(column(t, alias, virtuals), 1000));
        }
    }

    String join(const ShapeTable & t1, const ShapeTable & t2, const uint32_t m)
    {
        static const DB::Strings kinds
            = {"INNER", "LEFT", "RIGHT", "FULL", "ANY LEFT", "ANY INNER", "SEMI LEFT", "ANTI LEFT", "SEMI RIGHT", "ASOF LEFT", "GLOBAL INNER", "GLOBAL LEFT", "PASTE"};
        bool va = false;
        bool vb = false;
        const String from_a = source(t1, "a", va);
        const String kind = rg.pickRandomly(kinds);

        if (kind == "PASTE")
        {
            return fmt::format(
                "SELECT count(), sum(a.k), sum(b.k) FROM ({}) AS a PASTE JOIN ({}) AS b", keys(t1, m), keys(t2, m));
        }
        if (rg.nextSmallNumber() < 3)
        {
            /// `USING` a column both sides compute
            return fmt::format("SELECT count(), sum(k) FROM ({}) AS a {} JOIN ({}) AS b USING (k)", keys(t1, m), kind == "ASOF LEFT" ? "LEFT" : kind, keys(t2, m));
        }
        const String from_b = source(t2, "b", vb);
        String on = fmt::format("{} = {}", key(column(t1, "a", va), m), key(column(t2, "b", vb), m));

        if (kind == "ASOF LEFT")
        {
            on += fmt::format(" AND cityHash64({}) >= cityHash64({})", column(t1, "a", va), column(t2, "b", vb));
        }
        String res = fmt::format(
            "SELECT {}, {} FROM {} {} JOIN {} ON {}{}",
            kind.starts_with("SEMI RIGHT") ? "count()" : aggregate(t1, "a", va),
            kind.starts_with("SEMI LEFT") || kind.starts_with("ANTI") ? "count()" : aggregate(t2, "b", vb),
            from_a,
            kind,
            from_b,
            on,
            where(t1, "a", va));
        if (rg.nextSmallNumber() < 3)
        {
            /// A third read, of either table
            const ShapeTable & t3 = rg.nextBool() ? t1 : table();
            bool vc = false;
            const String from_c = source(t3, "c", vc);

            res = fmt::format(
                "SELECT {}, {} FROM {} INNER JOIN {} ON {} LEFT JOIN {} ON {} = {}{}",
                aggregate(t1, "a", va),
                aggregate(t3, "c", vc),
                from_a,
                from_b,
                on,
                from_c,
                key(column(t2, "b", vb), m),
                key(column(t3, "c", vc), m),
                where(t1, "a", va));
        }
        return res;
    }
};

}

/// Tables the shapes create and fill themselves, so there is always data to read whatever the catalog holds
static constexpr uint32_t num_shape_tables = 3;
static const std::array<String, num_shape_tables> shape_sorting_keys = {"k", "g, k", ""};

static String createShapeTable(RandomGenerator & rg, const uint32_t i)
{
    static const DB::Strings engines = {"MergeTree", "MergeTree", "MergeTree", "ReplacingMergeTree", "AggregatingMergeTree"};
    static const DB::Strings granularities = {"1", "8", "64", "8192"};
    static const DB::Strings partitions = {"", " PARTITION BY g % 2", " PARTITION BY g", " PARTITION BY toYYYYMM(d)"};

    return fmt::format(
        "CREATE TABLE IF NOT EXISTS `default`.`shape{}` (`k` UInt64, `g` UInt8, `s` String, `n` Nullable(Int64), `a` Array(UInt16), `d` DateTime, "
        "INDEX `ix` `s` TYPE bloom_filter GRANULARITY 1{}) ENGINE = {}(){} ORDER BY ({}) "
        "SETTINGS index_granularity = {}, min_bytes_for_wide_part = {}, deduplicate_merge_projection_mode = 'rebuild';",
        i,
        /// Not always: automatic parallel replicas declines the aggregates a projection answers
        rg.nextBool() ? ", PROJECTION `p` (SELECT `g`, count() GROUP BY `g`)" : "",
        rg.pickRandomly(engines),
        rg.nextBool() ? rg.pickRandomly(partitions) : "",
        shape_sorting_keys[i],
        rg.pickRandomly(granularities),
        rg.nextBool() ? "0" : "1000000000");
}

static String fillShapeTable(RandomGenerator & rg, const uint32_t i)
{
    static const std::vector<uint32_t> rows = {1, 100, 1000, 10000, 100000};

    switch (rg.randomInt<uint32_t>(0, 9))
    {
        case 0:
            return fmt::format("OPTIMIZE TABLE `default`.`shape{}`{};", i, rg.nextBool() ? " FINAL" : "");
        case 1:
            /// Masked rows, which every replica has to skip in the parts it reads
            return fmt::format("DELETE FROM `default`.`shape{}` WHERE cityHash64(`k`) % 10 = {};", i, rg.randomInt<uint32_t>(0, 9));
        default:
            return fmt::format(
                "INSERT INTO `default`.`shape{}` SELECT number, number % 7, toString(number % 100), if(number % 5 = 0, NULL, number), "
                "range(number % 4), toDateTime('2026-01-01 00:00:00') + number * 600 FROM numbers({}, {});",
                i,
                rg.randomInt<uint32_t>(0, 1000000),
                rg.pickRandomly(rows));
    }
}

DB::Strings StatementGenerator::generateShapedQueries(RandomGenerator & rg)
{
    DB::Strings res;
    std::vector<ShapeTable> sources;

    /// Created again now and then: the fuzzer may drop or truncate them, knowing nothing about them
    if (!shape_tables_created || rg.nextMediumNumber() < 4)
    {
        for (uint32_t i = 0; i < num_shape_tables; i++)
        {
            res.emplace_back(createShapeTable(rg, i));
            for (uint32_t j = 0; j < 3; j++)
            {
                res.emplace_back(fillShapeTable(rg, i));
            }
        }
        shape_tables_created = true;
    }
    if (rg.nextMediumNumber() < 16)
    {
        res.emplace_back(fillShapeTable(rg, rg.randomInt<uint32_t>(0, num_shape_tables - 1)));
    }
    for (uint32_t i = 0; i < num_shape_tables; i++)
    {
        sources.push_back({"default", fmt::format("shape{}", i), {"`k`", "`g`", "`s`", "`n`", "`a`", "`d`"}, shape_sorting_keys[i]});
    }
    for (const SQLTable * t : shapeTables())
    {
        DB::Strings cols = shapeColumns(*t);

        if (!cols.empty())
        {
            sources.push_back({t->getDatabaseName(), t->getBaseName(false), std::move(cols), ""});
        }
    }
    ShapeBuilder builder(rg, sources, fc.parallel_replicas_clusters);
    String query = builder.query();

    query += " SETTINGS ";
    bool first = true;
    for (const auto & [name, value] : parallelReplicasSettings(rg))
    {
        query += (first ? "" : ", ") + name + " = " + value;
        first = false;
    }
    query += " FORMAT Null;";
    res.emplace_back(std::move(query));
    return res;
}

}
