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
    /// TODO: temporary, so that the reach of these queries can be measured in `system.query_log`
    add("log_queries", "1");
    add("log_profile_events", "1");
    add("log_queries_min_query_duration_ms", "0");
    add("log_queries_min_type", "'QUERY_START'");
    add("log_queries_probability", "1");
    add("log_query_settings", "1");
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

String StatementGenerator::generateShapedQuery(RandomGenerator & rg)
{
    struct ShapeTable
    {
        String name;
        DB::Strings cols;
    };
    std::vector<ShapeTable> sources;

    for (const SQLTable * t : shapeTables())
    {
        DB::Strings cols = shapeColumns(*t);

        if (!cols.empty())
        {
            sources.push_back({"`" + t->getDatabaseName() + "`.`" + t->getBaseName(false) + "`", std::move(cols)});
        }
    }
    if (sources.empty())
    {
        return "";
    }
    static const std::vector<uint32_t> moduli = {2, 3, 10, 100};
    static const std::vector<uint32_t> lengths = {1, 31, 32, 33, 255, 256, 257, 300, 1000};
    const auto pick_table = [&]() -> const ShapeTable & { return rg.pickRandomly(sources); };
    const auto column = [&](const ShapeTable & t, const String & alias)
    { return (alias.empty() ? "" : alias + ".") + rg.pickRandomly(t.cols); };
    /// `cityHash64` takes any of the columns, so the two sides of a comparison always have the same type
    const auto key = [&](const String & col, const uint32_t modulus) { return fmt::format("cityHash64({}) % {}", col, modulus); };
    /// A constant longer than `optimize_const_name_size` is shipped to the replicas as a `__getScalar` call
    const auto long_constant = [&]() { return fmt::format("materialize(repeat('x', {})) != ''", rg.pickRandomly(lengths)); };
    const auto in_subquery = [&](const uint32_t modulus)
    {
        const ShapeTable & t = pick_table();
        String res = fmt::format("SELECT {} FROM {}", key(column(t, ""), modulus), t.name);

        if (rg.nextMediumNumber() < 61)
        {
            res += " WHERE " + long_constant();
        }
        return res;
    };
    const auto predicate = [&](const ShapeTable & t, const String & alias)
    {
        const uint32_t modulus = rg.pickRandomly(moduli);

        switch (rg.randomInt<uint32_t>(0, 4))
        {
            case 0:
                return fmt::format("{} < {}", key(column(t, alias), modulus), rg.randomInt<uint32_t>(0, modulus));
            case 1:
                return long_constant();
            case 2:
                return fmt::format("{} {}IN ({})", key(column(t, alias), modulus), rg.nextSmallNumber() < 3 ? "GLOBAL " : "", in_subquery(modulus));
            case 3: {
                /// The same subquery twice is one set, built once and looked up by every use
                const String sub = in_subquery(modulus);

                return fmt::format(
                    "{} IN ({}) {} {} IN ({})",
                    key(column(t, alias), modulus),
                    sub,
                    rg.nextBool() ? "AND" : "OR",
                    key(column(t, alias), modulus),
                    sub);
            }
            default:
                return fmt::format("NOT ({} = {})", key(column(t, alias), modulus), rg.randomInt<uint32_t>(0, modulus));
        }
    };
    const auto where = [&](const ShapeTable & t, const String & alias)
    {
        String res;
        const uint32_t npreds = rg.randomInt<uint32_t>(0, 2);

        for (uint32_t i = 0; i < npreds; i++)
        {
            res += (i == 0 ? " WHERE " : " AND ") + predicate(t, alias);
        }
        return res;
    };
    const auto aggregate = [&](const ShapeTable & t, const String & alias)
    {
        switch (rg.randomInt<uint32_t>(0, 4))
        {
            case 0:
                return String("count()");
            case 1:
                return fmt::format("sum(cityHash64({}))", column(t, alias));
            case 2:
                return fmt::format("uniqExact({})", column(t, alias));
            case 3:
                return fmt::format("min(cityHash64({}))", column(t, alias));
            default:
                return fmt::format("max({})", key(column(t, alias), 1000));
        }
    };
    const ShapeTable & t1 = pick_table();
    /// A table read twice in one query: the reads must be told apart when the plans are paired
    const ShapeTable & t2 = rg.nextBool() ? t1 : pick_table();
    const uint32_t modulus = rg.pickRandomly(moduli);
    String query;

    switch (rg.randomInt<uint32_t>(0, 7))
    {
        case 0:
            query = fmt::format(
                "SELECT {} AS k, {}, {} FROM {}{} GROUP BY k ORDER BY k LIMIT 100",
                key(column(t1, ""), modulus),
                aggregate(t1, ""),
                aggregate(t1, ""),
                t1.name,
                where(t1, ""));
            break;
        case 1:
            query = fmt::format("SELECT {} FROM {} {} {}", aggregate(t1, ""), t1.name, rg.nextSmallNumber() < 4 ? "PREWHERE" : "WHERE", predicate(t1, ""));
            break;
        case 2: {
            static const DB::Strings joins = {"INNER", "LEFT", "RIGHT", "FULL", "ANY LEFT", "SEMI LEFT", "ANTI LEFT", "GLOBAL INNER"};

            query = fmt::format(
                "SELECT {}, {} FROM {} AS a {} JOIN {} AS b ON {} = {}{}",
                aggregate(t1, "a"),
                aggregate(t2, "b"),
                t1.name,
                rg.pickRandomly(joins),
                /// The columns by name: `*` skips `ALIAS` and `MATERIALIZED` ones
                rg.nextSmallNumber() < 3 ? fmt::format("(SELECT {} FROM {}{})", fmt::join(t2.cols, ", "), t2.name, where(t2, "")) : t2.name,
                key(column(t1, "a"), modulus),
                key(column(t2, "b"), modulus),
                where(t1, "a"));
            break;
        }
        case 3:
            query = fmt::format(
                "SELECT k, count() FROM (SELECT {} AS k FROM {}{}) GROUP BY k ORDER BY k",
                key(column(t1, ""), modulus),
                t1.name,
                where(t1, ""));
            break;
        case 4:
            query = fmt::format(
                "SELECT count() FROM {} WHERE cityHash64({}) > (SELECT min(cityHash64({})) FROM {}{})",
                t1.name,
                column(t1, ""),
                column(t2, ""),
                t2.name,
                where(t2, ""));
            break;
        case 5:
            query = fmt::format(
                "SELECT k, count() FROM (SELECT {} AS k FROM {}{} UNION ALL SELECT {} AS k FROM {}{}) GROUP BY k ORDER BY k",
                key(column(t1, ""), modulus),
                t1.name,
                where(t1, ""),
                key(column(t2, ""), modulus),
                t2.name,
                where(t2, ""));
            break;
        case 6:
            query = fmt::format(
                "WITH s AS (SELECT {} AS k FROM {}{}) SELECT {} FROM {} WHERE {} IN (SELECT k FROM s)",
                key(column(t2, ""), modulus),
                t2.name,
                where(t2, ""),
                aggregate(t1, ""),
                t1.name,
                key(column(t1, ""), modulus));
            break;
        default:
            query = fmt::format(
                "SELECT {}, {} FROM {}{} ORDER BY ALL LIMIT {}",
                column(t1, ""),
                column(t1, ""),
                t1.name,
                where(t1, ""),
                rg.randomInt<uint32_t>(1, 1000));
            break;
    }
    query += " SETTINGS ";
    bool first = true;
    for (const auto & [name, value] : parallelReplicasSettings(rg))
    {
        query += (first ? "" : ", ") + name + " = " + value;
        first = false;
    }
    query += " FORMAT Null;";
    return query;
}

}
