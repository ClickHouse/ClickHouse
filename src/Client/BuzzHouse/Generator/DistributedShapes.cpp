#include <Client/BuzzHouse/Generator/StatementGenerator.h>
#include <Common/checkStackSize.h>

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

void StatementGenerator::addParallelReplicasSettings(RandomGenerator & rg, SettingValues * svs)
{
    const auto add = [&](const String & name, const String & value)
    {
        SetValue * setv = svs->has_set_value() ? svs->add_other_values() : svs->mutable_set_value();

        setv->set_property(name);
        setv->set_value(value);
    };
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
}

}
