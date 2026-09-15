#include <Processors/QueryPlan/QueryPlanToJSON.h>

#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/StepStatsJSONPrinter.h>
#include <Processors/QueryPlan/StepStatsStorage.h>
#include <IO/WriteBufferFromString.h>
#include <Common/StringUtils.h>
#include <base/types.h>

#include <memory>
#include <vector>


namespace DB
{

namespace
{

/// What the step says about itself, as EXPLAIN ANALYZE prints it: `Filter column: ...`,
/// `Sort description: ...`, `Limit ...`. One array entry per line.
///
/// The structured form, describeActions(JSONBuilder::JSONMap &), is not an option: it rebuilds its
/// output from the step's ActionsDAG, and buildQueryPipeline has moved that out of every
/// ExpressionStep by the time an executed plan is serialized. Compact and pretty are pinned rather
/// than taken from the caller: they are the only settings under which the describe methods do not
/// read the DAG at all, and FilterStep would otherwise look its filter column up with
/// ActionsDAG::findInOutputs, which throws UNKNOWN_IDENTIFIER on the empty remains. Under pretty
/// the expressions come from the names captured while the DAGs were still intact.
String addStepDetails(const IQueryPlanStep & step, JSONBuilder::JSONMap & map, const PrettyNames * plan_pretty_names)
{
    PrettyNames empty_pretty_names;
    WriteBufferFromOwnString out;

    IQueryPlanStep::FormatSettings settings{
        .out = out,
        .header_prefix = "",
        .detail_prefix = "",
        .compact = true,
        .pretty = true,
        .pretty_names = plan_pretty_names ? plan_pretty_names->pretty_names : empty_pretty_names.pretty_names,
        .runtime_filter_names = plan_pretty_names ? plan_pretty_names->runtime_filter_names : empty_pretty_names.runtime_filter_names};

    step.describeActions(settings);

    auto details = std::make_unique<JSONBuilder::JSONArray>();

    const auto & text = out.str();
    size_t line_begin = 0;
    while (line_begin < text.size())
    {
        size_t line_end = text.find('\n', line_begin);
        if (line_end == String::npos)
            line_end = text.size();

        if (line_end > line_begin)
            details->add(text.substr(line_begin, line_end - line_begin));

        line_begin = line_end + 1;
    }

    map.add("Details", std::move(details));

    /// Returned so that references between plans can be resolved against what a step actually
    /// printed -- a set is named here by its `subqueryN` alias, not by the key the sub-plan knows.
    return text;
}

/// The names are scoped per plan: a sub-plan is its own naming scope and has its own entry.
const PrettyNames * findPrettyNames(const PrettyNamesPerPlan * pretty_names, const QueryPlan * plan)
{
    if (!pretty_names)
        return nullptr;

    auto it = pretty_names->names.find(plan);
    return it == pretty_names->names.end() ? nullptr : &it->second;
}

/// A serialized node, with the text it rendered kept beside it so that one plan can find the step
/// in another that refers to it. Ids and text only -- nothing here points into the plan, which the
/// caller may drop before the document is assembled.
struct CollectedNode
{
    String id;
    String text;
    std::unique_ptr<JSONBuilder::JSONMap> map;
};

CollectedNode makeNode(
    const IQueryPlanStep & step,
    const ExplainPlanOptions & options,
    size_t max_description_length,
    const StepStatsStorage * steps_to_stats,
    const PrettyNames * plan_pretty_names)
{
    auto map = std::make_unique<JSONBuilder::JSONMap>();
    String text;

    /// `Node Type` and `Node Id` keep the names EXPLAIN json=1 gives them, so that a reader who
    /// knows one form recognises the other.
    map->add("Node Type", step.getName());
    map->add("Node Id", step.getUniqID());

    if (options.description)
    {
        std::string_view description = step.getStepDescription();

        /// Backs the view when trimming produces a new string, as the text renderer does.
        String pretty_description;
        if (options.pretty)
        {
            pretty_description = QueryPlanFormat::trimColumnIdentifier(description);
            description = pretty_description;
        }

        if (max_description_length)
            description = description.substr(0, max_description_length);

        if (!description.empty())
        {
            map->add("Description", description);
            text += description;
            text += '\n';
        }
    }

    if (options.actions)
        text += addStepDetails(step, *map, plan_pretty_names);

    if (options.indexes)
        step.describeIndexes(*map);

    if (options.projections)
        step.describeProjections(*map);

    if (steps_to_stats)
        map->add("Statistics", StepStatsJSONPrinter::toJSON(steps_to_stats->analyzeStep(&step)));

    return {step.getUniqID(), std::move(text), std::move(map)};
}


/// Walks a plan and serializes every node, children included. Shared by the main document and by
/// the sub-plans that are serialized on their own.
std::vector<CollectedNode> collectNodes(
    const QueryPlan & plan,
    const ExplainPlanOptions & options,
    size_t max_description_length,
    const StepStatsStorage * steps_to_stats,
    const PrettyNamesPerPlan * pretty_names)
{
    struct Frame
    {
        const QueryPlan * plan = nullptr;
        QueryPlan::Node * node = nullptr;
    };

    std::vector<CollectedNode> collected;

    std::vector<Frame> stack;
    stack.push_back({&plan, plan.getRootNode()});

    while (!stack.empty())
    {
        auto frame = stack.back();
        stack.pop_back();

        if (!frame.node)
            continue;

        auto & step = *frame.node->step;
        auto collected_node = makeNode(
            step, options, max_description_length, steps_to_stats, findPrettyNames(pretty_names, frame.plan));

        auto children = std::make_unique<JSONBuilder::JSONArray>();

        for (auto * child : frame.node->children)
        {
            children->add(child->step->getUniqID());
            stack.push_back({frame.plan, child});
        }

        /// A sub-plan root is just another node here, referenced like any child. In the tree form
        /// it needed a second kind of nesting.
        for (auto * child_plan : step.getChildPlans())
        {
            if (!child_plan)
                continue;

            auto * child_root = child_plan->getRootNode();
            if (!child_root)
                continue;

            children->add(child_root->step->getUniqID());
            stack.push_back({child_plan, child_root});
        }

        collected_node.map->add("Children", std::move(children));
        collected.push_back(std::move(collected_node));
    }

    return collected;
}

/// The `__set_<hash>` -> alias entries of a plan's pretty names. `buildPrettyNamesPerPlan` puts
/// them in the same map as the column names, keyed by exactly the string `PreparedSets::toString`
/// produces, which is what a captured sub-plan records for the set it builds.
std::unordered_map<String, String> extractSetAliases(const PrettyNames * names)
{
    std::unordered_map<String, String> aliases;
    if (!names)
        return aliases;

    for (const auto & [key, pretty] : names->pretty_names)
        if (key.starts_with("__set_"))
            aliases.emplace(key, pretty.expression);

    return aliases;
}

/// Whether `text` names `name` as a whole word, so that `subquery1` does not match inside
/// `subquery10`. Set aliases are the only thing looked up this way, and they are exactly of that
/// shape -- a fixed prefix followed by a number.
bool mentionsWord(std::string_view text, std::string_view name)
{
    const auto is_word_char = [](char c) { return isWordCharASCII(c); };

    for (size_t pos = text.find(name); pos != std::string_view::npos; pos = text.find(name, pos + 1))
    {
        const size_t after = pos + name.size();
        const bool starts_word = pos == 0 || !is_word_char(text[pos - 1]);
        const bool ends_word = after >= text.size() || !is_word_char(text[after]);

        if (starts_word && ends_word)
            return true;
    }

    return false;
}

}

SerializedSubPlan serializeSubPlan(
    const QueryPlan & plan,
    const ExplainPlanOptions & options,
    size_t max_description_length,
    std::string_view origin,
    const StepStatsStorage * steps_to_stats,
    const PrettyNamesPerPlan * pretty_names)
{
    SerializedSubPlan result;
    if (!plan.isInitialized() || !plan.getRootNode())
        return result;

    result.root_id = plan.getRootNode()->step->getUniqID();

    for (auto & node : collectNodes(plan, options, max_description_length, steps_to_stats, pretty_names))
    {
        // Says where the node came from, so a reader does not take it for part of the main tree.
        node.map->add("Origin", String(origin));
        result.node_text.emplace_back(node.id, std::move(node.text));
        result.nodes.push_back(std::move(node.map));
    }

    result.set_aliases = extractSetAliases(findPrettyNames(pretty_names, &plan));

    if (steps_to_stats)
    {
        result.execution_time_ns = steps_to_stats->getExecutionTimeNs();
        result.max_threads = steps_to_stats->getMaxThreads();
    }

    return result;
}

JSONBuilder::ItemPtr queryPlanToJSON(
    const QueryPlan & plan,
    const ExplainPlanOptions & options,
    size_t max_description_length,
    const StepStatsStorage * steps_to_stats,
    const PrettyNamesPerPlan * pretty_names,
    std::vector<SerializedSubPlan> * sub_plans)
{
    auto collected = collectNodes(plan, options, max_description_length, steps_to_stats, pretty_names);

    /// What each step of the query's own plan printed, kept only long enough to work out which of
    /// them uses each set built during planning. See `findSetConsumers`.
    std::vector<std::pair<String, String>> main_plan_text;
    main_plan_text.reserve(collected.size());

    auto nodes_array = std::make_unique<JSONBuilder::JSONArray>();
    for (auto & node : collected)
    {
        main_plan_text.emplace_back(node.id, std::move(node.text));
        nodes_array->add(std::move(node.map));
    }

    /// A sub-plan knows the set it builds by key; the steps that use that set print a `subqueryN`
    /// alias instead, and the pretty names of the plan those steps belong to hold the translation.
    ///
    /// Searched one plan at a time rather than over every node, because the aliases are numbered
    /// per plan: `subquery1` in the query's plan and `subquery1` in a sub-plan are different sets,
    /// so a document-wide search for the name would invent links that do not exist.
    struct Scope
    {
        const std::unordered_map<String, String> * aliases;
        const std::vector<std::pair<String, String>> * node_text;
    };

    const auto main_set_aliases = extractSetAliases(findPrettyNames(pretty_names, &plan));

    std::vector<Scope> scopes;
    scopes.push_back({&main_set_aliases, &main_plan_text});
    if (sub_plans)
        for (const auto & sub_plan : *sub_plans)
            scopes.push_back({&sub_plan.set_aliases, &sub_plan.node_text});

    /// The alias the consuming steps print, and the ids of those steps. Empty when nothing in the
    /// document refers to the set -- a subquery whose result was used only by index analysis that
    /// did not name it, for instance.
    const auto findConsumers = [&](const String & set_key, const std::vector<std::pair<String, String>> * own_nodes)
    {
        struct Found
        {
            String alias;
            std::vector<String> node_ids;
        };
        Found found;

        for (const auto & scope : scopes)
        {
            /// A plan does not consume the set it builds itself.
            if (scope.node_text == own_nodes)
                continue;

            const auto alias_it = scope.aliases->find(set_key);
            if (alias_it == scope.aliases->end())
                continue;

            for (const auto & [node_id, node_text] : *scope.node_text)
            {
                if (!mentionsWord(node_text, alias_it->second))
                    continue;

                found.alias = alias_it->second;
                found.node_ids.push_back(node_id);
            }

            if (!found.node_ids.empty())
                return found;

            /// Remember the name even when no step spelled it out, so the entry can still say what
            /// the query called this subquery.
            if (found.alias.empty())
                found.alias = alias_it->second;
        }

        return found;
    };

    // Plans that ran for this query without being part of its tree, listed among the nodes so a
    // reader walks one array, and named at the root so their roots can be told from the main one.
    auto sub_plan_roots = std::make_unique<JSONBuilder::JSONArray>();
    if (sub_plans)
    {
        // Moved rather than copied: JSONBuilder items are not copyable, and `render` keeps its
        // result and releases the plan afterwards, so nothing serializes these a second time.
        for (auto & sub_plan : *sub_plans)
        {
            if (sub_plan.nodes.empty())
                continue;

            // Shaped like the root of the document, one level down: same keys, same meanings, for
            // the pipeline this subquery ran in. `ExecutionTimeNs` is that pipeline's own -- it ran
            // before the main one existed, so it is not part of the query's and the two do not sum.
            auto entry = std::make_unique<JSONBuilder::JSONMap>();
            entry->add("Root", sub_plan.root_id);

            /// Which step uses this set. Without it a reader sees a sub-plan that reads a large
            /// table and nothing at all saying what the query wanted it for.
            if (!sub_plan.set_key.empty())
            {
                const auto found = findConsumers(sub_plan.set_key, &sub_plan.node_text);

                if (!found.alias.empty())
                    entry->add("Name", found.alias);

                if (!found.node_ids.empty())
                {
                    auto consumers = std::make_unique<JSONBuilder::JSONArray>();
                    for (const auto & node_id : found.node_ids)
                        consumers->add(node_id);
                    entry->add("ConsumedBy", std::move(consumers));
                }
            }

            if (sub_plan.execution_time_ns)
                entry->add("ExecutionTimeNs", *sub_plan.execution_time_ns);
            if (sub_plan.max_threads)
                entry->add("MaxThreads", *sub_plan.max_threads);
            sub_plan_roots->add(std::move(entry));

            for (auto & node : sub_plan.nodes)
                nodes_array->add(std::move(node));
        }
    }

    auto result = std::make_unique<JSONBuilder::JSONMap>();
    result->add("Version", QUERY_PLAN_JSON_VERSION);
    result->add("Root", plan.getRootNode()->step->getUniqID());

    if (steps_to_stats)
    {
        result->add("ExecutionTimeNs", steps_to_stats->getExecutionTimeNs());
        result->add("MaxThreads", steps_to_stats->getMaxThreads());
    }

    /// The columns the query produces. The text renderer prints these once above the tree rather
    /// than against a step, so they belong to the plan, not to any node.
    if (options.pretty)
    {
        auto output_array = std::make_unique<JSONBuilder::JSONArray>();

        const auto * root_pretty_names = findPrettyNames(pretty_names, &plan);
        PrettyNames empty_pretty_names;
        const auto & names
            = root_pretty_names ? root_pretty_names->pretty_names : empty_pretty_names.pretty_names;

        const auto & root_step = *plan.getRootNode()->step;
        if (root_step.hasOutputHeader() && root_step.getOutputHeader())
            for (const auto & column : *root_step.getOutputHeader())
                output_array->add(QueryPlanFormat::formatColumnPretty(column.name, names));

        result->add("Output", std::move(output_array));
    }

    if (sub_plans && !sub_plans->empty())
        result->add("SetSubqueries", std::move(sub_plan_roots));

    result->add("Nodes", std::move(nodes_array));

    return result;
}

}
