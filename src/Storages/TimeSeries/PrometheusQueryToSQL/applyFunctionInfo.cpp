#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionInfo.h>

#include <Common/Exception.h>
#include <Common/re2.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Prometheus/stepsInTimeSeriesRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/fromSelector.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/toVectorGrid.h>

#include <cmath>
#include <unordered_set>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    using Matcher = PrometheusQueryTree::Matcher;
    using MatcherList = PrometheusQueryTree::MatcherList;
    using MatcherType = PrometheusQueryTree::MatcherType;

    constexpr const char * kTargetInfo = "target_info";

    ASTPtr col(const String & name)
    {
        return make_intrusive<ASTIdentifier>(name);
    }

    ASTPtr lit(Field value)
    {
        return make_intrusive<ASTLiteral>(std::move(value));
    }

    ASTPtr lambda(const String & argument, ASTPtr body)
    {
        return makeASTFunction("lambda", makeASTFunction("tuple", col(argument)), std::move(body));
    }

    ASTPtr makeFunction(const String & name, ASTs arguments)
    {
        auto function = makeASTFunction(name);
        function->arguments->children = std::move(arguments);
        return function;
    }

    /// The labels Prometheus joins info series by.
    ASTPtr identifyingLabels()
    {
        return lit(Array{"instance", "job"});
    }

    ASTPtr labelNames(ASTPtr group)
    {
        return makeASTFunction(
            "arrayMap", lambda("x", makeASTFunction("tupleElement", col("x"), lit(1u))), makeASTFunction("timeSeriesGroupToTags", group));
    }

    /// values[step + 1]
    ASTPtr valueAtStep(ASTPtr step)
    {
        return makeASTFunction("arrayElement", col(ColumnNames::Values), makeASTFunction("plus", std::move(step), lit(1u)));
    }

    /// arrayJoin(arrayFilter(i -> isNotNull(values[i + 1]), range(length(values)))): one row for each step with a value.
    ASTPtr stepsWithValues()
    {
        return makeASTFunction(
            "arrayJoin",
            makeASTFunction(
                "arrayFilter",
                lambda("i", makeASTFunction("isNotNull", valueAtStep(col("i")))),
                makeASTFunction("range", makeASTFunction("length", col(ColumnNames::Values)))));
    }

    ASTPtr matchesAST(const Matcher & matcher, ASTPtr value)
    {
        switch (matcher.matcher_type)
        {
            case MatcherType::EQ: return makeASTFunction("equals", value, lit(matcher.label_value));
            case MatcherType::NE: return makeASTFunction("notEquals", value, lit(matcher.label_value));
            case MatcherType::RE: return makeASTFunction("match", value, lit("^(?:" + matcher.label_value + ")$"));
            case MatcherType::NRE: return makeASTFunction("not", makeASTFunction("match", value, lit("^(?:" + matcher.label_value + ")$")));
        }
        UNREACHABLE();
    }

    bool matchesEmptyString(const Matcher & matcher)
    {
        switch (matcher.matcher_type)
        {
            case MatcherType::EQ: return matcher.label_value.empty();
            case MatcherType::NE: return !matcher.label_value.empty();
            case MatcherType::RE:
            case MatcherType::NRE:
            {
                re2::RE2::Options options;
                options.set_log_errors(false);
                re2::RE2 regexp(matcher.label_value, options);
                return re2::RE2::FullMatch("", regexp) == (matcher.matcher_type == MatcherType::RE);
            }
        }
        UNREACHABLE();
    }

    /// Whether the ticks of the sample timestamps read over `range` may not fit in a Float64 exactly (2^53 ticks), for the finest scale 9.
    bool needsWideTicks(const NodeEvaluationRange & range, const ConverterContext & context)
    {
        const long double span = static_cast<long double>(range.end_time.value - range.start_time.value + range.window.value);
        return span * std::pow(10.0L, 9.0L - static_cast<long double>(context.result_timestamp_scale)) >= 9007199254740992.0L;
    }

    /// The values of the label in the base series as the alternatives of a PromQL regexp matcher, ".*" if they can't be listed.
    ASTPtr labelValuesAlternatives(const String & label_name)
    {
        ASTPtr label_value = makeASTFunction("ifNull", makeASTFunction("timeSeriesExtractTag", col("join_group"), lit(label_name)), lit(""));
        ASTPtr values = makeASTFunction("groupUniqArray", std::move(label_value));

        /// An empty value means the label is absent, a control character can't be put into the selector.
        ASTPtr unlistable = makeASTFunction(
            "or",
            makeASTFunction("empty", values->clone()),
            makeASTFunction(
                "arrayExists",
                lambda("v", makeASTFunction("or", makeASTFunction("empty", col("v")), makeASTFunction("match", col("v"), lit(R"([\x00-\x1f\x7f])")))),
                values->clone()));

        /// Escapes the regexp metacharacters, then the backslashes and the quotes of the PromQL string.
        ASTPtr escaped = makeASTFunction("replaceRegexpAll", col("v"), lit(R"(([!-/:-@\[-^`{-~]))"), lit(R"(\\\1)"));
        escaped = makeASTFunction("replaceAll", std::move(escaped), lit("\\"), lit("\\\\"));
        escaped = makeASTFunction("replaceAll", std::move(escaped), lit("\""), lit("\\\""));
        return makeASTFunction(
            "if",
            std::move(unlistable),
            lit(".*"),
            makeASTFunction("arrayStringConcat", makeASTFunction("arrayMap", lambda("v", std::move(escaped)), makeASTFunction("arraySort", std::move(values))), lit("|")));
    }

    /// Whether Prometheus evaluates `node` only once, at the first step, see preprocessExprHelper() in Prometheus.
    bool isStepInvariant(const Node * node)
    {
        switch (node->node_type)
        {
            case NodeType::Scalar:
            case NodeType::StringLiteral:
                return true;
            case NodeType::InstantSelector:
            case NodeType::RangeSelector:
            case NodeType::Subquery:
                return false;
            case NodeType::Offset:
                return static_cast<const PrometheusQueryTree::Offset *>(node)->hasAtModifier() || isStepInvariant(node->children.at(0));
            case NodeType::AggregationOperator:
                return isStepInvariant(node->children.back());
            case NodeType::Function:
            {
                static const std::unordered_set<std::string_view> unsafe_functions = {
                    "days_in_month", "day_of_month", "day_of_week", "day_of_year", "hour", "minute", "month", "year",
                    "predict_linear", "time", "timestamp"};
                if (unsafe_functions.contains(static_cast<const PrometheusQueryTree::Function *>(node)->function_name))
                    return false;
                return std::ranges::all_of(node->children, isStepInvariant);
            }
            case NodeType::UnaryOperator:
            case NodeType::BinaryOperator:
                return std::ranges::all_of(node->children, isStepInvariant);
        }
        UNREACHABLE();
    }

    /// Returns the first instant selector in `node` in the order Prometheus inspects the nodes.
    const Node * findFirstSelector(const Node * node)
    {
        if (node->node_type == NodeType::InstantSelector)
            return node;
        /// Prometheus visits an aggregation's expression before its parameter.
        const Node * expression = node->node_type == NodeType::AggregationOperator ? node->children.back() : nullptr;
        if (expression)
        {
            if (const auto * selector = findFirstSelector(expression))
                return selector;
        }
        for (const auto * child : node->children)
        {
            if (child == expression)
                continue;
            if (const auto * selector = findFirstSelector(child))
                return selector;
        }
        return nullptr;
    }

    /// Prometheus 3.5 reads the info series only up to the @ and offset modifiers of the first selector in `v`.
    std::optional<std::pair<TimestampType, TimestampType>> getInfoTimeBounds(
        const PrometheusQueryTree::Function * function_node, const ConverterContext & context)
    {
        const auto * modifiers = findFirstSelector(function_node->getArguments().at(0));
        if (modifiers && modifiers->parent && modifiers->parent->node_type == NodeType::RangeSelector)
            modifiers = modifiers->parent;
        if (!modifiers || !modifiers->parent || modifiers->parent->node_type != NodeType::Offset)
            return {};
        const auto & offset_node = static_cast<const PrometheusQueryTree::Offset &>(*modifiers->parent);

        const auto & info_range = context.node_range_getter.get(function_node);
        const auto & query_range = context.node_range_getter.get(context.promql_tree->getRoot());
        TimestampType start = info_range.start_time;
        TimestampType end = info_range.end_time;
        switch (offset_node.at_modifier)
        {
            case PrometheusQueryTree::Offset::AtModifier::None: break;
            case PrometheusQueryTree::Offset::AtModifier::Timestamp: start = end = *offset_node.at_timestamp; break;
            case PrometheusQueryTree::Offset::AtModifier::Start: start = end = query_range.start_time; break;
            case PrometheusQueryTree::Offset::AtModifier::End: start = end = query_range.end_time; break;
        }
        if (offset_node.offset_value)
        {
            start -= *offset_node.offset_value;
            end -= *offset_node.offset_value;
        }
        return std::make_pair(start - info_range.window + 1, end);
    }

    String addSubquery(ASTPtr query, SQLSubqueryType type, ConverterContext & context)
    {
        context.subqueries.emplace_back(context.subqueries.size(), std::move(query), type);
        return context.subqueries.back().name;
    }
}


bool isFunctionInfo(std::string_view function_name)
{
    return function_name == "info";
}


SQLQueryPiece applyFunctionInfo(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    if (arguments.empty() || arguments.size() > 2)
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "Function 'info' expects 1 or 2 arguments, but was called with {} arguments", arguments.size());
    }

    if (arguments[0].type != ResultType::INSTANT_VECTOR)
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "Function 'info' expects the first argument of type {}, but expression {} has type {}",
                        ResultType::INSTANT_VECTOR, getPromQLText(arguments[0], context), arguments[0].type);
    }

    /// The second argument isn't evaluated, only its matchers are used.
    MatcherList matchers;
    if (arguments.size() == 2)
    {
        const auto * selector = function_node->getArguments().at(1);
        while (selector->node_type == NodeType::Offset)
            selector = selector->children.at(0);
        if (selector->node_type != NodeType::InstantSelector)
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY, "Function 'info' expects label selectors in the second argument");
        matchers = static_cast<const PrometheusQueryTree::InstantSelector *>(selector)->matchers;
    }

    /// Without the second argument only `target_info` is used, and its series aren't enriched.
    MatcherList name_matchers;
    if (arguments.size() == 1)
        name_matchers.push_back(Matcher{kMetricName, kTargetInfo, MatcherType::EQ});

    Array data_label_names;
    bool drop_without_data_labels = false;
    /// Prometheus 3.5 also drops by the metric name matchers if no series of `v` has identifying labels.
    bool drop_without_identifying_labels = false;
    for (const auto & matcher : matchers)
    {
        if (matcher.label_name == kMetricName)
        {
            name_matchers.push_back(matcher);
            drop_without_identifying_labels |= !matchesEmptyString(matcher);
            continue;
        }
        if (std::find(data_label_names.begin(), data_label_names.end(), Field{matcher.label_name}) == data_label_names.end())
            data_label_names.push_back(matcher.label_name);
        drop_without_data_labels |= !matchesEmptyString(matcher);
    }

    PrometheusQueryTree::InstantSelector info_selector;
    info_selector.matchers = matchers;
    if (std::ranges::none_of(matchers, [](const Matcher & matcher) { return matcher.label_name == kMetricName; }))
        info_selector.matchers.insert(info_selector.matchers.begin(), Matcher{kMetricName, kTargetInfo, MatcherType::EQ});
    /// timeSeriesSelector() needs a matcher not matching "", and every series has a name.
    if (std::ranges::all_of(info_selector.matchers, matchesEmptyString))
        info_selector.matchers.push_back(Matcher{kMetricName, ".+", MatcherType::RE});
    auto base = toVectorGrid(std::move(arguments[0]), context);
    if (base.store_method == StoreMethod::EMPTY || context.node_range_getter.get(function_node).empty())
        return SQLQueryPiece{function_node, ResultType::INSTANT_VECTOR, StoreMethod::EMPTY};

    /// Step 1: one row for each sample of `v`, with its identifying labels and its step number.
    String samples;
    {
        SelectQueryBuilder builder;
        builder.select_list.push_back(col(ColumnNames::Group));
        builder.select_list.push_back(makeASTFunction("timeSeriesRemoveAllTagsExcept", col(ColumnNames::Group), identifyingLabels()));
        builder.select_list.back()->setAlias("join_group");

        ASTs name_matches;
        for (const auto & matcher : name_matchers)
            name_matches.push_back(matchesAST(
                matcher,
                makeASTFunction("ifNull", makeASTFunction("timeSeriesExtractTag", col(ColumnNames::Group), lit(kMetricName)), lit(""))));
        if (name_matches.empty())
            builder.select_list.push_back(lit(false));
        else if (name_matches.size() == 1)
            builder.select_list.push_back(name_matches[0]);
        else
            builder.select_list.push_back(makeFunction("or", std::move(name_matches)));
        builder.select_list.back()->setAlias("ignored");

        builder.select_list.push_back(stepsWithValues());
        builder.select_list.back()->setAlias("idx");
        builder.select_list.push_back(makeASTFunction("assumeNotNull", valueAtStep(col("idx"))));
        builder.select_list.back()->setAlias("value");

        builder.from_table = addSubquery(std::move(base.select_query), SQLSubqueryType::TABLE, context);
        samples = addSubquery(builder.getSelectQuery(), SQLSubqueryType::MATERIALIZED_TABLE, context);
    }

    /// Step 2: the number of identifying labels found on `v`, Prometheus enriches only series having all of them.
    String num_identifying_labels;
    {
        SelectQueryBuilder builder;
        builder.select_list.push_back(makeASTFunction("length", makeASTFunction("groupUniqArrayArray", labelNames(col("join_group")))));
        builder.from_table = samples;
        builder.where = makeASTFunction("not", col("ignored"));
        num_identifying_labels = addSubquery(builder.getSelectQuery(), SQLSubqueryType::SCALAR, context);
    }

    /// The series of `v` which can be enriched: those having all the identifying labels found on `v`, Prometheus enriches only them.
    auto enrichable = [&]
    {
        return makeASTFunction(
            "and",
            makeASTFunction("not", col("ignored")),
            makeASTFunction("notEquals", col("join_group"), lit(0u)),
            makeASTFunction(
                "equals",
                makeASTFunction("length", makeASTFunction("timeSeriesGroupToTags", col("join_group"))),
                col(num_identifying_labels)));
    };

    /// Step 3: the identifying labels of the series which can be enriched. Only the info series with these labels are read.
    String base_groups;
    {
        SelectQueryBuilder builder;
        builder.select_list.push_back(col("join_group"));
        builder.from_table = samples;
        builder.where = enrichable();
        builder.group_by = {col("join_group")};
        base_groups = addSubquery(builder.getSelectQuery(), SQLSubqueryType::MATERIALIZED_TABLE, context);
    }

    /// The selector of the info series is the user's matchers followed by the identifying labels of `v`, so that the other info series
    /// aren't read. The user's text is only concatenated, never searched.
    const String info_selector_text = info_selector.toString(*context.promql_tree);
    chassert(info_selector_text.ends_with('}'));
    ASTs selector_parts;
    selector_parts.push_back(lit(info_selector_text.substr(0, info_selector_text.size() - 1)));
    for (const char * label : {"instance", "job"})
    {
        String alternatives = addSubquery(
            [&]
            {
                SelectQueryBuilder builder;
                builder.select_list.push_back(labelValuesAlternatives(label));
                builder.from_table = base_groups;
                return builder.getSelectQuery();
            }(),
            SQLSubqueryType::SCALAR,
            context);
        selector_parts.push_back(lit(String(",") + label + "=~\""));
        selector_parts.push_back(col(alternatives));
        selector_parts.push_back(lit("\""));
    }
    selector_parts.push_back(lit("}"));
    ASTPtr selector_argument = makeFunction("concat", std::move(selector_parts));

    const bool wide_ticks = needsWideTicks(context.node_range_getter.get(function_node), context);
    const bool first_step_only = std::ranges::all_of(function_node->getArguments(), isStepInvariant);
    auto read_info = [&](SampleTimestampKind timestamp_kind)
    {
        return fromSelectorSampleTimestamps(
            function_node,
            info_selector_text,
            getInfoTimeBounds(function_node, context),
            makeASTFunction("in", makeASTFunction("timeSeriesRemoveAllTagsExcept", col(ColumnNames::Group), identifyingLabels()), col(base_groups)),
            selector_argument->clone(),
            timestamp_kind,
            context);
    };

    /// Returns the table with the grid of `info`, which has one row for each series and a value for the first step only if `v` is
    /// evaluated once (the value is repeated for every step as Prometheus does).
    auto addInfoGrid = [&](SQLQueryPiece & info)
    {
        String table = addSubquery(std::move(info.select_query), SQLSubqueryType::TABLE, context);
        if (!first_step_only)
            return table;
        /// SELECT group, arrayResize([], <count_of_time_steps>, values[1]) AS values FROM <info>
        SelectQueryBuilder builder;
        builder.select_list.push_back(col(ColumnNames::Group));
        builder.select_list.push_back(makeASTFunction(
            "arrayResize",
            lit(Array{}),
            lit(stepsInTimeSeriesRange(info.start_time, info.end_time, info.step)),
            makeASTFunction("arrayElement", col(ColumnNames::Values), lit(1u))));
        builder.select_list.back()->setAlias(ColumnNames::Values);
        builder.from_table = table;
        return addSubquery(builder.getSelectQuery(), SQLSubqueryType::TABLE, context);
    };

    auto info = read_info(wide_ticks ? SampleTimestampKind::TICKS_HIGH : SampleTimestampKind::TICKS);
    if (info.store_method == StoreMethod::EMPTY)
        return SQLQueryPiece{function_node, ResultType::INSTANT_VECTOR, StoreMethod::EMPTY};

    /// Step 4: one row for each sample of the info series, its value is the sample's timestamp in ticks.
    String info_samples;
    {
        SelectQueryBuilder builder;
        builder.select_list.push_back(col(ColumnNames::Group));
        builder.select_list.back()->setAlias("info_group");
        builder.select_list.push_back(makeASTFunction("timeSeriesRemoveAllTagsExcept", col(ColumnNames::Group), identifyingLabels()));
        builder.select_list.back()->setAlias("info_join_group");
        builder.select_list.push_back(
            makeASTFunction("ifNull", makeASTFunction("timeSeriesExtractTag", col(ColumnNames::Group), lit(kMetricName)), lit("")));
        builder.select_list.back()->setAlias("info_name");
        builder.select_list.push_back(stepsWithValues());
        builder.select_list.back()->setAlias("info_idx");

        ASTPtr timestamp = makeASTFunction("assumeNotNull", valueAtStep(col("info_idx")));
        builder.from_table = addInfoGrid(info);
        if (wide_ticks)
        {
            /// The timestamp is a tuple (the high part, the low part), the low parts are in another grid of the same series.
            auto low_info = read_info(SampleTimestampKind::TICKS_LOW);
            SelectQueryBuilder low_builder;
            low_builder.select_list.push_back(col(ColumnNames::Group));
            low_builder.select_list.back()->setAlias("low_group");
            low_builder.select_list.push_back(col(ColumnNames::Values));
            low_builder.select_list.back()->setAlias("low_values");
            low_builder.from_table = addInfoGrid(low_info);
            builder.join_table = addSubquery(low_builder.getSelectQuery(), SQLSubqueryType::TABLE, context);
            builder.join_on = makeASTFunction("equals", col(ColumnNames::Group), col("low_group"));
            timestamp = makeASTFunction(
                "tuple",
                std::move(timestamp),
                makeASTFunction("assumeNotNull", makeASTFunction("arrayElement", col("low_values"), makeASTFunction("plus", col("info_idx"), lit(1u)))));
        }
        builder.select_list.push_back(std::move(timestamp));
        builder.select_list.back()->setAlias("info_timestamp");

        info_samples = addSubquery(builder.getSelectQuery(), SQLSubqueryType::TABLE, context);
    }

    /// Step 5: the identifying labels and steps of the samples which can be enriched.
    String base_steps;
    {
        SelectQueryBuilder builder;
        builder.select_list.push_back(col("join_group"));
        builder.select_list.back()->setAlias("base_join_group");
        builder.select_list.push_back(col("idx"));
        builder.select_list.back()->setAlias("base_idx");
        builder.from_table = samples;
        builder.where = enrichable();
        builder.group_by = {col("join_group"), col("idx")};
        base_steps = addSubquery(builder.getSelectQuery(), SQLSubqueryType::TABLE, context);
    }

    /// Step 6: the newest series of each info metric at each step, a tie is an error.
    String newest_info;
    {
        SelectQueryBuilder builder;
        builder.select_list.push_back(col("info_join_group"));
        builder.select_list.push_back(col("info_idx"));
        builder.select_list.push_back(makeASTFunction("argMax", col("info_group"), col("info_timestamp")));
        builder.select_list.back()->setAlias("newest_info_group");

        builder.from_table = info_samples;
        builder.join_table = base_steps;
        builder.join_on = makeASTFunction(
            "and",
            makeASTFunction("equals", col("info_join_group"), col("base_join_group")),
            makeASTFunction("equals", col("info_idx"), col("base_idx")));

        builder.group_by = {col("info_join_group"), col("info_idx"), col("info_name")};

        builder.having = makeASTFunction(
            "equals",
            makeASTFunction(
                "throwIf",
                makeASTFunction(
                    "greater",
                    makeASTFunction(
                        "countEqual", makeASTFunction("groupArray", col("info_timestamp")), makeASTFunction("max", col("info_timestamp"))),
                    lit(1u)),
                lit("found duplicate series for info metric")),
            lit(0u));

        newest_info = addSubquery(builder.getSelectQuery(), SQLSubqueryType::TABLE, context);
    }

    /// Step 7: the info series used at each step.
    String chosen_info;
    {
        SelectQueryBuilder builder;
        builder.select_list.push_back(col("info_join_group"));
        builder.select_list.back()->setAlias("chosen_join_group");
        builder.select_list.push_back(col("info_idx"));
        builder.select_list.back()->setAlias("chosen_idx");
        builder.select_list.push_back(makeASTFunction("arraySort", makeASTFunction("groupArray", col("newest_info_group"))));
        builder.select_list.back()->setAlias("info_groups");
        builder.from_table = newest_info;
        builder.group_by = {col("info_join_group"), col("info_idx")};
        chosen_info = addSubquery(builder.getSelectQuery(), SQLSubqueryType::TABLE, context);
    }

    /// Step 8: splits each series of `v` into parts enriched by the same info series.
    String series_parts;
    {
        SelectQueryBuilder builder;
        builder.select_list.push_back(col(ColumnNames::Group));
        builder.select_list.push_back(col("ignored"));
        builder.select_list.push_back(col("info_groups"));
        builder.select_list.push_back(makeASTFunction("groupArray", col("idx")));
        builder.select_list.back()->setAlias("idxs");
        builder.select_list.push_back(makeASTFunction("groupArray", col("value")));
        builder.select_list.back()->setAlias("step_values");

        builder.from_table = samples;
        builder.join_kind = JoinKind::Left;
        builder.join_strictness = JoinStrictness::Any;
        builder.join_table = chosen_info;
        builder.join_on = makeASTFunction(
            "and",
            makeASTFunction("equals", col("join_group"), col("chosen_join_group")),
            makeASTFunction("equals", col("idx"), col("chosen_idx")));

        builder.group_by = {col(ColumnNames::Group), col("ignored"), col("info_groups")};
        series_parts = addSubquery(builder.getSelectQuery(), SQLSubqueryType::TABLE, context);
    }

    /// Step 9: adds the data labels, two info metrics giving a label different values is an error.
    String enriched_parts;
    {
        SelectQueryBuilder builder;
        builder.select_list.push_back(makeASTFunction(
            "if",
            makeASTFunction("empty", col("info_tags")),
            col(ColumnNames::Group),
            makeASTFunction(
                "timeSeriesTagsToGroup",
                makeASTFunction("arrayConcat", makeASTFunction("timeSeriesGroupToTags", col(ColumnNames::Group)), col("info_tags")))));
        builder.select_list.back()->setAlias(ColumnNames::NewGroup);
        builder.select_list.push_back(col("idxs"));
        builder.select_list.push_back(col("step_values"));

        ASTPtr tag_name = makeASTFunction("tupleElement", col("t"), lit(1u));
        ASTs keep_tag;
        keep_tag.push_back(makeASTFunction("notEquals", tag_name, lit(kMetricName)));
        keep_tag.push_back(makeASTFunction("not", makeASTFunction("has", labelNames(col(ColumnNames::Group)), tag_name)));
        if (!data_label_names.empty())
            keep_tag.push_back(makeASTFunction("has", lit(data_label_names), tag_name));

        builder.select_list.push_back(makeASTFunction(
            "if",
            col("ignored"),
            lit(Array{}),
            makeASTFunction(
                "arrayDistinct",
                makeASTFunction(
                    "arrayFilter",
                    lambda("t", makeFunction("and", std::move(keep_tag))),
                    makeASTFunction(
                        "arrayFlatten",
                        makeASTFunction("arrayMap", lambda("g", makeASTFunction("timeSeriesGroupToTags", col("g"))), col("info_groups")))))));
        builder.select_list.back()->setAlias("info_tags");

        builder.from_table = series_parts;

        ASTs conditions;
        conditions.push_back(makeASTFunction(
            "equals",
            makeASTFunction(
                "throwIf",
                makeASTFunction(
                    "notEquals",
                    makeASTFunction(
                        "length",
                        makeASTFunction(
                            "arrayDistinct",
                            makeASTFunction("arrayMap", lambda("t", makeASTFunction("tupleElement", col("t"), lit(1u))), col("info_tags")))),
                    makeASTFunction("length", col("info_tags"))),
                lit("conflicting label")),
            lit(0u)));

        /// A series without data labels is dropped if a data label matcher doesn't match the empty string.
        if (drop_without_data_labels)
            conditions.push_back(makeASTFunction("or", col("ignored"), makeASTFunction("notEmpty", col("info_tags"))));
        else if (drop_without_identifying_labels)
            conditions.push_back(makeASTFunction(
                "or",
                col("ignored"),
                makeASTFunction("notEmpty", col("info_tags")),
                makeASTFunction("notEquals", col(num_identifying_labels), lit(0u))));

        builder.where = (conditions.size() == 1) ? conditions[0] : makeFunction("and", std::move(conditions));
        enriched_parts = addSubquery(builder.getSelectQuery(), SQLSubqueryType::TABLE, context);
    }

    /// Step 10: collects the parts of each new series back into one grid.
    const UInt64 num_steps = stepsInTimeSeriesRange(base.start_time, base.end_time, base.step);
    SelectQueryBuilder builder;
    builder.select_list.push_back(col(ColumnNames::NewGroup));
    builder.select_list.back()->setAlias(ColumnNames::Group);
    builder.select_list.push_back(makeASTFunction(
        "arrayMap",
        makeASTFunction(
            "lambda",
            makeASTFunction("tuple", col("v"), col("p")),
            makeASTFunction("if", col("p"), col("v"), lit(Field{}))),
        addParametersToAggregateFunction(
            makeASTFunction("groupArrayInsertAtArray", col("step_values"), col("idxs")), lit(Float64{0}), lit(num_steps)),
        addParametersToAggregateFunction(
            makeASTFunction("groupArrayInsertAtArray", makeASTFunction("arrayMap", lambda("i", lit(1u)), col("idxs")), col("idxs")),
            lit(0u),
            lit(num_steps))));
    builder.select_list.back()->setAlias(ColumnNames::Values);
    builder.from_table = enriched_parts;
    builder.group_by.push_back(col(ColumnNames::NewGroup));
    builder.having = makeASTFunction(
        "equals",
        makeASTFunction(
            "timeSeriesThrowDuplicateSeriesIf",
            makeASTFunction(
                "notEquals",
                makeASTFunction("sum", makeASTFunction("length", col("idxs"))),
                makeASTFunction("uniqExactArray", col("idxs"))),
            col(ColumnNames::NewGroup)),
        lit(0u));

    SQLQueryPiece res = base;
    res.node = function_node;
    res.select_query = builder.getSelectQuery();
    return res;
}

}
