#include <Storages/TimeSeries/makeASTSelectFromTimeSeries.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/IQueryTreeNode.h>
#include <Columns/IColumn.h>
#include <Common/FieldAccurateComparison.h>
#include <Common/RegexpUtils.h>
#include <Common/SettingsChanges.h>
#include <Common/StringUtils.h>
#include <base/EnumReflection.h>
#include <Core/Field.h>
#include <Core/Joins.h>
#include <Core/Names.h>
#include <DataTypes/IDataType.h>
#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Parsers/makeASTForLogicalFunction.h>
#include <Storages/IStorage.h>
#include <Storages/KeyDescription.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>

#include <algorithm>
#include <functional>
#include <limits>
#include <optional>
#include <span>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace TimeSeriesSetting
{
    extern const TimeSeriesSettingsMap tags_to_columns;
    extern const TimeSeriesSettingsBool filter_by_min_time_and_max_time;
    extern const TimeSeriesSettingsBool store_min_time_and_max_time;
    extern const TimeSeriesSettingsBool skip_reading_samples_for_timestamp_filter;
}

namespace
{
    /// Extracts the filters that reference the specified columns.
    ASTPtr extractFilterOverInputs(
        const ActionsDAG * filter_actions_dag,
        std::span<const std::string_view> input_columns,
        std::string_view error_message_if_unconvertible)
    {
        if (!filter_actions_dag || filter_actions_dag->getOutputs().empty())
            return nullptr;

        /// If `node` is an INPUT of one of `input_columns`, returns that column.
        auto get_input_column = [&](const ActionsDAG::Node * node) -> std::optional<std::string_view>
        {
            if (node->type != ActionsDAG::ActionType::INPUT)
                return std::nullopt;
            for (auto column_name : input_columns)
            {
                if (node->result_name == column_name)
                    return column_name;
                /// A name can be qualified, e.g. `__table1.timestamp`.
                if (node->result_name.ends_with(column_name) && (node->result_name.length() > column_name.length())
                    && (node->result_name[node->result_name.length() - column_name.length() - 1] == '.'))
                    return column_name;
            }
            return std::nullopt;
        };

        /// Whether the subtree of `node` references any of `input_columns`.
        std::function<bool(const ActionsDAG::Node *)> references_input = [&](const ActionsDAG::Node * node) -> bool
        {
            if (get_input_column(node))
                return true;
            for (const auto * child : node->children)
                if (references_input(child))
                    return true;
            return false;
        };

        /// Follows ALIAS nodes down to the underlying node.
        auto unwrap_alias = [](const ActionsDAG::Node * node)
        {
            while (node->type == ActionsDAG::ActionType::ALIAS && !node->children.empty())
                node = node->children.front();
            return node;
        };

        /// Converts a node referencing only `input_columns` to AST.
        std::function<ASTPtr(const ActionsDAG::Node *)> node_to_ast = [&](const ActionsDAG::Node * node) -> ASTPtr
        {
            node = unwrap_alias(node);
            switch (node->type)
            {
                case ActionsDAG::ActionType::INPUT:
                    if (auto column_name = get_input_column(node))
                        return make_intrusive<ASTIdentifier>(String{*column_name});
                    break;
                case ActionsDAG::ActionType::COLUMN:
                    if (node->column && isColumnConst(*node->column))
                        return make_intrusive<ASTLiteral>((*node->column)[0]);
                    break;
                case ActionsDAG::ActionType::FUNCTION:
                    if (node->function_base && node->function_base->isDeterministicInScopeOfQuery())
                    {
                        ASTs args;
                        args.reserve(node->children.size());
                        for (const auto * child : node->children)
                        {
                            auto arg = node_to_ast(child);
                            if (!arg)
                                return nullptr;
                            args.push_back(std::move(arg));
                        }
                        return makeASTFunction(node->function_base->getName(), std::move(args));
                    }
                    break;
                default:
                    break;
            }
            bool throw_if_unconvertible = !error_message_if_unconvertible.empty();
            if (throw_if_unconvertible)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{}", error_message_if_unconvertible);
            return nullptr;
        };

        ASTs asts;
        for (const auto * conjunct : ActionsDAG::extractConjunctionAtoms(filter_actions_dag->getOutputs().front()))
        {
            if (references_input(conjunct))
                if (auto ast = node_to_ast(conjunct))
                    asts.push_back(std::move(ast));
        }
        return asts.empty() ? nullptr : makeASTForLogicalAnd(std::move(asts));
    }

    /// Extracts the filters that reference the filter-only `timestamp` virtual column.
    /// Returns NULL if no filter mentions `timestamp`.
    ASTPtr extractTimestampFilter(const ActionsDAG * filter_actions_dag)
    {
        static constexpr std::string_view input_columns[] = {TimeSeriesColumnNames::Timestamp};
        /// A condition mixing `timestamp` with another column, or a non-deterministic one, can't be pushed.
        std::string_view error_if_unconvertible =
            "A condition on the `timestamp` column of a TimeSeries table can only be pushed down when it is a "
            "deterministic expression over `timestamp` and constants";
        return extractFilterOverInputs(filter_actions_dag, input_columns, error_if_unconvertible);
    }

    /// Extracts the filters that reference the `tags` and `metric_name` columns.
    /// Returns NULL if no filter mentions `tags` or `metric_name`.
    ASTPtr extractTagsFilter(const ActionsDAG * filter_actions_dag)
    {
        static constexpr std::string_view input_columns[]
            = {TimeSeriesColumnNames::Tags, TimeSeriesColumnNames::MetricName};
        return extractFilterOverInputs(filter_actions_dag, input_columns, /* error_if_unconvertible = */ {});
    }

    /// Extracts the filters that reference the `metric_family` column.
    /// Returns NULL if no filter mentions `metric_family`.
    ASTPtr extractMetricFamilyFilter(const ActionsDAG * filter_actions_dag)
    {
        static constexpr std::string_view input_columns[] = {TimeSeriesColumnNames::MetricFamily};
        return extractFilterOverInputs(filter_actions_dag, input_columns, /* error_if_unconvertible = */ {});
    }


    /// Rewrites every `timeSeriesSelectorMatchTags('<selector>', tags)` call,
    /// replacing it with the AND of its constant PromQL selector's matchers over `tags['<tag>']`.
    /// Example:
    ///   for `timeSeriesSelectorMatchTags('{job="api", path=~"/v1.*", env!="prod"}', tags)` the function returns
    ///   equals(tags['job'],'api') AND startsWith(tags['path'],'/v1') AND notEquals(tags['env'],'prod')
    ASTPtr expandTimeSeriesSelector(const ASTPtr & tags_filter)
    {
        if (!tags_filter)
            return nullptr;

        using MatcherType = PrometheusQueryTree::MatcherType;

        /// The condition for one matcher as a predicate over tags['<tag>'].
        auto matcher_condition = [&](const PrometheusQueryTree::Matcher & matcher) -> ASTPtr
        {
            auto tag = makeASTFunction("arrayElement",
                make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Tags),
                make_intrusive<ASTLiteral>(matcher.label_name));

            switch (matcher.matcher_type)
            {
                case MatcherType::EQ:
                {
                    /// <tag> = '<value>'
                    return makeASTFunction("equals", tag, make_intrusive<ASTLiteral>(Field{matcher.label_value}));
                }
                case MatcherType::NE:
                {
                    /// <tag> != '<value>'
                    return makeASTFunction("notEquals", tag, make_intrusive<ASTLiteral>(Field{matcher.label_value}));
                }

                case MatcherType::RE:
                case MatcherType::NRE:
                {
                    /// <tag> =~ '<regexp>'
                    /// <tag> !~ '<regexp>'
                    bool positive = (matcher.matcher_type == MatcherType::RE);

                    /// Prometheus fully anchors a label regular expression.
                    String anchored = anchorRegularExpression(matcher.label_value,
                        /* anchor_begin = */ true, /* anchor_end = */ true);

                    /// A regular expression matching a fixed list of values (`api`, `api|server`) becomes
                    /// `=` / `!=` or `IN` / `NOT IN`, which is exactly equivalent to it.
                    if (auto values = getAllStringsMatchedByRegularExpression(anchored))
                    {
                        if (values->size() == 1)
                            return makeASTFunction(positive ? "equals" : "notEquals",
                                tag, make_intrusive<ASTLiteral>(Field{values->front()}));

                        ASTs tuple_args;
                        for (const auto & value : *values)
                            tuple_args.push_back(make_intrusive<ASTLiteral>(Field{value}));
                        return makeASTFunction(positive ? "in" : "notIn", tag, makeASTFunction("tuple", std::move(tuple_args)));
                    }

                    /// A fixed prefix of the value becomes startsWith(), which is cheaper to evaluate than
                    /// `match` and is usable by the skip indexes, which support `equals` but not `match`.
                    /// The rewrite is for `=~` only: an imperfect prefix is just a necessary condition,
                    /// and negating a necessary condition would drop values satisfying `!~ 'server[0-9]+'`.
                    if (positive)
                    {
                        RegexpFixedPrefix fixed_prefix
                            = extractFixedPrefixFromRegularExpression(anchored, /* requires_perfect_prefix = */ false);

                        if (!fixed_prefix.prefix.empty())
                        {
                            auto starts_with
                                = makeASTFunction("startsWith", tag, make_intrusive<ASTLiteral>(Field{fixed_prefix.prefix}));

                            /// A perfect prefix ('server.*') is the whole condition and replaces `match` entirely
                            /// (`match` sets RE_DOT_NL, so the trailing `$` after `.*` doesn't exclude anything).
                            /// Otherwise ('server[0-9]+') the prefix is only a necessary condition, so `match` is
                            /// kept to filter the rows exactly and startsWith() is added just for the indexes.
                            if (fixed_prefix.is_perfect)
                                return starts_with;

                            return makeASTForLogicalAnd(ASTs{std::move(starts_with),
                                makeASTFunction("match", tag, make_intrusive<ASTLiteral>(Field{anchored}))});
                        }
                    }

                    /// Anything else stays a `match` / NOT `match`.
                    auto matched = makeASTFunction("match", tag, make_intrusive<ASTLiteral>(Field{anchored}));
                    return positive ? std::move(matched) : makeASTFunction("not", std::move(matched));
                }
            }
            UNREACHABLE();
        };

        /// Expands a selector call into the AND of its matchers over tags['<tag>'].
        auto expand_selector = [&](const ASTFunction & selector_function) -> ASTPtr
        {
            String selector;
            if (selector_function.arguments && !selector_function.arguments->children.empty())
                if (const auto * literal = selector_function.arguments->children[0]->as<ASTLiteral>();
                    literal && literal->value.getType() == Field::Types::String)
                    selector = literal->value.safeGet<String>();

            PrometheusQueryTree query_tree;
            const PrometheusQueryTree::Node * root = nullptr;
            if (!selector.empty() && query_tree.tryParse(selector))
                root = query_tree.getRoot();
            if (!root || root->node_type != PrometheusQueryTree::NodeType::InstantSelector)
                return nullptr;

            ASTs conditions;
            for (const auto & matcher : static_cast<const PrometheusQueryTree::InstantSelector &>(*root).matchers)
                conditions.push_back(matcher_condition(matcher));

            if (conditions.empty())
                return make_intrusive<ASTLiteral>(Field{static_cast<UInt64>(1)});
            return makeASTForLogicalAnd(std::move(conditions));
        };

        std::function<ASTPtr(const ASTPtr &)> rewrite = [&](const ASTPtr & ast) -> ASTPtr
        {
            if (const auto * function = ast->as<ASTFunction>())
            {
                if (function->name == "timeSeriesSelectorMatchTags")
                    if (auto expanded = expand_selector(*function))
                        return expanded;

                auto result = makeASTFunction(function->name);
                if (function->arguments)
                    for (const auto & argument : function->arguments->children)
                        result->arguments->children.push_back(rewrite(argument));
                return result;
            }
            return ast->clone();
        };

        return rewrite(tags_filter);
    }


    /// Splits a `filter` into its top-level `AND` conjuncts, flattening nested ANDs (`a AND (b AND c)` -> a, b, c);
    /// for a filter that isn't an AND operator the function returns a single conjunct.
    /// Handling conjuncts separately keeps each analyzable by KeyCondition once the result is pushed onto a scan.
    ASTs splitConjuncts(const ASTPtr & filter)
    {
        ASTs conjuncts;
        std::function<void(const ASTPtr &)> collect = [&](const ASTPtr & ast)
        {
            const auto * function_ast = ast->as<ASTFunction>();
            if (function_ast && function_ast->name == "and" && function_ast->arguments)
                for (const auto & argument : function_ast->arguments->children)
                    collect(argument);
            else
                conjuncts.push_back(ast);
        };
        collect(filter);
        chassert(!conjuncts.empty());
        return conjuncts;
    }

    /// Rewrites every reference to the `metric_name` column as `tags['__name__']`, so that the whole tags filter
    /// is expressed over `tags[...]` and `prepareTagsFilterForPushDown` can map it onto the inner columns uniformly.
    ASTPtr replaceMetricNameWithTagsElement(const ASTPtr & tags_filter)
    {
        if (!tags_filter)
            return nullptr;

        std::function<ASTPtr(const ASTPtr &)> replace = [&](const ASTPtr & ast) -> ASTPtr
        {
            if (const auto * identifier = ast->as<ASTIdentifier>();
                identifier && identifier->name() == TimeSeriesColumnNames::MetricName)
                return makeASTFunction("arrayElement",
                    make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Tags),
                    make_intrusive<ASTLiteral>(TimeSeriesTagNames::MetricName));

            if (const auto * function = ast->as<ASTFunction>())
            {
                auto result = makeASTFunction(function->name);
                if (function->arguments)
                    for (const auto & argument : function->arguments->children)
                        result->arguments->children.push_back(replace(argument));
                return result;
            }
            return ast->clone();
        };

        return replace(tags_filter);
    }

    /// Rewrites a tags filter so it can be pushed onto the inner "tags" table scan.
    /// Example:
    ///   If tag `job` has a dedicated column in `columns_by_tags`, and tag `env` doesn't, then for the filter
    ///     equals(tags['__name__'],'http') AND startsWith(tags['job'],'api') AND equals(tags['env'],'prod')
    ///   the function returns
    ///     (equals(metric_name,'http') OR metric_name='' OR isNull(metric_name))
    ///     AND (startsWith(job,'api')  OR job='' OR isNull(job))
    ///     AND equals(tags['env'],'prod')
    ASTPtr prepareTagsFilterForPushDown(const ASTPtr & tags_filter,
                                        const std::unordered_map<String, String> & columns_by_tags)
    {
        if (!tags_filter)
            return nullptr;

        ASTs conjuncts = splitConjuncts(tags_filter);

        /// Rewrite `ast`: replace each column-backed `tags['<tag>']` with its dedicated column and append
        /// that column to `substituted_columns`.
        std::function<ASTPtr(const ASTPtr &, std::vector<String> &)> substitute_columns =
            [&](const ASTPtr & ast, std::vector<String> & substituted_columns) -> ASTPtr
        {
            if (const auto * function = ast->as<ASTFunction>())
            {
                /// Replace `tags['<tag>']` with the tag's own inner column, if it has one.
                if (function->name == "arrayElement" && function->arguments && function->arguments->children.size() == 2)
                {
                    const auto * map = function->arguments->children[0]->as<ASTIdentifier>();
                    const auto * key = function->arguments->children[1]->as<ASTLiteral>();
                    if (map && map->name() == TimeSeriesColumnNames::Tags
                        && key && key->value.getType() == Field::Types::String)
                    {
                        if (auto it = columns_by_tags.find(key->value.safeGet<String>());
                            it != columns_by_tags.end())
                        {
                            substituted_columns.push_back(it->second);
                            return make_intrusive<ASTIdentifier>(it->second);
                        }
                    }
                }

                auto result = makeASTFunction(function->name);
                if (function->arguments)
                    for (const auto & argument : function->arguments->children)
                        result->arguments->children.push_back(substitute_columns(argument, substituted_columns));
                return result;
            }
            return ast->clone();
        };

        /// Rewrite each conjunct: substitute column-backed `tags['<tag>']` with their dedicated columns,
        /// add empty/NULL checks for those columns.
        ASTs result_conjuncts;
        std::vector<String> substituted_columns;
        for (const auto & conjunct : conjuncts)
        {
            substituted_columns.clear();
            ASTPtr result_conjunct = substitute_columns(conjunct, substituted_columns);

            std::sort(substituted_columns.begin(), substituted_columns.end());
            substituted_columns.erase(std::unique(substituted_columns.begin(), substituted_columns.end()),
                                      substituted_columns.end());

            if (!substituted_columns.empty())
            {
                /// Transform `result_conjunct -> result_conjunct OR <col> = '' OR isNull(<col>)`
                /// for each substituted column. We need to do this because some tags are allowed
                /// to be in the tags map column and not in their dedicated columns.
                ASTs branches;
                branches.push_back(std::move(result_conjunct));
                for (const auto & column : substituted_columns)
                {
                    branches.push_back(makeASTFunction("equals",
                        make_intrusive<ASTIdentifier>(column), make_intrusive<ASTLiteral>(Field{String{}})));
                    branches.push_back(makeASTFunction("isNull", make_intrusive<ASTIdentifier>(column)));
                }
                result_conjunct = makeASTForLogicalOr(std::move(branches));
            }

            result_conjuncts.push_back(std::move(result_conjunct));
        }

        return makeASTForLogicalAnd(std::move(result_conjuncts));
    }


    /// A comparison of a column to a constant, normalized to the column on the left. Named after the matching
    /// ClickHouse comparison functions so `magic_enum` can convert between the enum value and the function name.
    enum class Comparison { equals, notEquals, greater, greaterOrEquals, less, lessOrEquals };

    /// The negated comparison, for example `NOT (a < b)` is `a >=  b`.
    Comparison negate(Comparison cmp)
    {
        switch (cmp)
        {
            case Comparison::equals:          return Comparison::notEquals;
            case Comparison::notEquals:       return Comparison::equals;
            case Comparison::greater:         return Comparison::lessOrEquals;
            case Comparison::greaterOrEquals: return Comparison::less;
            case Comparison::less:            return Comparison::greaterOrEquals;
            case Comparison::lessOrEquals:    return Comparison::greater;
        }
        UNREACHABLE();
    }

    /// If `node` is a `timestamp <cmp> <literal>` comparison (in either argument order), returns the comparison
    /// together with the literal constant, normalized to `timestamp` on the left (so `<const> <cmp> timestamp` is
    /// reversed via swapArguments). Returns nullopt otherwise (not a comparison function, both/neither side is
    /// `timestamp`, or the other side isn't a literal).
    std::optional<std::pair<Comparison, Field>> extractTimestampComparison(const ASTPtr & node)
    {
        const auto * function = node->as<ASTFunction>();
        if (!function || !function->arguments || function->arguments->children.size() != 2)
            return std::nullopt;

        const auto & lhs = function->arguments->children[0];
        const auto & rhs = function->arguments->children[1];

        auto is_timestamp = [](const ASTPtr & ast)
        {
            const auto * identifier = ast->as<ASTIdentifier>();
            return identifier && identifier->name() == TimeSeriesColumnNames::Timestamp;
        };

        /// The same comparison with its two operands swapped,
        /// for example `a < b` is equivalent to `b > a`.
        auto swap_arguments = [](Comparison cmp) -> Comparison
        {
            switch (cmp)
            {
                case Comparison::equals:          return Comparison::equals;
                case Comparison::notEquals:       return Comparison::notEquals;
                case Comparison::greater:         return Comparison::less;
                case Comparison::greaterOrEquals: return Comparison::lessOrEquals;
                case Comparison::less:            return Comparison::greater;
                case Comparison::lessOrEquals:    return Comparison::greaterOrEquals;
            }
            UNREACHABLE();
        };

        const ASTLiteral * literal = nullptr;
        bool timestamp_on_left = false;
        if (is_timestamp(lhs) && (literal = rhs->as<ASTLiteral>()))
            timestamp_on_left = true;
        else if (is_timestamp(rhs) && (literal = lhs->as<ASTLiteral>()))
            timestamp_on_left = false;
        else
            return std::nullopt;

        auto comparison = magic_enum::enum_cast<Comparison>(function->name);
        if (!comparison)
            return std::nullopt;

        return std::make_pair(timestamp_on_left ? *comparison : swap_arguments(*comparison), literal->value);
    }


    /// Derives a predicate on `min_time` or `max_time` from the `timestamp_filter` AST produced by
    /// extractTimestampFilter(). The function can return NULL if it can't derive any filter on
    /// `min_time` or `max_time` from the timestamp filter.
    /// Example:
    ///   for `(A <= timestamp) AND (timestamp <= B)` the function returns
    ///   (max_time >= A OR isNull(max_time)) AND (min_time <= B OR isNull(min_time))
    ASTPtr timestampFilterToMinMaxTimeFilter(const ASTPtr & timestamp_filter)
    {
        if (!timestamp_filter)
            return nullptr;

        /// Returns (`column` cmp `value`) OR isNull(`column`).
        /// Here `column` is either `min_time` or `max_time`. The `min_time` and `max_time` are Nullable,
        /// if it's NULL it means "no check", that's why we add isNull(`column`) to the condition.
        auto make_bound_condition = [](const char * column, Comparison cmp, const Field & value) -> ASTPtr
        {
            return makeASTFunction("or",
                makeASTFunction(magic_enum::enum_name(cmp), make_intrusive<ASTIdentifier>(column), make_intrusive<ASTLiteral>(value)),
                makeASTFunction("isNull", make_intrusive<ASTIdentifier>(column)));
        };

        /// Derive a condition on `min_time`/`max_time` from condition `timestamp` cmp `value.
        /// The function can returns NULL which means "no check".
        auto make_bound_conditions = [&](Comparison cmp, const Field & value) -> ASTPtr
        {
            switch (cmp)
            {
                case Comparison::greater:
                case Comparison::greaterOrEquals:
                    return make_bound_condition(TimeSeriesColumnNames::MaxTime, cmp, value);

                case Comparison::less:
                case Comparison::lessOrEquals:
                    return make_bound_condition(TimeSeriesColumnNames::MinTime, cmp, value);

                case Comparison::equals:
                    return makeASTForLogicalAnd(ASTs{
                        make_bound_condition(TimeSeriesColumnNames::MinTime, Comparison::lessOrEquals, value),
                        make_bound_condition(TimeSeriesColumnNames::MaxTime, Comparison::greaterOrEquals, value)});

                case Comparison::notEquals:
                    /// `timestamp != C` can't be converted to a condition on min_time/max_time
                    /// and isn't worth it anyway because it wouldn't help us skip granules.
                    return nullptr;
            }
            UNREACHABLE();
        };

        /// We recursively walk through the boolean structure (`and`/`or`/`not`) and replace every
        /// `timestamp <cmp> <const>` leaf (cmp in `<`, `<=`, `>`, `>=`, `=`, either argument order) with the
        /// necessary condition on `min_time`/`max_time` for a series to have a sample satisfying that leaf.
        /// convert_to_min_max() can return NULL if it can't derive any filter on `min_time` or `max_time`.
        std::function<ASTPtr(const ASTPtr &, bool)> convert_to_min_max = [&](const ASTPtr & node, bool negated) -> ASTPtr
        {
            const auto * function_ast = node->as<ASTFunction>();
            if (!function_ast || !function_ast->arguments)
                return nullptr;

            const auto & args = function_ast->arguments->children;

            if (function_ast->name == "not" && args.size() == 1)
                return convert_to_min_max(args[0], !negated);

            if (function_ast->name == "and" || function_ast->name == "or")
            {
                /// Under negation `and` becomes `or` and vice versa.
                bool is_and = (function_ast->name == "and") != negated;
                ASTs operands;
                for (const auto & arg : args)
                {
                    if (auto converted_to_min_max = convert_to_min_max(arg, negated))
                    {
                        operands.push_back(std::move(converted_to_min_max));
                    }
                    else if (!is_and)
                    {
                        /// For a filter like
                        /// `(timestamp BETWEEN a AND b) AND f(timestamp)`
                        /// we derive a condition for `min_time` or `max_time` from the convertable part
                        /// `timestamp BETWEEN a AND b`, so we use condition
                        /// `(max_time >= a OR isNull(max_time)) AND (min_time <= b OR isNull(min_time))`
                        /// for `min_time` and `max_time` to skip granules.
                        /// `f(timestamp)` will be calculated and checked later.

                        /// For a filter like
                        /// `(timestamp BETWEEN a AND b) OR f(timestamp)`
                        /// we can't really derive any conditions for `min_time` or `max_time`
                        /// so we don't checks any condition on `min_time` and `max_time`.
                        return nullptr;
                    }
                }
                if (operands.empty())
                    return nullptr;
                if (operands.size() == 1)
                    return std::move(operands.front());
                return is_and ? makeASTForLogicalAnd(std::move(operands)) : makeASTForLogicalOr(std::move(operands));
            }

            /// A `timestamp <cmp> <const>` comparison (either argument order); anything else can't be converted.
            auto comparison = extractTimestampComparison(node);
            if (!comparison)
                return nullptr;
            return make_bound_conditions(negated ? negate(comparison->first) : comparison->first, comparison->second);
        };

        return convert_to_min_max(timestamp_filter, /* negated= */ false);
    }


    /// The next representable value after `value` (`value` + one unit in its integer representation), or nullopt
    /// for kinds we don't step or on overflow. Turns an open lower bound into the first value it allows.
    std::optional<Field> next(const Field & value)
    {
        switch (value.getType())
        {
            case Field::Types::Decimal64:
            {
                const auto & decimal = value.safeGet<DecimalField<Decimal64>>();
                Int64 underlying = decimal.getValue().value;
                if (underlying == std::numeric_limits<Int64>::max())
                    return std::nullopt;
                return Field(DecimalField<Decimal64>(Decimal64(underlying + 1), decimal.getScale()));
            }
            case Field::Types::UInt64:
            {
                UInt64 underlying = value.safeGet<UInt64>();
                if (underlying == std::numeric_limits<UInt64>::max())
                    return std::nullopt;
                return Field(underlying + 1);
            }
            case Field::Types::Int64:
            {
                Int64 underlying = value.safeGet<Int64>();
                if (underlying == std::numeric_limits<Int64>::max())
                    return std::nullopt;
                return Field(underlying + 1);
            }
            default:
                return std::nullopt;
        }
    }

    /// The previous representable value before `value` (`value` - one unit), or nullopt for kinds we don't step
    /// or on underflow. Turns an open upper bound into the last value it allows.
    std::optional<Field> previous(const Field & value)
    {
        switch (value.getType())
        {
            case Field::Types::Decimal64:
            {
                const auto & decimal = value.safeGet<DecimalField<Decimal64>>();
                Int64 underlying = decimal.getValue().value;
                if (underlying == std::numeric_limits<Int64>::min())
                    return std::nullopt;
                return Field(DecimalField<Decimal64>(Decimal64(underlying - 1), decimal.getScale()));
            }
            case Field::Types::UInt64:
            {
                UInt64 underlying = value.safeGet<UInt64>();
                if (underlying == 0)
                    return std::nullopt;
                return Field(underlying - 1);
            }
            case Field::Types::Int64:
            {
                Int64 underlying = value.safeGet<Int64>();
                if (underlying == std::numeric_limits<Int64>::min())
                    return std::nullopt;
                return Field(underlying - 1);
            }
            default:
                return std::nullopt;
        }
    }


    /// Compute a constant timestamp that satisfies the `timestamp_filter` predicate for every returned row.
    /// When the "samples" table is skipped (see `skip_reading_samples_for_timestamp_filter`), the read still has
    /// to expose a `timestamp` column because the planner re-applies the original `timestamp` predicate on top of
    /// the storage output.
    /// The function handles `AND`/`OR`/`NOT` of `timestamp <cmp> <const>` comparisons (cmp: `<`, `<=`, `>`, `>=`, `=`, `!=`)
    /// to find a satifying timestamp.
    /// The function can return NULL if it can't find a satisfying timestamp.
    ASTPtr findSatisfyingTimestamp(const ASTPtr & timestamp_filter, const DataTypePtr & timestamp_type)
    {
        /// `timestamp` is always a registered virtual column, so its type is known (see `createVirtuals`).
        chassert(timestamp_type);
        if (!timestamp_filter)
            return nullptr;

        /// Intervals represents a feasible set of timestamps as a union of closed intervals `[low, high]`.
        /// A missing bound (nullopt) means unbounded.
        struct Interval { std::optional<Field> low, high; };
        using Intervals = std::vector<Interval>;

        /// Intersects two intervals; returns null when the result is empty.
        auto intersect_one = [](const Interval & a, const Interval & b) -> std::optional<Interval>
        {
            Interval result;
            /// Lower bound: the greater of the two lower bounds (unbounded loses).
            if (!a.low)
                result.low = b.low;
            else if (!b.low)
                result.low = a.low;
            else
                result.low = accurateLess(*a.low, *b.low) ? b.low : a.low;
            /// Upper bound: the lesser of the two upper bounds (unbounded loses).
            if (!a.high)
                result.high = b.high;
            else if (!b.high)
                result.high = a.high;
            else
                result.high = accurateLess(*a.high, *b.high) ? a.high : b.high;
            /// Empty only when low > high (low == high means the single point `[low, low]`, not empty).
            if (result.low && result.high && accurateLess(*result.high, *result.low))
                return std::nullopt;
            return result;
        };

        auto intersect = [&](const Intervals & a, const Intervals & b)
        {
            Intervals result;
            for (const auto & x : a)
                for (const auto & y : b)
                    if (auto intersection = intersect_one(x, y))
                        result.push_back(std::move(*intersection));
            return result;
        };

        /// Collect non-empty intervals with feasible timestamps.
        std::function<std::optional<Intervals>(const ASTPtr &, bool)> collect_intervals =
            [&](const ASTPtr & node, bool negated) -> std::optional<Intervals>
        {
            const auto * function_ast = node->as<ASTFunction>();
            if (!function_ast || !function_ast->arguments)
                return std::nullopt;
            const auto & args = function_ast->arguments->children;

            if (function_ast->name == "not" && args.size() == 1)
                return collect_intervals(args[0], !negated);

            if (function_ast->name == "and" || function_ast->name == "or")
            {
                /// Under negation `and` becomes `or` and vice versa.
                bool is_and = (function_ast->name == "and") != negated;
                std::optional<Intervals> accumulated;
                for (const auto & arg : args)
                {
                    if (auto intervals = collect_intervals(arg, negated))
                    {
                        if (!accumulated)
                            accumulated = std::move(*intervals);
                        else if (is_and)
                            accumulated = intersect(*accumulated, *intervals);
                        else
                            accumulated->insert(accumulated->end(), intervals->begin(), intervals->end());  /// union
                    }
                    else
                    {
                        /// A branch that isn't a `timestamp <cmp> const` comparison (e.g. `f(timestamp)`) can't be
                        /// turned into intervals -> fall back to reading "samples".
                        /// NOTE: For `OR` (e.g. `(timestamp BETWEEN a AND b) OR f(timestamp)`) a satisfying timestamp
                        /// can still exist, but using such a satisfying timestamp would mean ignoring the explicit filter
                        /// (i.e. `f(timestamp)`) which is better not to do.
                        return std::nullopt;
                    }
                }
                return accumulated;
            }

            /// A `timestamp <cmp> <const>` comparison (either argument order); anything else can't be modeled.
            auto comparison = extractTimestampComparison(node);
            if (!comparison)
                return std::nullopt;
            Comparison cmp = negated ? negate(comparison->first) : comparison->first;
            const Field & value = comparison->second;

            /// Build closed intervals, turning open bounds into closed ones via next()/previous().
            switch (cmp)
            {
                case Comparison::equals:
                    return Intervals{Interval{value, value}};
                case Comparison::greaterOrEquals:
                    return Intervals{Interval{value, std::nullopt}};
                case Comparison::lessOrEquals:
                    return Intervals{Interval{std::nullopt, value}};
                case Comparison::greater:
                    if (auto low = next(value))
                        return Intervals{Interval{low, std::nullopt}};
                    return Intervals{};
                case Comparison::less:
                    if (auto high = previous(value))
                        return Intervals{Interval{std::nullopt, high}};
                    return Intervals{};
                case Comparison::notEquals:
                {
                    Intervals result;
                    if (auto high = previous(value))
                        result.push_back(Interval{std::nullopt, high});
                    if (auto low = next(value))
                        result.push_back(Interval{low, std::nullopt});
                    return result;
                }
            }
            UNREACHABLE();
        };

        auto intervals = collect_intervals(timestamp_filter, /* negated= */ false);
        if (!intervals)
            return nullptr;  /// `timestamp_filter` isn't AND/OR/NOT of `timestamp <cmp> <const>` comparisons.

        /// Pick one satisfying timestamp.
        auto pick_timestamp = [](const Interval & interval) -> std::optional<Field>
        {
            if (interval.low)
                return interval.low;
            if (interval.high)
                return interval.high;
            return std::nullopt;
        };

        for (const auto & interval : *intervals)
            if (auto timestamp = pick_timestamp(interval))
                return makeASTFunction("toNullable",
                    makeASTFunction("_CAST", make_intrusive<ASTLiteral>(*timestamp),
                        make_intrusive<ASTLiteral>(timestamp_type->getName())));

        return nullptr;
    }


    /// Derives a filter on the metric name tags['__name__'] from a metric_family filter if possible.
    /// `metric_name` always starts with its `metric_family` (metric_name = metric_family + a type suffix
    /// such as '', '_total', '_count', '_sum', '_bucket').
    /// Example:
    ///   for filter `metric_family = 'foo'` the function returns `startsWith(tags['__name__'], 'foo')`
    ASTPtr metricFamilyFilterToTagsFilter(const ASTPtr & metric_family_filter)
    {
        if (!metric_family_filter)
            return nullptr;

        auto is_metric_family = [](const ASTPtr & ast)
        {
            const auto * identifier = ast->as<ASTIdentifier>();
            return identifier && identifier->name() == TimeSeriesColumnNames::MetricFamily;
        };

        auto get_string_literal = [](const ASTPtr & ast) -> std::optional<String>
        {
            const auto * literal = ast->as<ASTLiteral>();
            if (literal && literal->value.getType() == Field::Types::String)
                return literal->value.safeGet<String>();
            return std::nullopt;
        };

        /// Gets a non-empty prefix that `metric_family` is known to start with:
        /// `metric_family = <prefix>` (in either argument order), or `startsWith(metric_family, <prefix>)`,
        /// or `metric_family LIKE <prefix>%`.
        auto get_metric_family_prefix = [&](const ASTPtr & conjunct) -> std::optional<String>
        {
            const auto * function = conjunct->as<ASTFunction>();
            if (!function || !function->arguments || function->arguments->children.size() != 2)
                return std::nullopt;
            const auto & args = function->arguments->children;

            std::optional<String> prefix;
            if (function->name == "equals")
            {
                if (is_metric_family(args[0]))
                    prefix = get_string_literal(args[1]);
                else if (is_metric_family(args[1]))
                    prefix = get_string_literal(args[0]);
            }
            else if (function->name == "startsWith" && is_metric_family(args[0]))
            {
                prefix = get_string_literal(args[1]);
            }
            else if (function->name == "like" && is_metric_family(args[0]))
            {
                if (auto pattern = get_string_literal(args[1]))
                    prefix = extractFixedPrefixFromLikePattern(*pattern, /* requires_perfect_prefix= */ false).prefix;
            }

            if (prefix && !prefix->empty())
                return prefix;
            return std::nullopt;
        };

        ASTs result_conjuncts;
        for (const auto & conjunct : splitConjuncts(metric_family_filter))
        {
            if (auto prefix = get_metric_family_prefix(conjunct))
            {
                /// `startsWith(tags['__name__'], <prefix>)`
                auto metric_name = makeASTFunction("arrayElement",
                    make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Tags),
                    make_intrusive<ASTLiteral>(String{TimeSeriesTagNames::MetricName}));
                result_conjuncts.push_back(makeASTFunction("startsWith", metric_name, make_intrusive<ASTLiteral>(Field{*prefix})));
            }
        }

        return result_conjuncts.empty() ? nullptr : makeASTForLogicalAnd(std::move(result_conjuncts));
    }


    /// Prepares a `metric_family` filter (from `extractMetricFamilyFilter`) to push onto the "metrics" table scan,
    /// in two steps.
    ///
    /// First, drop the top-level conjuncts that aren't safe to push inside the "metrics" `FULL JOIN`: a pushed
    /// condition removes metrics rows, which turns a series matching a removed family into an unmatched row whose
    /// reconstructed `metric_family` is empty (`join_use_nulls=0`); if the condition is true on that empty value it
    /// lets such a row slip past the outer filter. So a conjunct is kept only when it is provably false for an
    /// empty `metric_family` — the positive forms (`=`/`startsWith`/`LIKE`/`match` against a non-empty constant,
    /// and `AND`/`OR` compositions of them); negative forms (`!=`, `= ''`, `NOT ...`) are dropped. Dropping only
    /// loses granule skipping — the outer filter still applies every condition exactly.
    ///
    /// Then, rewrite the surviving conjuncts for the inner table: every reference to the outer `metric_family`
    /// column becomes the inner `metric_family_name` column. Null in (or everything dropped) -> null out.
    ASTPtr prepareMetricFamilyFilterForPushDown(const ASTPtr & metric_family_filter)
    {
        if (!metric_family_filter)
            return nullptr;

        auto is_metric_family = [](const ASTPtr & ast)
        {
            const auto * identifier = ast->as<ASTIdentifier>();
            return identifier && identifier->name() == TimeSeriesColumnNames::MetricFamily;
        };
        /// A non-empty string literal (the constant such a positive matcher compares against). An empty literal
        /// would make the matcher true on an empty `metric_family`, so it isn't accepted here.
        auto non_empty_string = [](const ASTPtr & ast) -> bool
        {
            const auto * literal = ast->as<ASTLiteral>();
            return literal && literal->value.getType() == Field::Types::String && !literal->value.safeGet<String>().empty();
        };

        /// Whether `node` is provably false when `metric_family` is the empty string. Conservative: anything not
        /// recognized returns false ("not proven safe"), so it is dropped from the push-down.
        std::function<bool(const ASTPtr &)> is_false_when_metric_family_empty = [&](const ASTPtr & node) -> bool
        {
            const auto * function = node->as<ASTFunction>();
            if (!function || !function->arguments)
                return false;
            const auto & args = function->arguments->children;

            /// `AND` is false on an empty value if any operand is; `OR` only if all operands are.
            if (function->name == "and")
                return std::any_of(args.begin(), args.end(), is_false_when_metric_family_empty);
            if (function->name == "or")
                return !args.empty() && std::all_of(args.begin(), args.end(), is_false_when_metric_family_empty);

            /// Positive matchers `<fn>(metric_family, <non-empty const>)` don't match an empty `metric_family`.
            if (args.size() == 2 && is_metric_family(args[0]) && non_empty_string(args[1])
                && (function->name == "startsWith" || function->name == "like" || function->name == "match"))
                return true;
            /// `metric_family = <non-empty const>` (in either argument order).
            if (function->name == "equals" && args.size() == 2)
                return (is_metric_family(args[0]) && non_empty_string(args[1]))
                    || (is_metric_family(args[1]) && non_empty_string(args[0]));

            return false;
        };

        ASTs safe_conjuncts;
        for (const auto & conjunct : splitConjuncts(metric_family_filter))
            if (is_false_when_metric_family_empty(conjunct))
                safe_conjuncts.push_back(conjunct);

        if (safe_conjuncts.empty())
            return nullptr;

        /// Rewrite the surviving conjuncts onto the inner `metric_family_name` column.
        std::function<ASTPtr(const ASTPtr &)> replace_metric_family_with_column = [&](const ASTPtr & ast) -> ASTPtr
        {
            if (const auto * identifier = ast->as<ASTIdentifier>();
                identifier && identifier->name() == TimeSeriesColumnNames::MetricFamily)
                return make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricFamilyName);

            if (const auto * function = ast->as<ASTFunction>())
            {
                auto result = makeASTFunction(function->name);
                if (function->arguments)
                    for (const auto & argument : function->arguments->children)
                        result->arguments->children.push_back(replace_metric_family_with_column(argument));
                return result;
            }
            return ast->clone();
        };

        return replace_metric_family_with_column(makeASTForLogicalAnd(std::move(safe_conjuncts)));
    }


    /// Wraps a single SELECT in an ASTSelectWithUnionQuery, which is the shape
    /// InterpreterSelectQueryAnalyzer accepts at the top level.
    ASTPtr wrapInUnionQuery(ASTPtr select_query)
    {
        auto list_of_selects = make_intrusive<ASTExpressionList>();
        list_of_selects->children.push_back(std::move(select_query));

        auto union_query = make_intrusive<ASTSelectWithUnionQuery>();
        union_query->list_of_selects = list_of_selects;
        union_query->children.push_back(list_of_selects);
        return union_query;
    }

    /// Maps each tag name to the inner "tags" column that stores it instead of the `tags` Map: every tag with its
    /// own column via the `tags_to_columns` setting, plus the `__name__` tag, which lives in the dedicated
    /// `metric_name` column. Including `__name__` lets callers treat it like any other column-backed tag.
    std::unordered_map<String, String> getColumnsByTags(const TimeSeriesSettings & storage_settings)
    {
        const Map & tags_to_columns = storage_settings[TimeSeriesSetting::tags_to_columns];

        std::unordered_map<String, String> columns_by_tags;
        columns_by_tags.reserve(tags_to_columns.size() + 1);
        columns_by_tags.emplace(TimeSeriesTagNames::MetricName, TimeSeriesColumnNames::MetricName);
        for (const auto & tag_name_and_column_name : tags_to_columns)
        {
            const auto & tuple = tag_name_and_column_name.safeGet<Tuple>();
            columns_by_tags.emplace(tuple.at(0).safeGet<String>(), tuple.at(1).safeGet<String>());
        }
        return columns_by_tags;
    }

    /// Builds a plain-table `ASTTableExpression` referring to `table_id`.
    ASTPtr makeTableExpression(const StorageID & table_id)
    {
        auto table_exp = make_intrusive<ASTTableExpression>();
        table_exp->database_and_table_name = make_intrusive<ASTTableIdentifier>(table_id);
        table_exp->children.push_back(table_exp->database_and_table_name);
        return table_exp;
    }

    /// Wraps a table expression (a plain table or a subquery) into an `ASTTablesInSelectQueryElement`.
    ASTPtr makeTableElement(ASTPtr table_expression)
    {
        auto table_elem = make_intrusive<ASTTablesInSelectQueryElement>();
        table_elem->table_expression = table_expression;
        table_elem->children.push_back(std::move(table_expression));
        return table_elem;
    }

    /// Wraps a table expression as the single entry of an `ASTTablesInSelectQuery` (a one-table FROM).
    ASTPtr makeSingleTableList(ASTPtr table_expression)
    {
        auto tables = make_intrusive<ASTTablesInSelectQuery>();
        tables->children.push_back(makeTableElement(std::move(table_expression)));
        return tables;
    }

    /// Builds the outer `tags` column: combines the inner `tags` Map, the metric name (as the `__name__`
    /// tag), and the tags that have their own columns via the `tags_to_columns` setting into one
    /// Map(String, String), sorted by tag name with duplicates and empty values removed.
    ASTPtr makeNormalizedTagsColumn(const std::unordered_map<String, String> & columns_by_tags)
    {
        ASTs args;
        args.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Tags));
        /// `columns_by_tags` already includes `__name__` -> `metric_name`.
        for (const auto & [tag_name, column_name] : columns_by_tags)
        {
            args.push_back(make_intrusive<ASTLiteral>(tag_name));
            args.push_back(make_intrusive<ASTIdentifier>(column_name));
        }

        auto tags = makeASTFunction("timeSeriesTagsToMap", std::move(args));
        tags->setAlias(TimeSeriesColumnNames::Tags);
        return tags;
    }

    /// Builds the inner-Map access `tags['<key>']` — the value of a tag stored in the inner "tags" Map.
    ASTPtr makeInnerTagAccess(const String & key)
    {
        return makeASTFunction("arrayElement",
            make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Tags),
            make_intrusive<ASTLiteral>(key));
    }

    /// Builds `if(empty(ifNull(<column>, '')), tags['<tag_key>'], ifNull(<column>, ''))` — a tag value that
    /// normally lives in its own `<column>` but may instead be stored in the inner `tags` Map (an empty column
    /// means "look in the Map"). The `ifNull` is required because the inner "tags" table is allowed to declare
    /// `<column>` as `Nullable(String)`: a NULL must be treated as "absent" so the value is read from the Map.
    /// Without it `empty(NULL)` is NULL and the `if` would take the column branch and return NULL.
    ASTPtr makeColumnWithMapFallback(const String & column_name, const String & tag_key)
    {
        auto column = makeASTFunction("ifNull",
            make_intrusive<ASTIdentifier>(column_name), make_intrusive<ASTLiteral>(String{}));
        return makeASTFunction("if",
            makeASTFunction("empty", column->clone()),
            makeInnerTagAccess(tag_key),
            std::move(column));
    }

    /// Builds a reduced outer `tags` column containing only `keys`, each resolved from its cheapest source
    /// instead of reconstructing the whole normalized Map. Used when the query touches `tags` only as
    /// `tags['<const key>']`: it avoids reading the other tag columns and the per-row normalization while
    /// producing the same value for each requested key — a tag with its own column from that column (falling back
    /// to the inner Map), `__name__` from `metric_name` (falling back to the inner Map), any other tag from the inner Map.
    ASTPtr makeReducedTagsColumn(const std::unordered_map<String, String> & columns_by_tags,
                                 const NameSet & keys)
    {
        ASTs map_args;
        for (const auto & key : keys)
        {
            ASTPtr value;
            if (auto it = columns_by_tags.find(key); it != columns_by_tags.end())
                value = makeColumnWithMapFallback(it->second, key);
            else
                value = makeInnerTagAccess(key);

            map_args.push_back(make_intrusive<ASTLiteral>(key));
            /// The full reconstruction yields Map(String, String); `toString` keeps that even when a tag
            /// column has another string type (e.g. LowCardinality(String)).
            map_args.push_back(makeASTFunction("toString", std::move(value)));
        }

        auto tags = makeASTFunction("map", std::move(map_args));
        tags->setAlias(TimeSeriesColumnNames::Tags);
        return tags;
    }

    /// Builds the outer `tags` column: the reduced form when the query only accesses specific constant keys
    /// (`requested_tags`), otherwise the full normalized Map.
    ASTPtr makeTagsColumn(const std::unordered_map<String, String> & columns_by_tags,
                          const NameSet & requested_tags)
    {
        if (!requested_tags.empty())
            return makeReducedTagsColumn(columns_by_tags, requested_tags);
        return makeNormalizedTagsColumn(columns_by_tags);
    }

    /// Builds the outer `metric_name` column. The metric name is normally stored in the inner `metric_name`
    /// column, but it can instead live in the `tags` Map under `__name__` (e.g. a row inserted directly into
    /// the inner tags table with an empty `metric_name` column), so fall back to that.
    ASTPtr makeMetricNameColumn()
    {
        auto metric_name = makeColumnWithMapFallback(TimeSeriesColumnNames::MetricName, TimeSeriesTagNames::MetricName);
        metric_name->setAlias(TimeSeriesColumnNames::MetricName);
        return metric_name;
    }

    ASTPtr makeTagsTableElement(const StorageID & tags_table_id, const ASTPtr & index_filter, const ASTPtr & satisfying_timestamp);

    /// Combines two optional filter expressions with AND. Either may be null.
    ASTPtr combineFilters(ASTPtr first, ASTPtr second)
    {
        if (!first)
            return second;
        if (!second)
            return first;
        return makeASTForLogicalAnd(ASTs{std::move(first), std::move(second)});
    }

    /// Builds `SELECT <requested columns> FROM <tags_table_id>` — only the columns the caller
    /// asked for via `column_names` are returned.
    ASTPtr buildSelectQueryFromTagsOnly(const StorageID & tags_table_id, const NameSet & column_names,
                              const std::unordered_map<String, String> & columns_by_tags,
                              const NameSet & requested_tags,
                              const ASTPtr & index_filter,
                              const ASTPtr & satisfying_timestamp)
    {
        auto select_query = make_intrusive<ASTSelectQuery>();
        auto select_list = make_intrusive<ASTExpressionList>();

        if (column_names.contains(TimeSeriesColumnNames::MetricName))
            select_list->children.push_back(makeMetricNameColumn());
        if (column_names.contains(TimeSeriesColumnNames::Tags))
            select_list->children.push_back(makeTagsColumn(columns_by_tags, requested_tags));

        /// In min/max-only mode the filter-only `timestamp` is emitted directly as a `satisfying_timestamp`
        /// constant (there is no "metrics" FULL JOIN here, so no orphan rows to distinguish).
        if (satisfying_timestamp && column_names.contains(TimeSeriesColumnNames::Timestamp))
        {
            auto timestamp = satisfying_timestamp->clone();
            timestamp->setAlias(TimeSeriesColumnNames::Timestamp);
            select_list->children.push_back(std::move(timestamp));
        }

        /// If none of the "tags" table's columns are requested, we fall back to `1`.
        /// So for example `SELECT count() FROM time_series` is evaluated as
        /// `SELECT count() FROM (SELECT 1 FROM tags)`.
        if (select_list->children.empty())
            select_list->children.push_back(make_intrusive<ASTLiteral>(static_cast<UInt8>(1)));

        select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_list);

        auto tables = make_intrusive<ASTTablesInSelectQuery>();
        tables->children.push_back(makeTagsTableElement(tags_table_id, index_filter, /* satisfying_timestamp= */ nullptr));
        select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables);

        return wrapInUnionQuery(std::move(select_query));
    }

    /// Builds the `groupArray((timestamp, value)) AS time_series` aggregate expression used by
    /// the samples-side branches.
    ASTPtr makeTimeSeriesAggregate()
    {
        auto group_array = makeASTFunction("groupArray",
            makeASTFunction("tuple",
                make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp),
                make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Value)));
        group_array->setAlias(TimeSeriesColumnNames::TimeSeries);
        return group_array;
    }

    /// The internal alias for the matching timestamp inside a samples sub-select. It deliberately
    /// differs from `timestamp` so that `WHERE timestamp ...` and `groupArray((timestamp, value))` in the
    /// same sub-select still bind to the raw samples column rather than to this aggregate; a projection above
    /// renames it to `timestamp`.
    constexpr const char * matching_timestamp_alias = "__matching_timestamp";

    /// Alias of the full member metric name each "metrics" family is expanded to (family name + a type-specific
    /// suffix); the metrics `FULL JOIN` matches a series' reconstructed `metric_name` against it (see
    /// `makeDeduplicatedMetricsSubquery`).
    constexpr const char * expanded_metric_name_alias = "__expanded_metric_name";

    /// Builds `any(timestamp) AS __matching_timestamp`: a matching timestamp from a series' (already
    /// time-filtered) samples. The `timestamp` virtual column never appears in the output on its own, but the
    /// planner still re-applies the original `timestamp` condition on top of the storage read, so the read must
    /// expose a value that satisfies it. Every surviving sample does (the same condition was pushed onto the
    /// samples), so any one of them works.
    ASTPtr makeMatchingTimestamp()
    {
        /// `toNullable` so an unmatched row from the metrics FULL JOIN (a metric family with no in-window series)
        /// gets a NULL matching timestamp instead of the epoch-0 default. The planner re-applies the outer `timestamp`
        /// predicate on the storage output, and `NULL <cmp> const` is NULL, so such a row is filtered out instead
        /// of leaking through predicates that epoch 0 happens to satisfy (e.g. `timestamp <= C`, `timestamp != C`).
        auto any_timestamp = makeASTFunction("toNullable",
            makeASTFunction("any", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp)));
        any_timestamp->setAlias(matching_timestamp_alias);
        return any_timestamp;
    }

    /// Adds the per-series samples columns to a samples-side SELECT list: `groupArray((timestamp, value)) AS
    /// time_series` when the time series array is requested, and the matching `timestamp` when the
    /// filter-only `timestamp` column is requested.
    void addSamplesSelectList(ASTExpressionList & select_list, bool need_time_series, bool need_timestamp)
    {
        if (need_time_series)
            select_list.children.push_back(makeTimeSeriesAggregate());
        if (need_timestamp)
            select_list.children.push_back(makeMatchingTimestamp());
    }

    /// Sets `FROM <samples_table_id> [WHERE <samples_filter>] GROUP BY <group_by_keys>` on a samples-side SELECT.
    /// `group_by_keys` is the "samples" sorting-key prefix up to and including `id` (so the aggregation can run in
    /// sorting-key order); it always pins `id`, so each group is a single series (or one slice of it).
    void setSamplesFromGroupBy(ASTSelectQuery & select_query, const StorageID & samples_table_id,
                               const ASTPtr & samples_filter, const ASTs & group_by_keys)
    {
        select_query.setExpression(ASTSelectQuery::Expression::TABLES, makeSingleTableList(makeTableExpression(samples_table_id)));

        if (samples_filter)
            select_query.setExpression(ASTSelectQuery::Expression::WHERE, samples_filter->clone());

        auto group_by = make_intrusive<ASTExpressionList>();
        for (const auto & key : group_by_keys)
            group_by->children.push_back(key->clone());
        select_query.setExpression(ASTSelectQuery::Expression::GROUP_BY, group_by);
    }

    /// Builds `(SELECT id, groupArray((timestamp, value)) AS time_series [, any(timestamp) AS __matching_timestamp]
    /// FROM <samples> [WHERE <samples_filter>] GROUP BY id)` wrapped in ASTSelectWithUnionQuery so it can sit
    /// inside an ASTSubquery in a JOIN. `id` is always selected as the join key.
    ASTPtr makeSamplesGroupedSubquery(const StorageID & samples_table_id, const ASTPtr & samples_filter,
                                      bool need_time_series, bool need_timestamp, const ASTs & group_by_keys)
    {
        auto select_query = make_intrusive<ASTSelectQuery>();

        auto select_list = make_intrusive<ASTExpressionList>();
        select_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID));
        addSamplesSelectList(*select_list, need_time_series, need_timestamp);
        select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_list);

        setSamplesFromGroupBy(*select_query, samples_table_id, samples_filter, group_by_keys);
        return wrapInUnionQuery(std::move(select_query));
    }

    /// Builds the samples-only branch (no Tags involvement):
    /// `SELECT groupArray((timestamp, value)) AS time_series [, any(timestamp) AS __matching_timestamp]
    ///  FROM <samples> [WHERE <samples_filter>] GROUP BY id`.
    /// When the matching timestamp is needed, an extra projection renames `__matching_timestamp` to the
    /// requested `timestamp` column in a scope that doesn't reference the raw `timestamp` column.
    ASTPtr buildSelectQueryFromSamplesOnly(const StorageID & samples_table_id, const ASTPtr & samples_filter,
                                 bool need_time_series, bool need_timestamp, const ASTs & group_by_keys)
    {
        auto grouped = make_intrusive<ASTSelectQuery>();
        auto grouped_list = make_intrusive<ASTExpressionList>();
        addSamplesSelectList(*grouped_list, need_time_series, need_timestamp);
        grouped->setExpression(ASTSelectQuery::Expression::SELECT, grouped_list);
        setSamplesFromGroupBy(*grouped, samples_table_id, samples_filter, group_by_keys);

        if (!need_timestamp)
            return wrapInUnionQuery(std::move(grouped));

        auto outer = make_intrusive<ASTSelectQuery>();
        auto outer_list = make_intrusive<ASTExpressionList>();
        if (need_time_series)
            outer_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::TimeSeries));
        auto timestamp = make_intrusive<ASTIdentifier>(matching_timestamp_alias);
        timestamp->setAlias(TimeSeriesColumnNames::Timestamp);
        outer_list->children.push_back(std::move(timestamp));
        outer->setExpression(ASTSelectQuery::Expression::SELECT, outer_list);

        auto grouped_exp = make_intrusive<ASTTableExpression>();
        grouped_exp->subquery = make_intrusive<ASTSubquery>(wrapInUnionQuery(std::move(grouped)));
        grouped_exp->subquery->setAlias("__samples");
        grouped_exp->children.push_back(grouped_exp->subquery);
        auto grouped_elem = make_intrusive<ASTTablesInSelectQueryElement>();
        grouped_elem->table_expression = grouped_exp;
        grouped_elem->children.push_back(grouped_exp);
        auto tables = make_intrusive<ASTTablesInSelectQuery>();
        tables->children.push_back(grouped_elem);
        outer->setExpression(ASTSelectQuery::Expression::TABLES, tables);

        return wrapInUnionQuery(std::move(outer));
    }

    /// Builds the SELECT list of a multi-table read: one entry per requested outer column. `time_series`
    /// and the metadata columns are resolved from the joined "samples"/"metrics" sub-selects, so the caller
    /// must join those tables whenever the corresponding columns are requested.
    ASTPtr makeJoinedSelectList(const NameSet & column_names, const std::unordered_map<String, String> & columns_by_tags,
                                const NameSet & requested_tags)
    {
        auto select_list = make_intrusive<ASTExpressionList>();
        if (column_names.contains(TimeSeriesColumnNames::MetricName))
            select_list->children.push_back(makeMetricNameColumn());
        if (column_names.contains(TimeSeriesColumnNames::Tags))
            select_list->children.push_back(makeTagsColumn(columns_by_tags, requested_tags));
        if (column_names.contains(TimeSeriesColumnNames::TimeSeries))
            select_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::TimeSeries));
        /// The filter-only `timestamp` column, when requested, is the matching timestamp from the joined samples
        /// (selected there as `__matching_timestamp` to avoid clashing with the raw column).
        if (column_names.contains(TimeSeriesColumnNames::Timestamp))
        {
            auto timestamp = make_intrusive<ASTIdentifier>(matching_timestamp_alias);
            timestamp->setAlias(TimeSeriesColumnNames::Timestamp);
            select_list->children.push_back(std::move(timestamp));
        }
        if (column_names.contains(TimeSeriesColumnNames::MetricFamily))
        {
            auto metric_family = make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricFamilyName);
            metric_family->setAlias(TimeSeriesColumnNames::MetricFamily);
            select_list->children.push_back(std::move(metric_family));
        }
        if (column_names.contains(TimeSeriesColumnNames::Type))
            select_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Type));
        if (column_names.contains(TimeSeriesColumnNames::Unit))
            select_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Unit));
        if (column_names.contains(TimeSeriesColumnNames::Help))
            select_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Help));
        return select_list;
    }

    /// Builds the table expression `(SELECT * FROM tags [WHERE <index_filter>] [LIMIT 1 BY id]) AS __tags`. When
    /// `deduplicate_by_id` is set, `LIMIT 1 BY id` deduplicates series: the "tags" table is AggregatingMergeTree, so
    /// until a background merge runs, several unmerged parts can each hold a row for the same series `id` (whose
    /// identity columns are identical across them); without this an anchored tags read would return a series once
    /// per part. It is not needed when this expression is the right side of a `SEMI LEFT JOIN … USING id` (the SEMI
    /// strictness keeps each left row once regardless of the number of right matches, so per-part rows never fan
    /// out, and the exposed identity columns are identical across them). When set, `index_filter` is applied at the
    /// scan (before `LIMIT BY`) so the primary key can still skip granules.
    ASTPtr makeTagsTableExpression(const StorageID & tags_table_id, const ASTPtr & index_filter, bool deduplicate_by_id,
                                   const ASTPtr & satisfying_timestamp)
    {
        auto inner = make_intrusive<ASTSelectQuery>();
        auto select_list = make_intrusive<ASTExpressionList>();
        select_list->children.push_back(make_intrusive<ASTAsterisk>());
        /// In min/max-only mode the "samples" table is not read, so the `timestamp` the read must expose comes
        /// from the `satisfying_timestamp` constant. It is `Nullable`, so an unmatched "metrics" FULL JOIN row
        /// (a family with no series) gets NULL here and is dropped by the re-applied `timestamp` predicate. It
        /// reuses `matching_timestamp_alias` so `makeJoinedSelectList` picks it up as the `timestamp` column.
        if (satisfying_timestamp)
        {
            auto timestamp = satisfying_timestamp->clone();
            timestamp->setAlias(matching_timestamp_alias);
            select_list->children.push_back(std::move(timestamp));
        }
        inner->setExpression(ASTSelectQuery::Expression::SELECT, select_list);
        inner->setExpression(ASTSelectQuery::Expression::TABLES, makeSingleTableList(makeTableExpression(tags_table_id)));
        if (index_filter)
            inner->setExpression(ASTSelectQuery::Expression::WHERE, index_filter->clone());

        if (deduplicate_by_id)
        {
            inner->setExpression(ASTSelectQuery::Expression::LIMIT_BY_LENGTH, make_intrusive<ASTLiteral>(static_cast<UInt8>(1)));
            auto limit_by = make_intrusive<ASTExpressionList>();
            limit_by->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID));
            inner->setExpression(ASTSelectQuery::Expression::LIMIT_BY, limit_by);
        }

        auto tags_exp = make_intrusive<ASTTableExpression>();
        tags_exp->subquery = make_intrusive<ASTSubquery>(wrapInUnionQuery(std::move(inner)));
        tags_exp->subquery->setAlias("__tags");
        tags_exp->children.push_back(tags_exp->subquery);
        return tags_exp;
    }

    /// The deduplicated "tags" subquery as a plain `FROM` element — the anchor of a tags-anchored read.
    ASTPtr makeTagsTableElement(const StorageID & tags_table_id, const ASTPtr & index_filter, const ASTPtr & satisfying_timestamp)
    {
        return makeTableElement(makeTagsTableExpression(tags_table_id, index_filter, /* deduplicate_by_id= */ true, satisfying_timestamp));
    }

    /// The deduplicated "tags" subquery as a `SEMI LEFT JOIN … USING id` element, attaching a series'
    /// `metric_name`/`tags` onto the (anchoring) samples rows. `SEMI LEFT` keeps every left (samples) row that has
    /// a tags match without fanning out and without de-duplicating the left, so it works whether the samples read
    /// produces one row per series or one row per per-bucket slice; samples with no tags row (cannot happen for
    /// well-formed data) are dropped. The alias is required by `joined_subquery_requires_alias`.
    ASTPtr makeTagsSemiJoinElement(const StorageID & tags_table_id, const ASTPtr & index_filter)
    {
        auto join = make_intrusive<ASTTableJoin>();
        join->kind = JoinKind::Left;
        join->strictness = JoinStrictness::Semi;
        auto using_list = make_intrusive<ASTExpressionList>();
        using_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID));
        join->using_expression_list = using_list;
        join->children.push_back(join->using_expression_list);

        auto tags_exp = makeTagsTableExpression(tags_table_id, index_filter, /* deduplicate_by_id= */ false,
                                                /* satisfying_timestamp= */ nullptr);
        auto tags_elem = make_intrusive<ASTTablesInSelectQueryElement>();
        tags_elem->table_join = join;
        tags_elem->table_expression = tags_exp;
        tags_elem->children.push_back(join);
        tags_elem->children.push_back(tags_exp);
        return tags_elem;
    }

    /// The "samples" grouped subquery as a plain `FROM` element — the anchor of a samples-anchored read. It groups
    /// by `group_by_keys` (the sorting-key prefix up to `id`): one row per series, or — when a per-sample key
    /// column precedes `id` — one row per per-bucket slice of a series (a faithful, re-insertable representation).
    /// As the anchor (probe side) its aggregation-in-order output can stream through the joins. The alias is
    /// required by `joined_subquery_requires_alias`.
    ASTPtr makeSamplesAnchorElement(const StorageID & samples_table_id, const ASTPtr & samples_filter,
                                    bool need_time_series, bool need_timestamp, const ASTs & group_by_keys)
    {
        auto samples_exp = make_intrusive<ASTTableExpression>();
        samples_exp->subquery = make_intrusive<ASTSubquery>(
            makeSamplesGroupedSubquery(samples_table_id, samples_filter, need_time_series, need_timestamp, group_by_keys));
        samples_exp->subquery->setAlias("__samples");
        samples_exp->children.push_back(samples_exp->subquery);
        return makeTableElement(samples_exp);
    }

    /// Builds the metadata select list for the "metrics"-only read (no join): `metric_family_name` is exposed as
    /// the outer `metric_family` column, plus the requested `type`/`unit`/`help` as raw columns. The
    /// `LIMIT 1 BY metric_family_name` in `makeMetricsSelect` keeps one whole metadata row per family, so those
    /// columns stay coherent (all from the same row) rather than being mixed by independent `any(...)`.
    ASTPtr makeMetricsSelectList(const NameSet & column_names)
    {
        auto select_list = make_intrusive<ASTExpressionList>();
        if (column_names.contains(TimeSeriesColumnNames::MetricFamily))
        {
            auto metric_family = make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricFamilyName);
            metric_family->setAlias(TimeSeriesColumnNames::MetricFamily);
            select_list->children.push_back(std::move(metric_family));
        }
        if (column_names.contains(TimeSeriesColumnNames::Type))
            select_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Type));
        if (column_names.contains(TimeSeriesColumnNames::Unit))
            select_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Unit));
        if (column_names.contains(TimeSeriesColumnNames::Help))
            select_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Help));
        return select_list;
    }

    /// Builds `(SELECT <select_list> FROM <metrics> LIMIT 1 BY metric_family_name)`. `LIMIT 1 BY` keeps one whole
    /// row per family, deduplicating the metadata rows while keeping `type`/`unit`/`help` from a single row (when
    /// a family was re-inserted with conflicting metadata, the result is one consistent record, not a mix). The
    /// "metrics" engine isn't guaranteed to be ReplacingMergeTree, so we deduplicate here rather than with FINAL.
    ASTPtr makeMetricsSelect(const StorageID & metrics_table_id, ASTPtr select_list, const ASTPtr & index_filter)
    {
        auto select_query = make_intrusive<ASTSelectQuery>();
        select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_list));
        select_query->setExpression(ASTSelectQuery::Expression::TABLES, makeSingleTableList(makeTableExpression(metrics_table_id)));
        if (index_filter)
            select_query->setExpression(ASTSelectQuery::Expression::WHERE, index_filter->clone());

        select_query->setExpression(ASTSelectQuery::Expression::LIMIT_BY_LENGTH, make_intrusive<ASTLiteral>(static_cast<UInt8>(1)));
        auto limit_by = make_intrusive<ASTExpressionList>();
        limit_by->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricFamilyName));
        select_query->setExpression(ASTSelectQuery::Expression::LIMIT_BY, limit_by);

        return wrapInUnionQuery(std::move(select_query));
    }

    /// The "metrics"-only read (no join): `metric_family_name` is exposed as the outer `metric_family` column.
    /// A `metric_family` filter is pushed onto the scan (see `extractMetricFamilyFilter`).
    ASTPtr buildSelectQueryFromMetricsOnly(const StorageID & metrics_table_id, const NameSet & column_names, const ASTPtr & metrics_filter)
    {
        return makeMetricsSelect(metrics_table_id, makeMetricsSelectList(column_names), metrics_filter);
    }

    /// The deduplicated "metrics" subquery that sits inside the multi-table read's JOIN. Metadata rows are first
    /// deduplicated to one row per family (inner `LIMIT 1 BY metric_family_name`); the outer query then expands
    /// each family into the member series names its `type` emits —
    /// `concat(metric_family_name, arrayJoin(timeSeriesMetricTypeToSuffixes(type))) AS __expanded_metric_name` — which the
    /// FULL JOIN matches against a series' `metric_name`. The expansion must be nested: an `arrayJoin` alongside
    /// `LIMIT 1 BY` in one SELECT would collapse the members back to one row per family. `type` is read
    /// unconditionally (it drives the expansion even when not a requested output column); `metric_family_name` is
    /// kept raw because `makeJoinedSelectList` reads it (and renames it to `metric_family`). `notEmpty` guards
    /// against an empty family name expanding to bare-suffix members (e.g. `_total`) that could spuriously match.
    /// Because a family's `type` decides its members, a series links only to metadata of a matching type — so a
    /// metric whose name merely ends in `_count`/`_sum`/`_total`/`_bucket` is no longer misfiled.
    ASTPtr makeDeduplicatedMetricsSubquery(const StorageID & metrics_table_id, const NameSet & column_names, const ASTPtr & metrics_filter)
    {
        auto inner_list = make_intrusive<ASTExpressionList>();
        inner_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricFamilyName));
        inner_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Type));
        if (column_names.contains(TimeSeriesColumnNames::Unit))
            inner_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Unit));
        if (column_names.contains(TimeSeriesColumnNames::Help))
            inner_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Help));

        auto inner = make_intrusive<ASTSelectQuery>();
        inner->setExpression(ASTSelectQuery::Expression::SELECT, inner_list);
        inner->setExpression(ASTSelectQuery::Expression::TABLES, makeSingleTableList(makeTableExpression(metrics_table_id)));
        inner->setExpression(ASTSelectQuery::Expression::WHERE, combineFilters(
            makeASTFunction("notEmpty", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricFamilyName)),
            metrics_filter ? metrics_filter->clone() : nullptr));
        inner->setExpression(ASTSelectQuery::Expression::LIMIT_BY_LENGTH, make_intrusive<ASTLiteral>(static_cast<UInt8>(1)));
        auto limit_by = make_intrusive<ASTExpressionList>();
        limit_by->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricFamilyName));
        inner->setExpression(ASTSelectQuery::Expression::LIMIT_BY, limit_by);

        auto inner_exp = make_intrusive<ASTTableExpression>();
        inner_exp->subquery = make_intrusive<ASTSubquery>(wrapInUnionQuery(std::move(inner)));
        inner_exp->subquery->setAlias("__metrics_families");
        inner_exp->children.push_back(inner_exp->subquery);

        auto outer_list = make_intrusive<ASTExpressionList>();
        outer_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricFamilyName));
        if (column_names.contains(TimeSeriesColumnNames::Type))
            outer_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Type));
        if (column_names.contains(TimeSeriesColumnNames::Unit))
            outer_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Unit));
        if (column_names.contains(TimeSeriesColumnNames::Help))
            outer_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Help));

        auto expanded_metric_name = makeASTFunction("concat",
            make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricFamilyName),
            makeASTFunction("arrayJoin",
                makeASTFunction("timeSeriesMetricTypeToSuffixes", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Type))));
        expanded_metric_name->setAlias(expanded_metric_name_alias);
        outer_list->children.push_back(std::move(expanded_metric_name));

        auto outer = make_intrusive<ASTSelectQuery>();
        outer->setExpression(ASTSelectQuery::Expression::SELECT, outer_list);
        outer->setExpression(ASTSelectQuery::Expression::TABLES, makeSingleTableList(inner_exp));
        return wrapInUnionQuery(std::move(outer));
    }

    /// Builds `FULL JOIN (deduplicated + expanded metrics) AS __metrics ON <metric name> = __expanded_metric_name` —
    /// attaches the metadata columns by matching a time series' name against the member names its family's `type`
    /// emits (see `makeDeduplicatedMetricsSubquery`). `<metric name>` is reconstructed with the same
    /// `tags['__name__']` fallback as the output `metric_name` column, so a series whose name lives in the inner
    /// `tags` Map still links to its metadata. The FULL JOIN keeps every "tags" row (its metadata columns are
    /// empty when no member matches) and also every "metrics" member row that no time series belongs to (its
    /// "tags"/"samples" columns are then empty). The alias is required by `joined_subquery_requires_alias`.
    ASTPtr makeMetricsFullJoinElement(const StorageID & metrics_table_id, const NameSet & column_names, const ASTPtr & metrics_filter)
    {
        auto metrics_exp = make_intrusive<ASTTableExpression>();
        metrics_exp->subquery = make_intrusive<ASTSubquery>(makeDeduplicatedMetricsSubquery(metrics_table_id, column_names, metrics_filter));
        metrics_exp->subquery->setAlias("__metrics");
        metrics_exp->children.push_back(metrics_exp->subquery);

        auto join = make_intrusive<ASTTableJoin>();
        join->kind = JoinKind::Full;
        join->strictness = JoinStrictness::All;
        join->on_expression = makeASTFunction("equals",
            makeColumnWithMapFallback(TimeSeriesColumnNames::MetricName, TimeSeriesTagNames::MetricName),
            make_intrusive<ASTIdentifier>(expanded_metric_name_alias));
        join->children.push_back(join->on_expression);

        auto metrics_elem = make_intrusive<ASTTablesInSelectQueryElement>();
        metrics_elem->table_join = join;
        metrics_elem->table_expression = metrics_exp;
        metrics_elem->children.push_back(join);
        metrics_elem->children.push_back(metrics_exp);
        return metrics_elem;
    }

    /// Builds a multi-table read. When the "samples" table is read it anchors on samples: the samples-grouped
    /// subquery is the `FROM`, `tags` is attached by `SEMI LEFT JOIN … USING id` (so the aggregation-in-order
    /// output streams through the join as the probe side and every per-bucket slice is kept), and `metrics` by a
    /// `FULL JOIN` on top. When samples are not read it anchors on `tags` (`tags [FULL JOIN metrics]`). `tags` is
    /// always read — it bridges "samples" (by `id`) and "metrics" (by the family computed from `metric_name`).
    ASTPtr buildSelectQueryFromMultipleTables(
        const StorageID & tags_table_id,
        const std::optional<StorageID> & samples_table_id,
        const std::optional<StorageID> & metrics_table_id,
        const NameSet & column_names,
        const std::unordered_map<String, String> & columns_by_tags,
        const NameSet & requested_tags,
        const ASTPtr & tags_filter,
        const ASTPtr & samples_filter,
        const ASTPtr & metrics_filter,
        const ASTs & samples_group_by,
        const ASTPtr & satisfying_timestamp)
    {
        auto select_query = make_intrusive<ASTSelectQuery>();
        select_query->setExpression(ASTSelectQuery::Expression::SELECT,
            makeJoinedSelectList(column_names, columns_by_tags, requested_tags));

        auto tables = make_intrusive<ASTTablesInSelectQuery>();
        if (samples_table_id)
        {
            /// Samples-anchored: samples are the (streamed) probe side, tags/metrics the smaller build sides.
            tables->children.push_back(makeSamplesAnchorElement(*samples_table_id, samples_filter,
                column_names.contains(TimeSeriesColumnNames::TimeSeries),
                column_names.contains(TimeSeriesColumnNames::Timestamp), samples_group_by));
            tables->children.push_back(makeTagsSemiJoinElement(tags_table_id, tags_filter));
        }
        else
        {
            tables->children.push_back(makeTagsTableElement(tags_table_id, tags_filter, satisfying_timestamp));
        }
        if (metrics_table_id)
            tables->children.push_back(makeMetricsFullJoinElement(*metrics_table_id, column_names, metrics_filter));
        select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables);

        return wrapInUnionQuery(std::move(select_query));
    }

    /// Derives the samples `GROUP BY` from the "samples" table's sorting key: the prefix up to and including `id`,
    /// so the aggregation can run in sorting-key order. `id` (the series identity) ends the prefix and pins each
    /// group to a single series; a series-level key column before it (e.g. `metric_name`) is constant per series,
    /// while a per-sample column (e.g. `toStartOfHour(timestamp)`) splits the series into one row per distinct
    /// value — a faithful, re-insertable representation that the samples-anchored `SEMI LEFT JOIN` keeps. Falls
    /// back to `GROUP BY id` (not a sorting-key prefix, so no in-order aggregation) when `id` is not in the key.
    ASTs buildSamplesGroupBy(const StorageTimeSeries & storage, const ContextPtr & context)
    {
        auto samples_table = storage.getTargetTable(ViewTarget::Samples, context);
        auto samples_metadata = samples_table->getInMemoryMetadataPtr(context, false);
        const auto & sorting_key = samples_metadata->getSortingKey();
        ASTs group_by;
        if (sorting_key.expression_list_ast)
        {
            for (const auto & key : sorting_key.expression_list_ast->children)
            {
                group_by.push_back(key->clone());
                if (const auto * identifier = key->as<ASTIdentifier>();
                    identifier && identifier->name() == TimeSeriesColumnNames::ID)
                    return group_by;
            }
        }
        return {make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID)};
    }

    /// Recursively checks that every use of the storage's `tags` column is `arrayElement(tags, '<const>')`,
    /// collecting the constant keys into `requested_tags`. Returns false on any other use of `tags`.
    bool findRequestedTagsImpl(const IQueryTreeNode & node, const IQueryTreeNode & column_source, NameSet & requested_tags)
    {
        /// Returns true if `checked_node` is this table's `tags` Map column (and not, say, a column named
        /// `tags` from a different table expression in the query).
        auto is_tags_column = [&](const IQueryTreeNode & checked_node)
        {
            const auto * column = checked_node.as<ColumnNode>();
            return column && column->getColumnName() == TimeSeriesColumnNames::Tags
                && column->getColumnSource().get() == &column_source;
        };

        if (const auto * function = node.as<FunctionNode>(); function && function->getFunctionName() == "arrayElement")
        {
            const auto & arguments = function->getArguments().getNodes();
            if (arguments.size() == 2 && is_tags_column(*arguments[0]))
            {
                const auto * key = arguments[1]->as<ConstantNode>();
                if (key && key->getValue().getType() == Field::Types::String)
                {
                    requested_tags.insert(key->getValue().safeGet<String>());
                    return true;  /// a keyed access — don't descend into the `tags` argument
                }
                return false;  /// arrayElement(tags, <non-const>) — key unknown
            }
        }

        if (is_tags_column(node))
            return false;  /// `tags` used some other way (whole Map, mapKeys, ...)

        for (const auto & child : node.getChildren())
        {
            /// A query-tree node can have null children (optional clauses); skip them.
            if (child && !findRequestedTagsImpl(*child, column_source, requested_tags))
                return false;
        }
        return true;
    }

    /// If the query reads the `tags` column only as `tags['<const key>']`, returns those keys so the read can
    /// build a reduced `tags` column; otherwise returns an empty set (the full normalized Map must be built).
    NameSet findRequestedTags(const SelectQueryInfo & query_info)
    {
        NameSet requested_tags;
        if (!query_info.query_tree || !query_info.table_expression
            || !findRequestedTagsImpl(*query_info.query_tree, *query_info.table_expression, requested_tags))
            return {};

        return requested_tags;
    }
}


ASTPtr makeASTSelectFromTimeSeries(
    const StorageTimeSeries & storage,
    const NameSet & requested_columns,
    const SelectQueryInfo & query_info,
    const ContextPtr & context)
{
    /// `timestamp` is a filter-only virtual column.
    /// Filtering by `timestamp` normally requires the "samples" table (unless served from `min_time`/`max_time`).
    bool need_timestamp = requested_columns.contains(TimeSeriesColumnNames::Timestamp);
    bool need_time_series = requested_columns.contains(TimeSeriesColumnNames::TimeSeries);

    bool need_tags = requested_columns.contains(TimeSeriesColumnNames::MetricName)
                  || requested_columns.contains(TimeSeriesColumnNames::Tags);

    bool need_metrics = requested_columns.contains(TimeSeriesColumnNames::MetricFamily)
                     || requested_columns.contains(TimeSeriesColumnNames::Type)
                     || requested_columns.contains(TimeSeriesColumnNames::Unit)
                     || requested_columns.contains(TimeSeriesColumnNames::Help);

    auto storage_settings = storage.getStorageSettings();

    /// A condition on the filter-only `timestamp` column becomes an exact predicate on the "samples" table
    /// and (if filter_by_min_time_and_max_time == true) a range predicate on the "tags" table.
    const ActionsDAG * filter_actions_dag = query_info.filter_actions_dag.get();

    /// The `timestamp` filter is shared by the "samples" and "tags" inner tables.
    ASTPtr timestamp_filter = extractTimestampFilter(filter_actions_dag);

    /// The `metric_family` filter is shared by the "tags" and "metrics" inner tables.
    ASTPtr metric_family_filter = extractMetricFamilyFilter(filter_actions_dag);

    /// When `timestamp` is used only as a filter (not selected, and `time_series` not requested), we can skip the
    /// heavy "samples" table and rely on the "tags" `min_time`/`max_time` range filter alone (a coarser
    /// over-approximation). The re-applied outer `timestamp` predicate still needs a `timestamp` column, so we
    /// emit a constant that satisfies it (`satisfying_timestamp`); we engage only when such a constant exists.
    ASTPtr satisfying_timestamp;
    if (need_timestamp && !need_time_series && timestamp_filter
        && (*storage_settings)[TimeSeriesSetting::skip_reading_samples_for_timestamp_filter]
        && (*storage_settings)[TimeSeriesSetting::filter_by_min_time_and_max_time])
    {
        DataTypePtr timestamp_type;
        auto metadata = storage.getInMemoryMetadataPtr(context, /* bypass_metadata_cache= */ false);
        for (const auto & virtual_column : metadata->virtuals)
            if (virtual_column.name == TimeSeriesColumnNames::Timestamp)
                timestamp_type = virtual_column.type;
        satisfying_timestamp = findSatisfyingTimestamp(timestamp_filter, timestamp_type);
    }
    bool min_max_only = static_cast<bool>(satisfying_timestamp);

    bool need_samples = need_time_series || (need_timestamp && !min_max_only);

    /// If we read both "samples" and "metrics" tables then we also need to read the "tags" table as a bridge between them.
    /// If we read neither "samples" nor "metrics" tables then we need to read the "tags" table even if it's not requested
    /// (so that `SELECT count() FROM time_series` returns the number of time series).
    if (need_samples == need_metrics)
        need_tags = true;

    /// In min/max-only mode the "tags" table carries the authoritative `min_time`/`max_time` filter and the
    /// `satisfying_timestamp`, and anchors the "metrics" FULL JOIN, so it must always be read (otherwise
    /// `SELECT metric_family … WHERE timestamp…` would skip the time window entirely).
    if (min_max_only)
        need_tags = true;

    /// Collect information about each target table we're going to read.
    std::optional<StorageID> samples_table_id;
    ASTs samples_group_by;
    ASTPtr samples_filter;

    if (need_samples)
    {
        samples_table_id = storage.getTargetTableID(ViewTarget::Samples, context);
        samples_filter = timestamp_filter;
        samples_group_by = buildSamplesGroupBy(storage, context);
    }

    std::optional<StorageID> tags_table_id;
    ASTPtr tags_filter;
    /// `requested_tags` is non-empty only if some specific tags are requested
    /// (for example `SELECT tags['job'] FROM time_series`).
    NameSet requested_tags;
    /// Setting `tags_to_columns`.
    std::unordered_map<String, String> columns_by_tags;

    if (need_tags)
    {
        tags_table_id = storage.getTargetTableID(ViewTarget::Tags, context);
        requested_tags = findRequestedTags(query_info);
        columns_by_tags = getColumnsByTags(*storage_settings);

        /// The "tags" scan predicate: the metric_name/tag conditions (`extractTagsFilter` +
        /// `expandTimeSeriesSelector` + `replaceMetricNameWithTagsElement` normalize them to `tags['<tag>']`) plus
        /// `metric_name` prefixes derived from the `metric_family` filter (`metricFamilyFilterToTagsFilter`); then
        /// `prepareTagsFilterForPushDown` maps each tag with its own column onto that column so the primary key can
        /// skip granules, combined with the `min_time`/`max_time` range from the `timestamp` filter (when enabled).
        auto tags_conditions = combineFilters(
            replaceMetricNameWithTagsElement(expandTimeSeriesSelector(extractTagsFilter(filter_actions_dag))),
            metricFamilyFilterToTagsFilter(metric_family_filter));
        tags_filter = prepareTagsFilterForPushDown(tags_conditions, columns_by_tags);

        if ((*storage_settings)[TimeSeriesSetting::filter_by_min_time_and_max_time] && timestamp_filter)
            tags_filter = combineFilters(std::move(tags_filter), timestampFilterToMinMaxTimeFilter(timestamp_filter));
    }

    std::optional<StorageID> metrics_table_id;
    ASTPtr metrics_filter;
    if (need_metrics)
    {
        metrics_table_id = storage.getTargetTableID(ViewTarget::Metrics, context);
        metrics_filter = prepareMetricFamilyFilterForPushDown(metric_family_filter);
    }

    /// Single-table reads (no join).
    if (need_samples && !need_tags && !need_metrics)
        return buildSelectQueryFromSamplesOnly(*samples_table_id, samples_filter,
                                            requested_columns.contains(TimeSeriesColumnNames::TimeSeries), need_timestamp,
                                            samples_group_by);

    if (need_tags && !need_samples && !need_metrics)
        return buildSelectQueryFromTagsOnly(*tags_table_id, requested_columns, columns_by_tags, requested_tags, tags_filter, satisfying_timestamp);

    if (need_metrics && !need_tags && !need_samples)
        return buildSelectQueryFromMetricsOnly(*metrics_table_id, requested_columns, metrics_filter);

    /// Multi-table reads: anchored on "samples" when it is read, otherwise on "tags".
    chassert(need_tags);
    return buildSelectQueryFromMultipleTables(*tags_table_id, samples_table_id, metrics_table_id, requested_columns,
                                           columns_by_tags, requested_tags, tags_filter, samples_filter, metrics_filter, samples_group_by,
                                           satisfying_timestamp);
}

SettingsChanges getSettingsForSelectFromTimeSeries()
{
    SettingsChanges changes;

    /// The outer TimeSeries columns are non-Nullable, and the reconstruction relies on the default value (empty
    /// string / empty array), not NULL, for an unmatched join row. `join_use_nulls=1` would instead make the
    /// unmatched side of the `FULL JOIN` with "metrics" produce NULLs (a series with no metadata row, or an
    /// orphan metric family with no series) — injecting NULLs into the non-Nullable outer columns.
    changes.emplace_back("join_use_nulls", Field{false});

    /// `aggregate_functions_null_for_empty=1` rewrites every aggregate to its `...OrNull` variant, so
    /// `groupArray((timestamp, value)) AS time_series` would become Nullable and an empty group would yield NULL
    /// instead of an empty array.
    changes.emplace_back("aggregate_functions_null_for_empty", Field{false});

    /// The generated query uses SEMI / FULL joins, which the merge-join algorithms (`full_sorting_merge`,
    /// `partial_merge`) do not implement. Pin `join_algorithm` to the hash family so a caller's setting can't make
    /// the read throw NOT_IMPLEMENTED: `hash` is the final fallback and supports every join kind, while
    /// `parallel_hash` keeps the faster parallel build of the join's right side where applicable.
    changes.emplace_back("join_algorithm", Field{"parallel_hash,hash"});

    /// The "samples" read groups by the sorting-key prefix up to `id` (see buildSamplesGroupBy),
    /// so the aggregation can stream in sorting-key order instead of building a full hash table.
    changes.emplace_back("optimize_aggregation_in_order", Field{true});

    return changes;
}

}
