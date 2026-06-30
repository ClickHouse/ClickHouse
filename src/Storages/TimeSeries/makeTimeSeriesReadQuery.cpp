#include <Storages/TimeSeries/makeTimeSeriesReadQuery.h>

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <Core/Joins.h>
#include <Core/Names.h>
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
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>

#include <optional>


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
}

namespace
{
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

    /// Reads the `tags_to_columns` setting into a list of (tag_name, column_name) pairs — the tags that
    /// the "tags" table stores in their own columns instead of in the `tags` Map.
    std::vector<std::pair<String, String>> getPromotedTagColumns(const StorageTimeSeries & storage)
    {
        auto storage_settings = storage.getStorageSettings();
        const Map & tags_to_columns = (*storage_settings)[TimeSeriesSetting::tags_to_columns];

        std::vector<std::pair<String, String>> tag_columns;
        tag_columns.reserve(tags_to_columns.size());
        for (const auto & tag_name_and_column_name : tags_to_columns)
        {
            const auto & tuple = tag_name_and_column_name.safeGet<Tuple>();
            tag_columns.emplace_back(tuple.at(0).safeGet<String>(), tuple.at(1).safeGet<String>());
        }
        return tag_columns;
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
    /// tag), and the tags promoted to their own columns by the `tags_to_columns` setting into one
    /// Map(String, String), sorted by tag name with duplicates and empty values removed.
    ASTPtr makeNormalizedTagsColumn(const std::vector<std::pair<String, String>> & tag_columns)
    {
        ASTs args;
        args.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Tags));
        args.push_back(make_intrusive<ASTLiteral>(String{TimeSeriesTagNames::MetricName}));
        args.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricName));
        for (const auto & [tag_name, column_name] : tag_columns)
        {
            args.push_back(make_intrusive<ASTLiteral>(tag_name));
            args.push_back(make_intrusive<ASTIdentifier>(column_name));
        }

        auto tags = makeASTFunction("timeSeriesNormalizeTags", std::move(args));
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
    /// `tags['<const key>']`: it avoids reading the other promoted columns and the per-row normalization while
    /// producing the same value for each requested key — a promoted tag from its column (falling back to the
    /// inner Map), `__name__` from `metric_name` (falling back to the inner Map), any other tag from the inner Map.
    ASTPtr makeReducedTagsColumn(const std::vector<std::pair<String, String>> & tag_columns,
                                 const std::vector<String> & keys)
    {
        std::unordered_map<String, String> column_by_tag_name;
        for (const auto & [tag_name, column_name] : tag_columns)
            column_by_tag_name[tag_name] = column_name;

        ASTs map_args;
        for (const auto & key : keys)
        {
            ASTPtr value;
            if (key == TimeSeriesTagNames::MetricName)
                value = makeColumnWithMapFallback(TimeSeriesColumnNames::MetricName, key);
            else if (auto it = column_by_tag_name.find(key); it != column_by_tag_name.end())
                value = makeColumnWithMapFallback(it->second, key);
            else
                value = makeInnerTagAccess(key);

            map_args.push_back(make_intrusive<ASTLiteral>(key));
            /// The full reconstruction yields Map(String, String); `toString` keeps that even when a promoted
            /// column has another string type (e.g. LowCardinality(String)).
            map_args.push_back(makeASTFunction("toString", std::move(value)));
        }

        auto tags = makeASTFunction("map", std::move(map_args));
        tags->setAlias(TimeSeriesColumnNames::Tags);
        return tags;
    }

    /// Builds the outer `tags` column: the reduced form when the query only accesses specific constant keys
    /// (`projected_tag_keys`), otherwise the full normalized Map.
    ASTPtr makeTagsColumn(const std::vector<std::pair<String, String>> & tag_columns,
                          const std::optional<std::vector<String>> & projected_tag_keys)
    {
        if (projected_tag_keys && !projected_tag_keys->empty())
            return makeReducedTagsColumn(tag_columns, *projected_tag_keys);
        return makeNormalizedTagsColumn(tag_columns);
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

    ASTPtr makeTagsTableElement(const StorageID & tags_table_id, const ASTPtr & index_filter);

    /// If `regex` (a Prometheus label regex, matched fully as `^(?:<regex>)$`) is an alternation of plain
    /// literals — e.g. `api|server` or a single literal `api` — returns those literals so the matcher can be
    /// pushed as an `IN` set. Returns nullopt for anything containing a regex metacharacter (groups,
    /// quantifiers, character classes, anchors, ...), so the caller falls back to a general regex push-down.
    std::optional<std::vector<String>> extractRegexLiteralAlternatives(const String & regex)
    {
        static constexpr std::string_view metacharacters = ".^$*+?()[]{}|\\";

        std::vector<String> alternatives;
        String current;
        for (size_t i = 0; i != regex.size();)
        {
            char c = regex[i];
            if (c == '|')
            {
                alternatives.push_back(std::move(current));
                current.clear();
                ++i;
            }
            else if (c == '\\')
            {
                /// Only an escaped metacharacter is a literal; `\d`, `\w`, etc. are not.
                if (i + 1 == regex.size() || metacharacters.find(regex[i + 1]) == std::string_view::npos)
                    return std::nullopt;
                current.push_back(regex[i + 1]);
                i += 2;
            }
            else if (metacharacters.find(c) != std::string_view::npos)
            {
                return std::nullopt;  /// an unescaped metacharacter -> not a plain alternation
            }
            else
            {
                current.push_back(c);
                ++i;
            }
        }
        alternatives.push_back(std::move(current));
        return alternatives;
    }

    /// Whether `regex` has a top-level `|` (an alternation not nested inside a group or character class).
    bool hasTopLevelAlternation(const String & regex)
    {
        int depth = 0;
        bool in_class = false;
        for (size_t i = 0; i != regex.size(); ++i)
        {
            char c = regex[i];
            if (c == '\\')
                ++i;  // skip the escaped character
            else if (in_class)
                in_class = (c != ']');
            else if (c == '[')
                in_class = true;
            else if (c == '(')
                ++depth;
            else if (c == ')' && depth > 0)
                --depth;
            else if (c == '|' && depth == 0)
                return true;
        }
        return false;
    }

    /// Anchors a Prometheus label regex the way the matcher is evaluated (`^(?:<re>)$`), but drops the
    /// non-capturing group when `<re>` has no top-level alternation. The group is only needed so the anchors
    /// bind to the whole alternation; without it KeyCondition can derive a prefix range from `^<literal>...$`.
    String anchorRegexForMatch(const String & regex)
    {
        if (hasTopLevelAlternation(regex))
            return "^(?:" + regex + ")$";
        return "^" + regex + "$";
    }

    /// Follows ALIAS nodes down to the underlying node.
    const ActionsDAG::Node * unwrapAlias(const ActionsDAG::Node * node)
    {
        while (node->type == ActionsDAG::ActionType::ALIAS && !node->children.empty())
            node = node->children.front();
        return node;
    }

    /// Whether `node` is the input column `column_name` (possibly qualified, e.g. `__table1.timestamp`).
    bool isInputNamed(const ActionsDAG::Node * node, std::string_view column_name)
    {
        node = unwrapAlias(node);
        return node->type == ActionsDAG::ActionType::INPUT
            && (node->result_name == column_name || node->result_name.ends_with(String(".") + String(column_name)));
    }

    /// Whether the input column `column_name` is referenced anywhere in the subtree of `node`.
    bool referencesInput(const ActionsDAG::Node * node, std::string_view column_name)
    {
        if (isInputNamed(node, column_name))
            return true;
        for (const auto * child : node->children)
            if (referencesInput(child, column_name))
                return true;
        return false;
    }

    /// Whether any input column other than `allowed_column` is referenced in the subtree of `node`.
    bool referencesOtherInput(const ActionsDAG::Node * node, std::string_view allowed_column)
    {
        if (node->type == ActionsDAG::ActionType::INPUT && !isInputNamed(node, allowed_column))
            return true;
        for (const auto * child : node->children)
            if (referencesOtherInput(child, allowed_column))
                return true;
        return false;
    }

    /// Reads a constant value from a constant node.
    bool getConstField(const ActionsDAG::Node * node, Field & out)
    {
        node = unwrapAlias(node);
        if (node->type != ActionsDAG::ActionType::COLUMN || !node->column)
            return false;
        out = (*node->column)[0];
        return true;
    }

    /// Reads a String constant from a constant node (false if it isn't a String constant).
    bool getConstString(const ActionsDAG::Node * node, String & out)
    {
        Field value;
        if (!getConstField(node, value) || value.getType() != Field::Types::String)
            return false;
        out = value.safeGet<String>();
        return true;
    }

    /// Builds an index-usable predicate to push onto the raw "tags" table scan, from the query filter's
    /// top-level conjuncts on key-like columns. `<col>` below is written in the query as `metric_name` or as
    /// `tags['<tag>']` where `<tag>` is promoted to its own column via `tags_to_columns`:
    ///   - `<col> = <const>`                  -> `<col> IN ('', <consts>)`
    ///   - `timeSeriesSelectorMatchTags('<selector>', ...)` -> the matchers of the constant PromQL selector:
    ///                                        each `=` (and each `=~` that is an alternation of literals like
    ///                                        `api|server`) as an `IN`; every other `=~` as a `match` (below);
    ///                                        negative matchers (`!=`, `!~`) are ignored
    ///   - `startsWith(<col>, <const>)`, `<col> LIKE <const>`, `match(<col>, <const>)`
    ///                                        -> `<fn>(<col>, <const>) OR <col> = ''`
    /// Conditions on different columns/atoms are combined with AND. The empty string keeps rows whose value is
    /// stored in the `tags` Map instead of the column; correctness is still guaranteed by the outer filter on
    /// the reconstructed columns, so this only needs to be a sound over-approximation. KeyCondition derives a
    /// range from `=`/`IN` and from `startsWith`/`LIKE`/`match` with a constant prefix, so a column in the
    /// "tags" table's sorting key can skip granules for those (other patterns just become a PREWHERE filter).
    /// Returns nullptr when no such conjunct is found.
    ASTPtr makeTagsTableIndexFilter(const ActionsDAG * filter_actions_dag,
                                    const std::vector<std::pair<String, String>> & tag_columns)
    {
        if (!filter_actions_dag || filter_actions_dag->getOutputs().empty())
            return nullptr;

        std::unordered_map<String, String> column_by_tag_name;
        for (const auto & [tag_name, column_name] : tag_columns)
            column_by_tag_name[tag_name] = column_name;

        /// Maps a tag name to the inner column that stores it: `__name__` -> "metric_name", a promoted tag ->
        /// its column, otherwise none (the tag lives only in the Map, so there's no column to push onto).
        auto column_of_tag = [&](const String & tag_name) -> std::optional<String>
        {
            if (tag_name == TimeSeriesTagNames::MetricName)
                return String{TimeSeriesColumnNames::MetricName};
            if (auto it = column_by_tag_name.find(tag_name); it != column_by_tag_name.end())
                return it->second;
            return std::nullopt;
        };

        /// Resolves an expression to the tag it reads: the "metric_name" input -> `__name__`,
        /// or `arrayElement(tags, '<tag>')` -> `<tag>`. Returns none for anything else.
        auto expr_tag_name = [&](const ActionsDAG::Node * expr) -> std::optional<String>
        {
            if (isInputNamed(expr, TimeSeriesColumnNames::MetricName))
                return String{TimeSeriesTagNames::MetricName};
            if (expr->type == ActionsDAG::ActionType::FUNCTION && expr->function_base
                && expr->function_base->getName() == "arrayElement" && expr->children.size() == 2)
            {
                String tag_name;
                if (isInputNamed(unwrapAlias(expr->children[0]), TimeSeriesColumnNames::Tags)
                    && getConstString(unwrapAlias(expr->children[1]), tag_name))
                    return tag_name;
            }
            return std::nullopt;
        };

        /// Inner column name -> the constant values the filter restricts it to. Ordered for stable output.
        std::map<String, std::vector<String>> values_by_column;

        /// Extra index-usable conditions that aren't equality (prefix/regex), each `<fn>(<col>, <arg>) OR <col> = ''`.
        ASTs extra_conditions;

        /// Records `<tag> = <value>` on the tag's column, if it has one.
        auto record_tag_value = [&](const String & tag_name, const String & value)
        {
            if (auto column = column_of_tag(tag_name))
                values_by_column[*column].push_back(value);
        };

        /// `ifNull(<column>, '')` — a promoted tag / `metric_name` inner column may be declared `Nullable`, where
        /// a NULL means "the value is stored in the Map". Pushed predicates must treat such a NULL as the empty
        /// string, otherwise `NULL IN (...)` / `<fn>(NULL, ...)` is NULL (falsy) and the row is wrongly pruned at
        /// the tags scan. KeyCondition still derives the same point/prefix range through `ifNull(col, '')`.
        auto null_safe_column = [](const String & column_name)
        {
            return makeASTFunction("ifNull",
                make_intrusive<ASTIdentifier>(column_name), make_intrusive<ASTLiteral>(Field{String{}}));
        };

        /// Records `<function_name>(<tag's column>, '<argument>') OR <column> = ''`, if the tag has a column.
        /// The empty-string branch keeps rows whose value is stored in the Map; the outer filter stays exact.
        /// Use `= ''`, not the equivalent `empty(col)`: KeyCondition derives a point range from `equals` but
        /// not from `empty`, and one unanalyzable branch makes the whole OR drop to `true` (no granule skipping).
        auto record_tag_match = [&](const String & tag_name, const String & function_name, const String & argument)
        {
            auto column = column_of_tag(tag_name);
            if (!column)
                return;
            ASTs branches;
            branches.push_back(makeASTFunction(function_name,
                null_safe_column(*column), make_intrusive<ASTLiteral>(Field{argument})));
            branches.push_back(makeASTFunction("equals",
                null_safe_column(*column), make_intrusive<ASTLiteral>(Field{String{}})));
            extra_conditions.push_back(makeASTForLogicalOr(std::move(branches)));
        };

        /// Records the equality matchers of a constant PromQL selector (the argument of
        /// timeSeriesSelectorMatchTags); an unparseable selector simply disables the push-down.
        auto record_selector_matchers = [&](const String & selector)
        {
            PrometheusQueryTree query_tree;
            if (!query_tree.tryParse(selector))
                return;
            const auto * root = query_tree.getRoot();
            if (!root || root->node_type != PrometheusQueryTree::NodeType::InstantSelector)
                return;
            for (const auto & matcher : static_cast<const PrometheusQueryTree::InstantSelector &>(*root).matchers)
            {
                if (matcher.matcher_type == PrometheusQueryTree::MatcherType::EQ)
                {
                    /// `label = '<value>'`.
                    record_tag_value(matcher.label_name, matcher.label_value);
                }
                else if (matcher.matcher_type == PrometheusQueryTree::MatcherType::RE)
                {
                    /// `label =~ '<regex>'` (fully anchored as `^(?:<regex>)$`). An alternation of plain
                    /// literals becomes an `IN` set; any other regex is pushed as a `match` on the column
                    /// (KeyCondition derives a prefix range from it when possible).
                    if (auto literals = extractRegexLiteralAlternatives(matcher.label_value))
                    {
                        for (const auto & literal : *literals)
                            record_tag_value(matcher.label_name, literal);
                    }
                    else
                    {
                        record_tag_match(matcher.label_name, "match", anchorRegexForMatch(matcher.label_value));
                    }
                }
                /// Negative matchers (`!=`, `!~`) can't prune via the index and are ignored.
            }
        };

        for (const auto * atom : ActionsDAG::extractConjunctionAtoms(filter_actions_dag->getOutputs().front()))
        {
            const auto * node = unwrapAlias(atom);
            if (node->type != ActionsDAG::ActionType::FUNCTION || !node->function_base)
                continue;
            const auto function_name = node->function_base->getName();

            /// `metric_name = <value>` or `tags['<tag>'] = <value>`.
            if (function_name == "equals" && node->children.size() == 2)
            {
                const auto * lhs = unwrapAlias(node->children[0]);
                const auto * rhs = unwrapAlias(node->children[1]);

                /// One side must be a String constant (the compared value).
                String value;
                const ActionsDAG::Node * expr = nullptr;
                if (getConstString(rhs, value))
                    expr = lhs;
                else if (getConstString(lhs, value))
                    expr = rhs;
                else
                    continue;

                if (auto tag = expr_tag_name(expr))
                    record_tag_value(*tag, value);
            }
            /// `startsWith(<col>, <const>)`, `<col> LIKE <const>`, `match(<col>, <const>)`.
            else if ((function_name == "startsWith" || function_name == "like" || function_name == "match")
                && node->children.size() == 2)
            {
                String argument;
                if (auto tag = expr_tag_name(unwrapAlias(node->children[0]));
                    tag && getConstString(unwrapAlias(node->children[1]), argument))
                    record_tag_match(*tag, function_name, argument);
            }
            /// `timeSeriesSelectorMatchTags('<selector>', ...)` -> the selector's equality matchers.
            else if (function_name == "timeSeriesSelectorMatchTags" && !node->children.empty())
            {
                String selector;
                if (getConstString(unwrapAlias(node->children[0]), selector))
                    record_selector_matchers(selector);
            }
        }

        ASTs conditions;

        /// `ifNull(<column>, '') IN ('', <values>)` for each column with equality conditions.
        for (const auto & [column_name, values] : values_by_column)
        {
            ASTs tuple_args;
            tuple_args.push_back(make_intrusive<ASTLiteral>(Field{String{}}));
            for (const auto & value : values)
                tuple_args.push_back(make_intrusive<ASTLiteral>(Field{value}));
            conditions.push_back(makeASTFunction("in",
                null_safe_column(column_name),
                makeASTFunction("tuple", std::move(tuple_args))));
        }

        for (auto & condition : extra_conditions)
            conditions.push_back(std::move(condition));

        if (conditions.empty())
            return nullptr;
        return makeASTForLogicalAnd(std::move(conditions));
    }

    /// Reconstructs the AST of an expression over the `timestamp` column so it can be pushed onto the "samples"
    /// table, rebinding the `timestamp` input to that table's `timestamp` column. The expression must be
    /// deterministic within the query and depend only on `timestamp` and constants; otherwise the representative
    /// timestamp wouldn't be guaranteed to satisfy the outer condition, so we fail closed.
    ASTPtr timestampConditionToSamplesAST(const ActionsDAG::Node * node)
    {
        node = unwrapAlias(node);
        switch (node->type)
        {
            case ActionsDAG::ActionType::INPUT:
                if (isInputNamed(node, TimeSeriesColumnNames::Timestamp))
                    return make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp);
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
                        args.push_back(timestampConditionToSamplesAST(child));
                    return makeASTFunction(node->function_base->getName(), std::move(args));
                }
                break;
            default:
                break;
        }

        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "A condition on the `timestamp` column of a TimeSeries table can only be pushed down when it is a "
            "deterministic expression over `timestamp` and constants");
    }

    /// If `node` is a `timestamp <cmp> <const>` comparison (cmp one of `=`, `<`, `<=`, `>`, `>=`, in either
    /// argument order), appends the matching coarse bound on the "tags" table's `min_time`/`max_time` columns:
    /// a series can hold a sample at time `t` only when `min_time <= t <= max_time`. Does nothing for any other
    /// shape — the samples-table predicate still enforces correctness; this is only a series-pruning optimization.
    void addTagsRangePredicate(const ActionsDAG::Node * node, ASTs & tags_conditions)
    {
        /// Maps a comparison to the one with the arguments swapped (for `<const> <cmp> timestamp`).
        static const std::unordered_map<String, String> flipped_comparison = {
            {"equals", "equals"},
            {"less", "greater"}, {"lessOrEquals", "greaterOrEquals"},
            {"greater", "less"}, {"greaterOrEquals", "lessOrEquals"}};

        if (node->type != ActionsDAG::ActionType::FUNCTION || !node->function_base || node->children.size() != 2)
            return;

        const auto * lhs = unwrapAlias(node->children[0]);
        const auto * rhs = unwrapAlias(node->children[1]);
        String op = node->function_base->getName();
        Field value;

        if (isInputNamed(lhs, TimeSeriesColumnNames::Timestamp) && getConstField(rhs, value))
        {
            if (!flipped_comparison.contains(op))
                return;
        }
        else if (isInputNamed(rhs, TimeSeriesColumnNames::Timestamp) && getConstField(lhs, value))
        {
            auto it = flipped_comparison.find(op);
            if (it == flipped_comparison.end())
                return;
            op = it->second;
        }
        else
        {
            return;
        }

        /// `min_time`/`max_time` are Nullable and unset (NULL) for a series whose samples were written without
        /// maintaining them (e.g. inserted directly into the inner "samples" table). A bare `max_time >= c`
        /// would evaluate to NULL (falsy) and wrongly drop such a series, so keep it when the bound is unknown:
        /// the predicate stays a sound over-approximation, and `isNull` is still usable by the primary key.
        if (op == "greater" || op == "greaterOrEquals" || op == "equals")
            tags_conditions.push_back(makeASTFunction("or",
                makeASTFunction("isNull", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MaxTime)),
                makeASTFunction("greaterOrEquals",
                    make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MaxTime),
                    make_intrusive<ASTLiteral>(value))));
        if (op == "less" || op == "lessOrEquals" || op == "equals")
            tags_conditions.push_back(makeASTFunction("or",
                makeASTFunction("isNull", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MinTime)),
                makeASTFunction("lessOrEquals",
                    make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MinTime),
                    make_intrusive<ASTLiteral>(value))));
    }

    /// Splits a condition on the `timestamp` virtual column out of the query filter into a predicate for the
    /// "samples" table and one for the "tags" table. `timestamp` is filter-only (it never appears in the
    /// output). Each top-level conjunct that mentions `timestamp` must depend only on `timestamp` and constants
    /// (and be deterministic); such a conjunct is pushed onto the "samples" table verbatim, so the time series
    /// array keeps only matching samples. A conjunct that mixes `timestamp` with other columns (for example
    /// `timestamp > C OR metric_name = '...'`) can't be turned into a samples-table predicate and throws.
    /// `tags_time_filter` is a sound over-approximation on the `min_time`/`max_time` columns used to skip whole
    /// series, derived only from `timestamp <cmp> <const>` comparisons. Both outputs are null when the filter
    /// doesn't mention `timestamp`.
    void extractTimestampFilters(const ActionsDAG * filter_actions_dag, ASTPtr & samples_filter, ASTPtr & tags_time_filter)
    {
        samples_filter = nullptr;
        tags_time_filter = nullptr;
        if (!filter_actions_dag || filter_actions_dag->getOutputs().empty())
            return;

        ASTs samples_conditions;
        ASTs tags_conditions;

        for (const auto * atom : ActionsDAG::extractConjunctionAtoms(filter_actions_dag->getOutputs().front()))
        {
            const auto * node = unwrapAlias(atom);
            if (!referencesInput(node, TimeSeriesColumnNames::Timestamp))
                continue;

            if (referencesOtherInput(node, TimeSeriesColumnNames::Timestamp))
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "A condition on the `timestamp` column of a TimeSeries table that also depends on other "
                    "columns can't be pushed down");

            samples_conditions.push_back(timestampConditionToSamplesAST(node));
            addTagsRangePredicate(node, tags_conditions);
        }

        if (!samples_conditions.empty())
            samples_filter = makeASTForLogicalAnd(std::move(samples_conditions));
        if (!tags_conditions.empty())
            tags_time_filter = makeASTForLogicalAnd(std::move(tags_conditions));
    }

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
    ASTPtr makeTagsOnlySelect(const StorageID & tags_table_id, const NameSet & column_names,
                              const std::vector<std::pair<String, String>> & tag_columns,
                              const std::optional<std::vector<String>> & projected_tag_keys,
                              const ASTPtr & index_filter)
    {
        auto select_query = make_intrusive<ASTSelectQuery>();
        auto select_list = make_intrusive<ASTExpressionList>();

        if (column_names.contains(TimeSeriesColumnNames::MetricName))
            select_list->children.push_back(makeMetricNameColumn());
        if (column_names.contains(TimeSeriesColumnNames::Tags))
            select_list->children.push_back(makeTagsColumn(tag_columns, projected_tag_keys));

        /// If none of the "tags" table's columns are requested, we fall back to `1`.
        /// So for example `SELECT count() FROM time_series` is evaluated as
        /// `SELECT count() FROM (SELECT 1 FROM tags)`.
        if (select_list->children.empty())
            select_list->children.push_back(make_intrusive<ASTLiteral>(static_cast<UInt8>(1)));

        select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_list);

        auto tables = make_intrusive<ASTTablesInSelectQuery>();
        tables->children.push_back(makeTagsTableElement(tags_table_id, index_filter));
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

    /// The internal alias for the representative timestamp inside a samples sub-select. It deliberately
    /// differs from `timestamp` so that `WHERE timestamp ...` and `groupArray((timestamp, value))` in the
    /// same sub-select still bind to the raw samples column rather than to this aggregate; a projection above
    /// renames it to `timestamp`.
    constexpr const char * representative_timestamp_alias = "__rep_timestamp";

    /// Builds `any(timestamp) AS __rep_timestamp`: a representative timestamp from a series' (already
    /// time-filtered) samples. The `timestamp` virtual column never appears in the output on its own, but the
    /// planner still re-applies the original `timestamp` condition on top of the storage read, so the read must
    /// expose a value that satisfies it. Every surviving sample does (the same condition was pushed onto the
    /// samples), so any one of them works.
    ASTPtr makeRepresentativeTimestamp()
    {
        /// `toNullable` so an unmatched row from the metrics FULL JOIN (a metric family with no in-window series)
        /// gets a NULL representative instead of the epoch-0 default. The planner re-applies the outer `timestamp`
        /// predicate on the storage output, and `NULL <cmp> const` is NULL, so such a row is filtered out instead
        /// of leaking through predicates that epoch 0 happens to satisfy (e.g. `timestamp <= C`, `timestamp != C`).
        auto any_timestamp = makeASTFunction("toNullable",
            makeASTFunction("any", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp)));
        any_timestamp->setAlias(representative_timestamp_alias);
        return any_timestamp;
    }

    /// Adds the per-series samples columns to a samples-side SELECT list: `groupArray((timestamp, value)) AS
    /// time_series` when the time series array is requested, and the representative `timestamp` when the
    /// filter-only `timestamp` column is requested.
    void addSamplesSelectList(ASTExpressionList & select_list, bool need_time_series, bool need_timestamp)
    {
        if (need_time_series)
            select_list.children.push_back(makeTimeSeriesAggregate());
        if (need_timestamp)
            select_list.children.push_back(makeRepresentativeTimestamp());
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

    /// Builds `(SELECT id, groupArray((timestamp, value)) AS time_series [, any(timestamp) AS __rep_timestamp]
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
    /// `SELECT groupArray((timestamp, value)) AS time_series [, any(timestamp) AS __rep_timestamp]
    ///  FROM <samples> [WHERE <samples_filter>] GROUP BY id`.
    /// When the representative timestamp is needed, an extra projection renames `__rep_timestamp` to the
    /// requested `timestamp` column in a scope that doesn't reference the raw `timestamp` column.
    ASTPtr makeSamplesOnlySelect(const StorageID & samples_table_id, const ASTPtr & samples_filter,
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
        auto timestamp = make_intrusive<ASTIdentifier>(representative_timestamp_alias);
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
    ASTPtr makeJoinedSelectList(const NameSet & column_names, const std::vector<std::pair<String, String>> & tag_columns,
                                const std::optional<std::vector<String>> & projected_tag_keys)
    {
        auto select_list = make_intrusive<ASTExpressionList>();
        if (column_names.contains(TimeSeriesColumnNames::MetricName))
            select_list->children.push_back(makeMetricNameColumn());
        if (column_names.contains(TimeSeriesColumnNames::Tags))
            select_list->children.push_back(makeTagsColumn(tag_columns, projected_tag_keys));
        if (column_names.contains(TimeSeriesColumnNames::TimeSeries))
            select_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::TimeSeries));
        /// The filter-only `timestamp` column, when requested, is the representative from the joined samples
        /// (selected there as `__rep_timestamp` to avoid clashing with the raw column).
        if (column_names.contains(TimeSeriesColumnNames::Timestamp))
        {
            auto timestamp = make_intrusive<ASTIdentifier>(representative_timestamp_alias);
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

    /// Builds the `FROM` element that the multi-table reads are anchored on, always wrapped as
    /// Builds the table expression `(SELECT * FROM tags [WHERE <index_filter>] LIMIT 1 BY id) AS __tags`. The
    /// `LIMIT 1 BY id` deduplicates series: the "tags" table is AggregatingMergeTree, so until a background merge
    /// runs, several unmerged parts can each hold a row for the same series `id` (whose identity columns are
    /// identical across them); without this the read would return a series once per part. When set, `index_filter`
    /// is applied at the scan (before `LIMIT BY`) so the primary key can still skip granules.
    ASTPtr makeTagsTableExpression(const StorageID & tags_table_id, const ASTPtr & index_filter)
    {
        auto inner = make_intrusive<ASTSelectQuery>();
        auto select_list = make_intrusive<ASTExpressionList>();
        select_list->children.push_back(make_intrusive<ASTAsterisk>());
        inner->setExpression(ASTSelectQuery::Expression::SELECT, select_list);
        inner->setExpression(ASTSelectQuery::Expression::TABLES, makeSingleTableList(makeTableExpression(tags_table_id)));
        if (index_filter)
            inner->setExpression(ASTSelectQuery::Expression::WHERE, index_filter->clone());

        inner->setExpression(ASTSelectQuery::Expression::LIMIT_BY_LENGTH, make_intrusive<ASTLiteral>(static_cast<UInt8>(1)));
        auto limit_by = make_intrusive<ASTExpressionList>();
        limit_by->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID));
        inner->setExpression(ASTSelectQuery::Expression::LIMIT_BY, limit_by);

        auto tags_exp = make_intrusive<ASTTableExpression>();
        tags_exp->subquery = make_intrusive<ASTSubquery>(wrapInUnionQuery(std::move(inner)));
        tags_exp->subquery->setAlias("__tags");
        tags_exp->children.push_back(tags_exp->subquery);
        return tags_exp;
    }

    /// The deduplicated "tags" subquery as a plain `FROM` element — the anchor of a tags-anchored read.
    ASTPtr makeTagsTableElement(const StorageID & tags_table_id, const ASTPtr & index_filter)
    {
        return makeTableElement(makeTagsTableExpression(tags_table_id, index_filter));
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

        auto tags_exp = makeTagsTableExpression(tags_table_id, index_filter);
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

    /// Builds the metadata select list for a "metrics" table read. With `as_join_key`, the raw
    /// `metric_family_name` is always selected (the join key — the outer query renames it to `metric_family`);
    /// otherwise `metric_family_name` is exposed as the outer `metric_family` column only when requested. The
    /// requested `type`/`unit`/`help` columns are selected as raw columns: the `LIMIT 1 BY metric_family_name`
    /// in `makeMetricsSelect` keeps one whole metadata row per family, so they stay coherent (all from the same
    /// row) rather than being mixed across conflicting rows by independent `any(...)`.
    ASTPtr makeMetricsSelectList(const NameSet & column_names, bool as_join_key)
    {
        auto select_list = make_intrusive<ASTExpressionList>();
        if (as_join_key)
        {
            select_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricFamilyName));
        }
        else if (column_names.contains(TimeSeriesColumnNames::MetricFamily))
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
    ASTPtr makeMetricsSelect(const StorageID & metrics_table_id, ASTPtr select_list)
    {
        auto select_query = make_intrusive<ASTSelectQuery>();
        select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_list));
        select_query->setExpression(ASTSelectQuery::Expression::TABLES, makeSingleTableList(makeTableExpression(metrics_table_id)));

        select_query->setExpression(ASTSelectQuery::Expression::LIMIT_BY_LENGTH, make_intrusive<ASTLiteral>(static_cast<UInt8>(1)));
        auto limit_by = make_intrusive<ASTExpressionList>();
        limit_by->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricFamilyName));
        select_query->setExpression(ASTSelectQuery::Expression::LIMIT_BY, limit_by);

        return wrapInUnionQuery(std::move(select_query));
    }

    /// The "metrics"-only read (no join): `metric_family_name` is exposed as the outer `metric_family` column.
    ASTPtr makeMetricsOnlySelect(const StorageID & metrics_table_id, const NameSet & column_names)
    {
        return makeMetricsSelect(metrics_table_id, makeMetricsSelectList(column_names, /* as_join_key= */ false));
    }

    /// The deduplicated "metrics" subquery that sits inside the multi-table read's JOIN; `metric_family_name`
    /// (the join key) is always selected.
    ASTPtr makeDeduplicatedMetricsSubquery(const StorageID & metrics_table_id, const NameSet & column_names)
    {
        return makeMetricsSelect(metrics_table_id, makeMetricsSelectList(column_names, /* as_join_key= */ true));
    }

    /// Builds `FULL JOIN (deduplicated metrics) AS __metrics ON metric_family_name = timeSeriesMetricNameToFamily(<metric name>)`
    /// — attaches the metadata columns. The metric family computed from a time series' metric name links it
    /// to its metadata row. `<metric name>` is reconstructed with the same `tags['__name__']` fallback as the
    /// output `metric_name` column, so a series whose name lives in the inner `tags` Map still links to its metadata. The FULL JOIN keeps every "tags" row (its metadata columns are empty when the
    /// family has no metadata row) and also every "metrics" row that no time series belongs to (its "tags"
    /// and "samples" columns are then empty). The alias is required by `joined_subquery_requires_alias`.
    ASTPtr makeMetricsFullJoinElement(const StorageID & metrics_table_id, const NameSet & column_names)
    {
        auto metrics_exp = make_intrusive<ASTTableExpression>();
        metrics_exp->subquery = make_intrusive<ASTSubquery>(makeDeduplicatedMetricsSubquery(metrics_table_id, column_names));
        metrics_exp->subquery->setAlias("__metrics");
        metrics_exp->children.push_back(metrics_exp->subquery);

        auto join = make_intrusive<ASTTableJoin>();
        join->kind = JoinKind::Full;
        join->strictness = JoinStrictness::All;
        join->on_expression = makeASTFunction("equals",
            make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricFamilyName),
            makeASTFunction("timeSeriesMetricNameToFamily",
                makeColumnWithMapFallback(TimeSeriesColumnNames::MetricName, TimeSeriesTagNames::MetricName)));
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
    ASTPtr makeTagsBasedSelect(
        const StorageID & tags_table_id,
        const std::optional<StorageID> & samples_table_id,
        const std::optional<StorageID> & metrics_table_id,
        const NameSet & column_names,
        const std::vector<std::pair<String, String>> & tag_columns,
        const std::optional<std::vector<String>> & projected_tag_keys,
        const ASTPtr & tags_filter,
        const ASTPtr & samples_filter,
        const ASTs & samples_group_by)
    {
        auto select_query = make_intrusive<ASTSelectQuery>();
        select_query->setExpression(ASTSelectQuery::Expression::SELECT,
            makeJoinedSelectList(column_names, tag_columns, projected_tag_keys));

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
            tables->children.push_back(makeTagsTableElement(tags_table_id, tags_filter));
        }
        if (metrics_table_id)
            tables->children.push_back(makeMetricsFullJoinElement(*metrics_table_id, column_names));
        select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables);

        return wrapInUnionQuery(std::move(select_query));
    }

    /// Derives the samples `GROUP BY` from the "samples" table's sorting key: the prefix up to and including `id`,
    /// so the aggregation can run in sorting-key order. `id` (the series identity) ends the prefix and pins each
    /// group to a single series; a series-level key column before it (e.g. `metric_name`) is constant per series,
    /// while a per-sample column (e.g. `toStartOfHour(timestamp)`) splits the series into one row per distinct
    /// value — a faithful, re-insertable representation that the samples-anchored `SEMI LEFT JOIN` keeps. Falls
    /// back to `GROUP BY id` (not a sorting-key prefix, so no in-order aggregation) when `id` is not in the key.
    ASTs computeSamplesGroupBy(const StorageTimeSeries & storage, const ContextPtr & context)
    {
        auto samples_table = storage.getTargetTable(ViewTarget::Samples, context);
        const auto & sorting_key = samples_table->getInMemoryMetadataPtr(context, false)->getSortingKey();
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
}


ASTPtr makeTimeSeriesReadQuery(
    const StorageTimeSeries & storage,
    const Names & column_names,
    const ContextPtr & context,
    const ActionsDAG * filter_actions_dag,
    const std::optional<std::vector<String>> & projected_tag_keys)
{
    NameSet requested{column_names.begin(), column_names.end()};

    /// Check which target tables are requested by outer columns.
    bool need_tags    = requested.contains(TimeSeriesColumnNames::MetricName)
                     || requested.contains(TimeSeriesColumnNames::Tags);

    /// `timestamp` is a filter-only virtual column. Both filtering and reading it require the "samples" table
    /// (to apply the time condition and to produce a representative value satisfying the outer condition).
    bool need_timestamp = requested.contains(TimeSeriesColumnNames::Timestamp);
    bool need_samples = requested.contains(TimeSeriesColumnNames::TimeSeries) || need_timestamp;

    bool need_metrics = requested.contains(TimeSeriesColumnNames::MetricFamily)
                     || requested.contains(TimeSeriesColumnNames::Type)
                     || requested.contains(TimeSeriesColumnNames::Unit)
                     || requested.contains(TimeSeriesColumnNames::Help);

    /// All-false default: read from the "tags" table so that `SELECT count() FROM time_series` returns
    /// the number of time series.
    if (!need_tags && !need_samples && !need_metrics)
        need_tags = true;

    auto tag_columns = getPromotedTagColumns(storage);

    /// Conditions on `metric_name` and promoted tag columns are pushed onto the "tags" table read so its
    /// primary key (or a custom sorting key over a promoted column) can skip granules.
    auto tags_index_filter = makeTagsTableIndexFilter(filter_actions_dag, tag_columns);

    /// A condition on the filter-only `timestamp` column becomes an exact predicate on the "samples" table
    /// and a coarse range predicate on the "tags" table.
    ASTPtr samples_filter;
    ASTPtr tags_time_filter;
    extractTimestampFilters(filter_actions_dag, samples_filter, tags_time_filter);

    /// The tags-table range pruning is valid only when the table stores and filters by min/max time.
    auto storage_settings = storage.getStorageSettings();
    if (tags_time_filter
        && !((*storage_settings)[TimeSeriesSetting::filter_by_min_time_and_max_time]
             && (*storage_settings)[TimeSeriesSetting::store_min_time_and_max_time]))
        tags_time_filter = nullptr;

    auto tags_filter = combineFilters(tags_index_filter, tags_time_filter);

    /// When the "samples" table is read, group it by its sorting-key prefix up to `id` so the aggregation can run
    /// in sorting-key order; a per-sample key column in that prefix splits a series into one row per group.
    ASTs samples_group_by;
    if (need_samples)
        samples_group_by = computeSamplesGroupBy(storage, context);

    /// Single-table reads (no join).
    if (!need_samples && !need_metrics)
        return makeTagsOnlySelect(storage.getTargetTableID(ViewTarget::Tags, context), requested,
                                  tag_columns, projected_tag_keys, tags_filter);
    if (need_samples && !need_tags && !need_metrics)
        return makeSamplesOnlySelect(storage.getTargetTableID(ViewTarget::Samples, context), samples_filter,
                                     requested.contains(TimeSeriesColumnNames::TimeSeries), need_timestamp,
                                     samples_group_by);
    if (need_metrics && !need_tags && !need_samples)
        return makeMetricsOnlySelect(storage.getTargetTableID(ViewTarget::Metrics, context), requested);

    /// Multi-table reads: anchored on "samples" when it is read, otherwise on "tags" (which always bridges
    /// "samples" by `id` and "metrics" by the family computed from `metric_name`).
    auto tags_table_id = storage.getTargetTableID(ViewTarget::Tags, context);

    std::optional<StorageID> samples_table_id;
    if (need_samples)
        samples_table_id = storage.getTargetTableID(ViewTarget::Samples, context);

    std::optional<StorageID> metrics_table_id;
    if (need_metrics)
        metrics_table_id = storage.getTargetTableID(ViewTarget::Metrics, context);

    return makeTagsBasedSelect(tags_table_id, samples_table_id, metrics_table_id, requested,
                               tag_columns, projected_tag_keys, tags_filter, samples_filter, samples_group_by);
}

}
