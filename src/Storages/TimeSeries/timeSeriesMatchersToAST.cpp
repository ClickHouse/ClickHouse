#include <Storages/TimeSeries/timeSeriesMatchersToAST.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>

#include <utility>


namespace DB
{

ASTPtr timeSeriesTagNameToAST(const String & tag_name, const std::unordered_map<String, String> & column_name_by_tag_name)
{
    if (tag_name == TimeSeriesTagNames::MetricName)
        return make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricName);

    /// arrayElement() extracts a value from the `tags` Map and returns '' for a missing key, which matches
    /// Prometheus semantics: a missing label is equal to the empty label value.
    auto make_map_value = [&]
    {
        return makeASTFunction("arrayElement", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Tags), make_intrusive<ASTLiteral>(tag_name));
    };

    auto it = column_name_by_tag_name.find(tag_name);
    if (it == column_name_by_tag_name.end())
        return make_map_value();

    /// A dedicated tag column is allowed to be Nullable (e.g. `LowCardinality(Nullable(String))` in an external
    /// tags table), and a series without this tag stores NULL there. Prometheus treats a missing label as equal
    /// to the empty label value, so normalize NULL to '' - otherwise matchers like {host=""}, {host!="prod"} or
    /// {host=~".*"} would evaluate to NULL and filter such series out, unlike the `tags` Map path, where
    /// arrayElement() returns '' for a missing key.
    auto make_column_value = [&]
    {
        return makeASTFunction("ifNull", make_intrusive<ASTIdentifier>(it->second), make_intrusive<ASTLiteral>(""));
    };

    /// A tag configured via `tags_to_columns` normally lives in the dedicated column, but a supported external
    /// tags table can also carry it in the residual `tags` Map (e.g. legacy rows written before the dedicated
    /// column was adopted). Resolve the tag with the same precedence as `timeSeriesStoreTags` on the write path:
    /// use the dedicated column when it is non-empty and fall back to the Map otherwise. Conflict validation is
    /// deliberately kept out of this expression because it is also used in matcher predicates, where an
    /// exception could be evaluated for a row that another matcher would have excluded.
    return makeASTFunction(
        "if",
        makeASTFunction("notEquals", make_column_value(), make_intrusive<ASTLiteral>("")),
        make_column_value(),
        make_map_value());
}

ASTPtr timeSeriesMatcherToAST(
    const PrometheusQueryTree::Matcher & matcher,
    const std::unordered_map<String, String> & column_name_by_tag_name)
{
    std::string_view function_name;
    bool add_anchors = false;
    bool add_not = false;

    auto matcher_type = matcher.matcher_type;
    switch (matcher_type)
    {
        case PrometheusQueryTree::MatcherType::EQ:  function_name = "equals"; break;
        case PrometheusQueryTree::MatcherType::NE:  function_name = "notEquals"; break;
        case PrometheusQueryTree::MatcherType::RE:  function_name = "match"; add_anchors = true; break;
        case PrometheusQueryTree::MatcherType::NRE: function_name = "match"; add_anchors = true; add_not = true; break;
    }

    String value = matcher.label_value;
    if (add_anchors)
    {
        /// Prometheus regexp matchers are fully anchored: the pattern must match the whole label value.
        /// The pattern is wrapped in a non-capturing group before anchoring - the same way Prometheus does it -
        /// because otherwise a top-level alternation would bind the anchors to its first and last branches only
        /// (e.g. "a|b" would become "^a|b$", which also matches "ax" and "xb").
        value = "^(?:" + value + ")$";
    }
    auto make_matcher = [&](ASTPtr value_ast)
    {
        return makeASTFunction(function_name, std::move(value_ast), make_intrusive<ASTLiteral>(value));
    };

    /// A dedicated column normally contains the tag value, but external tables can still contain
    /// legacy rows where the value exists only in the residual Map. Keep the two cases as separate
    /// branches so MergeTree can use the dedicated-column comparison for index pruning:
    ///
    ///   (column is non-empty AND matcher(column))
    ///   OR (column is NULL/empty AND matcher(tags[tag_name]))
    ///
    /// The unguarded `direct_match OR canonical_match` form is equivalent for row evaluation, but
    /// its Map branch can match any granule and therefore prevents the primary key analyzer from
    /// pruning on the dedicated column. Regex matchers stay on the canonical path because `match`
    /// is not a key-condition atom and the extra branch would only add work.
    const auto dedicated_column_it = column_name_by_tag_name.find(matcher.label_name);
    const bool use_dedicated_column_fast_path =
        (matcher_type == PrometheusQueryTree::MatcherType::EQ || matcher_type == PrometheusQueryTree::MatcherType::NE)
        && matcher.label_name != TimeSeriesTagNames::MetricName
        && dedicated_column_it != column_name_by_tag_name.end();

    ASTPtr res;
    if (use_dedicated_column_fast_path)
    {
        auto make_dedicated_value = [&]
        {
            return make_intrusive<ASTIdentifier>(dedicated_column_it->second);
        };
        auto make_map_value = [&]
        {
            return makeASTFunction(
                "arrayElement",
                make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Tags),
                make_intrusive<ASTLiteral>(matcher.label_name));
        };

        auto direct_match = makeASTFunction(
            "and",
            makeASTFunction("notEquals", make_dedicated_value(), make_intrusive<ASTLiteral>("")),
            make_matcher(make_dedicated_value()));

        auto dedicated_value_is_empty = makeASTFunction(
            "or",
            makeASTFunction("isNull", make_dedicated_value()),
            makeASTFunction("equals", make_dedicated_value(), make_intrusive<ASTLiteral>("")));
        auto map_match = makeASTFunction(
            "and",
            std::move(dedicated_value_is_empty),
            make_matcher(make_map_value()));

        /// `and()` over a Nullable dedicated column can be Nullable. Normalize it to false so a
        /// missing dedicated value reaches the Map branch instead of propagating NULL through `or()`.
        res = makeASTFunction(
            "or",
            makeASTFunction("ifNull", std::move(direct_match), make_intrusive<ASTLiteral>(false)),
            std::move(map_match));
    }
    else
    {
        res = make_matcher(timeSeriesTagNameToAST(matcher.label_name, column_name_by_tag_name));
    }

    if (add_not)
        res = makeASTFunction("not", res);
    return res;
}

}
