#include <Storages/TimeSeries/timeSeriesMatchersToAST.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>
#include <Common/quoteString.h>

#include <fmt/format.h>


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
    /// column was adopted). Resolve the tag with the same normalization rules as `timeSeriesStoreTags` on the
    /// write path and `/api/v1/series` on the read path: use the dedicated column when it is non-empty, fall
    /// back to the Map otherwise, and reject a row carrying different non-empty values in the two carriers
    /// (fail closed) instead of silently preferring one of them.
    auto conflict = makeASTFunction(
        "and",
        makeASTFunction("notEquals", make_column_value(), make_intrusive<ASTLiteral>("")),
        makeASTFunction("notEquals", make_map_value(), make_intrusive<ASTLiteral>("")),
        makeASTFunction("notEquals", make_column_value(), make_map_value()));

    /// The conflict condition must be correct on every row on its own (not rely on being evaluated only in
    /// some branch of the surrounding `if`), because with `short_circuit_function_evaluation = 'disable'`
    /// all the arguments of `if` are evaluated on all rows.
    auto reject_conflict = makeASTFunction(
        "throwIf",
        conflict,
        make_intrusive<ASTLiteral>(fmt::format(
            "Found two tags with the same name {} but different values in a row of the 'tags' table",
            quoteString(tag_name))));

    auto value = makeASTFunction(
        "if",
        makeASTFunction("notEquals", make_column_value(), make_intrusive<ASTLiteral>("")),
        make_column_value(),
        make_map_value());

    /// throwIf() returns 0 when it does not throw, so this `if` always selects `value`; it only forces the
    /// conflict check to be evaluated as a part of this expression.
    return makeASTFunction("if", reject_conflict, make_intrusive<ASTLiteral>(""), value);
}

ASTPtr timeSeriesMatcherToAST(const PrometheusQueryTree::Matcher & matcher, const std::unordered_map<String, String> & column_name_by_tag_name)
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
    ASTPtr res = makeASTFunction(function_name, timeSeriesTagNameToAST(matcher.label_name, column_name_by_tag_name), make_intrusive<ASTLiteral>(value));
    if (add_not)
        res = makeASTFunction("not", res);
    return res;
}

}
