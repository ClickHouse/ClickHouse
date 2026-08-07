#include <Storages/TimeSeries/timeSeriesMatchersToAST.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>


namespace DB
{

ASTPtr timeSeriesTagNameToAST(const String & tag_name, const std::unordered_map<String, String> & column_name_by_tag_name)
{
    if (tag_name == TimeSeriesTagNames::MetricName)
        return make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricName);

    auto it = column_name_by_tag_name.find(tag_name);
    if (it != column_name_by_tag_name.end())
        return make_intrusive<ASTIdentifier>(it->second);

    /// arrayElement() can be used to extract a value from a Map too.
    return makeASTFunction("arrayElement", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Tags), make_intrusive<ASTLiteral>(tag_name));
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
