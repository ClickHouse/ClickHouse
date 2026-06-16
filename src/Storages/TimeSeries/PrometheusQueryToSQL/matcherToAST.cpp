#include <Storages/TimeSeries/PrometheusQueryToSQL/matcherToAST.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Builds an AST that references the value of a tag (label) as it is stored in the Tags
    /// inner table; the resolution rules are documented on `matcherToAST`.
    ASTPtr tagNameToAST(const String & tag_name, const std::unordered_map<String, String> & column_name_by_tag_name)
    {
        if (tag_name == TimeSeriesTagNames::MetricName)
            return make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricName);

        auto it = column_name_by_tag_name.find(tag_name);
        if (it != column_name_by_tag_name.end())
            return make_intrusive<ASTIdentifier>(it->second);

        /// arrayElement() can be used to extract a value from a Map too.
        return makeASTFunction("arrayElement",
            make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Tags),
            make_intrusive<ASTLiteral>(tag_name));
    }
}


ASTPtr matcherToAST(
    const PrometheusQueryTree::Matcher & matcher,
    const std::unordered_map<String, String> & column_name_by_tag_name)
{
    std::string_view function_name;
    bool add_anchors = false;
    bool add_not = false;

    switch (matcher.matcher_type)
    {
        case PrometheusQueryTree::MatcherType::EQ:  function_name = "equals";    break;
        case PrometheusQueryTree::MatcherType::NE:  function_name = "notEquals"; break;
        case PrometheusQueryTree::MatcherType::RE:  function_name = "match"; add_anchors = true; break;
        case PrometheusQueryTree::MatcherType::NRE: function_name = "match"; add_anchors = true; add_not = true; break;
    }

    String value = matcher.label_value;
    if (add_anchors)
    {
        if (!value.starts_with('^'))
            value = '^' + value;
        if (!value.ends_with('$'))
            value += '$';
    }

    ASTPtr res = makeASTFunction(
        function_name,
        tagNameToAST(matcher.label_name, column_name_by_tag_name),
        make_intrusive<ASTLiteral>(value));

    if (add_not)
        res = makeASTFunction("not", res);

    return res;
}

}
