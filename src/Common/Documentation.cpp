#include <Common/Documentation.h>

#include <boost/algorithm/string/trim.hpp>


namespace DB
{

/// Documentation is often defined with raw strings, therefore we need to trim leading and trailing whitespace + newlines.
String Documentation::syntaxAsString() const
{
    return boost::algorithm::trim_copy(syntax);
}

String Documentation::examplesAsString() const
{
    String res;
    for (const auto & [name, query, result] : examples)
    {
        res += "**" + name + "**" + "\n\n";

        const String trimmed_query = boost::algorithm::trim_copy(query);
        if (!trimmed_query.empty())
            res += "```sql title=""Query""\n" + trimmed_query + "\n```\n\n";

        /// Only emit the response block when there is a response; otherwise an empty example would
        /// render as an empty code box.
        const String trimmed_result = boost::algorithm::trim_copy(result);
        if (!trimmed_result.empty())
            res += "```response title=""Response""\n" + trimmed_result + "\n```\n\n";
    }
    return res;
}

String Documentation::introducedInAsString() const
{
    if (introduced_in == Documentation::VERSION_UNKNOWN)
        return ""; /// we could show "unknown" here but for consistency with other fields return the empty string
    else
        return introduced_in.toString();
}

}
