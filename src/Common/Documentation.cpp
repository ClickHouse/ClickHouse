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
        res += "```sql title=""Query""\n";
        res += boost::algorithm::trim_copy(query) + "\n";
        res += "```\n\n";
        res += "```response title=""Response""\n";
        res += boost::algorithm::trim_copy(result) + "\n";
        res += "```";
        res += "\n\n";
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
