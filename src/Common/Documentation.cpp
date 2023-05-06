#include <Common/Documentation.h>

namespace DB
{

std::string Documentation::examplesAsString() const
{
    std::string res;
    for (const auto & [example_name, example_query] : examples)
    {
        res += example_name + ":\n\n";
        res += "```sql\n";
        res += example_query + "\n";
        res += "```\n";
    }
    return res;
}

std::string Documentation::categoriesAsString() const
{
    if (categories.empty())
        return "";

    std::string res = categories[0];
    for (size_t i = 1; i < categories.size(); ++i)
        res += ", " + categories[i];
    return res;
}

}
