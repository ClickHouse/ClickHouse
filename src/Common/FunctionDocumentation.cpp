#include <Common/FunctionDocumentation.h>

#include <Common/Exception.h>

#include <unordered_map>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

std::string FunctionDocumentation::argumentsAsString() const
{
    std::string res;
    for (const auto & [name, desc] : arguments)
        res += "- `" + name + "` — " + desc + "\n";
    return res;
}

std::string FunctionDocumentation::examplesAsString() const
{
    std::string res;
    for (const auto & [name, query, result] : examples)
    {
        res += name + ":\n\n";
        res += "```sql\n";
        res += query + "\n";
        res += "```\n\n";
        res += "Result:\n\n";
        res += "```text\n";
        res += result + "\n";
        res += "```\n";
    }
    return res;
}

std::string FunctionDocumentation::categoryAsString() const
{
    static const std::unordered_map<Category, std::string> category_to_string = {
        {Category::Array, "Arrays"},
        {Category::DatesAndTimes, "Dates and Times"},
        {Category::Other, "Other"},
        {Category::UUID, "UUIDs"},
    };

    if (auto it = category_to_string.find(category); it != category_to_string.end())
        return it->second;
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Category has no mapping to string");
}

}
