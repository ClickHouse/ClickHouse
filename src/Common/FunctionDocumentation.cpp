#include <Common/FunctionDocumentation.h>

namespace DB
{

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

}
