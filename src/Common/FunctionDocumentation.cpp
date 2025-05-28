#include <Common/FunctionDocumentation.h>

#include <Common/Exception.h>

#include <unordered_map>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

VersionNumber VERSION_UNKNOWN = {0};

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

std::string FunctionDocumentation::introducedInAsString() const
{
    if (introduced_in == FunctionDocumentation::VERSION_UNKNOWN)
        return ""; /// we could show "unknown" here but for consistency with other fields return the empty string
    else
        return introduced_in.toString();
}

std::string FunctionDocumentation::categoryAsString() const
{
    static const std::unordered_map<Category, std::string> category_to_string = {
        {Category::Unknown, ""}, /// Default enum value for default-constructed FunctionDocumentation objects. Be consistent with other default fields (empty).
        {Category::Arithmetic, "Arithmetic"},
        {Category::Array, "Arrays"},
        {Category::Bit, "Bit"},
        {Category::Bitmap, "Bitmap"},
        {Category::Comparison, "Comparison"},
        {Category::Conditional, "Conditional"},
        {Category::DateAndTime, "Dates and Times"},
        {Category::Dictionary, "Dictionary"},
        {Category::Distance, "Distance"},
        {Category::EmbeddedDictionary, "Embedded Dictionary"},
        {Category::Geo, "Geo"},
        {Category::Encoding, "Encoding"},
        {Category::Encryption, "Encryption"},
        {Category::File, "File"},
        {Category::Hash, "Hash"},
        {Category::IPAddress, "IP Address"},
        {Category::Introspection, "Introspection"},
        {Category::JSON, "JSON"},
        {Category::Logical, "Logical"},
        {Category::MachineLearning, "Machine Learning"},
        {Category::Map, "Map"},
        {Category::Mathematical, "Mathematical"},
        {Category::NLP, "Natural Language Processing"},
        {Category::Nullable, "Nullable"},
        {Category::Other, "Other"},
        {Category::RandomNumber, "Random Number"},
        {Category::Rounding, "Rounding"},
        {Category::StringReplacement, "String Replacement"},
        {Category::StringSearch, "String Search"},
        {Category::StringSplitting, "String Splitting"},
        {Category::String, "String"},
        {Category::TimeSeries, "Time Series"},
        {Category::TimeWindow, "Time Window"},
        {Category::Tuple, "Tuple"},
        {Category::TypeConversion, "Type Conversion"},
        {Category::ULID, "ULID"},
        {Category::URL, "URL"},
        {Category::UUID, "UUID"},
        {Category::UniqTheta, "UniqTheta"},
        {Category::TableFunction, "Table Functions"}
    };

    if (auto it = category_to_string.find(category); it != category_to_string.end())
        return it->second;
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Category has no mapping to string");
}

}
