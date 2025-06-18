#include <Common/FunctionDocumentation.h>

#include <Common/Exception.h>
#include <boost/algorithm/string/trim.hpp>
#include <unordered_map>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

VersionNumber VERSION_UNKNOWN = {0};

/// Documentation is often defined with raw strings, therefore need to trim leading and trailing whitespace + newlines.
/// Example:
///
///     FunctionDocumentation::ReturnedValue returned_value = R"(
/// Returns the difference between `x` and the nearest integer not greater than
/// `x` divisible by `y`.
/// )";

void formatTypes(std::string & result, const std::vector<std::string> & types)
{
    /// Generate the argument/return type. To save writing, append links to the documentation on-the-fly
    ///
    /// We assume that if 'type' is empty(), 'description' already ends with the elegible type. This check can be removed after all
    /// existing FunctionDocumentation definitions were changed to provide proper values for the 'type' vector.
    ///
    /// Thinking about it, there may be special cases where the exact argument type depends on something else or needs explanation
    /// otherwise. This cannot be represented using the 'types' vector. Perhaps it is a good idea to leave the check just for that
    /// reason.
    if (!types.empty())
    {
        /// Vector 'types' are expected to look like: {"(U)Int*", "Float*"}
        /// The output is expected to look like: [`(U)Int*`](/sql-reference/data-types/int-uint) or [`Float*`](/sql-reference/data-types/float)
        bool is_first = true;
        for (const auto & type : types)
        {
            if (is_first)
                is_first = false;
            else
                result += " or "; /// or whatever other separator we want here :-)

            result += "[`" + type;

            if (type.starts_with("String"))
                result += "`](/sql-reference/data-types/string)";
            else if (type.starts_with("FixedString"))
                result += "`](/sql-reference/data-types/fixedstring)";
            else if (type.starts_with("Int") || type.starts_with("UInt") || type == "(U)Int*") /// matches 'Int8', 'Int16' etc. and 'UInt8', 'UInt16' etc. and 'Int*' and '(U)Int*'
                result += "`](/sql-reference/data-types/int-uint)";
            else if (type == "Date")
                result += "`](/sql-reference/data-types/date)";
            else if (type == "Date32")
                result += "`](/sql-reference/data-types/date32)";
            else if (type == "DateTime")
                result += "`](/sql-reference/data-types/datetime)";
            else if (type == "DateTime64")
                result += "`](/sql-reference/data-types/datetime64)";
            else if (type == "Enum")
                result += "`](/sql-reference/data-types/enum)";
            else if (type == "UUID")
                result += "`](/sql-reference/data-types/uuid)";
            else if (type == "Object")
                result += "`](/sql-reference/data-types/object-data-type)";
            else if (type == "IPv4")
                result += "`](/sql-reference/data-types/ipv4)";
            else if (type == "IPv6")
                result += "`](/sql-reference/data-types/ipv6)";
            else if (type.starts_with("Array")) /// matches 'Array(T)', 'Array(UInt8)', 'Array(String)' etc
                result += "`](/sql-reference/data-types/array)";
            else if (type == "Bool")
                result += "`](/sql-reference/data-types/boolean)";
            else if (type.starts_with("Tuple")) /// matches 'Tuple(T1, T2)', 'Tuple(UInt8, String)' etc
                result += "`](/sql-reference/data-types/tuple)";
            else if (type.starts_with("Map")) /// matches 'Map(K, V)'
                result += "`](/sql-reference/data-types/map)";
            else if (type.starts_with("Variant")) /// matches 'Variant(T1, T2, ...)', 'Variant(UInt8, String)' etc
                result += "`](/sql-reference/data-types/variant)";
            else if (type.starts_with("LowCardinality")) /// matches 'LowCardinality(T)', 'LowCardinality(UInt8)', 'LowCardinality(String)' etc
                result += "`](/sql-reference/data-types/lowcardinality)";
            else if (type.starts_with("Nullable")) /// matches 'Nullable(T)', 'Nullable(UInt8)', 'Nullable(String)' etc
                result += "`](/sql-reference/data-types/nullable)";
            else if (type.starts_with("AggregateFunction")) /// matches 'AggregateFunction(agg_func, T)', 'AggregateFunction(any, UInt8)' etc
                result += "`](/sql-reference/data-types/aggregatefunction)";
            else if (type.starts_with("SimpleAggregateFunction")) /// matches 'SimpleAggregateFunction(agg_func, T)', 'SimpleAggregateFunction(any, UInt8)' etc
                result += "`](/sql-reference/data-types/simpleaggregatefunction)";
            else if (type == "Point")
                result += "`](/sql-reference/data-types/geo#point)";
            else if (type == "Ring")
                result += "`](/sql-reference/data-types/geo#ring)";
            else if (type == "LineString")
                result += "`](/sql-reference/data-types/geo#linestring)";
            else if (type == "MultiLineString")
                result += "`](/sql-reference/data-types/geo#multilinestring)";
            else if (type == "Polygon")
                result += "`](/sql-reference/data-types/geo#polygon)";
            else if (type == "MultiPolygon")
                result += "`](/sql-reference/data-types/geo#multipolygon)";
            else if (type == "Expression")
                result += "`](/sql-reference/data-types/special-data-types/expression)";
            else if (type == "Set")
                result += "`](/sql-reference/data-types/special-data-types/set)";
            else if (type == "Nothing")
                result += "`](/sql-reference/data-types/special-data-types/nothing)";
            else if (type == "Interval")
                result += "`](/sql-reference/data-types/special-data-types/interval)";
            else if (type.starts_with("Nested")) /// matches 'Nested(N1 T1, N2 T2, ...)' etc
                result += "`](/sql-reference/data-types/nested-data-structures/nested)";
            else if (type == "Dynamic")
                result += "`](/sql-reference/data-types/dynamic)";
            else if (type == "JSON")
                result += "`](/sql-reference/data-types/newjson)";
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected data type: {}", type);

        }
    }
    result += "\n";
}

std::string FunctionDocumentation::argumentsAsString() const
{
    std::string result;
    for (const auto & [name, description_, types] : arguments)
    {
        /// Generate formatted name and description (boring)
        result += "- `" + name + "` — " + description_;
        formatTypes(result, types);
    }
    return result;
}

std::string FunctionDocumentation::syntaxAsString() const
{
    return boost::algorithm::trim_copy(syntax);
}

std::string FunctionDocumentation::returnedValueAsString() const
{
    std::string result;
    result += returned_value.description;
    formatTypes(result, returned_value.types);
    return boost::algorithm::trim_copy(result);
}

std::string FunctionDocumentation::examplesAsString() const
{
    std::string res;
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
        {Category::NumericIndexedVector, "NumericIndexedVector"},
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
