#include <Common/FunctionDocumentation.h>

#include <Common/Exception.h>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <optional>
#include <unordered_map>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Maps a documentation type literal to the relative documentation URL it links to, or nullopt if the
/// type is not recognized. A leading "const " has already been stripped by the caller. The order of the
/// checks is significant (prefix matches must stay in their current relative order).
std::optional<std::string_view> documentationTypeLink(const std::string & type)
{
    if (type == "NULL")
        return "/sql-reference/syntax#null";
    else if (type == "Any")
        return "/sql-reference/data-types";
    else if (type == "String" || type == "String literal")
        return "/sql-reference/data-types/string";
    else if (type.starts_with("FixedString"))
        return "/sql-reference/data-types/fixedstring";
    else if (type.starts_with("Int") || type.starts_with("UInt") || type.starts_with("(U)Int")) /// "Int8", "Int16", ... || "UInt8", "UInt16", ... || "(U)Int*", "(U)Int8", "(U)Int16", ...
        return "/sql-reference/data-types/int-uint";
    else if (type.starts_with("Float") || type == "BFloat16") /// "Float32", "Float64", "BFloat16"
        return "/sql-reference/data-types/float";
    else if (type.starts_with("Decimal")) /// "Decimal(P, S)", "Decimal32", "Decimal64", ...
        return "/sql-reference/data-types/decimal";
    else if (type == "Date")
        return "/sql-reference/data-types/date";
    else if (type == "Date32")
        return "/sql-reference/data-types/date32";
    else if (type == "DateTime")
        return "/sql-reference/data-types/datetime";
    else if (type.starts_with("DateTime64")) /// "DateTime64(P)", "DateTime64(3)", "DateTime64(6)", ...
        return "/sql-reference/data-types/datetime64";
    else if (type == "Time")
        return "/sql-reference/data-types/time";
    else if (type.starts_with("Time64")) //// "Time64(P)", "Time64(3)", ...
        return "/sql-reference/data-types/time64";
    else if (type == "Enum")
        return "/sql-reference/data-types/enum";
    else if (type == "UUID")
        return "/sql-reference/data-types/uuid";
    else if (type == "Object")
        return "/sql-reference/data-types/object-data-type";
    else if (type == "IPv4")
        return "/sql-reference/data-types/ipv4";
    else if (type == "IPv6")
        return "/sql-reference/data-types/ipv6";
    else if (type.starts_with("Array")) /// "Array(T)", "Array(UInt8)", "Array(String)", ...
        return "/sql-reference/data-types/array";
    else if (type == "Bool")
        return "/sql-reference/data-types/boolean";
    else if (type.starts_with("Tuple")) /// "Tuple(T1, T2)", "Tuple(UInt8, String)", ...
        return "/sql-reference/data-types/tuple";
    else if (type.starts_with("Map")) /// "Map(K, V)"
        return "/sql-reference/data-types/map";
    else if (type.starts_with("Variant")) /// "Variant(T1, T2, ...)", "Variant(UInt8, String)", ...
        return "/sql-reference/data-types/variant";
    else if (type.starts_with("Geometry"))
        return "/sql-reference/data-types/geo";
    else if (type.starts_with("LowCardinality")) /// "LowCardinality(T)", "LowCardinality(UInt8)", "LowCardinality(String)", ...
        return "/sql-reference/data-types/lowcardinality";
    else if (type.starts_with("Nullable")) /// "Nullable(T)", "Nullable(UInt8)", "Nullable(String)", ...
        return "/sql-reference/data-types/nullable";
    else if (type.starts_with("AggregateFunction")) /// "AggregateFunction(agg_func, T)", "AggregateFunction(any, UInt8)", ...
        return "/sql-reference/data-types/aggregatefunction";
    else if (type.starts_with("SimpleAggregateFunction")) /// "SimpleAggregateFunction(agg_func, T)", "SimpleAggregateFunction(any, UInt8)", ...
        return "/sql-reference/data-types/simpleaggregatefunction";
    else if (type == "Geo")
        return "/sql-reference/data-types/geo";
    else if (type == "Point")
        return "/sql-reference/data-types/geo#point";
    else if (type == "Ring")
        return "/sql-reference/data-types/geo#ring";
    else if (type == "LineString")
        return "/sql-reference/data-types/geo#linestring";
    else if (type == "MultiLineString")
        return "/sql-reference/data-types/geo#multilinestring";
    else if (type == "Polygon")
        return "/sql-reference/data-types/geo#polygon";
    else if (type == "MultiPolygon")
        return "/sql-reference/data-types/geo#multipolygon";
    else if (type == "numericIndexedVector")
        return "/sql-reference/functions/numeric-indexed-vector-functions#create-numeric-indexed-vector-object";
    else if (type == "Expression")
        return "/sql-reference/data-types/special-data-types/expression";
    else if (type == "Set")
        return "/sql-reference/data-types/special-data-types/set";
    else if (type == "Nothing")
        return "/sql-reference/data-types/special-data-types/nothing";
    else if (type == "Interval")
        return "/sql-reference/data-types/special-data-types/interval";
    else if (type.starts_with("Nested")) /// "Nested(N1 T1, N2 T2, ...)", ...
        return "/sql-reference/data-types/nested-data-structures/nested";
    else if (type == "Dynamic")
        return "/sql-reference/data-types/dynamic";
    else if (type == "JSON")
        return "/sql-reference/data-types/newjson";
    else if (type == "Lambda function")
        return "/sql-reference/functions/overview#arrow-operator-and-lambda";
    else if (type.starts_with("QBit")) /// "QBit(T, UInt64)", "QBit(Float64, UInt64)", ...
        return "/sql-reference/data-types/qbit";
    else
        return std::nullopt;
}

/// Example input 'types' vector: {"(U)Int*", "Float*"}
/// Example output string: [`(U)Int*`](/sql-reference/data-types/int-uint) or [`Float*`](/sql-reference/data-types/float)
///
/// A type that has no documentation link is rendered as plain inline code (`` `type` ``) instead of failing.
/// This keeps `system.functions` / `system.documentation` queryable even if one function's documentation
/// declares an unknown type; the dedicated unit test asserts every registered function uses only known types.
String mapTypesToTypesWithLinks(const std::vector<std::string> & types)
{
    String result;
    bool is_first = true;
    for (auto type : types)
    {
        if (is_first)
            is_first = false;
        else
            result += " or ";

        result += "[`" + type;

        if (type.starts_with("const "))
            type = type.substr(6); // Remove "const " prefix

        if (auto link = documentationTypeLink(type))
            result += "`](" + std::string(*link) + ")";
        else
            result += "`]"; /// Unknown type: render the type as code without a link rather than failing.
    }
    result += "\n";
    return result;
}
}

bool documentationTypeHasLink(const std::string & type)
{
    std::string stripped = type;
    if (stripped.starts_with("const "))
        stripped = stripped.substr(6);
    return documentationTypeLink(stripped).has_value();
}

template <typename Type>
String argumentsOrParametersAsString(const Type & arguments_or_parameters)
{
    String result;
    for (const auto & [name, description_, types] : arguments_or_parameters)
    {
        result += "- `" + name + "` — " + description_ + " ";

        /// We assume that if 'type' is empty(), 'description' already ends with a type definition. This is a reasonable assumption to be
        /// able to handle special cases which cannot be represented by the type mapping in mapTypesToTypesWithLinks.
        if (!types.empty())
            result += mapTypesToTypesWithLinks(types);
    }
    return result;
}

String FunctionDocumentation::argumentsAsString() const
{
    return argumentsOrParametersAsString(arguments);
}

String FunctionDocumentation::parametersAsString() const
{
    return argumentsOrParametersAsString(parameters);
}

/// Documentation is often defined with raw strings, therefore we need to trim leading and trailing whitespace + newlines.
/// Example:
///
///     FunctionDocumentation::ReturnedValue returned_value = R"(
/// Returns the difference between `x` and the nearest integer not greater than
/// `x` divisible by `y`.
/// )";

String FunctionDocumentation::syntaxAsString() const
{
    String trimmed_syntax = boost::algorithm::trim_copy(syntax);

    /// It is tempting to write 'SELECT someFunction(arg1, arg2)' in the syntax field but we
    /// really want 'someFunction(arg1, arg2)'.
    if (boost::algorithm::istarts_with(trimmed_syntax, "SELECT "))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Syntax field must not start with 'SELECT': {}", syntax);

    if (syntax.ends_with(";"))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Syntax field must not end with ';': {}", syntax);

    return trimmed_syntax;
}

String FunctionDocumentation::returnedValueAsString() const
{
    String result = returned_value.description + " ";

    /// We assume that if 'type' is empty(), 'description' already ends with a type definition. This is a reasonable assumption to be
    /// able to handle special cases which cannot be represented by the type mapping in mapTypesToTypesWithLinks.
    if (!returned_value.types.empty())
        result += mapTypesToTypesWithLinks(returned_value.types);
    return boost::algorithm::trim_copy(result);
}

String FunctionDocumentation::examplesAsString() const
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

String FunctionDocumentation::introducedInAsString() const
{
    if (introduced_in == FunctionDocumentation::VERSION_UNKNOWN)
        return ""; /// we could show "unknown" here but for consistency with other fields return the empty string
    else
        return introduced_in.toString();
}

String FunctionDocumentation::categoryAsString() const
{
    static const std::unordered_map<Category, std::string> category_to_string =
    {
        {Category::Unknown, ""}, /// Default enum value for default-constructed FunctionDocumentation objects. Be consistent with other default fields (empty).

        {Category::AI, "AI"},
        {Category::Arithmetic, "Arithmetic"},
        {Category::Array, "Arrays"},
        {Category::Bit, "Bit"},
        {Category::Bitmap, "Bitmap"},
        {Category::Comparison, "Comparison"},
        {Category::Conditional, "Conditional"},
        {Category::DateAndTime, "Dates and Times"},
        {Category::Decimal, "Decimal"},
        {Category::Dictionary, "Dictionary"},
        {Category::Distance, "Distance"},
        {Category::EmbeddedDictionary, "Embedded Dictionary"},
        {Category::Geo, "Geo"},
        {Category::GeoPolygon, "Geo Polygon"},
        {Category::Encoding, "Encoding"},
        {Category::Encryption, "Encryption"},
        {Category::Financial, "Financial"},
        {Category::Hash, "Hash"},
        {Category::IPAddress, "IP Address"},
        {Category::Introspection, "Introspection"},
        {Category::JSON, "JSON"},
        {Category::Logical, "Logical"},
        {Category::MachineLearning, "Machine Learning"},
        {Category::Map, "Map"},
        {Category::Mathematical, "Mathematical"},
        {Category::NLP, "Natural Language Processing"},
        {Category::Null, "Null"},
        {Category::NumericIndexedVector, "NumericIndexedVector"},
        {Category::Other, "Other"},
        {Category::QBit, "QBit"},
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

        {Category::Internal, "Internal"},

        {Category::AggregateFunction, "Aggregate Functions"},
        {Category::TableFunction, "Table Functions"}
    };

    if (auto it = category_to_string.find(category); it != category_to_string.end())
        return it->second;
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Category has no mapping to string");
}

FunctionDocumentation FunctionDocumentation::INTERNAL_FUNCTION_DOCS = {"", "", {}, {}, {"", {}}, {}, FunctionDocumentation::VERSION_UNKNOWN, FunctionDocumentation::Category::Internal};

}
