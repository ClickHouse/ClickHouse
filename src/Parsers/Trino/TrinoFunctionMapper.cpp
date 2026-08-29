#include <Parsers/Trino/TrinoFunctionMapper.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <base/defines.h>

#include <Poco/String.h>

#include <functional>
#include <limits>
#include <unordered_map>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

using Rewriter = std::function<void(ASTPtr & node, ASTFunction & function, ASTs & arguments)>;

ASTPtr makeFunctionWithArguments(const String & name, ASTs arguments)
{
    auto function = make_intrusive<ASTFunction>();
    function->name = name;
    function->arguments = make_intrusive<ASTExpressionList>();
    function->arguments->children = std::move(arguments);
    function->children.push_back(function->arguments);
    return function;
}

ASTPtr makeLambda(const std::vector<String> & parameters, ASTPtr body)
{
    ASTs parameter_asts;
    parameter_asts.reserve(parameters.size());
    for (const auto & parameter : parameters)
        parameter_asts.push_back(make_intrusive<ASTIdentifier>(parameter));
    return makeFunctionWithArguments("lambda", {makeFunctionWithArguments("tuple", std::move(parameter_asts)), std::move(body)});
}

[[noreturn]] void throwWrongArguments(const ASTFunction & function, std::string_view expected)
{
    throw Exception(
        ErrorCodes::BAD_ARGUMENTS, "Trino function {} expects {}", function.name, expected);
}

void requireArguments(const ASTFunction & function, const ASTs & arguments, size_t min_count, size_t max_count, std::string_view expected)
{
    if (arguments.size() < min_count || arguments.size() > max_count)
        throwWrongArguments(function, expected);
}

/// Renames the function and moves the trailing lambda argument to the front
/// (Trino passes lambdas last, ClickHouse first).
void moveLambdaToFront(ASTFunction & function, ASTs & arguments, const String & new_name)
{
    if (arguments.empty())
        throwWrongArguments(function, "at least one argument");
    ASTPtr lambda = arguments.back();
    arguments.pop_back();
    arguments.insert(arguments.begin(), lambda);
    function.name = new_name;
}

/// Extracts the parameters and the body from a lambda argument.
bool tryGetLambda(const ASTPtr & ast, std::vector<String> & parameters, ASTPtr & body)
{
    const auto * lambda = ast->as<ASTFunction>();
    if (!lambda || lambda->name != "lambda" || !lambda->arguments || lambda->arguments->children.size() != 2)
        return false;
    const auto * tuple = lambda->arguments->children[0]->as<ASTFunction>();
    if (!tuple || !tuple->arguments)
        return false;
    parameters.clear();
    for (const auto & parameter : tuple->arguments->children)
    {
        const auto * identifier = parameter->as<ASTIdentifier>();
        if (!identifier)
            return false;
        parameters.push_back(identifier->name());
    }
    body = lambda->arguments->children[1];
    return true;
}

/// Turns an aggregate function argument (which must be a literal or a literal
/// array, e.g. the percentile of approx_percentile) into aggregate parameters.
ASTs literalToParameters(const ASTFunction & function, const ASTPtr & argument)
{
    const auto * literal = argument->as<ASTLiteral>();
    if (!literal)
        throwWrongArguments(function, "a constant literal");

    ASTs parameters;
    if (literal->value.getType() == Field::Types::Array)
    {
        for (const auto & element : literal->value.safeGet<Array>())
            parameters.push_back(make_intrusive<ASTLiteral>(element));
    }
    else
        parameters.push_back(make_intrusive<ASTLiteral>(literal->value));
    return parameters;
}

/// Returns the underlying string expression of CAST(x AS JSON) — as produced by
/// json_parse or a JSON '...' literal — or nullptr. Trino JSON values are mapped
/// to the ClickHouse JSON type, but the string-based JSONPath functions
/// (JSON_VALUE, JSONExtract*, ...) do not accept it, so a cast flowing directly
/// into them is unwrapped back to the JSON text.
ASTPtr tryUnwrapCastToJSON(const ASTPtr & ast)
{
    const auto * function = ast->as<ASTFunction>();
    if (!function || !function->arguments || function->arguments->children.size() != 2)
        return nullptr;
    String name = Poco::toLower(function->name);
    if (name != "cast" && name != "accuratecastornull")
        return nullptr;
    const auto * type = function->arguments->children[1]->as<ASTLiteral>();
    if (!type || type->value.getType() != Field::Types::String || Poco::toUpper(type->value.safeGet<String>()) != "JSON")
        return nullptr;
    return function->arguments->children[0];
}

void unwrapJSONArgument(ASTs & arguments)
{
    if (!arguments.empty())
        if (ASTPtr unwrapped = tryUnwrapCastToJSON(arguments[0]))
            arguments[0] = unwrapped;
}

/// Parses a simple constant JSON path ($.a.b[0], $["key with spaces"]) into
/// JSONExtract*-style arguments ('a', 'b', 1). Trino array indexes are 0-based,
/// ClickHouse 1-based.
bool tryParseSimpleJSONPath(const ASTPtr & ast, ASTs & parts)
{
    const auto * literal = ast->as<ASTLiteral>();
    if (!literal || literal->value.getType() != Field::Types::String)
        return false;
    const String & path = literal->value.safeGet<String>();
    if (path.empty() || path[0] != '$')
        return false;

    parts.clear();
    size_t pos = 1;
    while (pos < path.size())
    {
        if (path[pos] == '.')
        {
            size_t key_begin = ++pos;
            while (pos < path.size() && (isWordCharASCII(path[pos])))
                ++pos;
            if (pos == key_begin)
                return false;
            parts.push_back(make_intrusive<ASTLiteral>(path.substr(key_begin, pos - key_begin)));
        }
        else if (path[pos] == '[')
        {
            ++pos;
            if (pos < path.size() && (path[pos] == '"' || path[pos] == '\''))
            {
                /// A bracket-quoted member key: ["key"] or ['key'].
                const char quote = path[pos];
                size_t key_begin = ++pos;
                while (pos < path.size() && path[pos] != quote && path[pos] != '\\')
                    ++pos;
                if (pos + 1 >= path.size() || path[pos] != quote || path[pos + 1] != ']')
                    return false;
                parts.push_back(make_intrusive<ASTLiteral>(path.substr(key_begin, pos - key_begin)));
                pos += 2;
            }
            else
            {
                size_t index_begin = pos;
                while (pos < path.size() && isNumericASCII(path[pos]))
                    ++pos;
                if (pos == index_begin || pos == path.size() || path[pos] != ']')
                    return false;
                parts.push_back(make_intrusive<ASTLiteral>(UInt64(std::stoull(path.substr(index_begin, pos - index_begin)) + 1)));
                ++pos;
            }
        }
        else
            return false;
    }
    return !parts.empty();
}

void attachParameters(ASTFunction & function, ASTs parameters)
{
    function.parameters = make_intrusive<ASTExpressionList>();
    function.parameters->children = std::move(parameters);
    /// `parameters` must precede `arguments` in children for correct formatting.
    function.children.clear();
    function.children.push_back(function.parameters);
    function.children.push_back(function.arguments);
    if (function.window_definition)
        function.children.push_back(function.window_definition);
}

/// Rewriters that replace an aggregate call with an expression over another
/// aggregate must carry the window of the original call over to it.
void transferWindow(const ASTFunction & from, const ASTPtr & to)
{
    if (!from.isWindowFunction())
        return;
    auto * target = to->as<ASTFunction>();
    target->setIsWindowFunction(true);
    target->window_name = from.window_name;
    if (from.window_definition)
    {
        target->window_definition = from.window_definition;
        target->children.push_back(target->window_definition);
    }
}

void requireNotWindow(const ASTFunction & function)
{
    if (function.isWindowFunction())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Trino function {} is not supported as a window function", function.name);
}

/// Substitutes an identifier with an expression (used to apply the output
/// lambda of `reduce` at translation time).
void replaceIdentifier(ASTPtr & ast, const String & name, const ASTPtr & replacement)
{
    if (const auto * identifier = ast->as<ASTIdentifier>(); identifier && identifier->name() == name)
    {
        ast = replacement->clone();
        return;
    }
    for (auto & child : ast->children)
        replaceIdentifier(child, name, replacement);
}

/// Simple renames: the argument order and semantics match.
/// Names that already resolve in ClickHouse with the same semantics (through
/// case-insensitive aliases, e.g. `concat`, `coalesce`, `cardinality`, `abs`,
/// `date_trunc`, `max_by`) are not listed.
const std::unordered_map<String, String> & getRenames()
{
    static const std::unordered_map<String, String> renames =
    {
        /// String functions. Trino character-position semantics are code-point-based,
        /// so the UTF8 variants are used where available in every ClickHouse build.
        {"ends_with", "endsWith"},
        {"format", "printf"},
        {"hamming_distance", "byteHammingDistance"},
        {"length", "lengthUTF8"},
        {"levenshtein_distance", "editDistanceUTF8"},
        {"lower", "lower"},
        {"lpad", "leftPadUTF8"},
        {"rpad", "rightPadUTF8"},
        {"overlay", "overlayUTF8"},
        {"position", "positionUTF8"},
        {"starts_with", "startsWith"},
        {"substr", "substringUTF8"},
        {"substring", "substringUTF8"},
        {"title_case", "initcapUTF8"},
        {"translate", "translateUTF8"},
        {"upper", "upper"},
        {"from_utf8", "toValidUTF8"},

        /// Math.
        {"is_finite", "isFinite"},
        {"is_infinite", "isInfinite"},
        {"is_nan", "isNaN"},
        {"cosine_distance", "cosineDistance"},
        {"euclidean_distance", "L2Distance"},
        {"dot_product", "dotProduct"},

        /// Date and time.
        {"last_day_of_month", "toLastDayOfMonth"},
        {"from_iso8601_date", "toDate"},
        {"at_timezone", "toTimeZone"},
        {"from_unixtime_nanos", "fromUnixTimestamp64Nano"},
        /// DANGER if unmapped: TO_UNIXTIME in ClickHouse is an alias of parseDateTime.
        {"to_unixtime", "toUnixTimestamp"},
        {"date_parse", "parseDateTime"},
        {"format_datetime", "formatDateTimeInJodaSyntax"},
        {"parse_datetime", "parseDateTimeInJodaSyntax"},
        {"day_of_month", "toDayOfMonth"},
        {"day_of_week", "toDayOfWeek"},
        {"dow", "toDayOfWeek"},
        {"day_of_year", "toDayOfYear"},
        {"doy", "toDayOfYear"},
        /// DANGER if unmapped: ClickHouse `week` defaults to the non-ISO Sunday-based mode.
        {"week", "toISOWeek"},
        {"week_of_year", "toISOWeek"},
        {"year_of_week", "toISOYear"},
        {"yow", "toISOYear"},
        {"current_timezone", "timezone"},
        /// DANGER if unmapped: ClickHouse `date_diff` counts unit-boundary crossings,
        /// Trino counts complete units, which is what `age` does. The parser
        /// canonicalizes the name to `dateDiff`, so both spellings are listed.
        {"date_diff", "age"},
        {"datediff", "age"},
        {"millisecond", "toMillisecond"},

        /// Arrays.
        {"array_distinct", "arrayDistinct"},
        {"array_intersect", "arrayIntersect"},
        {"array_union", "arrayUnion"},
        {"array_except", "arrayExcept"},
        {"array_max", "arrayMax"},
        {"array_min", "arrayMin"},
        {"array_position", "indexOf"},
        {"arrays_overlap", "hasAny"},
        {"contains", "has"},
        {"contains_sequence", "hasSubstr"},
        {"element_at", "arrayElementOrNull"},
        {"shuffle", "arrayShuffle"},
        {"slice", "arraySlice"},
        {"zip", "arrayZipUnaligned"},

        /// Maps.
        {"map_keys", "mapKeys"},
        {"map_values", "mapValues"},

        /// Regular expressions.
        {"regexp_count", "countMatches"},
        {"regexp_like", "match"},

        /// URL.
        {"url_extract_fragment", "fragment"},
        {"url_extract_host", "domain"},
        {"url_extract_parameter", "extractURLParameter"},
        {"url_extract_path", "path"},
        {"url_extract_protocol", "protocol"},
        {"url_extract_query", "queryString"},
        {"url_encode", "encodeURLFormComponent"},
        {"url_decode", "decodeURLFormComponent"},

        /// Bitwise.
        {"bitwise_and", "bitAnd"},
        {"bitwise_or", "bitOr"},
        {"bitwise_xor", "bitXor"},
        {"bitwise_left_shift", "bitShiftLeft"},
        {"bitwise_right_shift_arithmetic", "bitShiftRight"},

        /// Binary. ClickHouse hash names are case-sensitive, so lowercase
        /// spellings do not resolve without a rename.
        {"from_base64url", "base64URLDecode"},
        {"to_base64url", "base64URLEncode"},
        {"from_base32", "base32Decode"},
        {"to_base32", "base32Encode"},
        {"from_hex", "unhex"},
        {"to_hex", "hex"},
        {"md5", "MD5"},
        {"sha1", "SHA1"},
        {"sha256", "SHA256"},
        {"sha512", "SHA512"},
        {"murmur3", "murmurHash3_128"},

        /// UUID.
        {"uuid", "generateUUIDv4"},

        /// Aggregate functions.
        {"arbitrary", "any"},
        {"approx_set", "uniqState"},
        {"bool_and", "min"},
        {"bool_or", "max"},
        {"every", "min"},
        {"count_if", "countIf"},
        {"bitwise_and_agg", "groupBitAnd"},
        {"bitwise_or_agg", "groupBitOr"},
        {"bitwise_xor_agg", "groupBitXor"},
        {"skewness", "skewPop"},
        {"variance", "varSamp"},
    };
    return renames;
}

const std::unordered_map<String, Rewriter> & getRewriters()
{
    static const std::unordered_map<String, Rewriter> rewriters =
    {
        /// Higher-order functions: the lambda moves from the last to the first argument.
        {"transform", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 2, 2, "(array, function)");
            moveLambdaToFront(function, arguments, "arrayMap");
        }},
        {"filter", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 2, 2, "(array, function)");
            moveLambdaToFront(function, arguments, "arrayFilter");
        }},
        {"all_match", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 2, 2, "(array, function)");
            moveLambdaToFront(function, arguments, "arrayAll");
        }},
        {"any_match", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 2, 2, "(array, function)");
            moveLambdaToFront(function, arguments, "arrayExists");
        }},
        {"none_match", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 2, 2, "(array, function)");
            moveLambdaToFront(function, arguments, "arrayExists");
            node = makeFunctionWithArguments("not", {node});
        }},
        {"map_filter", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 2, 2, "(map, function)");
            moveLambdaToFront(function, arguments, "mapFilter");
        }},
        {"zip_with", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 3, 3, "(array, array, function)");
            moveLambdaToFront(function, arguments, "arrayMap");
        }},
        {"array_first", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 1, 2, "(array[, function])");
            if (arguments.size() == 1)
            {
                function.name = "arrayElementOrNull";
                arguments.push_back(make_intrusive<ASTLiteral>(UInt64(1)));
            }
            else
                moveLambdaToFront(function, arguments, "arrayFirstOrNull");
        }},
        {"array_last", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 1, 2, "(array[, function])");
            if (arguments.size() == 1)
            {
                function.name = "arrayElementOrNull";
                arguments.push_back(make_intrusive<ASTLiteral>(Int64(-1)));
            }
            else
                moveLambdaToFront(function, arguments, "arrayLastOrNull");
        }},
        {"array_sort", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 1, 1, "(array) - the comparator form has no ClickHouse counterpart");
            function.name = "arraySort";
        }},
        {"reduce", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 4, 4, "(array, initialState, inputFunction, outputFunction)");
            /// reduce(arr, s0, in, out) -> out applied to arrayFold(in, arr, s0);
            /// the output lambda is inlined by substituting its parameter.
            std::vector<String> parameters;
            ASTPtr body;
            if (!tryGetLambda(arguments[3], parameters, body) || parameters.size() != 1)
                throwWrongArguments(function, "a one-argument output lambda");
            ASTPtr fold = makeFunctionWithArguments("arrayFold", {arguments[2], arguments[0], arguments[1]});
            if (const auto * body_identifier = body->as<ASTIdentifier>(); body_identifier && body_identifier->name() == parameters[0])
            {
                node = fold;
                return;
            }
            ASTPtr result = body->clone();
            replaceIdentifier(result, parameters[0], fold);
            node = result;
        }},
        {"transform_keys", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 2, 2, "(map, function)");
            std::vector<String> parameters;
            ASTPtr body;
            if (!tryGetLambda(arguments[1], parameters, body) || parameters.size() != 2)
                throwWrongArguments(function, "a two-argument lambda");
            ASTPtr pair = makeFunctionWithArguments("tuple", {body, make_intrusive<ASTIdentifier>(parameters[1])});
            node = makeFunctionWithArguments("mapApply", {makeLambda(parameters, pair), arguments[0]});
        }},
        {"transform_values", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 2, 2, "(map, function)");
            std::vector<String> parameters;
            ASTPtr body;
            if (!tryGetLambda(arguments[1], parameters, body) || parameters.size() != 2)
                throwWrongArguments(function, "a two-argument lambda");
            ASTPtr pair = makeFunctionWithArguments("tuple", {make_intrusive<ASTIdentifier>(parameters[0]), body});
            node = makeFunctionWithArguments("mapApply", {makeLambda(parameters, pair), arguments[0]});
        }},

        /// String functions.
        {"concat_ws", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            /// Trino skips NULL arguments while ClickHouse concat_ws returns NULL,
            /// so translate through arrayStringConcat which skips NULLs.
            requireArguments(function, arguments, 2, std::numeric_limits<size_t>::max(), "(separator, values...)");
            ASTPtr values;
            if (arguments.size() == 2)
                values = arguments[1];  /// The concat_ws(separator, array) form.
            else
                values = makeFunctionWithArguments("array", ASTs(arguments.begin() + 1, arguments.end()));
            node = makeFunctionWithArguments("arrayStringConcat", {values, arguments[0]});
        }},
        {"replace", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 2, 3, "(string, search[, replace])");
            function.name = "replaceAll";
            if (arguments.size() == 2)
                arguments.push_back(make_intrusive<ASTLiteral>(String{}));
        }},
        {"split", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 2, 2, "(string, delimiter) - the limit form is not translated");
            function.name = "splitByString";
            std::swap(arguments[0], arguments[1]);
        }},
        {"split_part", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 3, 3, "(string, delimiter, index)");
            ASTPtr parts = makeFunctionWithArguments("splitByString", {arguments[1], arguments[0]});
            node = makeFunctionWithArguments("arrayElementOrNull", {parts, arguments[2]});
        }},
        {"split_to_map", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 3, 3, "(string, entryDelimiter, keyValueDelimiter)");
            function.name = "extractKeyValuePairs";
            std::swap(arguments[1], arguments[2]);
        }},
        {"strpos", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 2, 2, "(string, substring) - the instance form has no ClickHouse counterpart");
            function.name = "positionUTF8";
        }},
        {"word_stem", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 1, 2, "(word[, language])");
            function.name = "stem";
            if (arguments.size() == 1)
                arguments.push_back(make_intrusive<ASTLiteral>(String("en")));
        }},
        {"normalize", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 1, 2, "(string[, form])");
            String form = "NFC";
            if (arguments.size() == 2)
            {
                const auto * identifier = arguments[1]->as<ASTIdentifier>();
                if (!identifier)
                    throwWrongArguments(function, "NFC, NFD, NFKC or NFKD as the normalization form");
                form = Poco::toUpper(identifier->name());
                if (form != "NFC" && form != "NFD" && form != "NFKC" && form != "NFKD")
                    throwWrongArguments(function, "NFC, NFD, NFKC or NFKD as the normalization form");
                arguments.pop_back();
            }
            function.name = "normalizeUTF8" + form;
        }},
        {"to_utf8", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            /// ClickHouse String is already a byte string.
            requireArguments(function, arguments, 1, 1, "(string)");
            node = arguments[0];
        }},

        /// Math.
        {"log", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            /// Trino log(b, x) = log_b(x); ClickHouse log is the natural logarithm.
            if (arguments.size() != 2)
                return;
            node = makeFunctionWithArguments(
                "divide",
                {makeFunctionWithArguments("log", {arguments[1]}), makeFunctionWithArguments("log", {arguments[0]})});
            UNUSED(function);
        }},
        {"random", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 0, 2, "(), (bound) or (low, high)");
            if (arguments.empty())
            {
                function.name = "randCanonical";
                return;
            }
            /// random(n) is a uniformly distributed integer in [0, n).
            ASTPtr low = arguments.size() == 2 ? arguments[0] : nullptr;
            ASTPtr span = arguments.size() == 2
                ? makeFunctionWithArguments("minus", {arguments[1], arguments[0]})
                : arguments[0];
            ASTPtr value = makeFunctionWithArguments(
                "toInt64",
                {makeFunctionWithArguments(
                    "floor", {makeFunctionWithArguments("multiply", {makeFunctionWithArguments("randCanonical", {}), span})})});
            node = low ? makeFunctionWithArguments("plus", {low, value}) : value;
        }},
        {"width_bucket", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            /// The array-of-bounds form; the 4-argument form works natively.
            if (arguments.size() != 2)
                return;
            ASTPtr bound = make_intrusive<ASTIdentifier>(String("__trino_bound"));
            ASTPtr in_bucket = makeFunctionWithArguments("greaterOrEquals", {arguments[0], bound});
            node = makeFunctionWithArguments("arrayCount", {makeLambda({"__trino_bound"}, in_bucket), arguments[1]});
            UNUSED(function);
        }},
        {"cosine_similarity", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 2, 2, "(array, array)");
            node = makeFunctionWithArguments(
                "minus",
                {make_intrusive<ASTLiteral>(Float64(1.0)), makeFunctionWithArguments("cosineDistance", {arguments[0], arguments[1]})});
        }},
        /// (nan() and infinity() are handled at the token level: `nan` and `inf`
        /// are literal keywords in ClickHouse, so the call form does not parse.)

        /// Date and time. (date_add is handled at the token level: the unit is
        /// unquoted so the special DATE_ADD form of the ClickHouse parser applies.)
        {"from_unixtime", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            /// DANGER if unmapped: the second argument of ClickHouse FROM_UNIXTIME is a format string.
            requireArguments(function, arguments, 1, 2, "(unixtime[, zone]) - fixed offsets are not translated");
            function.name = "toDateTime64";
            arguments.insert(arguments.begin() + 1, make_intrusive<ASTLiteral>(UInt64(3)));
        }},
        {"from_iso8601_timestamp", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 1, 1, "(string)");
            function.name = "parseDateTime64BestEffort";
            arguments.push_back(make_intrusive<ASTLiteral>(UInt64(3)));
        }},
        {"from_iso8601_timestamp_nanos", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 1, 1, "(string)");
            function.name = "parseDateTime64BestEffort";
            arguments.push_back(make_intrusive<ASTLiteral>(UInt64(9)));
        }},
        {"with_timezone", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 2, 2, "(timestamp, zone)");
            /// Reinterprets the wall-clock reading in the given zone.
            node = makeFunctionWithArguments(
                "toDateTime64",
                {makeFunctionWithArguments("toString", {arguments[0]}), make_intrusive<ASTLiteral>(UInt64(3)), arguments[1]});
        }},
        {"to_iso8601", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 1, 1, "(timestamp)");
            function.name = "formatDateTime";
            arguments.push_back(make_intrusive<ASTLiteral>(String("%Y-%m-%dT%H:%i:%S")));
        }},
        {"timezone", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            /// Trino timezone(x) returns the zone of the value. The zero-argument
            /// form is left alone: it is the ClickHouse timeZone() (the session
            /// zone), which the parser itself generates when desugaring AT LOCAL.
            if (arguments.size() != 1)
                return;
            function.name = "timezoneOf";
        }},
        {"current_timestamp", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            /// current_timestamp(p): the precision argument requires now64.
            if (arguments.size() == 1)
                function.name = "now64";
        }},
        {"localtimestamp", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 0, 1, "([precision])");
            function.name = arguments.empty() ? "now" : "now64";
        }},
        {"timezone_hour", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 1, 1, "(timestamp)");
            node = makeFunctionWithArguments(
                "intDiv", {makeFunctionWithArguments("timeZoneOffset", {arguments[0]}), make_intrusive<ASTLiteral>(UInt64(3600))});
        }},
        {"timezone_minute", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 1, 1, "(timestamp)");
            node = makeFunctionWithArguments(
                "intDiv",
                {makeFunctionWithArguments(
                     "modulo", {makeFunctionWithArguments("timeZoneOffset", {arguments[0]}), make_intrusive<ASTLiteral>(UInt64(3600))}),
                 make_intrusive<ASTLiteral>(UInt64(60))});
        }},

        /// Arrays.
        {"array_join", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            /// DANGER: must never fall through to ClickHouse arrayJoin (row unnesting).
            requireArguments(function, arguments, 2, 3, "(array, delimiter[, null_replacement])");
            ASTPtr element = make_intrusive<ASTIdentifier>("__trino_element");
            ASTPtr to_string = makeFunctionWithArguments("toString", {element});
            if (arguments.size() == 3)
                to_string = makeFunctionWithArguments("ifNull", {to_string, arguments[2]});
            ASTPtr strings = makeFunctionWithArguments("arrayMap", {makeLambda({"__trino_element"}, to_string), arguments[0]});
            node = makeFunctionWithArguments("arrayStringConcat", {strings, arguments[1]});
        }},
        {"repeat", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            /// DANGER if unmapped: ClickHouse repeat is string repetition.
            requireArguments(function, arguments, 2, 2, "(element, count)");
            function.name = "arrayWithConstant";
            std::swap(arguments[0], arguments[1]);
        }},
        {"sequence", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            /// Trino sequence is inclusive and auto-descends; range is end-exclusive.
            requireArguments(function, arguments, 2, 3, "(start, stop[, step])");
            if (arguments.size() == 3)
            {
                ASTPtr stop = makeFunctionWithArguments("plus", {arguments[1], makeFunctionWithArguments("sign", {arguments[2]})});
                node = makeFunctionWithArguments("range", {arguments[0], stop, arguments[2]});
            }
            else
            {
                ASTPtr ascending = makeFunctionWithArguments("lessOrEquals", {arguments[0], arguments[1]});
                ASTPtr up = makeFunctionWithArguments(
                    "range", {arguments[0], makeFunctionWithArguments("plus", {arguments[1], make_intrusive<ASTLiteral>(UInt64(1))})});
                ASTPtr down = makeFunctionWithArguments(
                    "range",
                    {arguments[0],
                     makeFunctionWithArguments("minus", {arguments[1], make_intrusive<ASTLiteral>(UInt64(1))}),
                     make_intrusive<ASTLiteral>(Int64(-1))});
                node = makeFunctionWithArguments("if", {ascending, up, down});
            }
            UNUSED(function);
        }},
        {"trim_array", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 2, 2, "(array, count)");
            ASTPtr new_size = makeFunctionWithArguments("minus", {makeFunctionWithArguments("length", {arguments[0]}), arguments[1]});
            node = makeFunctionWithArguments("arrayResize", {arguments[0], new_size});
        }},

        /// Maps.
        {"map", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            /// DANGER if unmapped: ClickHouse map takes interleaved scalar keys and values.
            if (arguments.size() != 2)
                return;  /// map() constructs an empty map in both systems.
            function.name = "mapFromArrays";
        }},
        {"map_from_entries", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 1, 1, "(array of key-value tuples)");
            ASTPtr entry = make_intrusive<ASTIdentifier>("__trino_entry");
            ASTPtr keys = makeFunctionWithArguments(
                "arrayMap",
                {makeLambda({"__trino_entry"}, makeFunctionWithArguments("tupleElement", {entry, make_intrusive<ASTLiteral>(UInt64(1))})),
                 arguments[0]});
            ASTPtr values = makeFunctionWithArguments(
                "arrayMap",
                {makeLambda({"__trino_entry"}, makeFunctionWithArguments("tupleElement", {entry->clone(), make_intrusive<ASTLiteral>(UInt64(2))})),
                 arguments[0]->clone()});
            node = makeFunctionWithArguments("mapFromArrays", {keys, values});
        }},
        {"map_entries", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 1, 1, "(map)");
            node = makeFunctionWithArguments(
                "arrayZip",
                {makeFunctionWithArguments("mapKeys", {arguments[0]}), makeFunctionWithArguments("mapValues", {arguments[0]->clone()})});
        }},
        {"map_concat", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            /// DANGER: ClickHouse mapConcat keeps duplicate keys (left wins on lookup);
            /// Trino map_concat lets the right-most map win, like mapUpdate.
            requireArguments(function, arguments, 2, std::numeric_limits<size_t>::max(), "(map, map, ...)");
            ASTPtr result = arguments[0];
            for (size_t i = 1; i < arguments.size(); ++i)
                result = makeFunctionWithArguments("mapUpdate", {result, arguments[i]});
            node = result;
        }},

        /// Bitwise.
        {"bitwise_not", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            /// Trino bitwise functions operate on 64-bit two's complement; without
            /// the cast a small literal keeps its narrow unsigned type.
            requireArguments(function, arguments, 1, 1, "(value)");
            node = makeFunctionWithArguments("bitNot", {makeFunctionWithArguments("toInt64", {arguments[0]})});
        }},
        {"bitwise_right_shift", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            /// Trino wants a logical (zero-fill) shift; ClickHouse shifts signed types arithmetically.
            requireArguments(function, arguments, 2, 2, "(value, shift)");
            node = makeFunctionWithArguments(
                "reinterpretAsInt64",
                {makeFunctionWithArguments(
                    "bitShiftRight", {makeFunctionWithArguments("reinterpretAsUInt64", {arguments[0]}), arguments[1]})});
        }},
        {"bit_count", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 2, 2, "(value, bits)");
            const auto * bits = arguments[1]->as<ASTLiteral>();
            UInt64 width = bits && bits->value.getType() == Field::Types::UInt64 ? bits->value.safeGet<UInt64>() : 0;
            if (width == 8 || width == 16 || width == 32 || width == 64)
            {
                node = makeFunctionWithArguments(
                    "bitCount",
                    {makeFunctionWithArguments("CAST", {arguments[0], make_intrusive<ASTLiteral>("Int" + std::to_string(width))})});
                return;
            }
            if (width < 2 || width > 64)
                throwWrongArguments(function, "a constant bit width between 2 and 64");
            /// An arbitrary width counts the bits of the value truncated to the
            /// low `width` bits of its two's complement representation.
            node = makeFunctionWithArguments(
                "bitCount",
                {makeFunctionWithArguments(
                    "bitAnd",
                    {makeFunctionWithArguments("reinterpretAsUInt64", {makeFunctionWithArguments("toInt64", {arguments[0]})}),
                     make_intrusive<ASTLiteral>(UInt64((1ULL << width) - 1))})});
        }},

        /// Binary.
        {"xxhash64", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 1, 1, "(binary)");
            node = makeFunctionWithArguments(
                "reverse", {makeFunctionWithArguments("reinterpretAsFixedString", {makeFunctionWithArguments("xxHash64", {arguments[0]})})});
        }},
        {"hmac_md5", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 2, 2, "(binary, key)");
            function.name = "HMAC";
            arguments.insert(arguments.begin(), make_intrusive<ASTLiteral>(String("md5")));
        }},
        {"hmac_sha1", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 2, 2, "(binary, key)");
            function.name = "HMAC";
            arguments.insert(arguments.begin(), make_intrusive<ASTLiteral>(String("sha1")));
        }},
        {"hmac_sha256", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 2, 2, "(binary, key)");
            function.name = "HMAC";
            arguments.insert(arguments.begin(), make_intrusive<ASTLiteral>(String("sha256")));
        }},
        {"hmac_sha512", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 2, 2, "(binary, key)");
            function.name = "HMAC";
            arguments.insert(arguments.begin(), make_intrusive<ASTLiteral>(String("sha512")));
        }},
        {"from_big_endian_32", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 1, 1, "(binary)");
            node = makeFunctionWithArguments("reinterpretAsInt32", {makeFunctionWithArguments("reverse", {arguments[0]})});
        }},
        {"from_big_endian_64", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 1, 1, "(binary)");
            node = makeFunctionWithArguments("reinterpretAsInt64", {makeFunctionWithArguments("reverse", {arguments[0]})});
        }},
        {"to_big_endian_32", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 1, 1, "(integer)");
            node = makeFunctionWithArguments(
                "reverse", {makeFunctionWithArguments("reinterpretAsFixedString", {makeFunctionWithArguments("toInt32", {arguments[0]})})});
        }},
        {"to_big_endian_64", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 1, 1, "(integer)");
            node = makeFunctionWithArguments(
                "reverse", {makeFunctionWithArguments("reinterpretAsFixedString", {makeFunctionWithArguments("toInt64", {arguments[0]})})});
        }},
        {"from_ieee754_32", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 1, 1, "(binary)");
            node = makeFunctionWithArguments("reinterpretAsFloat32", {makeFunctionWithArguments("reverse", {arguments[0]})});
        }},
        {"from_ieee754_64", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 1, 1, "(binary)");
            node = makeFunctionWithArguments("reinterpretAsFloat64", {makeFunctionWithArguments("reverse", {arguments[0]})});
        }},
        {"to_ieee754_32", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 1, 1, "(real)");
            node = makeFunctionWithArguments(
                "reverse", {makeFunctionWithArguments("reinterpretAsFixedString", {makeFunctionWithArguments("toFloat32", {arguments[0]})})});
        }},
        {"to_ieee754_64", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 1, 1, "(double)");
            node = makeFunctionWithArguments(
                "reverse", {makeFunctionWithArguments("reinterpretAsFixedString", {makeFunctionWithArguments("toFloat64", {arguments[0]})})});
        }},

        /// URL.
        {"url_extract_port", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 1, 1, "(url)");
            node = makeFunctionWithArguments(
                "nullIf", {makeFunctionWithArguments("port", {arguments[0]}), make_intrusive<ASTLiteral>(UInt64(0))});
        }},

        /// Regular expressions.
        {"regexp_extract", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 2, 3, "(string, pattern[, group])");
            function.name = arguments.size() == 2 ? "extract" : "regexpExtract";
        }},
        {"regexp_extract_all", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 2, 3, "(string, pattern[, group])");
            if (arguments.size() == 2)
            {
                function.name = "extractAll";
                return;
            }
            node = makeFunctionWithArguments(
                "arrayElement",
                {makeFunctionWithArguments("extractAllGroupsHorizontal", {arguments[0], arguments[1]}), arguments[2]});
        }},
        {"regexp_replace", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 2, 3, "(string, pattern[, replacement])");
            function.name = "replaceRegexpAll";
            if (arguments.size() == 2)
            {
                arguments.push_back(make_intrusive<ASTLiteral>(String{}));
                return;
            }
            /// Trino replacement references groups as $1; ClickHouse as \1.
            if (auto * replacement = arguments[2]->as<ASTLiteral>();
                replacement && replacement->value.getType() == Field::Types::String)
            {
                String value = replacement->value.safeGet<String>();
                for (size_t i = 0; i + 1 < value.size(); ++i)
                {
                    if (value[i] == '$' && isNumericASCII(value[i + 1]))
                        value[i] = '\\';
                }
                replacement->value = value;
            }
        }},
        {"regexp_split", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 2, 2, "(string, pattern)");
            function.name = "splitByRegexp";
            std::swap(arguments[0], arguments[1]);
        }},

        /// Comparison. Trino greatest/least return NULL when any argument is NULL;
        /// ClickHouse skips NULL arguments.
        {"greatest", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            if (arguments.size() < 2)
                return;
            ASTs null_checks;
            for (const auto & argument : arguments)
                null_checks.push_back(makeFunctionWithArguments("isNull", {argument->clone()}));
            ASTPtr any_null = null_checks.size() == 1 ? null_checks[0] : makeFunctionWithArguments("or", std::move(null_checks));
            node = makeFunctionWithArguments(
                "if", {any_null, make_intrusive<ASTLiteral>(Field{}), makeFunctionWithArguments(Poco::toLower(function.name), arguments)});
        }},
        {"least", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            if (arguments.size() < 2)
                return;
            ASTs null_checks;
            for (const auto & argument : arguments)
                null_checks.push_back(makeFunctionWithArguments("isNull", {argument->clone()}));
            ASTPtr any_null = null_checks.size() == 1 ? null_checks[0] : makeFunctionWithArguments("or", std::move(null_checks));
            node = makeFunctionWithArguments(
                "if", {any_null, make_intrusive<ASTLiteral>(Field{}), makeFunctionWithArguments(Poco::toLower(function.name), arguments)});
        }},

        /// Conditional.
        {"if", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            /// Trino allows if(condition, value) returning NULL otherwise.
            if (arguments.size() == 2)
                arguments.push_back(make_intrusive<ASTLiteral>(Field{}));
            UNUSED(function);
        }},

        /// JSON. Trino JSON values are mapped to the ClickHouse JSON type (which
        /// stores objects); the path functions work on JSON text, so casts to
        /// JSON flowing directly into them are unwrapped by unwrapJSONArgument.
        {"json_parse", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 1, 1, "(string)");
            node = makeFunctionWithArguments("CAST", {arguments[0], make_intrusive<ASTLiteral>(String("JSON"))});
        }},
        {"json_format", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 1, 1, "(json)");
            node = makeFunctionWithArguments("toJSONString", {arguments[0]});
        }},
        {"json_extract_scalar", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 2, 2, "(json, json_path)");
            unwrapJSONArgument(arguments);
            function.name = "JSON_VALUE";
        }},
        {"json_value", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 2, 2, "(json, json_path)");
            unwrapJSONArgument(arguments);
            function.name = "JSON_VALUE";
        }},
        {"json_query", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 2, 2, "(json, json_path)");
            unwrapJSONArgument(arguments);
            function.name = "JSON_QUERY";
        }},
        {"json_exists", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 2, 2, "(json, json_path)");
            unwrapJSONArgument(arguments);
            function.name = "JSON_EXISTS";
        }},
        {"json_extract", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 2, 2, "(json, json_path)");
            unwrapJSONArgument(arguments);
            /// A simple constant path becomes JSONExtractRaw, which returns the
            /// bare element like Trino. There is no implicit fallback for the
            /// general paths: JSON_QUERY is close but wraps the result in an
            /// array (the SQL standard ARRAY WRAPPER), which silently changes
            /// valid Trino results.
            ASTs parts;
            if (!tryParseSimpleJSONPath(arguments[1], parts))
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Trino json_extract is translated only with a simple constant JSON path, "
                    "e.g. '$.a.b[0]' or '$[\"key\"]'. Consider json_query (note: it wraps the result into an array)");
            ASTs extract_arguments;
            extract_arguments.push_back(arguments[0]);
            extract_arguments.insert(extract_arguments.end(), parts.begin(), parts.end());
            node = makeFunctionWithArguments("JSONExtractRaw", std::move(extract_arguments));
        }},
        {"json_array_get", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            /// Deprecated in Trino but still documented; 0-based, negative from the end.
            requireArguments(function, arguments, 2, 2, "(json_array, index)");
            unwrapJSONArgument(arguments);
            const auto * index = arguments[1]->as<ASTLiteral>();
            if (!index)
                throwWrongArguments(function, "a constant index");
            ASTPtr translated_index;
            if (index->value.getType() == Field::Types::UInt64)
                translated_index = make_intrusive<ASTLiteral>(index->value.safeGet<UInt64>() + 1);
            else if (index->value.getType() == Field::Types::Int64 && index->value.safeGet<Int64>() < 0)
                translated_index = make_intrusive<ASTLiteral>(index->value);
            else
                throwWrongArguments(function, "a constant index");
            node = makeFunctionWithArguments("JSONExtractRaw", {arguments[0], translated_index});
        }},
        {"json_size", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 2, 2, "(json, json_path)");
            unwrapJSONArgument(arguments);
            ASTs parts;
            if (!tryParseSimpleJSONPath(arguments[1], parts))
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Trino json_size is translated only with a simple constant JSON path, e.g. '$.a.b[0]'");
            ASTs length_arguments;
            length_arguments.push_back(arguments[0]);
            length_arguments.insert(length_arguments.end(), parts.begin(), parts.end());
            node = makeFunctionWithArguments("JSONLength", std::move(length_arguments));
        }},
        {"is_json_scalar", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 1, 1, "(json)");
            unwrapJSONArgument(arguments);
            ASTPtr container_types = makeFunctionWithArguments(
                "tuple", {make_intrusive<ASTLiteral>(String("Object")), make_intrusive<ASTLiteral>(String("Array"))});
            node = makeFunctionWithArguments(
                "and",
                {makeFunctionWithArguments("isValidJSON", {arguments[0]}),
                 makeFunctionWithArguments(
                     "notIn", {makeFunctionWithArguments("JSONType", {arguments[0]->clone()}), container_types})});
        }},
        {"json_array_contains", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 2, 2, "(json, value)");
            unwrapJSONArgument(arguments);
            const auto * value = arguments[1]->as<ASTLiteral>();
            if (!value)
                throwWrongArguments(function, "a constant scalar value");
            String element_type;
            ASTPtr needle = arguments[1];
            switch (value->value.getType())
            {
                case Field::Types::String:
                    element_type = "Array(Nullable(String))";
                    break;
                case Field::Types::Bool:
                    element_type = "Array(Nullable(Bool))";
                    break;
                case Field::Types::UInt64:
                case Field::Types::Int64:
                case Field::Types::Float64:
                    element_type = "Array(Nullable(Float64))";
                    needle = makeFunctionWithArguments("toFloat64", {needle});
                    break;
                default:
                    throwWrongArguments(function, "a constant scalar value");
            }
            node = makeFunctionWithArguments(
                "has",
                {makeFunctionWithArguments("JSONExtract", {arguments[0], make_intrusive<ASTLiteral>(element_type)}), needle});
        }},

        /// Window functions.
        {"lead", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            /// ClickHouse fills with the type default outside the partition; Trino with NULL.
            requireArguments(function, arguments, 1, 3, "(value[, offset[, default]])");
            if (arguments.size() < 3)
                arguments[0] = makeFunctionWithArguments("toNullable", {arguments[0]});
        }},
        {"lag", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 1, 3, "(value[, offset[, default]])");
            if (arguments.size() < 3)
                arguments[0] = makeFunctionWithArguments("toNullable", {arguments[0]});
        }},

        /// Aggregate functions.
        {"approx_distinct", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 1, 2, "(value[, max_standard_error])");
            function.name = "uniq";
            arguments.resize(1);  /// The error bound has no ClickHouse counterpart.
        }},
        {"approx_percentile", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 2, 3, "(value[, weight], percentage)");
            bool weighted = arguments.size() == 3;
            ASTs parameters = literalToParameters(function, arguments.back());
            bool multiple = parameters.size() > 1 || arguments.back()->as<ASTLiteral>()->value.getType() == Field::Types::Array;
            arguments.pop_back();
            function.name = String(multiple ? "quantilesTDigest" : "quantileTDigest") + (weighted ? "Weighted" : "");
            attachParameters(function, std::move(parameters));
        }},
        {"checksum", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            requireNotWindow(function);
            requireArguments(function, arguments, 1, 1, "(value)");
            node = makeFunctionWithArguments("groupBitXor", {makeFunctionWithArguments("sipHash64", {arguments[0]})});
        }},
        {"geometric_mean", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            requireNotWindow(function);
            requireArguments(function, arguments, 1, 1, "(value)");
            node = makeFunctionWithArguments("exp", {makeFunctionWithArguments("avg", {makeFunctionWithArguments("log", {arguments[0]})})});
        }},
        {"histogram", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            requireNotWindow(function);
            /// DANGER if unmapped: the ClickHouse histogram aggregate is an adaptive
            /// numeric histogram, while Trino counts values into a map.
            requireArguments(function, arguments, 1, 1, "(value)");
            node = makeFunctionWithArguments(
                "sumMap", {makeFunctionWithArguments("map", {arguments[0], make_intrusive<ASTLiteral>(UInt64(1))})});
        }},
        {"listagg", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            requireArguments(function, arguments, 1, 2, "(value[, separator])");
            function.name = "groupConcat";
            if (arguments.size() == 2)
            {
                ASTs parameters;
                parameters.push_back(arguments[1]);
                arguments.pop_back();
                attachParameters(function, std::move(parameters));
            }
        }},
        {"array_agg", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            /// Trino `array_agg` keeps NULL elements, while the ClickHouse
            /// `groupArray` (which `array_agg` is an alias of) skips them:
            /// wrapping the value into a tuple makes the argument non-Nullable,
            /// so nothing is skipped.
            requireArguments(function, arguments, 1, 1, "(x)");
            ASTPtr group_array
                = makeFunctionWithArguments("groupArray", {makeFunctionWithArguments("tuple", {arguments[0]})});
            transferWindow(function, group_array);
            node = makeFunctionWithArguments("tupleElement", {group_array, make_intrusive<ASTLiteral>(UInt64(1))});
        }},
        {"array_aggdistinct", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            /// `array_agg(DISTINCT x)`: the same, over distinct values.
            requireArguments(function, arguments, 1, 1, "(x)");
            ASTPtr group_array
                = makeFunctionWithArguments("groupArrayDistinct", {makeFunctionWithArguments("tuple", {arguments[0]})});
            transferWindow(function, group_array);
            node = makeFunctionWithArguments("tupleElement", {group_array, make_intrusive<ASTLiteral>(UInt64(1))});
        }},
        {"map_agg", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            requireNotWindow(function);
            requireArguments(function, arguments, 2, 2, "(key, value)");
            node = makeFunctionWithArguments(
                "mapFromArrays",
                {makeFunctionWithArguments("groupArray", {arguments[0]}), makeFunctionWithArguments("groupArray", {arguments[1]})});
        }},
        {"regr_slope", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            requireNotWindow(function);
            requireArguments(function, arguments, 2, 2, "(y, x)");
            node = makeFunctionWithArguments(
                "tupleElement",
                {makeFunctionWithArguments("simpleLinearRegression", {arguments[1], arguments[0]}), make_intrusive<ASTLiteral>(UInt64(1))});
        }},
        {"regr_intercept", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            requireNotWindow(function);
            requireArguments(function, arguments, 2, 2, "(y, x)");
            node = makeFunctionWithArguments(
                "tupleElement",
                {makeFunctionWithArguments("simpleLinearRegression", {arguments[1], arguments[0]}), make_intrusive<ASTLiteral>(UInt64(2))});
        }},
        {"min", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            /// Trino min(x, n) returns the n smallest values as an array.
            if (arguments.size() != 2)
                return;
            ASTs parameters = literalToParameters(function, arguments.back());
            arguments.pop_back();
            function.name = "groupArraySorted";
            attachParameters(function, std::move(parameters));
        }},
        {"max", [](ASTPtr & node, ASTFunction & function, ASTs & arguments)
        {
            /// Trino max(x, n) returns the n largest values as an array.
            if (arguments.size() != 2)
                return;
            ASTPtr group_array = makeFunctionWithArguments("groupArray", {arguments[0]});
            transferWindow(function, group_array);
            ASTPtr sorted = makeFunctionWithArguments("arrayReverseSort", {group_array});
            node = makeFunctionWithArguments("arraySlice", {sorted, make_intrusive<ASTLiteral>(UInt64(1)), arguments[1]});
        }},

        /// Row.
        {"row", [](ASTPtr &, ASTFunction & function, ASTs & arguments)
        {
            function.name = "tuple";
            UNUSED(arguments);
        }},
    };
    return rewriters;
}

void applyToFunction(ASTPtr & node, ASTFunction & function)
{
    const String name = Poco::toLower(function.name);
    const auto & renames = getRenames();
    const auto & rewriters = getRewriters();

    if (auto it = renames.find(name); it != renames.end())
    {
        function.name = it->second;
        return;
    }

    /// Window functions with an explicit RESPECT/IGNORE NULLS keep the user's choice.
    if (function.getNullsAction() == NullsAction::EMPTY)
    {
        if (name == "first_value")
        {
            function.name = "first_value_respect_nulls";
            return;
        }
        if (name == "last_value")
        {
            function.name = "last_value_respect_nulls";
            return;
        }
    }

    auto it = rewriters.find(name);
    if (it == rewriters.end())
    {
        /// SUM(DISTINCT x) and friends are parsed into a name with the Distinct
        /// suffix; map the base name and re-attach the suffix (the combinator).
        /// This is checked after the full-name lookups: some Trino functions
        /// (approx_distinct, array_distinct) themselves end with "distinct".
        static constexpr std::string_view distinct_suffix = "distinct";
        if (name.size() > distinct_suffix.size() && name.ends_with(distinct_suffix))
        {
            String base = name.substr(0, name.size() - distinct_suffix.size());
            if (auto base_it = renames.find(base); base_it != renames.end())
                function.name = base_it->second + function.name.substr(name.size() - distinct_suffix.size());
        }
        return;
    }

    /// If the rewriter replaces the whole node, the alias must be carried over.
    const IAST * original_node = node.get();
    String alias = function.tryGetAlias();

    if (!function.arguments)
    {
        ASTs no_arguments;
        it->second(node, function, no_arguments);
    }
    else
        it->second(node, function, function.arguments->children);

    if (node.get() != original_node && !alias.empty())
        node->setAlias(alias);
}

void visit(ASTPtr & node)
{
    if (!node)
        return;

    for (auto & child : node->children)
        visit(child);

    if (auto * function = node->as<ASTFunction>())
    {
        applyToFunction(node, *function);

        /// ANSI/Trino: sum/avg/min/max over an empty set (or an empty window
        /// frame) return NULL, while ClickHouse returns the type default.
        if (auto * final_function = node->as<ASTFunction>())
        {
            String lower = Poco::toLower(final_function->name);
            /// `sum(DISTINCT x)` and friends are parsed into a name carrying the
            /// `Distinct` combinator; the base name decides, and `OrNull` is
            /// appended after it (combinators are applied from left to right).
            std::string_view base = lower;
            if (base.ends_with("distinct"))
                base.remove_suffix(std::string_view("distinct").size());
            bool value_aggregate = base == "sum" || base == "avg"
                || ((base == "min" || base == "max") && final_function->arguments
                    && final_function->arguments->children.size() == 1);
            if (value_aggregate && !final_function->parameters)
                final_function->name += "OrNull";

            /// Trino count returns bigint; the ClickHouse UInt64 has no common
            /// supertype with signed integers in set operations.
            if (lower == "count" || lower == "countif" || lower == "countdistinct")
            {
                String alias = final_function->tryGetAlias();
                if (!alias.empty())
                    final_function->setAlias("");
                node = makeFunctionWithArguments("toInt64", {node});
                if (!alias.empty())
                    node->setAlias(alias);
            }
        }
    }
}

}

void mapTrinoFunctions(ASTPtr & ast)
{
    visit(ast);
}

}
