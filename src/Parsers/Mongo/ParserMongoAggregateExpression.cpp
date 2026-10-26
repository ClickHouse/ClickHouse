#include <Parsers/Mongo/ParserMongoAggregateExpression.h>

#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>
#include <optional>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <Core/Field.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Mongo/MongoConstants.h>
#include <Parsers/Mongo/Utils.h>
#include <Common/Exception.h>
#include <Common/FieldVisitorToString.h>
#include <Poco/String.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NOT_IMPLEMENTED;
extern const int TOO_BIG_AST;
}

namespace Mongo
{

namespace
{

std::string_view stringView(const rapidjson::Value & value)
{
    return {value.GetString(), value.GetStringLength()};
}

ASTPtr makeLiteral(Field value)
{
    return make_intrusive<ASTLiteral>(std::move(value));
}

ASTPtr makeNull()
{
    return makeLiteral(Field());
}

/** The largest translation of one aggregation expression. Several operators refer to an argument
  * more than once - `$toDate` dispatches on its type, `$cmp` compares it twice - so nesting them
  * multiplies the size of the translation with every level: fifteen nested `$toDate` would make
  * millions of copies of the innermost field out of a short request, before the size of the query
  * is checked anywhere else. Every operator is checked against this bound as it is translated, so
  * the arguments it copies are bounded too, and so is the translation of the next level.
  */
constexpr size_t MAX_EXPRESSION_SIZE = 100000;

void checkExpressionSize(size_t size)
{
    if (size > MAX_EXPRESSION_SIZE)
        throw Exception(
            ErrorCodes::TOO_BIG_AST,
            "The translation of the aggregation expression is larger than {} elements: it nests operators that refer to their "
            "arguments more than once too deeply",
            MAX_EXPRESSION_SIZE);
}

ASTPtr checkExpressionSize(ASTPtr expression)
{
    /// `IAST::size` counts a subtree each time it is referenced, which is the size of the query text.
    checkExpressionSize(expression->size());
    return expression;
}

/** A piece of a translated Mongo format: either a ClickHouse format string, or Mongo's `%L` - the
  * milliseconds of the value, zero-padded to three digits. `formatDateTime` has no token for it:
  * its `%f` is always the six digits of the microseconds, whatever the scale of the value is, so
  * the milliseconds are formatted apart from the rest and the pieces are concatenated.
  */
struct DateFormatSegment
{
    std::string format;
    bool milliseconds = false;
};

/// Mongo format strings are not ClickHouse's MySQL format strings. Translate the supported,
/// unambiguous subset and reject the rest instead of producing a query with different semantics.
std::vector<DateFormatSegment> translateMongoDateFormat(const rapidjson::Value & value, std::string_view operator_name)
{
    if (!value.IsString())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The 'format' of '{}' must be a string literal", operator_name);

    const auto input = stringView(value);
    std::vector<DateFormatSegment> segments;
    std::string output;
    output.reserve(input.size());
    for (size_t pos = 0; pos < input.size(); ++pos)
    {
        if (input[pos] != '%')
        {
            output += input[pos];
            continue;
        }
        if (++pos == input.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'format' of '{}' ends with '%'", operator_name);

        switch (input[pos])
        {
            case '%': output += "%%"; break;
            case 'Y': output += "%Y"; break;
            case 'm': output += "%m"; break;
            case 'd': output += "%d"; break;
            case 'H': output += "%H"; break;
            case 'M': output += "%i"; break;
            case 'S': output += "%s"; break;
            case 'L':
                segments.push_back({.format = std::move(output), .milliseconds = false});
                segments.push_back({.format = {}, .milliseconds = true});
                output.clear();
                break;
            case 'z': output += "%z"; break;
            default:
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED, "The Mongo date format token '%{}' of '{}' is not supported", input[pos], operator_name);
        }
    }
    if (!output.empty() || segments.empty())
        segments.push_back({.format = std::move(output), .milliseconds = false});
    return segments;
}

/** The format a translated Mongo format is parsed by. `parseDateTime64` reads the three digits of
  * a millisecond with `%f` as well, so the milliseconds need nothing of their own here.
  */
ASTPtr makeDateFormatLiteral(const std::vector<DateFormatSegment> & segments)
{
    std::string format;
    for (const auto & segment : segments)
        format += segment.milliseconds ? "%f" : segment.format;
    return makeLiteral(Field(std::move(format)));
}

ASTPtr makeFunction(const std::string & name, std::vector<ASTPtr> arguments)
{
    auto function = makeASTFunction(name);
    for (auto & argument : arguments)
        function->arguments->children.push_back(std::move(argument));
    return function;
}

/// The text of a date in a translated Mongo format, formatted piece by piece: see
/// `DateFormatSegment` for why the milliseconds cannot be a token of one format string.
ASTPtr makeFormattedDate(const ASTPtr & date, const std::vector<DateFormatSegment> & segments, const ASTPtr & timezone)
{
    /// The date is copied once per piece and the number of pieces is chosen by the format, so the
    /// size of the translation is checked before each copy rather than only once it is built.
    const size_t piece_size = date->size() + timezone->size() + 8;
    size_t total_size = 0;
    auto formatted = [&](const std::string & format)
    {
        total_size += piece_size;
        checkExpressionSize(total_size);
        return makeASTFunction("formatDateTime", date->clone(), makeLiteral(Field(format)), timezone->clone());
    };

    std::vector<ASTPtr> parts;
    for (const auto & segment : segments)
    {
        if (segment.milliseconds)
            parts.push_back(
                makeASTFunction("substring", formatted("%f"), makeLiteral(Field(UInt64(1))), makeLiteral(Field(UInt64(3)))));
        else if (!segment.format.empty())
            parts.push_back(formatted(segment.format));
    }

    if (parts.empty())
        return makeLiteral(Field(String()));
    if (parts.size() == 1)
        return std::move(parts.front());
    return makeFunction("concat", std::move(parts));
}

/// The arguments of an operator: Mongo lets an operator that takes a single argument spell it
/// either as the argument itself or as an array holding it.
std::vector<ASTPtr> parseArguments(const rapidjson::Value & value)
{
    std::vector<ASTPtr> arguments;
    if (value.IsArray())
    {
        arguments.reserve(value.Size());
        for (const auto & element : value.GetArray())
            arguments.push_back(parseMongoAggregateExpression(element));
    }
    else
        arguments.push_back(parseMongoAggregateExpression(value));
    return arguments;
}

void requireArgumentCount(std::string_view operator_name, const std::vector<ASTPtr> & arguments, size_t expected)
{
    if (arguments.size() != expected)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'{}' takes {} arguments, got {}", operator_name, expected, arguments.size());
}

/// Operators that map onto a ClickHouse function of the same arity, argument for argument.
const std::unordered_map<std::string_view, std::string> direct_functions = {
    {"$strLenBytes", "length"},
    {"$strLenCP", "lengthUTF8"},
    {"$toUpper", "upper"},
    {"$toLower", "lower"},
    {"$abs", "abs"},
    {"$ceil", "ceil"},
    {"$floor", "floor"},
    {"$round", "round"},
    {"$trunc", "trunc"},
    {"$sqrt", "sqrt"},
    {"$exp", "exp"},
    {"$ln", "log"},
    {"$log10", "log10"},
    {"$reverseArray", "reverse"},
    {"$concatArrays", "arrayConcat"},
    {"$setIntersection", "arrayIntersect"},
    {"$range", "range"},
    {"$concat", "concat"},
    {"$divide", "divide"},
    {"$mod", "modulo"},
    {"$pow", "pow"},
    {"$ifNull", "coalesce"},
};

/** The operators that take a part of a date, with the `mode` of the ClickHouse function where it
  * has one: `$week` counts the weeks from Sunday, the first of them being week 0, and `$dayOfWeek`
  * and `$isoDayOfWeek` number the days from Sunday and from Monday as 1.
  */
const std::unordered_map<std::string_view, std::pair<std::string, std::optional<UInt64>>> date_part_functions = {
    {"$year", {"toYear", {}}},
    {"$month", {"toMonth", {}}},
    {"$dayOfMonth", {"toDayOfMonth", {}}},
    {"$dayOfYear", {"toDayOfYear", {}}},
    {"$dayOfWeek", {"toDayOfWeek", 3}},
    {"$isoDayOfWeek", {"toDayOfWeek", 0}},
    {"$week", {"toWeek", 0}},
    {"$isoWeek", {"toISOWeek", {}}},
    {"$isoWeekYear", {"toISOYear", {}}},
    {"$hour", {"toHour", {}}},
    {"$minute", {"toMinute", {}}},
    {"$second", {"toSecond", {}}},
    {"$millisecond", {"toMillisecond", {}}},
};

/// Operators that fold their arguments into a tree of binary applications.
const std::unordered_map<std::string_view, std::string> folded_functions = {
    {"$multiply", "multiply"},
};

/// A member of the document of an operator such as `$regexFind` or `$dateTrunc`.
const rapidjson::Value & requireMember(const rapidjson::Value & value, const char * name, std::string_view operator_name)
{
    if (!value.IsObject())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The argument of '{}' must be a document", operator_name);
    auto it = value.FindMember(name);
    if (it == value.MemberEnd())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'{}' must have a '{}' field", operator_name, name);
    return it->value;
}

/// Refuses a field of the document of an operator that this endpoint does not implement: a field
/// such as the `onError` of `$dateFromString` changes the value the operator returns, so it must
/// not be dropped silently and answered with a different expression.
void rejectUnknownMembers(
    const rapidjson::Value & value, std::string_view operator_name, const std::unordered_set<std::string_view> & supported)
{
    /// The members of the document are looked up after this check, which only a document has.
    if (!value.IsObject())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The argument of '{}' must be a document", operator_name);
    for (auto it = value.MemberBegin(); it != value.MemberEnd(); ++it)
    {
        auto name = stringView(it->name);
        if (!supported.contains(name))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The '{}' field of '{}' is not supported", name, operator_name);
    }
}

/** The argument of `$literal`, which is a value rather than an expression: an array becomes an
  * array of values and an embedded document a `JSON` value, the same shapes an inserted document
  * writes. A `$`-named field of it is a field, not an operator - that is what `$literal` is for.
  */
ASTPtr parseLiteralValue(const rapidjson::Value & value)
{
    if (value.IsArray())
    {
        auto array = makeASTFunction("array");
        for (const auto & element : value.GetArray())
            array->arguments->children.push_back(parseLiteralValue(element));
        return array;
    }
    if (auto constant = tryParseMongoConstant(value))
        return constant;
    if (value.IsObject())
        return makeMongoJSONValue(value, {}, /* keep_operator_documents */ true);
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot translate the argument of '$literal' into a value");
}

/// `x -> <body>`, the shape a ClickHouse higher order function takes.
ASTPtr makeLambda(const std::string & parameter, ASTPtr body)
{
    auto parameters = makeASTFunction("tuple", make_intrusive<ASTIdentifier>(parameter));
    return makeASTFunction("lambda", std::move(parameters), std::move(body));
}

/** The lambda parameter a variable `$$<name>` is translated into. A field path `$<name>` reads the
  * field of the document even inside the body of the operator that binds `<name>`, so the variable
  * cannot be the identifier of the same name, which is the field: `$$x` and `$x` are different
  * values in `{$add: ["$$x", "$x"]}`. A variable of an inner operator still shadows one of the same
  * name of an outer one, because the inner lambda parameter shadows the outer one.
  */
std::string variableIdentifier(std::string_view name)
{
    return fmt::format("__mongo_variable_{}", name);
}

/// The name a `$map` or a `$filter` binds its element to. Mongo writes it as `$$<name>` and
/// defaults it to `this`.
std::string parseVariableName(const rapidjson::Value & argument, const char * default_name)
{
    auto it = argument.FindMember("as");
    if (it == argument.MemberEnd())
        return default_name;
    if (!it->value.IsString())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'as' of a higher order operator must be a string");
    return std::string(stringView(it->value));
}

/// The user variables bound by the `$map` and `$filter` operators around the expression being
/// parsed, innermost last. A `$$<name>` is valid only inside the body of an operator that binds it:
/// Mongo refuses any other as an undefined variable, so it must not fall back to a column.
thread_local std::vector<std::string> bound_variables;

/// Binds a variable for as long as the body of the operator that declares it is parsed.
class BoundVariableScope
{
public:
    explicit BoundVariableScope(std::string name) { bound_variables.push_back(std::move(name)); }
    ~BoundVariableScope() { bound_variables.pop_back(); }
    BoundVariableScope(const BoundVariableScope &) = delete;
    BoundVariableScope & operator=(const BoundVariableScope &) = delete;
};

/// The function that adds a date unit of `$dateAdd` (and, with `add` replaced by `subtract`, of
/// `$dateSubtract`). Unlike `plus` with an interval, these take the time zone the calendar
/// arithmetic runs in, which is what the `timezone` of the operator names.
std::string dateAddFunction(const rapidjson::Value & unit, std::string_view operator_name)
{
    if (!unit.IsString())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'unit' of '{}' must be a string", operator_name);

    static const std::unordered_map<std::string_view, std::string> functions = {
        {"year", "addYears"},
        {"quarter", "addQuarters"},
        {"month", "addMonths"},
        {"week", "addWeeks"},
        {"day", "addDays"},
        {"hour", "addHours"},
        {"minute", "addMinutes"},
        {"second", "addSeconds"},
        {"millisecond", "addMilliseconds"},
    };

    auto it = functions.find(stringView(unit));
    if (it == functions.end())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The unit '{}' of '{}' is not supported", stringView(unit), operator_name);
    return it->second;
}

/// The `unit` of `$dateTrunc` and `$dateDiff`, which is a string naming a part of a date.
std::string_view dateUnit(const rapidjson::Value & argument, std::string_view operator_name)
{
    const auto & unit = requireMember(argument, "unit", operator_name);
    if (!unit.IsString())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'unit' of '{}' must be a string", operator_name);
    return stringView(unit);
}

/** The `timezone` of a date operator: the time zone its calendar arithmetic runs in, which decides
  * where a day, a week or a month begins and how long a day is. Mongo runs it in UTC unless the
  * operator names another one, whatever time zone the value was written with, so UTC is named
  * explicitly: otherwise the ClickHouse functions would use the time zone of the type of the value.
  */
ASTPtr parseTimezoneMember(const rapidjson::Value & argument)
{
    auto it = argument.FindMember("timezone");
    if (it == argument.MemberEnd())
        return makeLiteral(Field(String("UTC")));
    return parseMongoAggregateExpression(it->value);
}

/** The `mode` of `toStartOfWeek` for the `startOfWeek` of `$dateTrunc` and `$dateDiff`. Mongo
  * starts a week on Sunday unless the operator says otherwise, and ClickHouse can start one on
  * Sunday (mode 0) or on Monday (mode 1); a week that starts on another day is refused rather than
  * counted from a day the operator did not ask for. It matters for the `week` unit only, as in
  * Mongo, where the field is ignored for every other unit.
  */
UInt64 startOfWeekMode(const rapidjson::Value & argument, std::string_view operator_name)
{
    auto it = argument.FindMember("startOfWeek");
    if (it == argument.MemberEnd())
        return 0;
    if (!it->value.IsString())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'startOfWeek' of '{}' must be a string", operator_name);

    auto day = Poco::toLower(std::string(stringView(it->value)));
    if (day == "sunday" || day == "sun")
        return 0;
    if (day == "monday" || day == "mon")
        return 1;
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "A week of '{}' can start on Sunday or on Monday only, a 'startOfWeek' of '{}' is not supported",
        operator_name, stringView(it->value));
}

/** The `amount` of `$dateAdd` and `$dateSubtract`, which Mongo requires to be a whole number. The
  * ClickHouse functions it goes to would truncate a fraction instead - `1.5` days would add one -
  * so a literal is checked here and any other expression is checked per row: `throwIf` answers
  * `0` for a whole number and is always evaluated, as an argument of `plus` rather than a branch
  * of a conditional that could be skipped.
  */
ASTPtr parseDateAddAmount(const rapidjson::Value & value, std::string_view operator_name)
{
    if (value.IsNumber())
    {
        if (value.IsDouble() && (!std::isfinite(value.GetDouble()) || std::trunc(value.GetDouble()) != value.GetDouble()))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'amount' of '{}' must be a whole number", operator_name);
        return parseMongoAggregateExpression(value);
    }

    auto amount = parseMongoAggregateExpression(value);
    auto fractional = makeASTFunction("notEquals", amount->clone(), makeASTFunction("trunc", amount->clone()));
    auto check = makeASTFunction(
        "throwIf", std::move(fractional), makeLiteral(Field(fmt::format("The 'amount' of '{}' must be a whole number", operator_name))));
    return makeASTFunction("plus", makeASTFunction("toInt64", std::move(amount)), std::move(check));
}

/// `toStartOfWeek(date, mode, timezone)`: the first day of the week a date falls in.
ASTPtr makeStartOfWeek(ASTPtr date, UInt64 mode, const ASTPtr & timezone)
{
    return makeASTFunction("toStartOfWeek", std::move(date), makeLiteral(Field(mode)), timezone->clone());
}

/** The regular expression of `$regexFind` and `$regexMatch`: the `regex` field - a bare pattern
  * string, or the Extended JSON document a driver sends for a regular expression literal - with
  * the sibling `options` field of the operator applied to it. Mongo rejects `options` next to a
  * regular expression that carries options of its own, and so does this.
  */
std::string parseRegularExpressionField(const rapidjson::Value & argument, std::string_view operator_name)
{
    const auto & value = requireMember(argument, "regex", operator_name);

    std::string_view options;
    if (auto it = argument.FindMember("options"); it != argument.MemberEnd())
    {
        if (!it->value.IsString())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'options' of '{}' must be a string", operator_name);
        options = stringView(it->value);
        if (!value.IsString())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The 'options' of '{}' cannot be set next to a regular expression that carries options of its own",
                operator_name);
    }

    if (value.IsString())
        return applyMongoRegularExpressionOptions(stringView(value), options);
    if (auto pattern = tryParseMongoRegularExpression(value))
        return *pattern;
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'regex' of '{}' must be a string or a regular expression", operator_name);
}

/** Which capture groups of a regular expression may not take part in a match: the groups under a
  * quantifier that allows zero repetitions - `?`, `*`, `{0,n}` - and the groups inside a branch of
  * an alternation. Mongo answers `null` for the capture of such a group when it does not take part,
  * while the ClickHouse functions answer an empty string, the same as for a group that matched an
  * empty text. The capture of a group that always takes part is never `null`.
  */
std::vector<bool> findOptionalCaptureGroups(std::string_view pattern)
{
    struct Group
    {
        bool capturing = false;
        /// The index of the first capture group inside this group, or of the group itself.
        size_t first_capture = 0;
        bool has_alternation = false;
    };

    std::vector<bool> optional;
    std::vector<Group> groups;
    bool top_level_alternation = false;
    auto mark_from = [&](size_t first)
    {
        for (size_t i = first; i < optional.size(); ++i)
            optional[i] = true;
    };

    for (size_t pos = 0; pos < pattern.size(); ++pos)
    {
        const char c = pattern[pos];
        if (c == '\\')
        {
            /// `\Q...\E` quotes its text, and any other escape is one character.
            if (pos + 1 < pattern.size() && pattern[pos + 1] == 'Q')
            {
                auto end = pattern.find("\\E", pos + 2);
                if (end == std::string_view::npos)
                    break;
                pos = end + 1;
            }
            else
                ++pos;
        }
        else if (c == '[')
        {
            ++pos;
            if (pos < pattern.size() && pattern[pos] == '^')
                ++pos;
            if (pos < pattern.size() && pattern[pos] == ']')
                ++pos;
            while (pos < pattern.size() && pattern[pos] != ']')
            {
                if (pattern[pos] == '\\')
                    ++pos;
                else if (pattern.substr(pos).starts_with("[:"))
                {
                    auto end = pattern.find(":]", pos + 2);
                    if (end != std::string_view::npos)
                        pos = end + 1;
                }
                ++pos;
            }
        }
        else if (c == '(')
        {
            /// `(?P<name>...)` and `(?<name>...)` capture; every other group starting with `(?` does not.
            const auto rest = pattern.substr(pos + 1);
            const bool named = rest.starts_with("?P<") || (rest.starts_with("?<") && !rest.starts_with("?<=") && !rest.starts_with("?<!"));
            const bool capturing = !rest.starts_with('?') || named;
            groups.push_back({.capturing = capturing, .first_capture = optional.size(), .has_alternation = false});
            if (capturing)
                optional.push_back(false);
        }
        else if (c == ')')
        {
            if (groups.empty())
                continue;
            const Group group = groups.back();
            groups.pop_back();
            if (group.has_alternation)
                mark_from(group.first_capture + (group.capturing ? 1 : 0));
            const auto next = pattern.substr(pos + 1);
            if (next.starts_with('?') || next.starts_with('*') || next.starts_with("{0"))
                mark_from(group.first_capture);
        }
        else if (c == '|')
        {
            if (groups.empty())
                top_level_alternation = true;
            else
                groups.back().has_alternation = true;
        }
    }
    if (top_level_alternation)
        mark_from(0);
    return optional;
}

/** The lowering of `$toDecimal`, which is supposed to preserve the full value: Mongo's
  * `Decimal128` carries an exponent of its own, so no single fixed scale can hold every value,
  * and the scale has to be derived from the argument the way `$numberDecimal` derives it.
  * A constant carries its digits, so its exact scale is computed here; a value that fits no
  * scale of `Decimal128` is rejected rather than silently rounded. A non-constant argument
  * only converts exactly when it is an integer or a boolean - their scale is zero whatever
  * the value - so anything else is rejected when the query is analyzed, by a `throwIf` on the
  * type name, which is a constant.
  */
ASTPtr makeToDecimal(ASTPtr argument)
{
    if (const auto * literal = argument->as<ASTLiteral>())
    {
        std::optional<UInt32> scale;
        switch (literal->value.getType())
        {
            case Field::Types::Null:
            case Field::Types::Bool:
            case Field::Types::Int64:
            case Field::Types::UInt64: scale = 0; break;
            case Field::Types::String: scale = decimalScaleOfNumberDecimal(literal->value.safeGet<String>()); break;
            case Field::Types::Float64:
            {
                /// The shortest decimal text that reads back as the same double, which is what
                /// Mongo preserves when it converts a double to a decimal. That text is what is
                /// converted, rather than the double: `toDecimal128(1.15, 2)` multiplies the double
                /// by a power of ten and truncates the product, which is `1.14`.
                auto text = applyVisitor(FieldVisitorToString(), literal->value);
                scale = decimalScaleOfNumberDecimal(text);
                if (scale)
                    argument = makeLiteral(Field(std::move(text)));
                break;
            }
            default: break;
        }
        if (!scale)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The argument of '$toDecimal' cannot be represented exactly by a Decimal128");
        return makeASTFunction("toDecimal128", std::move(argument), makeLiteral(Field(UInt64(*scale))));
    }

    /// The `CAST` a `$numberDecimal` wrapper lowers into already carries the exact scale of its
    /// value (see `tryParseMongoConstant`), so it is a decimal already.
    if (const auto * function = argument->as<ASTFunction>(); function && function->name == "CAST")
        return argument;

    auto is_exact = makeASTFunction(
        "match", makeASTFunction("toTypeName", argument), makeLiteral(Field(String("^(U?Int(8|16|32|64|128|256)|Bool)$"))));
    auto guard = makeASTFunction(
        "throwIf",
        makeASTFunction("not", std::move(is_exact)),
        makeLiteral(Field(String(
            "'$toDecimal' of a non-constant value is only exact for an integer or a boolean; "
            "spell any other value as a '$numberDecimal' or a string literal, which carry their scale"))));
    return makeASTFunction("plus", makeASTFunction("toDecimal128", argument->clone(), makeLiteral(Field(UInt64(0)))), std::move(guard));
}

/** The lowering of `$toDate`. Mongo reads a numeric argument as Unix *milliseconds*, while
  * `toDateTime64` reads a number as seconds - `{"$toDate": 1546300800000}` is 2019 in Mongo,
  * not the year 2282 - so a numeric argument goes through `fromUnixTimestamp64Milli` instead.
  * A literal carries its type here; any other argument dispatches on the type of the value it holds
  * in each row (see `makeRowType`), which sees through a `Nullable` column and a `Dynamic` path of a
  * stored document - the type of either as a whole names no number.
  */
ASTPtr makeRowType(const ASTPtr & value);

ASTPtr makeToDate(ASTPtr argument)
{
    const auto parsed = [](ASTPtr arg)
    { return makeASTFunction("toDateTime64", std::move(arg), makeLiteral(Field(UInt64(3))), makeLiteral(Field(String("UTC")))); };

    if (const auto * literal = argument->as<ASTLiteral>())
    {
        switch (literal->value.getType())
        {
            case Field::Types::Int64:
            case Field::Types::UInt64:
            case Field::Types::Float64:
                /// A fractional count of milliseconds truncates, as a BSON double does.
                return makeASTFunction(
                    "fromUnixTimestamp64Milli", makeASTFunction("toInt64", std::move(argument)), makeLiteral(Field(String("UTC"))));
            default: return parsed(std::move(argument));
        }
    }

    /// Neither branch may throw for the type of the other: the losing branch is dropped by the
    /// analyzer and skipped by short-circuit evaluation, but with the old analyzer and
    /// `short_circuit_function_evaluation = 'disable'` it still runs. `toFloat64OrZero` over
    /// `toString` reads any type without throwing; the string it produces for a number is the
    /// number itself, and the branch taken for a string never uses it.
    auto milliseconds = makeASTFunction(
        "fromUnixTimestamp64Milli",
        makeASTFunction("toInt64", makeASTFunction("toFloat64OrZero", makeASTFunction("toString", argument->clone()))),
        makeLiteral(Field(String("UTC"))));
    auto is_a_number = makeASTFunction("match", makeRowType(argument), makeLiteral(Field(String("^(U?Int|Float|Decimal)"))));
    return makeASTFunction("if", std::move(is_a_number), std::move(milliseconds), parsed(argument->clone()));
}

/** The lowering of `$toBool`, which converts by the type of the value rather than by its text: a
  * number is `false` when it is zero, a boolean is itself, `null` stays `null`, and every other value
  * - every string, `"false"` and `""` included, a date, an array - is `true`. `toBool` would parse a
  * string instead. A literal carries its type here; any other value is dispatched on the type it
  * holds in each row, which `dynamicType` names for a value of any type cast to `Dynamic` - including
  * a path of a stored document, whose type is `Dynamic` as a whole. The text of a number is what is
  * tested, because it can be computed for a value of any type without an error in the branches that
  * are not taken.
  */
ASTPtr makeToBool(ASTPtr argument)
{
    if (const auto * literal = argument->as<ASTLiteral>())
    {
        switch (literal->value.getType())
        {
            case Field::Types::Null: return makeNull();
            case Field::Types::Bool: return argument;
            case Field::Types::Int64: return makeLiteral(Field(literal->value.safeGet<Int64>() != 0));
            case Field::Types::UInt64: return makeLiteral(Field(literal->value.safeGet<UInt64>() != 0));
            case Field::Types::Float64: return makeLiteral(Field(literal->value.safeGet<Float64>() != 0));
            case Field::Types::String: return makeLiteral(Field(true));
            default: break;
        }
    }

    auto type = makeASTFunction("dynamicType", makeASTFunction("CAST", argument->clone(), makeLiteral(Field(String("Dynamic")))));
    auto text = makeASTFunction("toString", argument->clone());
    auto is_true_number = makeASTFunction(
        "or",
        makeASTFunction("equals", text, makeLiteral(Field(String("true")))),
        makeASTFunction("notEquals", makeASTFunction("toFloat64OrZero", text->clone()), makeLiteral(Field(UInt64(0)))));
    return makeASTFunction(
        "toBool",
        makeASTFunction(
            "multiIf",
            makeASTFunction("equals", type, makeLiteral(Field(String("None")))),
            makeNull(),
            makeASTFunction("match", type->clone(), makeLiteral(Field(String("^(U?Int|Float|Decimal|Bool)")))),
            std::move(is_true_number),
            makeLiteral(Field(UInt64(1)))));
}

/** Whether a value counts as true where Mongo asks for a boolean - in `$and`, `$or`, `$not` and the
  * conditions of `$cond`, `$switch` and `$filter`. It is what `$toBool` converts the value to, except
  * that `null` and a missing value are false rather than `null`: SQL's three-valued logic would make
  * `{$not: [null]}` a `NULL` rather than `true`. A comparison or a boolean operator is a boolean
  * already, and only its `NULL` has to be turned into `false`.
  */
bool isBooleanFunction(const ASTPtr & argument)
{
    static const std::unordered_set<std::string_view> boolean_functions{
        "equals", "notEquals", "less", "lessOrEquals", "greater", "greaterOrEquals", "and", "or", "not", "isNull", "isNotNull",
        "isNotDistinctFrom"};

    const auto * function = argument->as<ASTFunction>();
    return function && boolean_functions.contains(function->name);
}

ASTPtr makeTruth(ASTPtr argument)
{
    if (argument->as<ASTLiteral>())
    {
        auto converted = makeToBool(std::move(argument));
        if (const auto * literal = converted->as<ASTLiteral>(); literal && literal->value.isNull())
            return makeLiteral(Field(false));
        if (converted->as<ASTLiteral>())
            return converted;
        return makeASTFunction("ifNull", std::move(converted), makeLiteral(Field(false)));
    }

    if (isBooleanFunction(argument))
        return makeASTFunction("ifNull", std::move(argument), makeLiteral(Field(false)));

    return makeASTFunction("ifNull", makeToBool(std::move(argument)), makeLiteral(Field(false)));
}

/** A comparison of two values: `$eq`, `$ne`, `$lt`, `$lte`, `$gt`, `$gte` and `$cmp`. Mongo compares
  * `null` as a value - it equals `null` and orders before every other value - while a comparison of
  * SQL answers `NULL` when either side is `NULL`: `{$eq: [null, null]}` would be `NULL` rather than
  * `true`, and `{$not: [{$eq: ["$a", null]}]}` would hold for a document whose `a` is `null`. So the
  * outcome for a side that is `NULL` is decided first, and the SQL comparison only compares two
  * values. Equality is `isNotDistinctFrom`, which is exactly that and which an index can use.
  */
ASTPtr makeComparison(std::string_view name, std::vector<ASTPtr> arguments)
{
    requireArgumentCount(name, arguments, 2);
    ASTPtr left = std::move(arguments[0]);
    ASTPtr right = std::move(arguments[1]);

    if (name == "$eq")
        return makeASTFunction("isNotDistinctFrom", std::move(left), std::move(right));
    if (name == "$ne")
        return makeASTFunction("not", makeASTFunction("isNotDistinctFrom", std::move(left), std::move(right)));

    auto is_null = [](const ASTPtr & value) { return makeASTFunction("isNull", value->clone()); };
    auto is_not_null = [](const ASTPtr & value) { return makeASTFunction("isNotNull", value->clone()); };
    /// The SQL comparison of the two values, when neither of them is `NULL`. It is still `NULL` by
    /// type when a side is, and that branch is never taken.
    auto compare = [&](const char * function)
    {
        return makeASTFunction("ifNull", makeASTFunction(function, left->clone(), right->clone()), makeLiteral(Field(UInt64(0))));
    };

    ASTPtr if_null;
    ASTPtr if_values;
    if (name == "$lt")
    {
        if_null = makeASTFunction("and", is_null(left), is_not_null(right));
        if_values = compare("less");
    }
    else if (name == "$lte")
    {
        if_null = is_null(left);
        if_values = compare("lessOrEquals");
    }
    else if (name == "$gt")
    {
        if_null = makeASTFunction("and", is_not_null(left), is_null(right));
        if_values = compare("greater");
    }
    else if (name == "$gte")
    {
        if_null = is_null(right);
        if_values = compare("greaterOrEquals");
    }
    else
    {
        /// `$cmp`: -1, 0 or 1.
        if_null = makeASTFunction("minus", is_not_null(left), is_not_null(right));
        if_values = makeASTFunction(
            "multiIf",
            makeASTFunction("less", left->clone(), right->clone()),
            makeLiteral(Field(Int64(-1))),
            makeASTFunction("greater", left->clone(), right->clone()),
            makeLiteral(Field(Int64(1))),
            makeLiteral(Field(Int64(0))));
    }

    return makeASTFunction("if", makeASTFunction("or", is_null(left), is_null(right)), std::move(if_null), std::move(if_values));
}

/// `$and` and `$or` over the truth of each argument. Mongo answers `true` for an `$and` of nothing and
/// `false` for an `$or` of nothing, while the functions `and` and `or` take at least two arguments.
ASTPtr makeLogical(std::string_view name, std::vector<ASTPtr> arguments)
{
    const bool is_and = name == "$and";
    if (arguments.empty())
        return makeLiteral(Field(is_and));

    std::vector<ASTPtr> truths;
    truths.reserve(arguments.size());
    for (auto & argument : arguments)
        truths.push_back(makeTruth(std::move(argument)));
    if (truths.size() == 1)
        return std::move(truths.front());
    return makeFunction(is_and ? "and" : "or", std::move(truths));
}

/** The BSON type of a value is the type it holds in each row, which `dynamicType` names for a value
  * of any type cast to `Dynamic` - including a path of a stored document, whose type is `Dynamic` as a
  * whole. The operators below dispatch on it, and none of their branches may throw for a value of the
  * type of another branch: the branch that is not taken may still be computed.
  */
ASTPtr makeRowType(const ASTPtr & value)
{
    return makeASTFunction("dynamicType", makeASTFunction("CAST", value->clone(), makeLiteral(Field(String("Dynamic")))));
}

ASTPtr makeIsDate(const ASTPtr & value)
{
    return makeASTFunction("match", makeRowType(value), makeLiteral(Field(String("^Date"))));
}

/// The value as a date of milliseconds, for a value that is a date; anything else is the Unix epoch
/// rather than an error, in the branch that is not taken for it.
ASTPtr makeDateOrEpoch(const ASTPtr & value)
{
    return makeASTFunction("accurateCastOrDefault", value->clone(), makeLiteral(Field(String("DateTime64(3, 'UTC')"))));
}

/// The milliseconds since the Unix epoch of a value that is a date, which is the number a BSON date is.
ASTPtr makeMillisecondsOfDate(const ASTPtr & value)
{
    return makeASTFunction("toUnixTimestamp64Milli", makeDateOrEpoch(value));
}

/// A number as a count of milliseconds to move a date by: a fractional one is rounded, and anything
/// that is not a number - a date, in the branch that is not taken for it - reads as zero.
ASTPtr makeNumberAsFloat(const ASTPtr & value)
{
    return makeASTFunction("toFloat64OrZero", makeASTFunction("toString", value->clone()));
}

/** The lowering of `$toString`. Mongo writes a date as ISO 8601 in UTC with milliseconds, the
  * same text as the default of `$dateToString`, while `toString` writes the local time of the type
  * of the value without the `T` and the `Z`. A value of any other type is converted by `toString`.
  */
ASTPtr makeToString(const ASTPtr & value)
{
    std::vector<DateFormatSegment> iso_format{
        {.format = "%Y-%m-%dT%H:%i:%s.", .milliseconds = false},
        {.format = {}, .milliseconds = true},
        {.format = "Z", .milliseconds = false}};
    auto as_date = makeFormattedDate(makeDateOrEpoch(value), iso_format, makeLiteral(Field(String("UTC"))));
    return makeASTFunction("if", makeIsDate(value), std::move(as_date), makeASTFunction("toString", value->clone()));
}

bool isNumericLiteral(const ASTPtr & value)
{
    const auto * literal = value->as<ASTLiteral>();
    if (!literal)
        return false;
    const auto type = literal->value.getType();
    return type == Field::Types::Int64 || type == Field::Types::UInt64 || type == Field::Types::Float64;
}

/** The lowering of `$toInt` and `$toLong`. Mongo truncates a fractional number toward zero, reads a
  * boolean as 0 or 1 and a string as the integer it spells, converts a date into its milliseconds
  * for `$toLong`, and raises an error for a value outside of the range of the result. The functions
  * `toInt32` and `toInt64` would instead wrap an integer that does not fit around and read a date as
  * seconds. So the value is turned into the text of the integer it converts to, and that text is
  * converted by `accurateCast`, which refuses a text that does not spell an integer of the range.
  */
ASTPtr makeToInteger(ASTPtr argument, bool is_long)
{
    const String target = is_long ? "Int64" : "Int32";

    if (const auto * literal = argument->as<ASTLiteral>())
    {
        switch (literal->value.getType())
        {
            case Field::Types::Null: return makeNull();
            case Field::Types::Bool:
                return makeLiteral(Field(literal->value.safeGet<bool>() ? Int64(1) : Int64(0)));
            case Field::Types::Int64:
            case Field::Types::UInt64:
            case Field::Types::String:
                return makeASTFunction("accurateCast", std::move(argument), makeLiteral(Field(target)));
            case Field::Types::Float64:
                return makeASTFunction("accurateCast", makeASTFunction("trunc", std::move(argument)), makeLiteral(Field(target)));
            default: break;
        }
    }

    auto type = makeRowType(argument);
    auto text = makeASTFunction("toString", argument->clone());
    auto truncated = makeASTFunction("toString", makeASTFunction("trunc", makeASTFunction("toFloat64OrZero", text->clone())));
    /// A decimal is truncated in its text rather than through a `Float64`, which would round an
    /// integer above 2^53 before `accurateCast` could check it: the digits after the point go.
    auto truncated_decimal = makeASTFunction("replaceRegexpOne", text->clone(), makeLiteral(Field(String("\\.[0-9]*$"))), makeLiteral(Field(String())));
    /// A date has no `int` in Mongo, and the text that says so is what `accurateCast` refuses.
    ASTPtr of_date = is_long ? makeASTFunction("toString", makeMillisecondsOfDate(argument))
                             : makeLiteral(Field(String("a date cannot be converted to an int")));
    auto integer_text = makeASTFunction(
        "multiIf",
        makeASTFunction("equals", type, makeLiteral(Field(String("None")))),
        makeNull(),
        makeASTFunction("equals", type->clone(), makeLiteral(Field(String("Bool")))),
        makeASTFunction(
            "if",
            makeASTFunction("equals", text, makeLiteral(Field(String("true")))),
            makeLiteral(Field(String("1"))),
            makeLiteral(Field(String("0")))),
        makeASTFunction("match", type->clone(), makeLiteral(Field(String("^Float")))),
        std::move(truncated),
        makeASTFunction("match", type->clone(), makeLiteral(Field(String("^Decimal")))),
        std::move(truncated_decimal),
        makeASTFunction("match", type->clone(), makeLiteral(Field(String("^Date")))),
        std::move(of_date),
        text->clone());
    return makeASTFunction("accurateCast", std::move(integer_text), makeLiteral(Field("Nullable(" + target + ")")));
}

/// The lowering of `$toDouble`, which converts a date into its milliseconds, as `$toLong` does, and a
/// boolean into 0 or 1; `toFloat64` would read a date as seconds.
ASTPtr makeToDouble(ASTPtr argument)
{
    /// A literal is never a date: `{"$date": ...}` is a function (see `tryParseMongoConstant`).
    if (argument->as<ASTLiteral>())
        return makeASTFunction("toFloat64", std::move(argument));

    auto type = makeRowType(argument);
    return makeASTFunction(
        "multiIf",
        makeASTFunction("match", type, makeLiteral(Field(String("^Date")))),
        makeASTFunction("toFloat64", makeMillisecondsOfDate(argument)),
        makeASTFunction("equals", type->clone(), makeLiteral(Field(String("Bool")))),
        makeASTFunction("toFloat64", makeASTFunction("equals", makeASTFunction("toString", argument->clone()), makeLiteral(Field(String("true"))))),
        makeASTFunction("toFloat64", argument->clone()));
}

/** The lowering of `$add`. Mongo adds a number to a date as milliseconds, while a ClickHouse date
  * plus a number adds seconds, so a sum that has a date among its terms - at most one, as in Mongo -
  * moves that date by the sum of the other terms, in milliseconds. The type of a value is known in
  * each row only, so both sums are computed, and the one of the date is cast to the type of the plain
  * sum, which is the type of the date for a column of dates and `Dynamic` for a path of a document.
  * A numeric literal is not a date, which keeps a sum of a field and constants small.
  */
ASTPtr makeAdd(const std::vector<ASTPtr> & arguments)
{
    if (arguments.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'$add' takes at least one argument");

    ASTPtr sum = arguments[0];
    for (size_t i = 1; i < arguments.size(); ++i)
        sum = makeASTFunction("plus", sum, arguments[i]);

    ASTPtr any_date;
    std::vector<ASTPtr> date_of_term;
    ASTPtr offset;
    for (const auto & argument : arguments)
    {
        ASTPtr milliseconds = isNumericLiteral(argument) ? argument->clone() : makeNumberAsFloat(argument);
        offset = offset ? makeASTFunction("plus", std::move(offset), std::move(milliseconds)) : std::move(milliseconds);

        if (isNumericLiteral(argument))
            continue;
        any_date = any_date ? makeASTFunction("or", std::move(any_date), makeIsDate(argument)) : makeIsDate(argument);
        date_of_term.push_back(makeIsDate(argument));
        date_of_term.push_back(makeDateOrEpoch(argument));
    }

    if (!any_date)
        return sum;

    /// The date among the terms: the last branch is the value of the last term that can be one.
    date_of_term.erase(date_of_term.end() - 2);
    ASTPtr date = date_of_term.size() == 1 ? std::move(date_of_term.front()) : makeFunction("multiIf", std::move(date_of_term));
    auto moved = makeASTFunction(
        "addMilliseconds", std::move(date), makeASTFunction("toInt64", makeASTFunction("round", std::move(offset))));
    return makeASTFunction(
        "if", std::move(any_date), makeASTFunction("CAST", std::move(moved), makeASTFunction("toTypeName", sum->clone())), sum);
}

/** The lowering of `$subtract`: the difference of two dates is the number of milliseconds between
  * them, and a date minus a number is the date moved back by that many milliseconds; ClickHouse would
  * answer seconds for both. The branches are cast to one type, as in `$add`: the difference of two
  * columns of dates is an integer of milliseconds rather than the decimal seconds `minus` answers.
  */
ASTPtr makeSubtract(const std::vector<ASTPtr> & arguments)
{
    requireArgumentCount("$subtract", arguments, 2);
    const auto & minuend = arguments[0];
    const auto & subtrahend = arguments[1];

    auto difference = makeASTFunction("minus", minuend, subtrahend);
    if (isNumericLiteral(minuend))
        return difference;

    auto is_date_type = [](const ASTPtr & value)
    {
        return makeASTFunction(
            "match", makeASTFunction("toTypeName", value->clone()), makeLiteral(Field(String("^(Nullable\\()?Date"))));
    };
    ASTPtr type = makeASTFunction("toTypeName", difference->clone());
    if (!isNumericLiteral(subtrahend))
        type = makeASTFunction(
            "if",
            makeASTFunction("and", is_date_type(minuend), is_date_type(subtrahend)),
            makeLiteral(Field(String("Nullable(Int64)"))),
            std::move(type));
    auto cast = [&](ASTPtr value) { return makeASTFunction("CAST", std::move(value), type->clone()); };

    std::vector<ASTPtr> branches;
    if (!isNumericLiteral(subtrahend))
    {
        branches.push_back(makeASTFunction("and", makeIsDate(minuend), makeIsDate(subtrahend)));
        branches.push_back(cast(makeASTFunction("minus", makeMillisecondsOfDate(minuend), makeMillisecondsOfDate(subtrahend))));
    }
    branches.push_back(makeIsDate(minuend));
    branches.push_back(cast(makeASTFunction(
        "addMilliseconds",
        makeDateOrEpoch(minuend),
        makeASTFunction("negate", makeASTFunction("toInt64", makeASTFunction("round", makeNumberAsFloat(subtrahend)))))));
    branches.push_back(cast(std::move(difference)));
    return makeFunction("multiIf", std::move(branches));
}

/** An operator that takes a part of a date, in the time zone it names and in UTC when it names none,
  * as in Mongo. The argument is the date itself, an array holding it, or the document of a `date` and
  * a `timezone`.
  */
ASTPtr makeDatePart(std::string_view name, const rapidjson::Value & argument)
{
    const auto & [function, mode] = date_part_functions.at(name);

    ASTPtr date;
    ASTPtr timezone;
    if (argument.IsObject() && argument.MemberCount() >= 1 && !stringView(argument.MemberBegin()->name).starts_with('$'))
    {
        static const std::unordered_set<std::string_view> supported_members{"date", "timezone"};
        rejectUnknownMembers(argument, name, supported_members);
        date = parseMongoAggregateExpression(requireMember(argument, "date", name));
        timezone = parseTimezoneMember(argument);
    }
    else
    {
        auto arguments = parseArguments(argument);
        requireArgumentCount(name, arguments, 1);
        date = std::move(arguments[0]);
        timezone = makeLiteral(Field(String("UTC")));
    }

    if (mode)
        return makeASTFunction(function, std::move(date), makeLiteral(Field(*mode)), std::move(timezone));
    return makeASTFunction(function, std::move(date), std::move(timezone));
}

ASTPtr parseOperator(std::string_view name, const rapidjson::Value & argument)
{
    if (auto it = direct_functions.find(name); it != direct_functions.end())
        return makeFunction(it->second, parseArguments(argument));

    if (date_part_functions.contains(name))
        return makeDatePart(name, argument);

    if (name == "$toString")
    {
        auto arguments = parseArguments(argument);
        requireArgumentCount(name, arguments, 1);
        return makeToString(arguments[0]);
    }

    if (name == "$toBool")
    {
        auto arguments = parseArguments(argument);
        requireArgumentCount(name, arguments, 1);
        return makeToBool(arguments[0]);
    }

    if (name == "$and" || name == "$or")
        return makeLogical(name, parseArguments(argument));

    if (name == "$not")
    {
        auto arguments = parseArguments(argument);
        requireArgumentCount(name, arguments, 1);
        return makeASTFunction("not", makeTruth(std::move(arguments[0])));
    }

    if (name == "$toInt" || name == "$toLong")
    {
        auto arguments = parseArguments(argument);
        requireArgumentCount(name, arguments, 1);
        return makeToInteger(arguments[0], /* is_long = */ name == "$toLong");
    }

    if (name == "$toDouble")
    {
        auto arguments = parseArguments(argument);
        requireArgumentCount(name, arguments, 1);
        return makeToDouble(arguments[0]);
    }

    if (name == "$add")
        return makeAdd(parseArguments(argument));

    if (name == "$subtract")
        return makeSubtract(parseArguments(argument));

    if (auto it = folded_functions.find(name); it != folded_functions.end())
    {
        auto arguments = parseArguments(argument);
        if (arguments.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'{}' takes at least one argument", name);
        ASTPtr result = arguments[0];
        for (size_t i = 1; i < arguments.size(); ++i)
            result = makeASTFunction(it->second, result, arguments[i]);
        return result;
    }

    if (name == "$literal")
        return parseLiteralValue(argument);

    if (name == "$size")
    {
        auto arguments = parseArguments(argument);
        requireArgumentCount(name, arguments, 1);
        /** `$size` is defined on arrays only. A string also has a `length`, so mapping `$size`
          * onto it directly would count the bytes of a string instead of rejecting it the way
          * Mongo does. Unlike the `$size` of a filter, which is a predicate and can simply not
          * match, this one has to produce a value, so the rejection is a `throwIf` on the type
          * of the argument: the type name is a constant, and a non-array argument fails when
          * the query is analyzed.
          */
        auto is_array = makeASTFunction("startsWith", makeASTFunction("toTypeName", arguments[0]), makeLiteral(Field(String("Array"))));
        auto guard = makeASTFunction(
            "throwIf", makeASTFunction("not", std::move(is_array)), makeLiteral(Field(String("The argument of '$size' must be an array"))));
        return makeASTFunction("plus", makeASTFunction("length", arguments[0]->clone()), std::move(guard));
    }

    if (name == "$toDecimal")
    {
        auto arguments = parseArguments(argument);
        requireArgumentCount(name, arguments, 1);
        return makeToDecimal(arguments[0]);
    }

    if (name == "$toDate")
    {
        auto arguments = parseArguments(argument);
        requireArgumentCount(name, arguments, 1);
        return makeToDate(arguments[0]);
    }

    if (name == "$first" || name == "$last")
    {
        /// Outside `$group`, `$first` and `$last` take an element of an array, and of an empty one none.
        auto arguments = parseArguments(argument);
        requireArgumentCount(name, arguments, 1);
        return makeASTFunction("arrayElementOrNull", arguments[0], makeLiteral(Field(name == "$first" ? Int64(1) : Int64(-1))));
    }

    if (name == "$arrayElemAt")
    {
        auto arguments = parseArguments(argument);
        requireArgumentCount(name, arguments, 2);
        /// Mongo indexes an array from zero and ClickHouse from one; a negative index counts from
        /// the end in both, and there `-1` already means the same element. An index past either end
        /// takes no element, which is `NULL` rather than the default value `arrayElement` makes up.
        if (const auto * index = arguments[1]->as<ASTLiteral>())
        {
            const bool counts_from_the_end = index->value.getType() == Field::Types::Int64 && index->value.safeGet<Int64>() < 0;
            if (!counts_from_the_end)
                arguments[1] = makeASTFunction("plus", arguments[1], makeLiteral(Field(UInt64(1))));
            return makeASTFunction("arrayElementOrNull", arguments[0], arguments[1]);
        }
        /// An index computed per document is shifted by its value: by one when it is not negative.
        auto shift = makeASTFunction("greaterOrEquals", arguments[1]->clone(), makeLiteral(Field(UInt64(0))));
        return makeASTFunction("arrayElementOrNull", arguments[0], makeASTFunction("plus", arguments[1], std::move(shift)));
    }

    if (name == "$in")
    {
        auto arguments = parseArguments(argument);
        requireArgumentCount(name, arguments, 2);
        return makeASTFunction("has", arguments[1], arguments[0]);
    }

    if (name == "$split")
    {
        auto arguments = parseArguments(argument);
        requireArgumentCount(name, arguments, 2);
        return makeASTFunction("splitByString", arguments[1], arguments[0]);
    }

    if (name == "$substr" || name == "$substrBytes" || name == "$substrCP")
    {
        auto arguments = parseArguments(argument);
        requireArgumentCount(name, arguments, 3);
        /// Mongo counts the offset from zero, ClickHouse from one.
        auto offset = makeASTFunction("plus", arguments[1], makeLiteral(Field(UInt64(1))));
        return makeASTFunction(name == "$substrCP" ? "substringUTF8" : "substring", arguments[0], offset, arguments[2]);
    }

    if (name == "$cond")
    {
        std::vector<ASTPtr> arguments;
        if (argument.IsObject())
        {
            arguments.push_back(parseMongoAggregateExpression(requireMember(argument, "if", name)));
            arguments.push_back(parseMongoAggregateExpression(requireMember(argument, "then", name)));
            arguments.push_back(parseMongoAggregateExpression(requireMember(argument, "else", name)));
        }
        else
            arguments = parseArguments(argument);
        requireArgumentCount(name, arguments, 3);
        arguments[0] = makeTruth(std::move(arguments[0]));
        return makeFunction("if", std::move(arguments));
    }

    if (name == "$switch")
    {
        const auto & branches = requireMember(argument, "branches", name);
        if (!branches.IsArray() || branches.Empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'branches' of '$switch' must be a non empty array");
        std::vector<ASTPtr> arguments;
        for (const auto & branch : branches.GetArray())
        {
            arguments.push_back(makeTruth(parseMongoAggregateExpression(requireMember(branch, "case", name))));
            arguments.push_back(parseMongoAggregateExpression(requireMember(branch, "then", name)));
        }
        auto default_it = argument.FindMember("default");
        if (default_it != argument.MemberEnd())
        {
            arguments.push_back(parseMongoAggregateExpression(default_it->value));
            return makeFunction("multiIf", std::move(arguments));
        }

        /** Without a `default`, Mongo fails a document that no branch matches. The check is a
          * `throwIf` on none of the cases holding: it does not depend on whether `multiIf` skips
          * the branch for the rows a case matched, because it answers `0` for those rows.
          */
        std::vector<ASTPtr> cases;
        for (size_t i = 0; i + 1 < arguments.size(); i += 2)
            cases.push_back(arguments[i]->clone());
        ASTPtr any_case = cases.size() == 1 ? cases[0] : makeFunction("or", std::move(cases));
        auto check = makeASTFunction(
            "throwIf",
            makeASTFunction("not", std::move(any_case)),
            makeLiteral(Field(String("'$switch' could not find a matching branch for an input, and no default was specified"))));
        arguments.push_back(makeASTFunction("if", std::move(check), makeNull(), makeNull()));
        return makeFunction("multiIf", std::move(arguments));
    }

    if (name == "$dateTrunc")
    {
        /// A `binSize` groups several units into one bin, counted from the year 2000; only the
        /// bin of one unit, which is what the field defaults to, is answered here.
        static const std::unordered_set<std::string_view> supported_members{"date", "unit", "binSize", "timezone", "startOfWeek"};
        rejectUnknownMembers(argument, name, supported_members);
        if (auto bin_size_it = argument.FindMember("binSize"); bin_size_it != argument.MemberEnd())
            if (!bin_size_it->value.IsNumber() || bin_size_it->value.GetDouble() != 1)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only a 'binSize' of 1 is supported in '{}'", name);

        auto date = parseMongoAggregateExpression(requireMember(argument, "date", name));
        auto unit = dateUnit(argument, name);
        auto timezone = parseTimezoneMember(argument);

        /// `dateTrunc` starts a week on Monday; the start of the week Mongo means is a different day.
        /// `startOfWeek` is read for the `week` unit only: Mongo ignores it for every other unit.
        if (unit == "week")
            return makeStartOfWeek(std::move(date), startOfWeekMode(argument, name), timezone);

        return makeASTFunction("dateTrunc", makeLiteral(Field(String(unit))), std::move(date), std::move(timezone));
    }

    if (name == "$regexMatch")
    {
        auto input = parseMongoAggregateExpression(requireMember(argument, "input", name));
        auto pattern = parseRegularExpressionField(argument, name);
        return makeASTFunction("match", input, makeLiteral(Field(pattern)));
    }

    if (name == "$rand")
        return makeASTFunction("randCanonical");

    if (name == "$log")
    {
        auto arguments = parseArguments(argument);
        requireArgumentCount(name, arguments, 2);
        return makeASTFunction("divide", makeASTFunction("log", arguments[0]), makeASTFunction("log", arguments[1]));
    }

    if (name == "$eq" || name == "$ne" || name == "$lt" || name == "$lte" || name == "$gt" || name == "$gte" || name == "$cmp")
        return makeComparison(name, parseArguments(argument));

    if (name == "$indexOfBytes" || name == "$indexOfCP" || name == "$indexOfArray")
    {
        auto arguments = parseArguments(argument);
        if (arguments.size() != 2)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only the two argument form of '{}' is supported", name);
        /// Both count from zero and answer -1 when there is no match, while the ClickHouse
        /// functions count from one and answer 0.
        const char * function = name == "$indexOfBytes" ? "position" : (name == "$indexOfCP" ? "positionUTF8" : "indexOf");
        return makeASTFunction("minus", makeFunction(function, std::move(arguments)), makeLiteral(Field(UInt64(1))));
    }

    if (name == "$trim" || name == "$ltrim" || name == "$rtrim")
    {
        /// `requireMember` checks that the argument is a document before its members are looked up.
        auto input = parseMongoAggregateExpression(requireMember(argument, "input", name));
        if (argument.FindMember("chars") != argument.MemberEnd())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The 'chars' of '{}' is not supported", name);
        const char * function = name == "$trim" ? "trimBoth" : (name == "$ltrim" ? "trimLeft" : "trimRight");
        return makeASTFunction(function, std::move(input));
    }

    if (name == "$replaceOne" || name == "$replaceAll")
    {
        auto input = parseMongoAggregateExpression(requireMember(argument, "input", name));
        auto find = parseMongoAggregateExpression(requireMember(argument, "find", name));
        auto replacement = parseMongoAggregateExpression(requireMember(argument, "replacement", name));
        return makeASTFunction(name == "$replaceOne" ? "replaceOne" : "replaceAll", input, find, replacement);
    }

    if (name == "$slice")
    {
        auto arguments = parseArguments(argument);
        if (arguments.size() == 2)
        {
            /// A negative count takes the elements at the end of the array.
            auto positive = makeASTFunction("greaterOrEquals", arguments[1], makeLiteral(Field(UInt64(0))));
            auto offset = makeASTFunction("if", positive, makeLiteral(Field(UInt64(1))), arguments[1]->clone());
            return makeASTFunction("arraySlice", arguments[0], offset, makeASTFunction("abs", arguments[1]->clone()));
        }
        if (arguments.size() == 3)
        {
            /// Mongo counts the position from zero, ClickHouse from one, and a negative position
            /// counts from the end in both.
            auto positive = makeASTFunction("greaterOrEquals", arguments[1], makeLiteral(Field(UInt64(0))));
            auto offset = makeASTFunction(
                "if", positive, makeASTFunction("plus", arguments[1]->clone(), makeLiteral(Field(UInt64(1)))), arguments[1]->clone());
            return makeASTFunction("arraySlice", arguments[0], offset, arguments[2]);
        }
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'$slice' takes two or three arguments, got {}", arguments.size());
    }

    if (name == "$setUnion")
        return makeASTFunction("arrayDistinct", makeFunction("arrayConcat", parseArguments(argument)));

    if (name == "$setDifference")
    {
        auto arguments = parseArguments(argument);
        requireArgumentCount(name, arguments, 2);
        auto element = make_intrusive<ASTIdentifier>("__mongo_element");
        auto body = makeASTFunction("not", makeASTFunction("has", arguments[1], element));
        /// A set operator answers a set: the elements the second array does not hold, each of them
        /// once, the way the neighbouring `$setUnion` and `$setEquals` answer.
        return makeASTFunction(
            "arrayDistinct", makeASTFunction("arrayFilter", makeLambda("__mongo_element", std::move(body)), arguments[0]));
    }

    if (name == "$setEquals")
    {
        auto arguments = parseArguments(argument);
        requireArgumentCount(name, arguments, 2);
        auto sorted = [](ASTPtr array) { return makeASTFunction("arraySort", makeASTFunction("arrayDistinct", std::move(array))); };
        return makeASTFunction("equals", sorted(arguments[0]), sorted(arguments[1]));
    }

    if (name == "$anyElementTrue" || name == "$allElementsTrue")
    {
        auto arguments = parseArguments(argument);
        requireArgumentCount(name, arguments, 1);
        /// Each element is true or false by its type, as `$toBool` decides, so that every string is
        /// true, and an element that is `null` is false.
        auto body = makeASTFunction("ifNull", makeToBool(make_intrusive<ASTIdentifier>("__mongo_element")), makeLiteral(Field(false)));
        return makeASTFunction(
            name == "$anyElementTrue" ? "arrayExists" : "arrayAll", makeLambda("__mongo_element", std::move(body)), arguments[0]);
    }

    if (name == "$map" || name == "$filter")
    {
        auto input = parseMongoAggregateExpression(requireMember(argument, "input", name));
        auto variable = parseVariableName(argument, "this");
        ASTPtr body;
        {
            BoundVariableScope scope(variable);
            body = parseMongoAggregateExpression(requireMember(argument, name == "$map" ? "in" : "cond", name));
        }
        /// The condition of a `$filter` keeps an element when it is true the way Mongo means it.
        if (name == "$filter")
            body = makeTruth(std::move(body));
        auto mapped = makeASTFunction(
            name == "$map" ? "arrayMap" : "arrayFilter", makeLambda(variableIdentifier(variable), std::move(body)), std::move(input));
        if (auto limit_it = argument.FindMember("limit"); limit_it != argument.MemberEnd() && name == "$filter")
            return makeASTFunction(
                "arraySlice", std::move(mapped), makeLiteral(Field(UInt64(1))), parseMongoAggregateExpression(limit_it->value));
        return mapped;
    }

    if (name == "$dateToString")
    {
        /// `onNull` answers a value of its own for a missing or `null` date, which is a different
        /// expression rather than a different text, so it is refused rather than dropped, the same
        /// way the `onNull` of `$dateFromString` is.
        static const std::unordered_set<std::string_view> supported_members{"date", "format", "timezone"};
        rejectUnknownMembers(argument, name, supported_members);

        auto date = parseMongoAggregateExpression(requireMember(argument, "date", name));
        /// Written in UTC unless the operator names a time zone, as in Mongo: not in the time zone of
        /// the type of the value, which would be a local time marked as UTC.
        auto timezone = parseTimezoneMember(argument);
        auto timezone_it = argument.FindMember("timezone");

        if (auto format_it = argument.FindMember("format"); format_it != argument.MemberEnd())
            return makeFormattedDate(date, translateMongoDateFormat(format_it->value, name), timezone);

        /** Without a `format`, Mongo writes the date in ISO 8601 with milliseconds, and marks it with
          * `Z` when it is written in UTC - that is, when the operator names no time zone or names
          * UTC. A date written in another time zone carries no designator at all. Which of the two a
          * time zone computed per document would pick is not known here, so it needs a `format`.
          */
        bool in_utc = true;
        if (timezone_it != argument.MemberEnd())
        {
            const auto & zone = timezone_it->value;
            if (!zone.IsString() || stringView(zone).starts_with('$'))
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "'{}' without a 'format' needs a 'timezone' that is a string literal: whether the text ends with 'Z' depends on it",
                    name);
            static const std::unordered_set<std::string_view> utc_zones{
                "UTC", "GMT", "Z", "+00", "-00", "+0000", "-0000", "+00:00", "-00:00"};
            in_utc = utc_zones.contains(stringView(zone));
        }
        rapidjson::Value default_format(rapidjson::StringRef(in_utc ? "%Y-%m-%dT%H:%M:%S.%LZ" : "%Y-%m-%dT%H:%M:%S.%L"));
        return makeFormattedDate(date, translateMongoDateFormat(default_format, name), timezone);
    }

    if (name == "$dateFromString")
    {
        /// `onError` and `onNull` answer a value of their own for a text that cannot be read, which
        /// is a different expression rather than a different time zone, so they are refused rather
        /// than dropped.
        static const std::unordered_set<std::string_view> supported_members{"dateString", "format", "timezone"};
        rejectUnknownMembers(argument, name, supported_members);

        auto text = parseMongoAggregateExpression(requireMember(argument, "dateString", name));
        /// A text that names no offset of its own is read in the time zone the operator names, and
        /// in UTC when it names none - the instant a Mongo date holds is the same either way.
        ASTPtr timezone = makeLiteral(Field(String("UTC")));
        if (auto timezone_it = argument.FindMember("timezone"); timezone_it != argument.MemberEnd())
            timezone = parseMongoAggregateExpression(timezone_it->value);
        if (auto format_it = argument.FindMember("format"); format_it != argument.MemberEnd())
            return makeASTFunction(
                "parseDateTime64", text, makeDateFormatLiteral(translateMongoDateFormat(format_it->value, name)), std::move(timezone));
        return makeASTFunction("parseDateTime64BestEffort", text, makeLiteral(Field(UInt64(3))), std::move(timezone));
    }

    if (name == "$dateAdd" || name == "$dateSubtract")
    {
        static const std::unordered_set<std::string_view> supported_members{"startDate", "unit", "amount", "timezone"};
        rejectUnknownMembers(argument, name, supported_members);

        auto start = parseMongoAggregateExpression(requireMember(argument, "startDate", name));
        auto amount = parseDateAddAmount(requireMember(argument, "amount", name), name);
        auto function = dateAddFunction(requireMember(argument, "unit", name), name);
        if (name == "$dateSubtract")
            function.replace(0, strlen("add"), "subtract");

        /// A day, a month or a year added in a time zone is as long as the calendar of that time
        /// zone makes it - 23 or 25 hours around a change of the daylight saving time - which the
        /// time zone argument of `addDays` and its kin decides.
        return makeASTFunction(function, std::move(start), std::move(amount), parseTimezoneMember(argument));
    }

    if (name == "$dateDiff")
    {
        static const std::unordered_set<std::string_view> supported_members{"startDate", "endDate", "unit", "timezone", "startOfWeek"};
        rejectUnknownMembers(argument, name, supported_members);

        auto unit = dateUnit(argument, name);
        auto start = parseMongoAggregateExpression(requireMember(argument, "startDate", name));
        auto end = parseMongoAggregateExpression(requireMember(argument, "endDate", name));
        auto timezone = parseTimezoneMember(argument);

        /// The difference in weeks is the number of starts of a week between the two dates, and
        /// where a week starts is what `startOfWeek` says; `dateDiff` counts from Monday only.
        if (unit == "week")
        {
            auto start_of_week = startOfWeekMode(argument, name);
            auto days = makeASTFunction(
                "dateDiff",
                makeLiteral(Field(String("day"))),
                makeStartOfWeek(std::move(start), start_of_week, timezone),
                makeStartOfWeek(std::move(end), start_of_week, timezone));
            return makeASTFunction("intDiv", std::move(days), makeLiteral(Field(UInt64(7))));
        }

        return makeASTFunction("dateDiff", makeLiteral(Field(String(unit))), std::move(start), std::move(end), std::move(timezone));
    }

    if (name == "$regexFind" || name == "$regexFindAll")
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "'{}' is only supported as the value of a '$project' or a '$set' field, where its result document becomes "
            "the columns 'match', 'idx' and 'captures'",
            name);

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The aggregation operator '{}' is not supported", name);
}

}

ASTPtr parseMongoAggregateExpression(const rapidjson::Value & value)
{
    if (value.IsString())
    {
        auto text = stringView(value);
        if (text.starts_with("$$"))
        {
            auto variable = text.substr(2);
            if (variable == "NOW")
                return makeASTFunction("now64", makeLiteral(Field(UInt64(3))));

            /// A system variable stands for something the translation has no counterpart for - the
            /// document being processed, the pruning decision of `$redact`, the current cluster
            /// time. A user variable, which only `$map` and `$filter` can bind, names the lambda
            /// parameter it is bound to (see `variableIdentifier`), and is an error anywhere it is
            /// not bound.
            static const std::unordered_set<std::string_view> system_variables
                = {"ROOT", "CURRENT", "REMOVE", "DESCEND", "PRUNE", "KEEP", "CLUSTER_TIME", "SEARCH_META", "USER_ROLES"};
            if (variable.empty() || system_variables.contains(variable))
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The aggregation system variable '{}' is not supported", text);
            if (std::find(bound_variables.begin(), bound_variables.end(), variable) == bound_variables.end())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Use of undefined variable: '{}'", variable);
            return make_intrusive<ASTIdentifier>(variableIdentifier(variable));
        }
        if (text.starts_with("$"))
        {
            /// A field path names the column of the same name, dots included: the dialect maps a
            /// nested document field onto an `a.b` column.
            auto field = text.substr(1);
            if (field.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "'$' by itself is not a valid field path");
            return make_intrusive<ASTIdentifier>(String(field));
        }
        return makeLiteral(Field(String(text)));
    }

    if (value.IsArray())
    {
        std::vector<ASTPtr> elements;
        elements.reserve(value.Size());
        for (const auto & element : value.GetArray())
            elements.push_back(parseMongoAggregateExpression(element));
        return makeFunction("array", std::move(elements));
    }

    if (value.IsObject())
    {
        if (auto constant = tryParseMongoConstant(value))
            return constant;

        if (value.MemberCount() != 1)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "An expression document must hold exactly one operator, got {}", value.MemberCount());

        const auto & member = *value.MemberBegin();
        auto name = stringView(member.name);
        if (!name.starts_with("$"))
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "A document expression is only supported as the value of a '$project' or a '$set' field, where it becomes "
                "one column per leaf");
        return checkExpressionSize(parseOperator(name, member.value));
    }

    if (auto constant = tryParseMongoConstant(value))
        return constant;

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot translate the aggregation expression");
}

namespace
{

/** The keys of a preceding `$sort` as one expression to compare the documents of a group by: a
  * tuple of them, which compares lexicographically the way a multiple key sort does.
  *
  * A `null` or a missing key orders before every value in Mongo, while `argMin` and `argMax` skip
  * a row whose key is `NULL` and `arraySort` puts it last. So each key is preceded by whether it
  * has a value, which orders the documents without one first, and the tuple is never `NULL`.
  *
  * The direction has to be the same for every key, because a tuple has one order for the whole of
  * it: a sort by `{"a": 1, "b": -1}` is not the order of any tuple of `a` and `b`, so it is
  * rejected rather than answered in a different order.
  */
std::pair<ASTPtr, int> makeGroupOrderKey(const MongoGroupOrder & order, std::string_view accumulator)
{
    if (!order.keys_in_scope)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "The accumulator '{}' reads the documents of the group in the order of the stream, and the keys the preceding '$sort' "
            "established are not fields of the documents the '$group' reads: sort right before the '$group', or keep the sort keys",
            accumulator);

    int direction = order.keys.front().second;
    for (const auto & key : order.keys)
        if (key.second != direction)
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "The accumulator '{}' after a '$sort' whose keys do not share one direction is not supported",
                accumulator);

    auto keys = makeASTFunction("tuple");
    for (const auto & key : order.keys)
    {
        keys->arguments->children.push_back(makeASTFunction("isNotNull", key.first->clone()));
        keys->arguments->children.push_back(key.first->clone());
    }
    return {keys, direction};
}

/** The values of an array accumulator out of the one-element tuples it collected. The array
  * aggregate functions skip a `NULL` value, while a `$push` of a document whose value is `null`
  * pushes it; a tuple is never `NULL`, so the values are collected wrapped into one.
  */
ASTPtr makeNullPreservingArray(ASTPtr tuples)
{
    auto element = makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>("x"), makeLiteral(Field(UInt64(1))));
    return makeASTFunction("arrayMap", makeLambda("x", std::move(element)), std::move(tuples));
}

/** The values of a group in the order of the stream: they are collected together with the sort key
  * and sorted by it, because `groupArray` appends them in the order the query reads the rows in.
  *
  * The documents with equal keys keep the order they were collected in, which is the order of the
  * elements of an array that a `$unwind` after the `$sort` expanded one document into: the rows of
  * one document are read together. The sort is by the key and then by the position in the group -
  * after the key, and not the value, which would order those elements by themselves.
  */
ASTPtr makeOrderedGroupValues(ASTPtr value, const MongoGroupOrder & order, std::string_view accumulator)
{
    auto [key, direction] = makeGroupOrderKey(order, accumulator);
    auto pairs = makeASTFunction("groupArray", makeASTFunction("tuple", std::move(key), std::move(value)));
    auto positions = makeASTFunction("arrayEnumerate", pairs->clone());

    /// A descending sort reverses the positions too, so they are negated to stay ascending.
    ASTPtr position = make_intrusive<ASTIdentifier>("__position");
    if (direction != 1)
        position = makeASTFunction("negate", makeASTFunction("toInt64", std::move(position)));
    auto pair_key = makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>("__pair"), makeLiteral(Field(UInt64(1))));
    auto sort_key = makeASTFunction("tuple", std::move(pair_key), std::move(position));
    auto parameters = makeASTFunction("tuple", make_intrusive<ASTIdentifier>("__pair"), make_intrusive<ASTIdentifier>("__position"));
    auto sorted = makeASTFunction(
        direction == 1 ? "arraySort" : "arrayReverseSort",
        makeASTFunction("lambda", std::move(parameters), std::move(sort_key)),
        std::move(pairs),
        std::move(positions));

    auto element = makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>("x"), makeLiteral(Field(UInt64(2))));
    return makeASTFunction("arrayMap", makeLambda("x", std::move(element)), std::move(sorted));
}

}

ASTPtr parseMongoAccumulator(const rapidjson::Value & value, const MongoGroupOrder & order)
{
    if (!value.IsObject() || value.MemberCount() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "An accumulator of '$group' must be a document holding one operator");

    const auto & member = *value.MemberBegin();
    auto name = stringView(member.name);

    static const std::unordered_map<std::string_view, std::string> accumulators = {
        {"$avg", "avg"},
        {"$min", "min"},
        {"$max", "max"},
        {"$stdDevPop", "stddevPop"},
        {"$stdDevSamp", "stddevSamp"},
    };

    /// The value of the first or the last document of the group, even when it is `null`: `any` and
    /// `anyLast` would skip such a document.
    static const std::unordered_map<std::string_view, std::string> null_respecting_accumulators = {
        {"$first", "any_respect_nulls"},
        {"$last", "anyLast_respect_nulls"},
    };

    /// The values of the group, `null` included (see `makeNullPreservingArray`).
    static const std::unordered_map<std::string_view, std::string> array_accumulators = {
        {"$push", "groupArray"},
        {"$addToSet", "groupUniqArray"},
    };

    if (name == "$count")
        return makeASTFunction("count");

    if (name == "$firstN" || name == "$lastN")
    {
        auto input = parseMongoAggregateExpression(requireMember(member.value, "input", name));

        /** Mongo asks for a positive whole number of values, and in a `$group` for one that does not
          * change within the group. Only a constant is supported: it is a parameter of the aggregate
          * function below, and anything else - zero, a negative or a fractional count, an expression of
          * the document - would only fail later, or mean something else, as a ClickHouse argument.
          */
        const auto & count_value = requireMember(member.value, "n", name);
        if (!count_value.IsNumber())
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED, "The 'n' of '{}' must be a constant number: an expression is not supported", name);
        const double count_number = count_value.GetDouble();
        if (count_number != std::floor(count_number) || count_number < 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'n' of '{}' must be a positive whole number", name);
        if (count_number > double(std::numeric_limits<Int64>::max()))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'n' of '{}' is too large", name);
        auto count = makeLiteral(Field(static_cast<UInt64>(count_number)));

        if (!order.empty())
        {
            /// The first - or the last - N documents of the group in the order of the stream.
            auto ordered = makeOrderedGroupValues(std::move(input), order, name);
            if (name == "$firstN")
                return makeASTFunction("arraySlice", std::move(ordered), makeLiteral(Field(UInt64(1))), std::move(count));
            /// A negative offset counts from the end, and one that reaches past the beginning of
            /// the array yields the whole of it, which is what Mongo answers for a small group.
            return makeASTFunction("arraySlice", std::move(ordered), makeASTFunction("negate", std::move(count)));
        }

        /// `groupArray(N)(x)` keeps the first N values of the group and `groupArrayLast(N)(x)` the
        /// last ones; the count is a parameter of the aggregate function rather than an argument.
        auto function = makeASTFunction(name == "$firstN" ? "groupArray" : "groupArrayLast", makeASTFunction("tuple", std::move(input)));
        auto parameters = make_intrusive<ASTExpressionList>();
        parameters->children.push_back(std::move(count));
        function->parameters = parameters;
        function->children.push_back(std::move(parameters));
        return makeNullPreservingArray(std::move(function));
    }

    if (name == "$sum")
    {
        /// `{"$sum": 1}` counts the documents of the group. Counting is much cheaper than summing a
        /// constant, and it is by far the most common accumulator.
        auto argument = parseMongoAggregateExpression(member.value);
        if (const auto * literal = argument->as<ASTLiteral>(); literal && literal->value == Field(Int64(1)))
            return makeASTFunction("count");
        return makeASTFunction("sum", std::move(argument));
    }

    /** `$first`, `$last` and `$push` read the documents of the group in the order of the stream,
      * so when a `$sort` established one they are lowered through its keys: the first document of
      * an ascending order is the one with the smallest key, and the last one the largest.
      * `$addToSet` is a set, whose order Mongo leaves undefined, and the remaining accumulators do
      * not depend on the order at all.
      */
    if (!order.empty() && (name == "$first" || name == "$last" || name == "$push"))
    {
        auto input = parseMongoAggregateExpression(member.value);
        if (name == "$push")
            return makeOrderedGroupValues(std::move(input), order, name);

        auto [key, direction] = makeGroupOrderKey(order, name);
        const bool takes_the_smallest_key = (name == "$first") == (direction == 1);
        /// The value is wrapped into a tuple, which is never `NULL`: `argMin` and `argMax` skip a
        /// `NULL` value, while the first document of the group is the first one even when its
        /// value is `null`.
        auto selected = makeASTFunction(
            takes_the_smallest_key ? "argMin" : "argMax", makeASTFunction("tuple", std::move(input)), std::move(key));
        return makeASTFunction("tupleElement", std::move(selected), makeLiteral(Field(UInt64(1))));
    }

    if (auto it = null_respecting_accumulators.find(name); it != null_respecting_accumulators.end())
        return makeASTFunction(it->second, parseMongoAggregateExpression(member.value));

    if (auto it = array_accumulators.find(name); it != array_accumulators.end())
        return makeNullPreservingArray(makeASTFunction(it->second, makeASTFunction("tuple", parseMongoAggregateExpression(member.value))));

    if (auto it = accumulators.find(name); it != accumulators.end())
        return makeASTFunction(it->second, parseMongoAggregateExpression(member.value));

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The accumulator '{}' is not supported", name);
}

ASTPtr parseMongoAggregateCondition(const rapidjson::Value & value)
{
    auto expression = parseMongoAggregateExpression(value);
    /// A filter drops a row whose condition is `NULL` already, so a boolean is kept as it is, where
    /// an index can still use it.
    if (isBooleanFunction(expression))
        return expression;
    return makeTruth(std::move(expression));
}

void expandMongoProjectedField(const std::string & name, const rapidjson::Value & value, std::vector<MongoProjectedField> & result)
{
    if (name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "A field name of a projection must not be empty");

    if (value.IsObject() && value.MemberCount() >= 1 && !tryParseMongoConstant(value))
    {
        auto first_member_name = stringView(value.MemberBegin()->name);

        if (first_member_name == "$regexFind" && value.MemberCount() == 1)
        {
            const auto & argument = value.MemberBegin()->value;
            auto input = parseMongoAggregateExpression(requireMember(argument, "input", "$regexFind"));
            auto pattern = makeLiteral(Field(parseRegularExpressionField(argument, "$regexFind")));

            /// Mongo returns null when the regular expression does not match, and the pipelines rely
            /// on that to fall back to another value, so every field is guarded by the match.
            auto matched = makeASTFunction("match", input, pattern);
            auto whole_match = makeASTFunction("regexpExtract", input, pattern, makeLiteral(Field(UInt64(0))));

            result.push_back({name + ".match", makeASTFunction("if", matched, whole_match, makeNull())});
            /** The index of the match is the number of code points before it. Searching for the text
              * of the match would find an earlier occurrence of the same text, which the expression
              * need not match there, and count bytes. So the text before the match is extracted:
              * the shortest prefix after which the expression matches is the one before the leftmost
              * match, which is the match `regexpExtract` returns. The expression is grouped, so that
              * its alternatives and its inline flags stay its own.
              */
            auto prefix_pattern = makeLiteral(Field("^((?s:.*?))(?:" + pattern->as<ASTLiteral &>().value.safeGet<String>() + ")"));
            auto prefix = makeASTFunction("regexpExtract", input->clone(), std::move(prefix_pattern), makeLiteral(Field(UInt64(1))));
            result.push_back({name + ".idx", makeASTFunction("if", matched->clone(), makeASTFunction("lengthUTF8", std::move(prefix)), makeNull())});
            /** `extractGroups` refuses an expression without groups, for which Mongo answers an empty
              * array of captures. So an empty group is appended to the grouped expression - it
              * matches the empty string and changes no match - and its capture is dropped.
              */
            const auto & pattern_text = pattern->as<ASTLiteral &>().value.safeGet<String>();
            auto captures_pattern = makeLiteral(Field("(?:" + pattern_text + ")()"));
            ASTPtr captures
                = makeASTFunction("arrayPopBack", makeASTFunction("extractGroups", input->clone(), std::move(captures_pattern)));
            /** A group that may not take part in the match answers `null` rather than an empty
              * string when it did not. `extractGroups` does not tell the two apart, so an empty
              * capture of such a group is taken as one that did not take part: only a group that
              * may both be skipped and match an empty text - `(a*)?` - is answered wrongly.
              */
            const auto optional_groups = findOptionalCaptureGroups(pattern_text);
            if (std::find(optional_groups.begin(), optional_groups.end(), true) != optional_groups.end())
            {
                auto flags = makeASTFunction("array");
                for (bool optional : optional_groups)
                    flags->arguments->children.push_back(makeLiteral(Field(UInt64(optional))));
                auto body = makeASTFunction(
                    "if",
                    makeASTFunction(
                        "and",
                        make_intrusive<ASTIdentifier>("__optional"),
                        makeASTFunction("empty", make_intrusive<ASTIdentifier>("__capture"))),
                    makeNull(),
                    make_intrusive<ASTIdentifier>("__capture"));
                auto parameters = makeASTFunction(
                    "tuple", make_intrusive<ASTIdentifier>("__capture"), make_intrusive<ASTIdentifier>("__optional"));
                captures = makeASTFunction(
                    "arrayMap", makeASTFunction("lambda", std::move(parameters), std::move(body)), std::move(captures), std::move(flags));
            }
            result.push_back({name + ".captures", makeASTFunction("if", matched->clone(), std::move(captures), makeNull())});
            return;
        }

        if (!first_member_name.starts_with("$"))
        {
            /// A nested document becomes one column per leaf, which is how the dialect maps a nested
            /// field of a document onto a column.
            for (auto it = value.MemberBegin(); it != value.MemberEnd(); ++it)
                expandMongoProjectedField(name + "." + std::string(stringView(it->name)), it->value, result);
            return;
        }
    }

    result.push_back({name, parseMongoAggregateExpression(value)});
}

}

}
