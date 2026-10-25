#include <Parsers/Mongo/ParserMongoAggregateExpression.h>

#include <algorithm>
#include <cctype>
#include <functional>
#include <limits>
#include <optional>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

#include <Core/Field.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Mongo/MongoConstants.h>
#include <Parsers/Mongo/Utils.h>
#include <Common/Exception.h>
#include <Common/FieldVisitorToString.h>
#include <base/scope_guard.h>

#include <Common/re2.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
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

ASTPtr makeFunction(const std::string & name, std::vector<ASTPtr> arguments)
{
    auto function = makeASTFunction(name);
    for (auto & argument : arguments)
        function->arguments->children.push_back(std::move(argument));
    return function;
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
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "'{}' takes {} arguments, got {}", operator_name, expected, arguments.size());
}

/// Operators that map onto a ClickHouse function of the same arity, argument for argument.
const std::unordered_map<std::string_view, std::string> direct_functions = {
    {"$toString", "toString"},
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
    {"$not", "not"},
    {"$concat", "concat"},
    {"$and", "and"},
    {"$or", "or"},
    {"$divide", "divide"},
    {"$mod", "modulo"},
    {"$pow", "pow"},
    {"$ifNull", "coalesce"},
};

struct DatePartFunction
{
    std::string function;
    /// The `mode` argument of the function, when it takes one.
    std::optional<UInt64> mode;
};

/** Operators that read a part of a date. Mongo reads it in UTC unless the `timezone` of the
  * operator says otherwise, while the ClickHouse functions read it in the time zone of the type
  * of the date, so the time zone is always passed. `$week` counts the weeks from Sunday, from
  * zero for the days before the first Sunday of the year, which is the mode 0 of `toWeek`.
  * `$dayOfWeek` numbers the days from Sunday as 1, `$isoDayOfWeek` from Monday as 1; those are
  * the modes 3 and 0 of `toDayOfWeek`.
  */
const std::unordered_map<std::string_view, DatePartFunction> date_part_functions = {
    {"$year", {"toYear", {}}},
    {"$month", {"toMonth", {}}},
    {"$dayOfMonth", {"toDayOfMonth", {}}},
    {"$dayOfYear", {"toDayOfYear", {}}},
    {"$week", {"toWeek", 0}},
    {"$isoWeek", {"toISOWeek", {}}},
    {"$isoWeekYear", {"toISOYear", {}}},
    {"$hour", {"toHour", {}}},
    {"$minute", {"toMinute", {}}},
    {"$second", {"toSecond", {}}},
    {"$millisecond", {"toMillisecond", {}}},
    {"$dayOfWeek", {"toDayOfWeek", 3}},
    {"$isoDayOfWeek", {"toDayOfWeek", 0}},
};

/// Operators that fold their arguments into a tree of binary applications.
const std::unordered_map<std::string_view, std::string> folded_functions = {
    {"$add", "plus"},
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

/// `x -> <body>`, the shape a ClickHouse higher order function takes.
ASTPtr makeLambda(const std::string & parameter, ASTPtr body)
{
    auto parameters = makeASTFunction("tuple", make_intrusive<ASTIdentifier>(parameter));
    return makeASTFunction("lambda", std::move(parameters), std::move(body));
}

/** The identifier a variable `$$<name>` translates to. A variable and a field of the same name are
  * different things in Mongo - `$$x` is the variable, `$x` the field of the document - so the
  * variable gets an identifier no field path produces.
  */
std::string variableIdentifier(std::string_view name)
{
    return "__mongo_variable_" + std::string(name);
}

/// The name a `$map` or a `$filter` binds its element to, see `variableIdentifier`. Mongo writes it
/// as `$$<name>` and defaults it to `this`.
std::string parseVariableName(const rapidjson::Value & argument, const char * default_name)
{
    auto it = argument.FindMember("as");
    if (it == argument.MemberEnd())
        return variableIdentifier(default_name);
    if (!it->value.IsString())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'as' of a higher order operator must be a string");
    if (it->value.GetStringLength() == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'as' of a higher order operator must not be empty");
    return variableIdentifier(stringView(it->value));
}

/// The number of `withValue` bodies being built, which names their parameters apart.
thread_local size_t with_value_depth = 0;

/** `body` applied to the value of `value`, which the body may refer to any number of times while
  * `value` is still evaluated - and present in the query - once. A translation that refers to its
  * argument several times would otherwise copy it, and a nesting of such operators would grow
  * exponentially with its depth. A literal or an identifier is cheap to copy, and is passed as it
  * is; anything else is bound to the parameter of a lambda applied to a single element array.
  * The parameters of nested bodies are named by their depth, so that a body can refer to the
  * values of the enclosing ones.
  */
ASTPtr withValue(ASTPtr value, const std::function<ASTPtr(const ASTPtr &)> & body)
{
    if (value->as<ASTLiteral>() || value->as<ASTIdentifier>())
        return body(value);

    const std::string parameter = "__mongo_value_" + std::to_string(with_value_depth + 1);
    ASTPtr result;
    {
        ++with_value_depth;
        SCOPE_EXIT({ --with_value_depth; });
        result = body(make_intrusive<ASTIdentifier>(parameter));
    }
    return makeASTFunction(
        "arrayElement",
        makeASTFunction("arrayMap", makeLambda(parameter, std::move(result)), makeASTFunction("array", std::move(value))),
        makeLiteral(Field(UInt64(1))));
}

/// Whether the type of `value` is a date, which the analyzer folds into a constant.
ASTPtr makeIsDate(const ASTPtr & value)
{
    return makeASTFunction(
        "match",
        makeASTFunction("toTypeName", value->clone()),
        makeLiteral(Field(String(R"(^(Nullable\(|LowCardinality\()*(Date|DateTime))"))));
}

/// `value`, which throws when the query is analyzed if `condition` - a constant - is true.
ASTPtr withGuard(ASTPtr value, ASTPtr condition, const std::string & message)
{
    auto guard = makeASTFunction("throwIf", std::move(condition), makeLiteral(Field(message)));
    return makeASTFunction("tupleElement", makeASTFunction("tuple", std::move(value), std::move(guard)), makeLiteral(Field(UInt64(1))));
}

/// The milliseconds since the epoch of a date.
ASTPtr makeEpochMilliseconds(const ASTPtr & date)
{
    return makeASTFunction(
        "toUnixTimestamp64Milli", makeASTFunction("toDateTime64", date->clone(), makeLiteral(Field(UInt64(3))), makeLiteral(Field(String("UTC")))));
}

/** The arithmetic of `$add` and `$subtract` on a date counts milliseconds in Mongo, and the
  * difference of two dates is a number of milliseconds, while ClickHouse adds a number to a date as
  * seconds. The types are only known when the query is analyzed, and the lowering of a date and of
  * a number would have different types, so a date operand is rejected there.
  */
constexpr auto date_arithmetic_message
    = "Date arithmetic in '$add' and '$subtract' is not supported: use '$dateAdd', '$dateSubtract' or '$dateDiff'";

/// The interval a date unit of `$dateAdd` and `$dateSubtract` adds up.
std::string dateIntervalFunction(const rapidjson::Value & unit, std::string_view operator_name)
{
    if (!unit.IsString())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'unit' of '{}' must be a string", operator_name);

    static const std::unordered_map<std::string_view, std::string> intervals = {
        {"year", "toIntervalYear"},
        {"quarter", "toIntervalQuarter"},
        {"month", "toIntervalMonth"},
        {"week", "toIntervalWeek"},
        {"day", "toIntervalDay"},
        {"hour", "toIntervalHour"},
        {"minute", "toIntervalMinute"},
        {"second", "toIntervalSecond"},
        {"millisecond", "toIntervalMillisecond"},
    };

    auto it = intervals.find(stringView(unit));
    if (it == intervals.end())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The unit '{}' of '{}' is not supported", stringView(unit), operator_name);
    return it->second;
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
            case Field::Types::UInt64:
                scale = 0;
                break;
            case Field::Types::String:
                scale = decimalScaleOfNumberDecimal(literal->value.safeGet<String>());
                break;
            case Field::Types::Float64:
            {
                /// The shortest decimal text that reads back as the same double, which is what
                /// Mongo preserves when it converts a double to a decimal. The text is what gets
                /// converted, not the double: `toDecimal128(1.15, 2)` multiplies in floating
                /// point and truncates to `1.14`, while the text `1.15` is parsed exactly.
                String text = applyVisitor(FieldVisitorToString(), literal->value);
                scale = decimalScaleOfNumberDecimal(text);
                argument = makeLiteral(Field(std::move(text)));
                break;
            }
            default:
                break;
        }
        if (!scale)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "The argument of '$toDecimal' cannot be represented exactly by a Decimal128");
        return makeASTFunction("toDecimal128", std::move(argument), makeLiteral(Field(UInt64(*scale))));
    }

    /// The `CAST` a `$numberDecimal` wrapper lowers into already carries the exact scale of its
    /// value (see `tryParseMongoConstant`), so it is a decimal already.
    if (const auto * function = argument->as<ASTFunction>(); function && function->name == "CAST")
        return argument;

    /// The wrappers `Nullable` and `LowCardinality` do not change the value space, so a wrapped
    /// integer or boolean converts exactly at scale 0 as well.
    auto is_exact = makeASTFunction(
        "match",
        makeASTFunction("toTypeName", argument),
        makeLiteral(Field(String(R"(^(Nullable\(|LowCardinality\()*(U?Int(8|16|32|64|128|256)|Bool)\)*$)"))));
    auto guard = makeASTFunction(
        "throwIf",
        makeASTFunction("not", std::move(is_exact)),
        makeLiteral(Field(String(
            "'$toDecimal' of a non-constant value is only exact for an integer or a boolean; "
            "spell any other value as a '$numberDecimal' or a string literal, which carry their scale"))));
    return makeASTFunction("plus", makeASTFunction("toDecimal128", argument->clone(), makeLiteral(Field(UInt64(0)))), std::move(guard));
}

/** A comparison of Mongo, which is always a boolean: `null` equals `null` and is less than any
  * other value, as in the BSON order, while a SQL comparison is `NULL` when an operand is. `$eq`
  * and `$ne` are the null-safe `isNotDistinctFrom` and `isDistinctFrom`. An ordering comparison
  * is `NULL` exactly when an operand is, and then its result depends only on which of them is:
  * `a < b` holds when `b` is not null (then `a` is), `a <= b` when `a` is null, and so on.
  */
ASTPtr makeComparison(std::string_view name, ASTPtr left, ASTPtr right)
{
    if (name == "$eq")
        return makeASTFunction("isNotDistinctFrom", std::move(left), std::move(right));
    if (name == "$ne")
        return makeASTFunction("isDistinctFrom", std::move(left), std::move(right));

    /// The comparison, and which operand decides the result when one of them is null and whether
    /// that result is that the operand is null or is not.
    std::string function;
    bool decided_by_left = false;
    bool decided_by_null = false;
    if (name == "$lt")
    {
        function = "less";
    }
    else if (name == "$lte")
    {
        function = "lessOrEquals";
        decided_by_left = true;
        decided_by_null = true;
    }
    else if (name == "$gt")
    {
        function = "greater";
        decided_by_left = true;
    }
    else if (name == "$gte")
    {
        function = "greaterOrEquals";
        decided_by_null = true;
    }
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected comparison '{}'", name);

    /// The deciding operand is used twice, so it is bound once (see `withValue`).
    ASTPtr & deciding = decided_by_left ? left : right;
    ASTPtr other = decided_by_left ? std::move(right) : std::move(left);
    return withValue(std::move(deciding), [&](const ASTPtr & value)
    {
        auto comparison = decided_by_left ? makeASTFunction(function, value->clone(), other) : makeASTFunction(function, other, value->clone());
        return makeASTFunction("ifNull", std::move(comparison), makeASTFunction(decided_by_null ? "isNull" : "isNotNull", value->clone()));
    });
}

/** The lowering of `$toDate`. Mongo reads a numeric argument as Unix *milliseconds*, while
  * `toDateTime64` reads a number as seconds - `{"$toDate": 1546300800000}` is 2019 in Mongo,
  * not the year 2282 - so a numeric argument goes through `fromUnixTimestamp64Milli` instead.
  * A literal carries its type here; any other argument dispatches on `toTypeName`, which the
  * analyzer folds into a constant, so exactly one of the branches survives the analysis.
  */
ASTPtr makeToDate(ASTPtr argument)
{
    const auto parsed = [](ASTPtr arg)
    {
        return makeASTFunction("toDateTime64", std::move(arg), makeLiteral(Field(UInt64(3))), makeLiteral(Field(String("UTC"))));
    };

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
            default:
                return parsed(std::move(argument));
        }
    }

    /// The argument is used by every branch, so it is bound once (see `withValue`).
    return withValue(std::move(argument), [&parsed](const ASTPtr & value)
    {
        /// Neither branch may throw for the type of the other: the losing branch is dropped by the
        /// analyzer and skipped by short-circuit evaluation, but with the old analyzer and
        /// `short_circuit_function_evaluation = 'disable'` it still runs. `toFloat64OrZero` over
        /// `toString` reads any type without throwing; the string it produces for a number is the
        /// number itself, and the branch taken for a string never uses it.
        auto milliseconds = makeASTFunction(
            "fromUnixTimestamp64Milli",
            makeASTFunction("toInt64", makeASTFunction("toFloat64OrZero", makeASTFunction("toString", value->clone()))),
            makeLiteral(Field(String("UTC"))));
        /// `Nullable` and `LowCardinality` wrappers of a numeric type are numeric as well - the type
        /// name of an ordinary column such as `Nullable(Int64)` must not fall through to the branch
        /// that reads a number as seconds.
        auto is_a_number = makeASTFunction(
            "match",
            makeASTFunction("toTypeName", value->clone()),
            makeLiteral(Field(String(R"(^(Nullable\(|LowCardinality\()*(U?Int|Float|Decimal))"))));
        return makeASTFunction("if", std::move(is_a_number), std::move(milliseconds), parsed(value->clone()));
    });
}

/** The lowering of `$toInt`. `toInt32` wraps a value outside the range of a 32-bit integer around,
  * while Mongo rejects it, so the value is checked first; the fraction of a value in the range is
  * truncated by both. The check reads the value as a `Float64`, which holds every integer of the
  * range exactly and is `NaN` - out of every range - for a `NaN`. A `NULL` passes the check and
  * converts to `NULL`, as it does in Mongo.
  */
ASTPtr makeToInt(ASTPtr argument)
{
    return withValue(std::move(argument), [](const ASTPtr & value)
    {
        auto number = makeASTFunction("toFloat64", value->clone());
        auto in_range = makeASTFunction(
            "and",
            makeASTFunction("greater", number, makeLiteral(Field(Int64(-2147483649)))),
            makeASTFunction("less", number->clone(), makeLiteral(Field(Int64(2147483648)))));
        auto guard = makeASTFunction(
            "throwIf",
            makeASTFunction("not", std::move(in_range)),
            makeLiteral(Field(String("The value of '$toInt' is out of the range of a 32-bit integer"))));
        /// The condition is never true: it throws instead.
        return makeASTFunction(
            "if",
            std::move(guard),
            makeASTFunction("toInt32", makeLiteral(Field(UInt64(0)))),
            makeASTFunction("toInt32", value->clone()));
    });
}

/** The lowering of `$toLong`. A date converts to its milliseconds since the epoch. Any other value
  * converts as `toInt64` does, which wraps a value outside the range of a 64-bit integer around,
  * while Mongo rejects it, so the truncated value is checked first. The truncation is done exactly
  * by `toInt128`: a check through `Float64` would round a decimal such as `9007199254740991.9`
  * independently of its truncation. `toInt128` rejects a `NaN` and wraps a value far out of its
  * own range, so a rough check through `Float64` comes first and bounds the magnitude.
  */
ASTPtr makeToLong(ASTPtr argument)
{
    const bool is_literal = argument->as<ASTLiteral>() != nullptr;
    return withValue(std::move(argument), [is_literal](const ASTPtr & value)
    {
        auto bounded = makeASTFunction(
            "less", makeASTFunction("abs", makeASTFunction("toFloat64", value->clone())), makeLiteral(Field(Float64(1e19))));
        auto truncated = makeASTFunction("toInt128", value->clone());
        auto in_range = makeASTFunction(
            "and",
            makeASTFunction("greaterOrEquals", truncated, makeLiteral(Field(std::numeric_limits<Int64>::min()))),
            makeASTFunction("lessOrEquals", truncated->clone(), makeLiteral(Field(std::numeric_limits<Int64>::max()))));
        auto guard = makeASTFunction(
            "throwIf",
            makeASTFunction("not", makeASTFunction("if", std::move(bounded), std::move(in_range), makeLiteral(Field(UInt64(0))))),
            makeLiteral(Field(String("The value of '$toLong' is out of the range of a 64-bit integer"))));
        /// The condition is never true: it throws instead.
        auto checked = makeASTFunction(
            "if", std::move(guard), makeASTFunction("toInt64", makeLiteral(Field(UInt64(0)))), makeASTFunction("toInt64", value->clone()));
        if (is_literal)
            return checked;
        return makeASTFunction("if", makeIsDate(value), makeEpochMilliseconds(value), std::move(checked));
    });
}

/// The lowering of `$toDouble`, where a date converts to its milliseconds since the epoch.
ASTPtr makeToDouble(ASTPtr argument)
{
    if (argument->as<ASTLiteral>())
        return makeASTFunction("toFloat64", std::move(argument));
    return withValue(std::move(argument), [](const ASTPtr & value)
    {
        return makeASTFunction(
            "if",
            makeIsDate(value),
            makeASTFunction("toFloat64", makeEpochMilliseconds(value)),
            makeASTFunction("toFloat64", value->clone()));
    });
}

/** The truth value of a document value in Mongo, which `$toBool` converts to: a number is true
  * unless it is zero, and every other value - a string, including `"false"` and `""`, a date, an
  * array - is true. `toBool` would parse a string instead. A number is read through its text,
  * which is the same for every numeric type and never throws; a boolean prints as `true` or
  * `false`. A `NULL` stays `NULL`.
  */
ASTPtr makeToBool(ASTPtr argument)
{
    return withValue(std::move(argument), [](const ASTPtr & value)
    {
        auto is_number = makeASTFunction(
            "match",
            makeASTFunction("toTypeName", value->clone()),
            makeLiteral(Field(String(R"(^(Nullable\(|LowCardinality\()*(U?Int|Float|Decimal|Bool))"))));
        auto text = makeASTFunction("toString", value->clone());
        auto number_truth = makeASTFunction(
            "toBool",
            makeASTFunction(
                "or",
                makeASTFunction("equals", text, makeLiteral(Field(String("true")))),
                makeASTFunction("notEquals", makeASTFunction("toFloat64OrZero", text->clone()), makeLiteral(Field(UInt64(0))))));
        auto other_truth = makeASTFunction(
            "if",
            makeASTFunction("isNull", value->clone()),
            makeASTFunction("CAST", makeNull(), makeLiteral(Field(String("Nullable(Bool)")))),
            makeLiteral(Field(true)));
        return makeASTFunction("if", std::move(is_number), std::move(number_truth), std::move(other_truth));
    });
}

/// The truth value of an element of `$anyElementTrue` and `$allElementsTrue`, where `null` is false.
ASTPtr makeTruthValue(ASTPtr argument)
{
    return makeASTFunction("ifNull", makeToBool(std::move(argument)), makeLiteral(Field(false)));
}

/// The text of the `format` of a date operator, which has to be a constant string.
std::string_view requireDateFormat(const rapidjson::Value & format, std::string_view operator_name)
{
    if (!format.IsString())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only a constant string is supported as the 'format' of '{}'", operator_name);
    return stringView(format);
}

/** The lowering of `$dateToString`. Mongo's format specifiers differ from the ones of
  * `formatDateTime` - Mongo's `%M` is the minute, while `formatDateTime` prints the name of the
  * month for it - so the format is translated specifier by specifier. The specifiers that
  * `formatDateTime` has no counterpart for become expressions of their own, concatenated with the
  * formatted rest, and a specifier that cannot be translated is rejected rather than forwarded.
  */
ASTPtr makeDateToString(std::string_view format, const ASTPtr & date, const ASTPtr & timezone)
{
    std::vector<ASTPtr> parts;
    std::string pending_format;

    const auto with_timezone = [&](ASTPtr function)
    {
        if (timezone)
            function->as<ASTFunction &>().arguments->children.push_back(timezone->clone());
        return function;
    };
    const auto flush = [&]
    {
        if (pending_format.empty())
            return;
        parts.push_back(with_timezone(makeASTFunction("formatDateTime", date->clone(), makeLiteral(Field(pending_format)))));
        pending_format.clear();
    };
    const auto zero_padded = [](ASTPtr number, UInt64 width)
    {
        return makeASTFunction(
            "leftPad", makeASTFunction("toString", std::move(number)), makeLiteral(Field(width)), makeLiteral(Field(String("0"))));
    };

    for (size_t i = 0; i < format.size(); ++i)
    {
        if (format[i] != '%')
        {
            pending_format.push_back(format[i]);
            continue;
        }
        if (i + 1 == format.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'format' of '$dateToString' must not end with a single '%'");

        const char specifier = format[++i];
        switch (specifier)
        {
            /// The same in both languages.
            case 'b':
            case 'd':
            case 'G':
            case 'H':
            case 'j':
            case 'm':
            case 'S':
            case 'u':
            case 'V':
            case 'Y':
            case 'z':
            case '%':
                pending_format.push_back('%');
                pending_format.push_back(specifier);
                break;
            /// The minute.
            case 'M':
                pending_format.append("%i");
                break;
            /// The milliseconds, `000` to `999`.
            case 'L':
                flush();
                parts.push_back(zero_padded(makeASTFunction("toMillisecond", date->clone()), 3));
                break;
            /// The day of the week, from Sunday as 1 to Saturday as 7: the mode 3 of `toDayOfWeek`.
            case 'w':
                flush();
                parts.push_back(makeASTFunction(
                    "toString", with_timezone(makeASTFunction("toDayOfWeek", date->clone(), makeLiteral(Field(UInt64(3)))))));
                break;
            /// The week of the year, `00` to `53`, where the week 1 starts on the first Sunday of the
            /// year: the mode 0 of `toWeek`.
            case 'U':
                flush();
                parts.push_back(zero_padded(with_timezone(makeASTFunction("toWeek", date->clone(), makeLiteral(Field(UInt64(0))))), 2));
                break;
            default:
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The format specifier '%{}' of '$dateToString' is not supported", specifier);
        }
    }
    flush();

    if (parts.empty())
        return makeLiteral(Field(String()));
    if (parts.size() == 1)
        return parts.front();
    return makeFunction("concat", std::move(parts));
}

/** The format of `$dateFromString` in the language of `parseDateTime`, see `makeDateToString`.
  * Only the specifiers that `parseDateTime` reads the same way are accepted.
  */
std::string translateDateFromStringFormat(std::string_view format)
{
    std::string result;
    for (size_t i = 0; i < format.size(); ++i)
    {
        if (format[i] != '%')
        {
            result.push_back(format[i]);
            continue;
        }
        if (i + 1 == format.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'format' of '$dateFromString' must not end with a single '%'");

        const char specifier = format[++i];
        switch (specifier)
        {
            case 'b':
            case 'd':
            case 'H':
            case 'j':
            case 'm':
            case 'S':
            case 'Y':
            case 'z':
            case '%':
                result.push_back('%');
                result.push_back(specifier);
                break;
            case 'M':
                result.append("%i");
                break;
            default:
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The format specifier '%{}' of '$dateFromString' is not supported", specifier);
        }
    }
    return result;
}

/** The lowering of `$dateTrunc`. Only `date` and `unit` are translated; an option that changes the
  * result - a `binSize` other than 1, a `timezone`, a `startOfWeek` that is not the Monday that
  * `dateTrunc` starts a week on - is rejected rather than ignored. Mongo starts a week on Sunday
  * by default, so the unit `week` needs an explicit `startOfWeek` of Monday.
  */
ASTPtr makeDateTrunc(const rapidjson::Value & argument)
{
    constexpr std::string_view name = "$dateTrunc";
    auto date = parseMongoAggregateExpression(requireMember(argument, "date", name));
    const auto & unit = requireMember(argument, "unit", name);
    if (!unit.IsString())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'unit' of '$dateTrunc' must be a string");

    bool starts_week_on_monday = false;
    for (const auto & member : argument.GetObject())
    {
        const auto member_name = stringView(member.name);
        if (member_name == "date" || member_name == "unit")
            continue;
        if (member_name == "binSize")
        {
            if (!member.value.IsNumber() || member.value.GetDouble() != 1)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only a 'binSize' of 1 is supported in '$dateTrunc'");
        }
        else if (member_name == "startOfWeek")
        {
            if (!member.value.IsString())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'startOfWeek' of '$dateTrunc' must be a string");
            String day(stringView(member.value));
            std::transform(day.begin(), day.end(), day.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
            if (day != "monday" && day != "mon")
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only a 'startOfWeek' of Monday is supported in '$dateTrunc'");
            starts_week_on_monday = true;
        }
        else if (member_name == "timezone")
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The 'timezone' of '$dateTrunc' is not supported");
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown argument '{}' of '$dateTrunc'", member_name);
    }

    if (stringView(unit) == "week" && !starts_week_on_monday)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "The unit 'week' of '$dateTrunc' is only supported with a 'startOfWeek' of Monday: Mongo starts a week on Sunday by default");

    /// Mongo truncates in UTC, `dateTrunc` in the time zone of the type of the date.
    return makeASTFunction("dateTrunc", makeLiteral(Field(String(stringView(unit)))), std::move(date), makeLiteral(Field(String("UTC"))));
}

/// The `timezone` of a date operator, UTC by default. Members other than `allowed` and `timezone`
/// are rejected.
ASTPtr parseDateTimezone(const rapidjson::Value & argument, std::string_view operator_name, std::initializer_list<std::string_view> allowed)
{
    ASTPtr timezone;
    for (const auto & member : argument.GetObject())
    {
        const auto member_name = stringView(member.name);
        if (member_name == "timezone")
            timezone = parseMongoAggregateExpression(member.value);
        else if (std::find(allowed.begin(), allowed.end(), member_name) == allowed.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown argument '{}' of '{}'", member_name, operator_name);
    }
    if (!timezone)
        timezone = makeLiteral(Field(String("UTC")));
    return timezone;
}

/** The lowering of `$dateDiff`, which counts the boundaries of the unit between the dates in the
  * `timezone`, as `dateDiff` does. A week starts on `startOfWeek`, Sunday by default, while
  * `dateDiff` starts it on Monday: both dates are moved by the days from that day to the next
  * Monday, which turns the boundaries of the weeks into the ones `dateDiff` counts.
  */
ASTPtr makeDateDiff(const rapidjson::Value & argument)
{
    constexpr std::string_view name = "$dateDiff";
    const auto & unit_value = requireMember(argument, "unit", name);
    if (!unit_value.IsString())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'unit' of '$dateDiff' must be a string");
    const String unit(stringView(unit_value));
    static const std::unordered_set<std::string_view> units
        = {"year", "quarter", "month", "week", "day", "hour", "minute", "second", "millisecond"};
    if (!units.contains(unit))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown unit '{}' of '$dateDiff'", unit);

    auto timezone = parseDateTimezone(argument, name, {"startDate", "endDate", "unit", "startOfWeek"});

    UInt64 days_to_monday = 1;
    if (auto it = argument.FindMember("startOfWeek"); it != argument.MemberEnd())
    {
        if (!it->value.IsString())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only a constant string is supported as the 'startOfWeek' of '$dateDiff'");
        String day(stringView(it->value));
        std::transform(day.begin(), day.end(), day.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        static const std::unordered_map<std::string_view, UInt64> days = {
            {"monday", 0}, {"mon", 0}, {"tuesday", 6}, {"tue", 6}, {"wednesday", 5}, {"wed", 5}, {"thursday", 4}, {"thu", 4},
            {"friday", 3}, {"fri", 3}, {"saturday", 2}, {"sat", 2}, {"sunday", 1}, {"sun", 1}};
        auto day_it = days.find(day);
        if (day_it == days.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown 'startOfWeek' '{}' of '$dateDiff'", day);
        days_to_monday = day_it->second;
    }

    const auto zoned = [&](const char * member)
    {
        ASTPtr date = makeASTFunction(
            "toDateTime64", parseMongoAggregateExpression(requireMember(argument, member, name)), makeLiteral(Field(UInt64(3))), timezone->clone());
        if (unit == "week" && days_to_monday != 0)
            date = makeASTFunction("plus", std::move(date), makeASTFunction("toIntervalDay", makeLiteral(Field(days_to_monday))));
        return date;
    };
    return makeASTFunction("dateDiff", makeLiteral(Field(unit)), zoned("startDate"), zoned("endDate"), timezone->clone());
}

ASTPtr parseOperator(std::string_view name, const rapidjson::Value & argument)
{
    if (auto it = direct_functions.find(name); it != direct_functions.end())
        return makeFunction(it->second, parseArguments(argument));

    if (name == "$eq" || name == "$ne" || name == "$lt" || name == "$lte" || name == "$gt" || name == "$gte")
    {
        auto arguments = parseArguments(argument);
        requireArgumentCount(name, arguments, 2);
        return makeComparison(name, arguments[0], arguments[1]);
    }

    if (auto it = date_part_functions.find(name); it != date_part_functions.end())
    {
        /// Either the date itself, or the document `{"date" : ..., "timezone" : ...}`.
        std::vector<ASTPtr> arguments;
        ASTPtr timezone;
        if (argument.IsObject() && argument.HasMember("date"))
        {
            arguments.push_back(parseMongoAggregateExpression(argument["date"]));
            timezone = parseDateTimezone(argument, name, {"date"});
        }
        else
        {
            arguments = parseArguments(argument);
            requireArgumentCount(name, arguments, 1);
            timezone = makeLiteral(Field(String("UTC")));
        }
        if (it->second.mode)
            arguments.push_back(makeLiteral(Field(*it->second.mode)));
        arguments.push_back(std::move(timezone));
        return makeFunction(it->second.function, std::move(arguments));
    }

    if (auto it = folded_functions.find(name); it != folded_functions.end())
    {
        auto arguments = parseArguments(argument);
        if (arguments.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'{}' takes at least one argument", name);
        ASTPtr result = arguments[0];
        for (size_t i = 1; i < arguments.size(); ++i)
            result = makeASTFunction(it->second, result, arguments[i]);
        if (name != "$add")
            return result;
        /// The sum is a date exactly when one of the operands is, see `date_arithmetic_message`.
        return withValue(std::move(result), [](const ASTPtr & sum) { return withGuard(sum, makeIsDate(sum), date_arithmetic_message); });
    }

    if (name == "$subtract")
    {
        auto arguments = parseArguments(argument);
        requireArgumentCount(name, arguments, 2);
        return withValue(arguments[0], [&](const ASTPtr & minuend)
        {
            return withValue(arguments[1], [&](const ASTPtr & subtrahend)
            {
                return withGuard(
                    makeASTFunction("minus", minuend->clone(), subtrahend->clone()),
                    makeASTFunction("or", makeIsDate(minuend), makeIsDate(subtrahend)),
                    date_arithmetic_message);
            });
        });
    }

    if (name == "$toLong" || name == "$toDouble" || name == "$toBool")
    {
        auto arguments = parseArguments(argument);
        requireArgumentCount(name, arguments, 1);
        if (name == "$toLong")
            return makeToLong(arguments[0]);
        if (name == "$toDouble")
            return makeToDouble(arguments[0]);
        return makeToBool(arguments[0]);
    }

    if (name == "$literal")
    {
        auto constant = tryParseMongoConstant(argument);
        if (!constant)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only a scalar is supported as the argument of '$literal'");
        return constant;
    }

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
        auto is_array = makeASTFunction(
            "startsWith", makeASTFunction("toTypeName", arguments[0]), makeLiteral(Field(String("Array"))));
        auto guard = makeASTFunction(
            "throwIf",
            makeASTFunction("not", std::move(is_array)),
            makeLiteral(Field(String("The argument of '$size' must be an array"))));
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

    /** An element past the end of the array is missing in Mongo, which `$ifNull` and the others
      * read as `null`, while `arrayElement` returns the default of the element type. So the
      * elements are taken with `arrayElementOrNull`.
      */
    if (name == "$first" || name == "$last")
    {
        /// Outside `$group`, `$first` and `$last` take an element of an array.
        auto arguments = parseArguments(argument);
        requireArgumentCount(name, arguments, 1);
        return makeASTFunction("arrayElementOrNull", arguments[0], makeLiteral(Field(name == "$first" ? Int64(1) : Int64(-1))));
    }

    if (name == "$arrayElemAt")
    {
        auto arguments = parseArguments(argument);
        requireArgumentCount(name, arguments, 2);
        /// Mongo indexes an array from zero and ClickHouse from one; a negative index counts from
        /// the end in both, and there `-1` already means the same element. Only a non-negative
        /// index is shifted. When the index is not a constant, its sign is known only at runtime,
        /// so the shift is chosen by a condition on it.
        const auto * index = arguments[1]->as<ASTLiteral>();
        if (index && index->value.getType() == Field::Types::Int64 && index->value.safeGet<Int64>() < 0)
            return makeASTFunction("arrayElementOrNull", arguments[0], arguments[1]);
        if (index)
            return makeASTFunction("arrayElementOrNull", arguments[0], makeASTFunction("plus", arguments[1], makeLiteral(Field(UInt64(1)))));

        return withValue(arguments[1], [&](const ASTPtr & value)
        {
            auto shifted_index = makeASTFunction(
                "if",
                makeASTFunction("less", value->clone(), makeLiteral(Field(Int64(0)))),
                value->clone(),
                makeASTFunction("plus", value->clone(), makeLiteral(Field(UInt64(1)))));
            return makeASTFunction("arrayElementOrNull", arguments[0], std::move(shifted_index));
        });
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
            arguments.push_back(parseMongoAggregateExpression(requireMember(branch, "case", name)));
            arguments.push_back(parseMongoAggregateExpression(requireMember(branch, "then", name)));
        }
        auto default_it = argument.FindMember("default");
        arguments.push_back(default_it == argument.MemberEnd() ? makeNull() : parseMongoAggregateExpression(default_it->value));
        return makeFunction("multiIf", std::move(arguments));
    }

    if (name == "$dateTrunc")
        return makeDateTrunc(argument);

    if (name == "$toInt")
    {
        auto arguments = parseArguments(argument);
        requireArgumentCount(name, arguments, 1);
        return makeToInt(arguments[0]);
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

    if (name == "$cmp")
    {
        auto arguments = parseArguments(argument);
        requireArgumentCount(name, arguments, 2);
        /// Both operands are compared twice, so they are bound once (see `withValue`).
        return withValue(arguments[0], [&](const ASTPtr & left)
        {
            return withValue(arguments[1], [&](const ASTPtr & right)
            {
                return makeASTFunction(
                    "multiIf",
                    makeComparison("$lt", left->clone(), right->clone()),
                    makeLiteral(Field(Int64(-1))),
                    makeComparison("$gt", left->clone(), right->clone()),
                    makeLiteral(Field(Int64(1))),
                    makeLiteral(Field(Int64(0))));
            });
        });
    }

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
        /// `FindMember` requires a document, so the argument is checked before it is searched.
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
        /// The result is a set, which holds a value of the first array once however many times
        /// the array does.
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
        auto body = makeTruthValue(make_intrusive<ASTIdentifier>("__mongo_element"));
        return makeASTFunction(
            name == "$anyElementTrue" ? "arrayExists" : "arrayAll", makeLambda("__mongo_element", std::move(body)), arguments[0]);
    }

    if (name == "$map" || name == "$filter")
    {
        auto input = parseMongoAggregateExpression(requireMember(argument, "input", name));
        auto variable = parseVariableName(argument, "this");
        auto body = parseMongoAggregateExpression(requireMember(argument, name == "$map" ? "in" : "cond", name));
        auto mapped = makeASTFunction(
            name == "$map" ? "arrayMap" : "arrayFilter", makeLambda(variable, std::move(body)), std::move(input));
        if (auto limit_it = argument.FindMember("limit"); limit_it != argument.MemberEnd() && name == "$filter")
            return makeASTFunction(
                "arraySlice", std::move(mapped), makeLiteral(Field(UInt64(1))), parseMongoAggregateExpression(limit_it->value));
        return mapped;
    }

    if (name == "$dateToString")
    {
        auto date = parseMongoAggregateExpression(requireMember(argument, "date", name));
        ASTPtr timezone;
        std::optional<std::string_view> format;
        for (const auto & member : argument.GetObject())
        {
            auto member_name = stringView(member.name);
            if (member_name == "format")
                format = requireDateFormat(member.value, name);
            else if (member_name == "timezone")
                timezone = parseMongoAggregateExpression(member.value);
            else if (member_name == "onNull")
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The 'onNull' of '{}' is not supported", name);
            else if (member_name != "date")
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown argument '{}' of '{}'", member_name, name);
        }

        /// Mongo formats a date in UTC unless asked otherwise, whatever the time zone of the
        /// column is. Its default format is an ISO 8601 date with milliseconds, which ends with
        /// the `Z` of UTC only when the date is formatted in UTC.
        bool in_utc = true;
        if (timezone)
        {
            const auto * literal = timezone->as<ASTLiteral>();
            in_utc = literal && literal->value.getType() == Field::Types::String
                && (literal->value.safeGet<String>() == "UTC" || literal->value.safeGet<String>() == "GMT"
                    || literal->value.safeGet<String>() == "Etc/UTC" || literal->value.safeGet<String>() == "Etc/GMT");
        }
        else
            timezone = makeLiteral(Field(String("UTC")));
        if (!format)
            format = in_utc ? "%Y-%m-%dT%H:%M:%S.%LZ" : "%Y-%m-%dT%H:%M:%S.%L";
        return makeDateToString(*format, date, timezone);
    }

    if (name == "$dateFromString")
    {
        auto text = parseMongoAggregateExpression(requireMember(argument, "dateString", name));
        for (const auto & member : argument.GetObject())
        {
            auto member_name = stringView(member.name);
            if (member_name == "onError" || member_name == "onNull")
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The '{}' of '{}' is not supported", member_name, name);
            if (member_name != "dateString" && member_name != "format" && member_name != "timezone")
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown argument '{}' of '{}'", member_name, name);
        }

        /// A text without an offset is read in `timezone`, or in UTC without it: without the time
        /// zone `parseDateTime` would read the text in the session's one, while every other date of
        /// this dialect - `$date` and `$toDate` - is UTC. The result is converted
        /// back to UTC, so that it is printed the same way as the other dates.
        ASTPtr timezone;
        if (auto timezone_it = argument.FindMember("timezone"); timezone_it != argument.MemberEnd())
            timezone = parseMongoAggregateExpression(timezone_it->value);
        else
            timezone = makeLiteral(Field(String("UTC")));

        ASTPtr parsed;
        if (auto format_it = argument.FindMember("format"); format_it != argument.MemberEnd())
        {
            auto format = translateDateFromStringFormat(requireDateFormat(format_it->value, name));
            parsed = makeASTFunction("parseDateTime", text, makeLiteral(Field(std::move(format))), timezone);
        }
        else
            parsed = makeASTFunction("parseDateTime64BestEffort", text, makeLiteral(Field(UInt64(3))), timezone);

        if (argument.HasMember("timezone"))
            parsed = makeASTFunction("toTimeZone", parsed, makeLiteral(Field(String("UTC"))));
        return parsed;
    }

    /** The calendar arithmetic of `$dateAdd`, `$dateSubtract` and `$dateDiff` happens in their
      * `timezone`, UTC by default: a day added across a daylight saving time transition is not 24
      * hours. ClickHouse computes it in the time zone of the date's type, so the date is converted
      * to one of that time zone first, and the result of `$dateAdd` back to UTC, the time zone of
      * every other date of this dialect.
      */
    if (name == "$dateAdd" || name == "$dateSubtract")
    {
        auto start = parseMongoAggregateExpression(requireMember(argument, "startDate", name));
        auto amount = parseMongoAggregateExpression(requireMember(argument, "amount", name));
        auto interval = makeASTFunction(dateIntervalFunction(requireMember(argument, "unit", name), name), std::move(amount));
        auto timezone = parseDateTimezone(argument, name, {"startDate", "unit", "amount"});
        auto zoned_start = makeASTFunction("toDateTime64", std::move(start), makeLiteral(Field(UInt64(3))), std::move(timezone));
        return makeASTFunction(
            "toTimeZone",
            makeASTFunction(name == "$dateAdd" ? "plus" : "minus", std::move(zoned_start), std::move(interval)),
            makeLiteral(Field(String("UTC"))));
    }

    if (name == "$dateDiff")
        return makeDateDiff(argument);

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
            /// time. A user variable, which only `$map` and `$filter` can bind, names their lambda
            /// parameter, see `variableIdentifier`.
            static const std::unordered_set<std::string_view> system_variables = {
                "ROOT", "CURRENT", "REMOVE", "DESCEND", "PRUNE", "KEEP", "CLUSTER_TIME", "SEARCH_META", "USER_ROLES"};
            if (variable.empty() || system_variables.contains(variable))
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The aggregation system variable '{}' is not supported", text);
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
        return parseOperator(name, member.value);
    }

    if (auto constant = tryParseMongoConstant(value))
        return constant;

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot translate the aggregation expression");
}

namespace
{

/** The keys of a preceding `$sort` as one expression to compare the documents of a group by: the
  * key itself for a single key, and a tuple of them for several, which compares lexicographically
  * the way a multiple key sort does.
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

    /** Every key is preceded by whether it is not `NULL`: Mongo orders `null` below every other
      * value, so a `NULL` key compares as the smallest one. The keys are a tuple even when there is
      * a single one, because a tuple is never `NULL` itself, and an aggregate function such as
      * `argMin` skips the rows whose argument is `NULL`. An array key is rejected, see
      * `makeMongoSortKey`.
      */
    auto keys = makeASTFunction("tuple");
    for (const auto & key : order.keys)
    {
        auto sort_key = makeMongoSortKey(key.first->clone());
        keys->arguments->children.push_back(makeASTFunction("isNotNull", sort_key->clone()));
        keys->arguments->children.push_back(std::move(sort_key));
    }
    return {keys, direction};
}

/// The values of a group in the order of the stream: they are collected together with the sort key
/// and sorted by it, because `groupArray` appends them in the order the query reads the rows in.
ASTPtr makeOrderedGroupValues(ASTPtr value, const MongoGroupOrder & order, std::string_view accumulator)
{
    auto [key, direction] = makeGroupOrderKey(order, accumulator);
    auto pairs = makeASTFunction("groupArray", makeASTFunction("tuple", std::move(key), std::move(value)));
    auto sorted = makeASTFunction(direction == 1 ? "arraySort" : "arrayReverseSort", std::move(pairs));
    auto element = makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>("x"), makeLiteral(Field(UInt64(2))));
    return makeASTFunction("arrayMap", makeLambda("x", std::move(element)), std::move(sorted));
}

/** An aggregate function skips the rows in which its argument is `NULL`, while Mongo's `$first`,
  * `$last`, `$push` and the others keep a `null` value of a document as any other one. So the value
  * is wrapped into a tuple, which is never `NULL` itself, and unwrapped from the result.
  */
ASTPtr wrapIntoTuple(ASTPtr value)
{
    return makeASTFunction("tuple", std::move(value));
}

ASTPtr unwrapFromTuple(ASTPtr aggregated)
{
    return makeASTFunction("tupleElement", std::move(aggregated), makeLiteral(Field(UInt64(1))));
}

/// The same for an aggregate function that collects the values into an array.
ASTPtr unwrapArrayFromTuples(ASTPtr aggregated)
{
    auto element = makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>("x"), makeLiteral(Field(UInt64(1))));
    return makeASTFunction("arrayMap", makeLambda("x", std::move(element)), std::move(aggregated));
}

}

ASTPtr parseMongoAccumulator(const rapidjson::Value & value, const MongoGroupOrder & order)
{
    if (!value.IsObject() || value.MemberCount() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "An accumulator of '$group' must be a document holding one operator");

    const auto & member = *value.MemberBegin();
    auto name = stringView(member.name);

    /// The accumulators that ignore a `null`, as the aggregate functions they map onto do.
    static const std::unordered_map<std::string_view, std::string> accumulators = {
        {"$avg", "avg"},
        {"$min", "min"},
        {"$max", "max"},
        {"$stdDevPop", "stddevPop"},
        {"$stdDevSamp", "stddevSamp"},
    };

    /// The accumulators that keep a `null`, see `wrapIntoTuple`; the boolean says whether the
    /// result is an array.
    static const std::unordered_map<std::string_view, std::pair<std::string, bool>> null_preserving_accumulators = {
        {"$first", {"any", false}},
        {"$last", {"anyLast", false}},
        {"$push", {"groupArray", true}},
        {"$addToSet", {"groupUniqArray", true}},
    };

    if (name == "$count")
        return makeASTFunction("count");

    if (name == "$firstN" || name == "$lastN")
    {
        auto input = parseMongoAggregateExpression(requireMember(member.value, "input", name));
        auto count = parseMongoAggregateExpression(requireMember(member.value, "n", name));

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
        auto function = makeASTFunction(name == "$firstN" ? "groupArray" : "groupArrayLast", wrapIntoTuple(std::move(input)));
        auto parameters = make_intrusive<ASTExpressionList>();
        parameters->children.push_back(std::move(count));
        function->parameters = parameters;
        function->children.push_back(std::move(parameters));
        return unwrapArrayFromTuples(std::move(function));
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
        return unwrapFromTuple(
            makeASTFunction(takes_the_smallest_key ? "argMin" : "argMax", wrapIntoTuple(std::move(input)), std::move(key)));
    }

    if (auto it = accumulators.find(name); it != accumulators.end())
        return makeASTFunction(it->second, parseMongoAggregateExpression(member.value));

    if (auto it = null_preserving_accumulators.find(name); it != null_preserving_accumulators.end())
    {
        const auto & [function, returns_array] = it->second;
        auto aggregated = makeASTFunction(function, wrapIntoTuple(parseMongoAggregateExpression(member.value)));
        return returns_array ? unwrapArrayFromTuples(std::move(aggregated)) : unwrapFromTuple(std::move(aggregated));
    }

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The accumulator '{}' is not supported", name);
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
            /// The index of the match is the number of code points before it. The text before the
            /// match is what the shortest prefix captures in front of the pattern: the leftmost
            /// match is the one the shortest prefix leaves.
            auto prefix_pattern = makeLiteral(Field("^((?s:.*?))(?:" + pattern->as<ASTLiteral &>().value.safeGet<String>() + ")"));
            result.push_back(
                {name + ".idx",
                 makeASTFunction(
                     "if",
                     matched->clone(),
                     makeASTFunction(
                         "toInt32",
                         makeASTFunction(
                             "lengthUTF8", makeASTFunction("regexpExtract", input->clone(), std::move(prefix_pattern), makeLiteral(Field(UInt64(1)))))),
                     makeNull())});
            /// `extractGroups` rejects a pattern without groups, while Mongo's `captures` is then empty.
            const re2::RE2 compiled(pattern->as<ASTLiteral &>().value.safeGet<String>(), re2::RE2::Quiet);
            ASTPtr captures;
            if (compiled.ok() && compiled.NumberOfCapturingGroups() == 0)
                captures = makeASTFunction("CAST", makeASTFunction("array"), makeLiteral(Field(String("Array(String)"))));
            else
                captures = makeASTFunction("extractGroups", input->clone(), pattern->clone());
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
