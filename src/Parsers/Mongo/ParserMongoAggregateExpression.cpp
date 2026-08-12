#include <Parsers/Mongo/ParserMongoAggregateExpression.h>

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

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
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
    {"$toLong", "toInt64"},
    {"$toInt", "toInt32"},
    {"$toDouble", "toFloat64"},
    {"$toBool", "toBool"},
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
    {"$year", "toYear"},
    {"$month", "toMonth"},
    {"$dayOfMonth", "toDayOfMonth"},
    {"$dayOfYear", "toDayOfYear"},
    {"$week", "toWeek"},
    {"$isoWeek", "toISOWeek"},
    {"$isoWeekYear", "toISOYear"},
    {"$hour", "toHour"},
    {"$minute", "toMinute"},
    {"$second", "toSecond"},
    {"$millisecond", "toMillisecond"},
    {"$concat", "concat"},
    {"$and", "and"},
    {"$or", "or"},
    {"$eq", "equals"},
    {"$ne", "notEquals"},
    {"$lt", "less"},
    {"$lte", "lessOrEquals"},
    {"$gt", "greater"},
    {"$gte", "greaterOrEquals"},
    {"$subtract", "minus"},
    {"$divide", "divide"},
    {"$mod", "modulo"},
    {"$pow", "pow"},
    {"$ifNull", "coalesce"},
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

/// The name a `$map` or a `$filter` binds its element to. Mongo writes it as `$$<name>` and
/// defaults it to `this`; the lambda parameter is given the same name, so that referring to the
/// variable inside the body resolves to it and shadows a column of that name exactly as in Mongo.
std::string parseVariableName(const rapidjson::Value & argument, const char * default_name)
{
    auto it = argument.FindMember("as");
    if (it == argument.MemberEnd())
        return default_name;
    if (!it->value.IsString())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'as' of a higher order operator must be a string");
    return std::string(stringView(it->value));
}

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
                /// The shortest decimal text that reads back as the same double, which is what
                /// Mongo preserves when it converts a double to a decimal.
                scale = decimalScaleOfNumberDecimal(applyVisitor(FieldVisitorToString(), literal->value));
                break;
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

    auto is_exact = makeASTFunction(
        "match",
        makeASTFunction("toTypeName", argument),
        makeLiteral(Field(String("^(U?Int(8|16|32|64|128|256)|Bool)$"))));
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

    /// Neither branch may throw for the type of the other: the losing branch is dropped by the
    /// analyzer and skipped by short-circuit evaluation, but with the old analyzer and
    /// `short_circuit_function_evaluation = 'disable'` it still runs. `toFloat64OrZero` over
    /// `toString` reads any type without throwing; the string it produces for a number is the
    /// number itself, and the branch taken for a string never uses it.
    auto milliseconds = makeASTFunction(
        "fromUnixTimestamp64Milli",
        makeASTFunction("toInt64", makeASTFunction("toFloat64OrZero", makeASTFunction("toString", argument->clone()))),
        makeLiteral(Field(String("UTC"))));
    auto is_a_number = makeASTFunction(
        "match", makeASTFunction("toTypeName", argument->clone()), makeLiteral(Field(String("^(U?Int|Float|Decimal)"))));
    return makeASTFunction("if", std::move(is_a_number), std::move(milliseconds), parsed(argument->clone()));
}

ASTPtr parseOperator(std::string_view name, const rapidjson::Value & argument)
{
    if (auto it = direct_functions.find(name); it != direct_functions.end())
        return makeFunction(it->second, parseArguments(argument));

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

    if (name == "$first" || name == "$last")
    {
        /// Outside `$group`, `$first` and `$last` take an element of an array.
        auto arguments = parseArguments(argument);
        requireArgumentCount(name, arguments, 1);
        return makeASTFunction("arrayElement", arguments[0], makeLiteral(Field(name == "$first" ? Int64(1) : Int64(-1))));
    }

    if (name == "$arrayElemAt")
    {
        auto arguments = parseArguments(argument);
        requireArgumentCount(name, arguments, 2);
        /// Mongo indexes an array from zero and ClickHouse from one; a negative index counts from
        /// the end in both, and there `-1` already means the same element.
        const auto * index = arguments[1]->as<ASTLiteral>();
        const bool counts_from_the_end
            = index && index->value.getType() == Field::Types::Int64 && index->value.safeGet<Int64>() < 0;
        if (!counts_from_the_end)
            arguments[1] = makeASTFunction("plus", arguments[1], makeLiteral(Field(UInt64(1))));
        return makeASTFunction("arrayElement", arguments[0], arguments[1]);
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
    {
        auto date = parseMongoAggregateExpression(requireMember(argument, "date", name));
        const auto & unit = requireMember(argument, "unit", name);
        if (!unit.IsString())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'unit' of '$dateTrunc' must be a string");
        return makeASTFunction("dateTrunc", makeLiteral(Field(String(stringView(unit)))), date);
    }

    if (name == "$regexMatch")
    {
        auto input = parseMongoAggregateExpression(requireMember(argument, "input", name));
        auto pattern = parseRegularExpressionField(argument, name);
        return makeASTFunction("match", input, makeLiteral(Field(pattern)));
    }

    if (name == "$rand")
        return makeASTFunction("randCanonical");

    if (name == "$dayOfWeek" || name == "$isoDayOfWeek")
    {
        auto arguments = parseArguments(argument);
        requireArgumentCount(name, arguments, 1);
        /// `$dayOfWeek` numbers the days from Sunday as 1, `$isoDayOfWeek` from Monday as 1; those
        /// are the modes 3 and 0 of `toDayOfWeek`.
        return makeASTFunction("toDayOfWeek", arguments[0], makeLiteral(Field(UInt64(name == "$dayOfWeek" ? 3 : 0))));
    }

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
        return makeASTFunction(
            "multiIf",
            makeASTFunction("less", arguments[0], arguments[1]),
            makeLiteral(Field(Int64(-1))),
            makeASTFunction("greater", arguments[0]->clone(), arguments[1]->clone()),
            makeLiteral(Field(Int64(1))),
            makeLiteral(Field(Int64(0))));
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
        if (argument.FindMember("chars") != argument.MemberEnd())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The 'chars' of '{}' is not supported", name);
        auto input = parseMongoAggregateExpression(requireMember(argument, "input", name));
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
        return makeASTFunction("arrayFilter", makeLambda("__mongo_element", std::move(body)), arguments[0]);
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
        auto body = makeASTFunction("toBool", make_intrusive<ASTIdentifier>("__mongo_element"));
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
        auto format = parseMongoAggregateExpression(requireMember(argument, "format", name));
        if (auto timezone_it = argument.FindMember("timezone"); timezone_it != argument.MemberEnd())
            return makeASTFunction("formatDateTime", date, format, parseMongoAggregateExpression(timezone_it->value));
        return makeASTFunction("formatDateTime", date, format);
    }

    if (name == "$dateFromString")
    {
        auto text = parseMongoAggregateExpression(requireMember(argument, "dateString", name));
        if (auto format_it = argument.FindMember("format"); format_it != argument.MemberEnd())
            return makeASTFunction("parseDateTime", text, parseMongoAggregateExpression(format_it->value));
        return makeASTFunction("parseDateTime64BestEffort", text, makeLiteral(Field(UInt64(3))), makeLiteral(Field(String("UTC"))));
    }

    if (name == "$dateAdd" || name == "$dateSubtract")
    {
        auto start = parseMongoAggregateExpression(requireMember(argument, "startDate", name));
        auto amount = parseMongoAggregateExpression(requireMember(argument, "amount", name));
        auto interval = makeASTFunction(dateIntervalFunction(requireMember(argument, "unit", name), name), std::move(amount));
        return makeASTFunction(name == "$dateAdd" ? "plus" : "minus", std::move(start), std::move(interval));
    }

    if (name == "$dateDiff")
    {
        const auto & unit = requireMember(argument, "unit", name);
        if (!unit.IsString())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'unit' of '$dateDiff' must be a string");
        return makeASTFunction(
            "dateDiff",
            makeLiteral(Field(String(stringView(unit)))),
            parseMongoAggregateExpression(requireMember(argument, "startDate", name)),
            parseMongoAggregateExpression(requireMember(argument, "endDate", name)));
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
            /// parameter of the same name.
            static const std::unordered_set<std::string_view> system_variables = {
                "ROOT", "CURRENT", "REMOVE", "DESCEND", "PRUNE", "KEEP", "CLUSTER_TIME", "SEARCH_META", "USER_ROLES"};
            if (variable.empty() || system_variables.contains(variable))
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The aggregation system variable '{}' is not supported", text);
            return make_intrusive<ASTIdentifier>(String(variable));
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

ASTPtr parseMongoAccumulator(const rapidjson::Value & value)
{
    if (!value.IsObject() || value.MemberCount() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "An accumulator of '$group' must be a document holding one operator");

    const auto & member = *value.MemberBegin();
    auto name = stringView(member.name);

    static const std::unordered_map<std::string_view, std::string> accumulators = {
        {"$avg", "avg"},
        {"$min", "min"},
        {"$max", "max"},
        {"$first", "any"},
        {"$last", "anyLast"},
        {"$push", "groupArray"},
        {"$addToSet", "groupUniqArray"},
        {"$stdDevPop", "stddevPop"},
        {"$stdDevSamp", "stddevSamp"},
    };

    if (name == "$count")
        return makeASTFunction("count");

    if (name == "$firstN" || name == "$lastN")
    {
        /// `groupArray(N)(x)` keeps the first N values of the group and `groupArrayLast(N)(x)` the
        /// last ones; the count is a parameter of the aggregate function rather than an argument.
        auto input = parseMongoAggregateExpression(requireMember(member.value, "input", name));
        auto count = parseMongoAggregateExpression(requireMember(member.value, "n", name));
        auto function = makeASTFunction(name == "$firstN" ? "groupArray" : "groupArrayLast", std::move(input));
        auto parameters = make_intrusive<ASTExpressionList>();
        parameters->children.push_back(std::move(count));
        function->parameters = parameters;
        function->children.push_back(std::move(parameters));
        return function;
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

    if (auto it = accumulators.find(name); it != accumulators.end())
        return makeASTFunction(it->second, parseMongoAggregateExpression(member.value));

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
            result.push_back(
                {name + ".idx",
                 makeASTFunction(
                     "if",
                     matched->clone(),
                     makeASTFunction("minus", makeASTFunction("position", input->clone(), whole_match->clone()), makeLiteral(Field(UInt64(1)))),
                     makeNull())});
            result.push_back(
                {name + ".captures",
                 makeASTFunction("if", matched->clone(), makeASTFunction("extractGroups", input->clone(), pattern->clone()), makeNull())});
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
