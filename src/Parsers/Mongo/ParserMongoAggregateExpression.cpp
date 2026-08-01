#include <Parsers/Mongo/ParserMongoAggregateExpression.h>

#include <string_view>
#include <unordered_map>

#include <Core/Field.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Mongo/MongoConstants.h>
#include <Common/Exception.h>

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
    {"$toUpper", "upperUTF8"},
    {"$toLower", "lowerUTF8"},
    {"$abs", "abs"},
    {"$ceil", "ceil"},
    {"$floor", "floor"},
    {"$round", "round"},
    {"$sqrt", "sqrt"},
    {"$exp", "exp"},
    {"$ln", "log"},
    {"$log10", "log10"},
    {"$size", "length"},
    {"$reverseArray", "reverse"},
    {"$not", "not"},
    {"$year", "toYear"},
    {"$month", "toMonth"},
    {"$dayOfMonth", "toDayOfMonth"},
    {"$dayOfWeek", "toDayOfWeek"},
    {"$dayOfYear", "toDayOfYear"},
    {"$week", "toWeek"},
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

/// The regular expression of `$regexFind` and `$regexMatch`: a bare pattern string, or the
/// Extended JSON document a driver sends for a regular expression literal.
std::string parseRegularExpressionField(const rapidjson::Value & value, std::string_view operator_name)
{
    if (auto pattern = tryParseMongoRegularExpression(value))
        return *pattern;
    if (value.IsString())
        return std::string(stringView(value));
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'regex' of '{}' must be a string or a regular expression", operator_name);
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

    if (name == "$toDecimal")
    {
        auto arguments = parseArguments(argument);
        requireArgumentCount(name, arguments, 1);
        return makeASTFunction("toDecimal128", arguments[0], makeLiteral(Field(UInt64(10))));
    }

    if (name == "$toDate")
    {
        auto arguments = parseArguments(argument);
        requireArgumentCount(name, arguments, 1);
        return makeASTFunction("toDateTime64", arguments[0], makeLiteral(Field(UInt64(3))), makeLiteral(Field(String("UTC"))));
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
        auto pattern = parseRegularExpressionField(requireMember(argument, "regex", name), name);
        return makeASTFunction("match", input, makeLiteral(Field(pattern)));
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
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The aggregation variable '{}' is not supported", text);
        if (text.starts_with("$"))
        {
            /// A field path names the column of the same name, dots included: the dialect maps a
            /// nested document field onto an `a.b` column.
            return make_intrusive<ASTIdentifier>(String(text.substr(1)));
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

    if (name == "$sum")
    {
        /// `{"$sum": 1}` counts the documents of the group. Counting is much cheaper than summing a
        /// constant, and it is by far the most common accumulator.
        const auto * literal = parseMongoAggregateExpression(member.value)->as<ASTLiteral>();
        if (literal && literal->value == Field(Int64(1)))
            return makeASTFunction("count");
        return makeASTFunction("sum", parseMongoAggregateExpression(member.value));
    }

    if (auto it = accumulators.find(name); it != accumulators.end())
        return makeASTFunction(it->second, parseMongoAggregateExpression(member.value));

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The accumulator '{}' is not supported", name);
}

void expandMongoProjectedField(const std::string & name, const rapidjson::Value & value, std::vector<MongoProjectedField> & result)
{
    if (value.IsObject() && value.MemberCount() >= 1 && !tryParseMongoConstant(value))
    {
        auto first_member_name = stringView(value.MemberBegin()->name);

        if (first_member_name == "$regexFind" && value.MemberCount() == 1)
        {
            const auto & argument = value.MemberBegin()->value;
            auto input = parseMongoAggregateExpression(requireMember(argument, "input", "$regexFind"));
            auto pattern = makeLiteral(Field(parseRegularExpressionField(requireMember(argument, "regex", "$regexFind"), "$regexFind")));

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
