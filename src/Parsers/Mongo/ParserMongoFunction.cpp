#include <Parsers/Mongo/ParserMongoFunction.h>
#include <memory>

#include <Core/Field.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST_fwd.h>

#include <Parsers/ASTAssignment.h>
#include <Parsers/Mongo/MongoConstants.h>
#include <Parsers/Mongo/ParserMongoAggregateExpression.h>
#include <Parsers/Mongo/ParserMongoQuery.h>
#include <Parsers/Mongo/Utils.h>
#include <Parsers/ASTExpressionList.h>

#include <cmath>
#include <string_view>
#include <unordered_map>

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

/// True when every member of the document belongs to a regular expression, so that translating it
/// into a single match does not drop a sibling operator.
bool isOnlyRegularExpression(const rapidjson::Value & value)
{
    for (auto it = value.MemberBegin(); it != value.MemberEnd(); ++it)
    {
        auto name = stringView(it->name);
        if (name != "$regex" && name != "$options" && name != "$regularExpression")
            return false;
    }
    return true;
}

/** One `<field>: {<operator>: <argument>}` condition of a filter.
  *
  * `document` is the whole operator document, which `$regex` needs because its `$options` are a
  * sibling member rather than part of its own argument.
  */
ASTPtr parseFieldOperator(
    const std::string & field,
    std::string_view name,
    const rapidjson::Value & argument,
    const rapidjson::Value & document,
    const std::shared_ptr<QueryMetadata> & metadata)
{
    static const std::unordered_map<std::string_view, std::string> comparisons = {
        {"$eq", "equals"},
        {"$ne", "notEquals"},
        {"$lt", "less"},
        {"$lte", "lessOrEquals"},
        {"$gt", "greater"},
        {"$gte", "greaterOrEquals"},
    };

    auto identifier = [&] { return make_intrusive<ASTIdentifier>(field); };

    if (auto it = comparisons.find(name); it != comparisons.end())
    {
        auto constant = tryParseMongoConstant(argument);
        if (!constant)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The argument of '{}' must be a constant", name);
        return makeASTFunction(it->second, identifier(), constant);
    }

    if (name == "$regex" || name == "$regularExpression")
    {
        auto pattern = tryParseMongoRegularExpression(document);
        if (!pattern)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot read the regular expression of '{}'", name);
        return makeASTFunction("match", identifier(), make_intrusive<ASTLiteral>(Field(*pattern)));
    }

    if (name == "$in" || name == "$nin")
    {
        if (!argument.IsArray())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The argument of '{}' must be an array", name);

        auto array = makeASTFunction("array");
        for (const auto & element : argument.GetArray())
        {
            auto constant = tryParseMongoConstant(element);
            if (!constant)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only constants are supported in the array of '{}'", name);
            array->arguments->children.push_back(std::move(constant));
        }

        /** Mongo applies `$in` to an array field element wise: the document matches when any
          * element of the array is among the candidates. The type of the column is not known
          * here, so one expression has to fit both shapes: wrapping the field into an array and
          * flattening turns a scalar into the one element array of itself and leaves an array
          * field as its elements, and `hasAny` is the membership test over them. What this does
          * not cover is the whole-array match - a candidate that is itself an array - because the
          * candidates are constants; and a field of nested arrays is flattened through every
          * level rather than one.
          */
        auto field_elements = makeASTFunction("flatten", makeASTFunction("array", identifier()));
        auto condition = makeASTFunction("hasAny", array, std::move(field_elements));
        return name == "$in" ? condition : makeASTFunction("not", condition);
    }

    if (name == "$not")
    {
        ASTPtr negated;
        if (!MongoIdentityFunction(copyValue(argument, metadata->getAllocator()), metadata, field).parseImpl(negated))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot translate the argument of '$not'");
        return makeASTFunction("not", negated);
    }

    if (name == "$exists")
    {
        if (!argument.IsBool())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The argument of '$exists' must be a boolean");
        /** A ClickHouse table has a fixed set of columns, so a field either is a column of it or
          * the query does not resolve at all. What a document can still leave out is the value, and
          * that is a `NULL`: a field is present when the column is not null, which for a column
          * that is not `Nullable` is always.
          */
        return makeASTFunction(argument.GetBool() ? "isNotNull" : "isNull", make_intrusive<ASTIdentifier>(field));
    }

    if (name == "$mod")
    {
        if (!argument.IsArray() || argument.Size() != 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The argument of '$mod' must be an array of a divisor and a remainder");
        auto divisor = tryParseMongoConstant(argument[0]);
        auto remainder = tryParseMongoConstant(argument[1]);
        if (!divisor || !remainder)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The divisor and the remainder of '$mod' must be constants");
        if (argument[0].IsNumber() && argument[0].GetDouble() == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The divisor of '$mod' must not be zero");
        return makeASTFunction("equals", makeASTFunction("modulo", identifier(), divisor), remainder);
    }

    if (name == "$size")
    {
        auto size = tryParseMongoConstant(argument);
        if (!size)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The argument of '$size' must be a number");
        /** `$size` is defined on arrays only. A string also has a `length`, but to Mongo a
          * document whose field is not an array matches no `$size` at all, so the length test is
          * guarded by the type of the column. A column of a type that has no `length` (a number,
          * for instance) makes the query fail instead of matching nothing - a controlled
          * rejection, since the type is not known when this is lowered.
          */
        auto is_array = makeASTFunction(
            "startsWith", makeASTFunction("toTypeName", identifier()), make_intrusive<ASTLiteral>(Field(String("Array"))));
        return makeASTFunction("and", std::move(is_array), makeASTFunction("equals", makeASTFunction("length", identifier()), size));
    }

    if (name == "$all")
    {
        if (!argument.IsArray())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The argument of '$all' must be an array");
        /// `$all` of nothing matches no document at all, while `hasAll` of an empty array holds for
        /// every one of them.
        if (argument.Empty())
            return make_intrusive<ASTLiteral>(Field(UInt64(0)));
        auto array = makeASTFunction("array");
        for (const auto & element : argument.GetArray())
        {
            auto constant = tryParseMongoConstant(element);
            if (!constant)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only constants are supported in the array of '$all'");
            array->arguments->children.push_back(std::move(constant));
        }
        return makeASTFunction("hasAll", identifier(), array);
    }

    if (name == "$elemMatch")
    {
        if (!argument.IsObject() || argument.MemberCount() == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The argument of '$elemMatch' must be a non empty document");
        if (!stringView(argument.MemberBegin()->name).starts_with("$"))
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED, "'$elemMatch' is only supported on an array of values, not on an array of documents");

        /// The element of the array is bound to a lambda parameter, and the operators of the
        /// document become the predicate applied to it.
        static constexpr auto element_name = "__mongo_element";
        ASTPtr predicate;
        if (!MongoIdentityFunction(copyValue(argument, metadata->getAllocator()), metadata, element_name).parseImpl(predicate))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot translate the argument of '$elemMatch'");

        auto parameters = makeASTFunction("tuple", make_intrusive<ASTIdentifier>(element_name));
        auto lambda = makeASTFunction("lambda", std::move(parameters), std::move(predicate));
        return makeASTFunction("arrayExists", std::move(lambda), identifier());
    }

    if (name.starts_with("$bits"))
    {
        /// The mask is a number, or the list of the bit positions that make it up.
        UInt64 mask = 0;
        if (argument.IsArray())
        {
            for (const auto & position : argument.GetArray())
            {
                /// A driver may send a whole number as a double, which names the same bit.
                if (!position.IsNumber() || position.GetDouble() < 0 || position.GetDouble() >= 64
                    || position.GetDouble() != std::floor(position.GetDouble()))
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS, "A bit position of '{}' must be a whole number between 0 and 63", name);
                mask |= UInt64(1) << static_cast<UInt64>(position.GetDouble());
            }
        }
        else if (argument.IsUint64())
            mask = argument.GetUint64();
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The argument of '{}' must be a bit mask or an array of bit positions", name);

        auto masked = makeASTFunction("bitAnd", identifier(), make_intrusive<ASTLiteral>(Field(mask)));
        auto mask_literal = make_intrusive<ASTLiteral>(Field(mask));
        auto zero = make_intrusive<ASTLiteral>(Field(UInt64(0)));

        if (name == "$bitsAllSet")
            return makeASTFunction("equals", masked, mask_literal);
        if (name == "$bitsAnySet")
            return makeASTFunction("notEquals", masked, zero);
        if (name == "$bitsAllClear")
            return makeASTFunction("equals", masked, zero);
        if (name == "$bitsAnyClear")
            return makeASTFunction("notEquals", masked, mask_literal);
    }

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The filter operator '{}' is not supported", name);
}

}

bool MongoIdentityFunction::parseImpl(ASTPtr & node)
{
    /// A regular expression matches the field against a pattern.
    if (data.IsObject() && isOnlyRegularExpression(data))
    {
        if (auto pattern = tryParseMongoRegularExpression(data))
        {
            node = makeASTFunction("match", make_intrusive<ASTIdentifier>(edge_name), make_intrusive<ASTLiteral>(Field(*pattern)));
            return true;
        }
    }

    /// A constant compares the field for equality. It covers every scalar the insert path can
    /// create a column from, and the Extended JSON wrappers a Mongo driver sends for the types
    /// JSON cannot represent.
    if (auto constant = tryParseMongoConstant(data))
    {
        node = makeASTFunction("equals", make_intrusive<ASTIdentifier>(edge_name), constant);
        return true;
    }

    if (!data.IsObject())
        return false;

    /// Otherwise the document holds operators, and all of them have to hold at once: Mongo spells
    /// a range as `{"$gte": <from>, "$lte": <to>}`.
    std::vector<ASTPtr> conditions;
    for (auto it = data.MemberBegin(); it != data.MemberEnd(); ++it)
    {
        auto name = stringView(it->name);
        /// The options belong to the `$regex` of the same document.
        if (name == "$options")
            continue;
        conditions.push_back(parseFieldOperator(edge_name, name, it->value, data, metadata));
    }

    if (conditions.empty())
        return false;

    if (conditions.size() == 1)
    {
        node = conditions.front();
        return true;
    }

    auto result = makeASTFunction("and");
    for (auto & condition : conditions)
        result->arguments->children.push_back(std::move(condition));
    node = result;
    return true;
}

bool MongoLiteralFunction::parseImpl(ASTPtr & node)
{
    if (data.IsString())
    {
        auto literal = make_intrusive<ASTIdentifier>(data.GetString());
        node = literal;
        return true;
    }
    if (data.IsObject())
    {
        if (data.MemberCount() != 1)
        {
            return false;
        }

        auto it = data.MemberBegin();

        const char * name = it->name.GetString();
        auto parser = createParser(copyValue(it->value, metadata->getAllocator()), metadata, name);
        ASTPtr child_node;
        if (!parser->parseImpl(child_node))
        {
            return false;
        }
        node = child_node;
        return true;
    }
    return false;
}


bool MongoExprFunction::parseImpl(ASTPtr & node)
{
    node = parseMongoAggregateExpression(data);
    return true;
}

bool IMongoLogicalFunction::parseImpl(ASTPtr & node)
{
    if (!data.IsArray())
    {
        return false;
    }

    if (data.Empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'{}' must be a non empty array of filters", getFunctionName());

    std::vector<ASTPtr> child_trees;
    for (unsigned int i = 0; i < data.Size(); ++i)
    {
        auto parser = createParser(copyValue(data[i], metadata->getAllocator()), metadata, "");
        ASTPtr child_node;
        if (!parser->parseImpl(child_node))
        {
            return false;
        }
        if (!child_node)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "The filter at the position {} of '{}' holds no condition", i, getFunctionName());
        child_trees.push_back(child_node);
    }

    if (child_trees.empty())
    {
        return false;
    }

    if (child_trees.size() == 1)
    {
        node = child_trees[0];
    }
    else
    {
        auto result = makeASTFunction(getFunctionAlias());
        for (const auto & elem : child_trees)
        {
            result->arguments->children.push_back(elem);
        }
        node = result;
    }

    if (isNegated())
        node = makeASTFunction("not", node);
    return true;
}


bool IMongoArithmeticFunction::parseImpl(ASTPtr & node)
{
    if (!data.IsArray() || data.Size() < 2)
    {
        return false;
    }

    std::vector<ASTPtr> children;
    for (unsigned int i = 0; i < data.Size(); ++i)
    {
        auto parser = createParser(copyValue(data[i], metadata->getAllocator()), metadata, "$arithmetic_function_element");
        ASTPtr child_node;
        if (!parser->parseImpl(child_node))
        {
            return false;
        }
        children.push_back(std::move(child_node));
    }

    /// Wrap function as tree of binary operators like
    ///
    ///      +
    ///     / \
    ///    c0  +
    ///       / \
    ///      c1  c2
    ///
    auto function = makeASTFunction(getFunctionAlias(), children[0], children[1]);
    for (size_t i = 2; i < children.size(); ++i)
    {
        function = makeASTFunction(getFunctionAlias(), function, children[i]);
    }
    node = function;

    return true;
}

bool MongoArithmeticFunctionElement::parseImpl(ASTPtr & node)
{
    if (data.IsBool())
    {
        auto literal = make_intrusive<ASTLiteral>(Field(data.GetBool()));
        node = literal;
        return true;
    }
    if (data.IsInt())
    {
        auto literal = make_intrusive<ASTLiteral>(Field(data.GetInt()));
        node = literal;
        return true;
    }
    if (data.IsInt64())
    {
        auto literal = make_intrusive<ASTLiteral>(Field(data.GetInt64()));
        node = literal;
        return true;
    }
    if (data.IsNumber())
    {
        auto literal = make_intrusive<ASTLiteral>(Field(data.GetDouble()));
        node = literal;
        return true;
    }
    if (data.IsString())
    {
        auto identifier = make_intrusive<ASTIdentifier>(data.GetString());
        node = identifier;
        return true;
    }
    if (data.IsObject())
    {
        auto parser = createParser(std::move(data), metadata, "");
        if (!parser->parseImpl(node))
        {
            return false;
        }
        return true;
    }
    return false;
}


}

}
