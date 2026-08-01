#include <Parsers/Mongo/ParserMongoFunction.h>
#include <memory>

#include <Core/Field.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST_fwd.h>

#include <Parsers/ASTAssignment.h>
#include <Parsers/Mongo/MongoConstants.h>
#include <Parsers/Mongo/ParserMongoQuery.h>
#include <Parsers/Mongo/Utils.h>
#include <Parsers/ASTExpressionList.h>

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

        auto condition = makeASTFunction("has", array, identifier());
        return name == "$in" ? condition : makeASTFunction("not", condition);
    }

    if (name == "$not")
    {
        ASTPtr negated;
        if (!MongoIdentityFunction(copyValue(argument, metadata->getAllocator()), metadata, field).parseImpl(negated))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot translate the argument of '$not'");
        return makeASTFunction("not", negated);
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


bool IMongoLogicalFunction::parseImpl(ASTPtr & node)
{
    if (!data.IsArray())
    {
        return false;
    }

    std::vector<ASTPtr> child_trees;
    for (unsigned int i = 0; i < data.Size(); ++i)
    {
        auto parser = createParser(copyValue(data[i], metadata->getAllocator()), metadata, "");
        ASTPtr child_node;
        if (!parser->parseImpl(child_node))
        {
            return false;
        }
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

bool MongoSetFunction::parseImpl(ASTPtr & node)
{
    if (!data.IsObject())
        return false;

    auto expression_list = make_intrusive<ASTExpressionList>();
    node = expression_list;

    for (auto it = data.MemberBegin(); it != data.MemberEnd(); ++it)
    {
        auto assignment_ast = make_intrusive<ASTAssignment>();
        assignment_ast->column_name = it->name.GetString();
        ASTPtr assigment_expr;
        auto parser = createParser(copyValue(it->value, metadata->getAllocator()), metadata, "$arithmetic_function_element");
        if (!parser->parseImpl(assigment_expr))
            return false;
        assignment_ast->children.push_back(assigment_expr);
        expression_list->children.push_back(assignment_ast);
    }

    return true;
}

bool MongoIncrementFunction::parseImpl(ASTPtr & node)
{
    if (!data.IsObject())
        return false;

    auto expression_list = make_intrusive<ASTExpressionList>();
    node = expression_list;

    /// `{"$inc": {"age": 1}}` increments the column `age` by the literal `1`: the member name
    /// is the column and the member value is the amount to add.
    for (auto it = data.MemberBegin(); it != data.MemberEnd(); ++it)
    {
        auto assignment_ast = make_intrusive<ASTAssignment>();
        assignment_ast->column_name = it->name.GetString();

        ASTPtr value_expression;
        if (!MongoArithmeticFunctionElement(copyValue(it->value, metadata->getAllocator()), metadata, "").parseImpl(value_expression))
            return false;

        auto column_identifier = make_intrusive<ASTIdentifier>(assignment_ast->column_name);
        auto increment = makeASTFunction("plus", column_identifier, value_expression);

        assignment_ast->children.push_back(increment);
        expression_list->children.push_back(assignment_ast);
    }

    return true;
}

}

}
