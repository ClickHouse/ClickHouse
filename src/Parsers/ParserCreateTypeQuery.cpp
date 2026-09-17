#include <Parsers/ParserCreateTypeQuery.h>

#include <Core/UUID.h>
#include <IO/ReadHelpers.h>
#include <Parsers/ASTCreateTypeQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserDataType.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>

#include <Poco/String.h>

#include <algorithm>
#include <array>
#include <limits>
#include <optional>
#include <string_view>
#include <unordered_map>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int SYNTAX_ERROR;
extern const int UNSUPPORTED_TYPE_CLAUSE;
}

namespace
{

struct ParameterInfo
{
    UDTParameterKind kind;
    UInt16 ordinal;
};

using ParameterKinds = std::unordered_map<String, ParameterInfo>;

bool parseTypeName(IParser::Pos & pos, Expected & expected, ASTPtr & database, ASTPtr & type_name)
{
    ParserIdentifier identifier_parser;
    ParserToken dot(TokenType::Dot);

    if (!identifier_parser.parse(pos, type_name, expected))
        return false;

    if (dot.ignore(pos, expected))
    {
        database = type_name;
        if (!identifier_parser.parse(pos, type_name, expected))
            return false;
        if (dot.check(pos, expected))
            throw Exception(ErrorCodes::SYNTAX_ERROR, "A type name can contain at most a database and a type component");
    }
    return true;
}

bool parseParameters(IParser::Pos & pos, Expected & expected, ASTPtr & parameters, ParameterKinds & parameter_kinds)
{
    ParserToken opening(TokenType::OpeningRoundBracket);
    if (!opening.ignore(pos, expected))
        return true;

    ParserToken closing(TokenType::ClosingRoundBracket);
    ParserToken comma(TokenType::Comma);
    ParserToken equals(TokenType::Equals);
    ParserIdentifier identifier_parser;

    if (closing.check(pos, expected))
        throw Exception(ErrorCodes::SYNTAX_ERROR, "An empty type parameter list is not allowed");

    auto list = make_intrusive<ASTExpressionList>();
    while (true)
    {
        ASTPtr name_ast;
        ASTPtr kind_ast;
        if (!identifier_parser.parse(pos, name_ast, expected) || !identifier_parser.parse(pos, kind_ast, expected))
            return false;

        const auto name = getIdentifierName(name_ast);
        const auto kind_name = getIdentifierName(kind_ast);
        const auto kind = tryParseUDTParameterKind(kind_name);
        if (!kind)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Unknown type parameter kind '{}'", kind_name);

        if (list->children.size() > std::numeric_limits<UInt16>::max())
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Too many user-defined type parameters");
        const auto ordinal = static_cast<UInt16>(list->children.size());
        if (!parameter_kinds.emplace(name, ParameterInfo{*kind, ordinal}).second)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Duplicate type parameter '{}'", name);
        if (equals.check(pos, expected))
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Default type parameter values are not supported");

        auto declaration = make_intrusive<ASTUDTParameterDeclaration>();
        declaration->name = name;
        declaration->ordinal = ordinal;
        declaration->kind = *kind;
        list->children.push_back(std::move(declaration));

        if (!comma.ignore(pos, expected))
            break;
    }

    if (!closing.ignore(pos, expected))
        return false;

    parameters = std::move(list);
    return true;
}

String parseDefinitionHash(IParser::Pos & pos, Expected & expected)
{
    ASTPtr literal;
    if (!ParserStringLiteral().parse(pos, literal, expected))
        throw Exception(ErrorCodes::SYNTAX_ERROR, "DEFINITION HASH requires a quoted SHA-256 value");

    String value = literal->as<ASTLiteral &>().value.safeGet<String>();
    if (value.size() != 64 || !std::all_of(value.begin(), value.end(), [](char character) { return isHexDigit(character); }))
        throw Exception(ErrorCodes::SYNTAX_ERROR, "DEFINITION HASH must contain exactly 64 hexadecimal digits");
    Poco::toLowerInPlace(value);
    return value;
}

void rejectInactiveClause(IParser::Pos & pos, Expected & expected)
{
    static constexpr std::array<std::string_view, 8> clauses{
        "INPUT", "OUTPUT", "DEFAULT", "CONSTRAINT", "CHECK", "PRIMARY KEY", "FOREIGN KEY", "UNIQUE"};

    for (const auto clause : clauses)
    {
        auto parser = ParserKeyword::createDeprecated(clause);
        if (parser.checkWithoutMoving(pos, expected))
            throw Exception(ErrorCodes::UNSUPPORTED_TYPE_CLAUSE, "The {} clause is not supported for user-defined types", clause);
    }
}

std::optional<UDTExpressionParserContext>
buildDefinitionParserContext(const ASTPtr & parameters, const ASTPtr & database, const ASTPtr & type_name, const ASTPtr & decreases)
{
    UDTExpressionParserContext context;
    if (!tryGetIdentifierNameInto(type_name, context.definition_name) || context.definition_name.empty())
        return std::nullopt;
    if (database && (!tryGetIdentifierNameInto(database, context.definition_database) || context.definition_database.empty()))
        return std::nullopt;

    if (parameters)
    {
        const auto * list = parameters->as<ASTExpressionList>();
        if (!list)
            return std::nullopt;

        context.parameters.reserve(list->children.size());
        for (size_t index = 0; index < list->children.size(); ++index)
        {
            const auto * declaration = list->children[index]->as<ASTUDTParameterDeclaration>();
            if (!declaration || declaration->name.empty() || static_cast<size_t>(declaration->ordinal) != index)
                return std::nullopt;

            context.parameters.push_back(
                UDTTemplateParameterDescriptor{
                    .name = declaration->name,
                    .ordinal = declaration->ordinal,
                    .kind = declaration->kind,
                });
        }
    }

    if (decreases)
    {
        const auto * reference = decreases->as<ASTUDTValueParameterReference>();
        if (!reference || static_cast<size_t>(reference->ordinal) >= context.parameters.size())
            return std::nullopt;

        const auto & declaration = context.parameters[reference->ordinal];
        if (declaration.name != reference->name || declaration.kind != reference->kind
            || !isUDTUnsignedParameterKind(declaration.kind))
            return std::nullopt;

        context.decreasing_parameter = reference->ordinal;
    }

    return context;
}

}

bool ParserCreateTypeQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto create = ParserKeyword::createDeprecated("CREATE");
    auto attach = ParserKeyword::createDeprecated("ATTACH");
    auto type = ParserKeyword::createDeprecated("TYPE");
    auto or_replace = ParserKeyword::createDeprecated("OR REPLACE");

    bool is_attach = false;
    bool has_or_replace = false;
    if (create.ignore(pos, expected))
        has_or_replace = or_replace.ignore(pos, expected);
    else if (attach.ignore(pos, expected))
        is_attach = true;
    else
        return false;

    if (!type.ignore(pos, expected))
        return false;
    if (has_or_replace)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "CREATE OR REPLACE TYPE is not supported");

    const bool if_not_exists = ParserKeyword(Keyword::IF_NOT_EXISTS).ignore(pos, expected);

    ASTPtr database;
    ASTPtr type_name;
    if (!parseTypeName(pos, expected, database, type_name))
        return false;

    ASTPtr parameters;
    ParameterKinds parameter_kinds;
    if (!parseParameters(pos, expected, parameters, parameter_kinds))
        return false;

    std::optional<UUID> uuid;
    std::optional<UInt64> revision;
    ASTPtr decreases;
    String cluster;
    bool saw_on_cluster = false;

    while (true)
    {
        if (ParserKeyword(Keyword::UUID).ignore(pos, expected))
        {
            if (!is_attach)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "UUID is accepted only in ATTACH TYPE records");
            if (uuid)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Duplicate UUID clause in ATTACH TYPE");

            ASTPtr literal;
            if (!ParserStringLiteral().parse(pos, literal, expected))
                return false;
            uuid = parseFromString<UUID>(literal->as<ASTLiteral &>().value.safeGet<String>());
            if (*uuid == UUIDHelpers::Nil)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "ATTACH TYPE UUID cannot be nil");
            continue;
        }

        auto revision_keyword = ParserKeyword::createDeprecated("REVISION");
        if (revision_keyword.ignore(pos, expected))
        {
            if (!is_attach)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "REVISION is accepted only in ATTACH TYPE records");
            if (revision)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Duplicate REVISION clause in ATTACH TYPE");

            ASTPtr literal;
            if (!ParserUnsignedInteger().parse(pos, literal, expected))
                return false;
            revision = literal->as<ASTLiteral &>().value.safeGet<UInt64>();
            if (*revision == 0)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "ATTACH TYPE REVISION must be positive");
            continue;
        }

        if (ParserKeyword(Keyword::ON).ignore(pos, expected))
        {
            if (saw_on_cluster)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Duplicate ON CLUSTER clause in TYPE query");
            if (!ASTQueryWithOnCluster::parse(pos, cluster, expected))
                return false;
            saw_on_cluster = true;
            continue;
        }

        auto decreases_keyword = ParserKeyword::createDeprecated("DECREASES");
        if (decreases_keyword.ignore(pos, expected))
        {
            if (decreases)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Duplicate DECREASES clause in TYPE query");
            if (!ParserIdentifier().parse(pos, decreases, expected))
                return false;

            const auto parameter_name = getIdentifierName(decreases);
            const auto it = parameter_kinds.find(parameter_name);
            if (it == parameter_kinds.end())
                throw Exception(ErrorCodes::SYNTAX_ERROR, "DECREASES refers to undeclared parameter '{}'", parameter_name);
            if (!isUDTUnsignedParameterKind(it->second.kind))
                throw Exception(ErrorCodes::SYNTAX_ERROR, "DECREASES parameter '{}' must have an unsigned integer kind", parameter_name);

            auto reference = make_intrusive<ASTUDTValueParameterReference>();
            reference->name = parameter_name;
            reference->ordinal = it->second.ordinal;
            reference->kind = it->second.kind;
            decreases = std::move(reference);
            continue;
        }

        break;
    }

    rejectInactiveClause(pos, expected);
    if (!ParserKeyword(Keyword::AS).ignore(pos, expected))
        return false;

    auto definition_parser_context = buildDefinitionParserContext(parameters, database, type_name, decreases);
    if (!definition_parser_context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Inconsistent CREATE TYPE parser context");

    ASTPtr definition;
    ParserUDTExpression definition_parser(std::move(*definition_parser_context));
    if (!definition_parser.parse(pos, definition, expected))
        return false;

    std::optional<String> definition_hash;
    auto definition_hash_keyword = ParserKeyword::createDeprecated("DEFINITION HASH");
    if (definition_hash_keyword.ignore(pos, expected))
    {
        if (!is_attach)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "DEFINITION HASH is accepted only in ATTACH TYPE records");
        definition_hash = parseDefinitionHash(pos, expected);
    }

    ASTPtr comment;
    if (ParserKeyword(Keyword::COMMENT).ignore(pos, expected))
    {
        if (!ParserStringLiteral().parse(pos, comment, expected))
            return false;
        if (comment->as<ASTLiteral &>().value.safeGet<String>().find('\0') != String::npos)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "CREATE TYPE COMMENT cannot contain a NUL byte");
    }

    rejectInactiveClause(pos, expected);

    if (is_attach && (!uuid || !revision || !definition_hash))
        throw Exception(ErrorCodes::SYNTAX_ERROR, "ATTACH TYPE requires UUID, REVISION, and DEFINITION HASH internal record fields");

    auto query = make_intrusive<ASTCreateTypeQuery>();
    query->attach = is_attach;
    query->if_not_exists = if_not_exists;
    query->database = std::move(database);
    query->type_name = std::move(type_name);
    query->parameters = std::move(parameters);
    query->decreases = std::move(decreases);
    query->definition = std::move(definition);
    query->comment = std::move(comment);
    query->uuid = uuid;
    query->revision = revision;
    query->definition_hash = std::move(definition_hash);
    query->cluster = std::move(cluster);
    query->normalizeChildrenOrder();

    node = std::move(query);
    return true;
}

}
