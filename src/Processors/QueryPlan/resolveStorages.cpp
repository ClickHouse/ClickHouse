#include <Analyzer/Resolve/IdentifierResolver.h>
#include <Analyzer/TableNode.h>

#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ASTIdentifier.h>

#include <Interpreters/Context.h>
#include <Core/Settings.h>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 max_query_size;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_parser_backtracks;
}

namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
    extern const int CANNOT_PARSE_TEXT;
}

Identifier parseTableIdentifier(const std::string & str, const ContextPtr & context)
{
    const auto & settings = context->getSettingsRef();

    Tokens tokens(str.data(), str.data() + str.size(), settings[Setting::max_query_size]);
    IParser::Pos pos(tokens, static_cast<unsigned>(settings[Setting::max_parser_depth]), static_cast<unsigned>(settings[Setting::max_parser_backtracks]));
    Expected expected;

    ParserCompoundIdentifier parser(false, false);
    ASTPtr res;
    if (!parser.parse(pos, res, expected))
        throw Exception(ErrorCodes::CANNOT_PARSE_TEXT, "Cannot parse itable identifier ({})", str);

    return Identifier(std::move(res->as<ASTIdentifier>()->name_parts));
}

std::shared_ptr<TableNode> resolveTable(const Identifier & identifier, const ContextPtr & context)
{
    auto table_node_ptr = IdentifierResolver::tryResolveTableIdentifierFromDatabaseCatalog(identifier, context);
    if (!table_node_ptr)
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Unknown table {}", identifier.getFullName());

    return table_node_ptr;
}

}
