#include <Parsers/MySQL/ASTAlterQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Common/quoteString.h>

namespace DB
{

namespace MySQLParser
{

ASTPtr ASTAlterQuery::clone() const
{
    auto res = std::make_shared<ASTAlterQuery>(*this);
    res->children.clear();

    if (command_list)
        res->set(res->command_list, command_list->clone());

    return res;
}

void ASTAlterQuery::formatImpl(const IAST::FormatSettings & settings, IAST::FormatState & state, IAST::FormatStateStacked frame) const
{
    frame.need_parens = false;

    std::string indent_str = settings.one_line ? "" : std::string(4u * frame.indent, ' ');

    settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "ALTER TABLE " << (settings.hilite ? hilite_none : "");

    if (!table.empty())
    {
        if (!database.empty())
        {
            settings.ostr << indent_str << backQuoteIfNeed(database);
            settings.ostr << ".";
        }
        settings.ostr << indent_str << backQuoteIfNeed(table);
    }

    settings.ostr << settings.nl_or_ws;
    FormatStateStacked frame_nested = frame;
    frame_nested.need_parens = false;
    ++frame_nested.indent;
    static_cast<IAST *>(command_list)->formatImpl(settings, state, frame_nested);
}

bool ParserAlterQuery::parseImpl(IParser::Pos & pos, ASTPtr & /*node*/, Expected & expected)
{
    ParserKeyword k_add("ADD");
    ParserKeyword k_alter_table("ALTER TABLE");

    ASTPtr table;

    if (!k_alter_table.ignore(pos, expected))
        return false;

    if (!ParserCompoundIdentifier(false).parse(pos, table, expected))
        return false;

    if (k_add.ignore(pos, expected))
    {
        ASTPtr declare_index;
        ParserDeclareIndex p_index;

        /// TODO: add column
        if (!p_index.parse(pos, declare_index, expected))
            return false;
    }
    return false;

}
}

}
