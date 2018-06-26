#include <Parsers/ASTAlterQuery.h>
#include <iomanip>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_AST_STRUCTURE;
}

ASTPtr ASTAlterCommand::clone() const
{
    auto res = std::make_shared<ASTAlterCommand>(*this);
    res->children.clear();

    if (col_decl)
    {
        res->col_decl = col_decl->clone();
        res->children.push_back(res->col_decl);
    }
    if (column)
    {
        res->column = column->clone();
        res->children.push_back(res->column);
    }
    if (primary_key)
    {
        res->primary_key = primary_key->clone();
        res->children.push_back(res->primary_key);
    }
    if (partition)
    {
        res->partition = partition->clone();
        res->children.push_back(res->partition);
    }
    if (predicate)
    {
        res->predicate = predicate->clone();
        res->children.push_back(res->predicate);
    }

    return res;
}

void ASTAlterCommand::formatImpl(
    const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');

    if (type == ASTAlterCommand::ADD_COLUMN)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "ADD COLUMN " << (settings.hilite ? hilite_none : "");
        col_decl->formatImpl(settings, state, frame);

        /// AFTER
        if (column)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << " AFTER " << (settings.hilite ? hilite_none : "");
            column->formatImpl(settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::DROP_COLUMN)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str
                      << (clear_column ? "CLEAR " : "DROP ") << "COLUMN " << (settings.hilite ? hilite_none : "");
        column->formatImpl(settings, state, frame);
        if (partition)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str<< " IN PARTITION " << (settings.hilite ? hilite_none : "");
            partition->formatImpl(settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::MODIFY_COLUMN)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "MODIFY COLUMN " << (settings.hilite ? hilite_none : "");
        col_decl->formatImpl(settings, state, frame);
    }
    else if (type == ASTAlterCommand::MODIFY_PRIMARY_KEY)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "MODIFY PRIMARY KEY " << (settings.hilite ? hilite_none : "");
        settings.ostr << "(";
        primary_key->formatImpl(settings, state, frame);
        settings.ostr << ")";
    }
    else if (type == ASTAlterCommand::DROP_PARTITION)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << (detach ? "DETACH" : "DROP") << " PARTITION "
                      << (settings.hilite ? hilite_none : "");
        partition->formatImpl(settings, state, frame);
    }
    else if (type == ASTAlterCommand::ATTACH_PARTITION)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "ATTACH "
                      << (part ? "PART " : "PARTITION ") << (settings.hilite ? hilite_none : "");
        partition->formatImpl(settings, state, frame);
    }
    else if (type == ASTAlterCommand::REPLACE_PARTITION)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << (replace ? "REPLACE" : "ATTACH") << " PARTITION "
                      << (settings.hilite ? hilite_none : "");
        partition->formatImpl(settings, state, frame);
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " FROM " << (settings.hilite ? hilite_none : "");
        if (!from_database.empty())
        {
            settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(from_database)
                          << (settings.hilite ? hilite_none : "") << ".";
        }
        settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(from_table) << (settings.hilite ? hilite_none : "");
    }
    else if (type == ASTAlterCommand::FETCH_PARTITION)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "FETCH "
                      << "PARTITION " << (settings.hilite ? hilite_none : "");
        partition->formatImpl(settings, state, frame);
        settings.ostr << (settings.hilite ? hilite_keyword : "")
                      << " FROM " << (settings.hilite ? hilite_none : "") << std::quoted(from, '\'');
    }
    else if (type == ASTAlterCommand::FREEZE_PARTITION)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "FREEZE PARTITION " << (settings.hilite ? hilite_none : "");
        partition->formatImpl(settings, state, frame);

        if (!with_name.empty())
        {
            settings.ostr << " " << (settings.hilite ? hilite_keyword : "") << "WITH NAME" << (settings.hilite ? hilite_none : "")
                          << " " << std::quoted(with_name, '\'');
        }
    }
    else if (type == ASTAlterCommand::DELETE)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "DELETE WHERE " << (settings.hilite ? hilite_none : "");
        predicate->formatImpl(settings, state, frame);
    }
    else
        throw Exception("Unexpected type of ALTER", ErrorCodes::UNEXPECTED_AST_STRUCTURE);
}


ASTPtr ASTAlterCommandList::clone() const
{
    auto res = std::make_shared<ASTAlterCommandList>();
    for (ASTAlterCommand * command : commands)
        res->add(command->clone());
    return res;
}

void ASTAlterCommandList::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');

    for (size_t i = 0; i < commands.size(); ++i)
    {
        static_cast<IAST *>(commands[i])->formatImpl(settings, state, frame);

        std::string comma = (i < (commands.size() - 1)) ? "," : "";
        settings.ostr << (settings.hilite ? hilite_keyword : "") << comma << (settings.hilite ? hilite_none : "");

        settings.ostr << settings.nl_or_ws;
    }
}


/** Get the text that identifies this element. */
String ASTAlterQuery::getID() const
{
    return "AlterQuery_" + database + "_" + table;
}

ASTPtr ASTAlterQuery::clone() const
{
    auto res = std::make_shared<ASTAlterQuery>(*this);
    res->children.clear();

    if (command_list)
        res->set(res->command_list, command_list->clone());

    return res;
}

ASTPtr ASTAlterQuery::getRewrittenASTWithoutOnCluster(const std::string & new_database) const
{
    auto query_ptr = clone();
    auto & query = static_cast<ASTAlterQuery &>(*query_ptr);

    query.cluster.clear();
    if (query.database.empty())
        query.database = new_database;

    return query_ptr;
}

void ASTAlterQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
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
    formatOnCluster(settings);
    settings.ostr << settings.nl_or_ws;

    FormatStateStacked frame_nested = frame;
    frame_nested.need_parens = false;
    ++frame_nested.indent;
    static_cast<IAST *>(command_list)->formatImpl(settings, state, frame_nested);
}

}
