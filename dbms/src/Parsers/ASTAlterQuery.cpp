#include <Parsers/ASTAlterQuery.h>
#include <iomanip>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_AST_STRUCTURE;
}

ASTAlterQuery::Parameters::Parameters() : type(NO_TYPE) {}

void ASTAlterQuery::Parameters::clone(Parameters & p) const
{
    p = *this;
    if (col_decl)           p.col_decl = col_decl->clone();
    if (column)             p.column = column->clone();
    if (partition)          p.partition = partition->clone();
    if (last_partition)     p.last_partition = last_partition->clone();
    if (weighted_zookeeper_paths) p.weighted_zookeeper_paths = weighted_zookeeper_paths->clone();
    if (sharding_key_expr)  p.sharding_key_expr = sharding_key_expr->clone();
    if (coordinator)        p.coordinator = coordinator->clone();
}

void ASTAlterQuery::addParameters(const Parameters & params)
{
    parameters.push_back(params);
    if (params.col_decl)
        children.push_back(params.col_decl);
    if (params.column)
        children.push_back(params.column);
    if (params.partition)
        children.push_back(params.partition);
    if (params.last_partition)
        children.push_back(params.last_partition);
    if (params.weighted_zookeeper_paths)
        children.push_back(params.weighted_zookeeper_paths);
    if (params.sharding_key_expr)
        children.push_back(params.sharding_key_expr);
    if (params.coordinator)
        children.push_back(params.coordinator);
    if (params.primary_key)
        children.push_back(params.primary_key);
}

ASTAlterQuery::ASTAlterQuery(StringRange range_) : IAST(range_)
{
}

/** Get the text that identifies this element. */
String ASTAlterQuery::getID() const
{
    return ("AlterQuery_" + database + "_" + table);
}

ASTPtr ASTAlterQuery::clone() const
{
    auto res = std::make_shared<ASTAlterQuery>(*this);
    for (ParameterContainer::size_type i = 0; i < parameters.size(); ++i)
        parameters[i].clone(res->parameters[i]);
    return res;
}

ASTPtr ASTAlterQuery::getRewrittenASTWithoutOnCluster(const std::string & new_database) const
{
    auto query_ptr = clone();
    ASTAlterQuery & query = static_cast<ASTAlterQuery &>(*query_ptr);

    query.cluster.clear();
    if (query.database.empty())
        query.database = new_database;

    return query_ptr;
}

void ASTAlterQuery::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;

    std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');

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

    for (size_t i = 0; i < parameters.size(); ++i)
    {
        const ASTAlterQuery::Parameters & p = parameters[i];

        if (p.type == ASTAlterQuery::ADD_COLUMN)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "ADD COLUMN " << (settings.hilite ? hilite_none : "");
            p.col_decl->formatImpl(settings, state, frame);

            /// AFTER
            if (p.column)
            {
                settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << " AFTER " << (settings.hilite ? hilite_none : "");
                p.column->formatImpl(settings, state, frame);
            }
        }
        else if (p.type == ASTAlterQuery::DROP_COLUMN)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "DROP COLUMN " << (settings.hilite ? hilite_none : "");
            p.column->formatImpl(settings, state, frame);
            if (p.partition)
            {
                settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << " FROM PARTITION " << (settings.hilite ? hilite_none : "");
                p.partition->formatImpl(settings, state, frame);
            }
        }
        else if (p.type == ASTAlterQuery::MODIFY_COLUMN)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "MODIFY COLUMN " << (settings.hilite ? hilite_none : "");
            p.col_decl->formatImpl(settings, state, frame);
        }
        else if (p.type == ASTAlterQuery::MODIFY_PRIMARY_KEY)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "MODIFY PRIMARY KEY " << (settings.hilite ? hilite_none : "");
            settings.ostr << "(";
            p.primary_key->formatImpl(settings, state, frame);
            settings.ostr << ")";
        }
        else if (p.type == ASTAlterQuery::DROP_PARTITION)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << (p.detach ? "DETACH" : "DROP") << " PARTITION "
            << (settings.hilite ? hilite_none : "");
            p.partition->formatImpl(settings, state, frame);
        }
        else if (p.type == ASTAlterQuery::ATTACH_PARTITION)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "ATTACH "
                << (p.part ? "PART " : "PARTITION ") << (settings.hilite ? hilite_none : "");
            p.partition->formatImpl(settings, state, frame);
        }
        else if (p.type == ASTAlterQuery::FETCH_PARTITION)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "FETCH "
                << "PARTITION " << (settings.hilite ? hilite_none : "");
            p.partition->formatImpl(settings, state, frame);
            settings.ostr << (settings.hilite ? hilite_keyword : "")
                << " FROM " << (settings.hilite ? hilite_none : "") << std::quoted(p.from, '\'');
        }
        else if (p.type == ASTAlterQuery::FREEZE_PARTITION)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "FREEZE PARTITION " << (settings.hilite ? hilite_none : "");
            p.partition->formatImpl(settings, state, frame);

            if (!p.with_name.empty())
            {
                settings.ostr << " " << (settings.hilite ? hilite_keyword : "") << "WITH NAME" << (settings.hilite ? hilite_none : "")
                    << " " << std::quoted(p.with_name, '\'');
            }
        }
        else if (p.type == ASTAlterQuery::RESHARD_PARTITION)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "RESHARD ";

            if (p.do_copy)
                settings.ostr << "COPY ";

            if (p.partition)
                settings.ostr << "PARTITION ";

            settings.ostr << (settings.hilite ? hilite_none : "");

            if (p.partition)
                p.partition->formatImpl(settings, state, frame);

            if (p.partition && p.last_partition)
                settings.ostr << "..";

            if (p.last_partition)
                p.last_partition->formatImpl(settings, state, frame);

            std::string ws = p.partition ? " " : "";
            settings.ostr << (settings.hilite ? hilite_keyword : "") << ws
                << "TO " << (settings.hilite ? hilite_none : "");

            FormatStateStacked frame_with_indent = frame;
            ++frame_with_indent.indent;
            p.weighted_zookeeper_paths->formatImpl(settings, state, frame_with_indent);

            settings.ostr << settings.nl_or_ws;

            settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str
                << "USING " << (settings.hilite ? hilite_none : "");

            p.sharding_key_expr->formatImpl(settings, state, frame);

            if (p.coordinator)
            {
                settings.ostr << settings.nl_or_ws;
                settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str
                    << "COORDINATE WITH " << (settings.hilite ? hilite_none : "");

                p.coordinator->formatImpl(settings, state, frame);
            }
        }
        else
            throw Exception("Unexpected type of ALTER", ErrorCodes::UNEXPECTED_AST_STRUCTURE);

        std::string comma = (i < (parameters.size() -1) ) ? "," : "";
        settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << comma << (settings.hilite ? hilite_none : "");

        settings.ostr << settings.nl_or_ws;
    }
}

}
