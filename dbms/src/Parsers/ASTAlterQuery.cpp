/* Some modifications Copyright (c) 2018 BlackBerry Limited

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */
#include <Parsers/ASTAlterQuery.h>
#include <iomanip>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_AST_STRUCTURE;
}

ASTAlterQuery::Parameters::Parameters() {}

void ASTAlterQuery::Parameters::clone(Parameters & p) const
{
    p = *this;
    if (col_decl)           p.col_decl = col_decl->clone();
    if (column)             p.column = column->clone();
    if (partition)          p.partition = partition->clone();
    if (weighted_zookeeper_paths) p.weighted_zookeeper_paths = weighted_zookeeper_paths->clone();
    if (sharding_key_expr)  p.sharding_key_expr = sharding_key_expr->clone();
    if (coordinator)        p.coordinator = coordinator->clone();
    if (parameter)          p.parameter = parameter->clone();
    if (values)             p.values = values->clone();
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
    if (params.weighted_zookeeper_paths)
        children.push_back(params.weighted_zookeeper_paths);
    if (params.sharding_key_expr)
        children.push_back(params.sharding_key_expr);
    if (params.coordinator)
        children.push_back(params.coordinator);
    if (params.primary_key)
        children.push_back(params.primary_key);
    if (params.parameter)
        children.push_back(params.parameter);
    if (params.values)
        children.push_back(params.values);
}

ASTAlterQuery::ASTAlterQuery(StringRange range_) : ASTQueryWithOutput(range_)
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
    cloneOutputOptions(*res);
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

    if (is_channel)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "ALTER CHANNEL " << (settings.hilite ? hilite_none : "");
    else
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
            settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str
                << (p.clear_column ? "CLEAR " : "DROP ") << "COLUMN " << (settings.hilite ? hilite_none : "");
            p.column->formatImpl(settings, state, frame);
            if (p.partition)
            {
                settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str<< " IN PARTITION " << (settings.hilite ? hilite_none : "");
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
        else if (p.type == ASTAlterQuery::ADD_TO_PARAMETER)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "ADD TO PARAMETER " << (settings.hilite ? hilite_none : "");
            p.parameter->formatImpl(settings, state, frame);

            settings.ostr << settings.nl_or_ws;

            p.values->formatImpl(settings, state, frame);
        }
        else if (p.type == ASTAlterQuery::DROP_FROM_PARAMETER)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "DROP FROM PARAMETER " << (settings.hilite ? hilite_none : "");
            p.parameter->formatImpl(settings, state, frame);

            settings.ostr << settings.nl_or_ws;

            p.values->formatImpl(settings, state, frame);
        }
        else if (p.type == ASTAlterQuery::MODIFY_PARAMETER)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "MODIFY PARAMETER " << (settings.hilite ? hilite_none : "");
            p.parameter->formatImpl(settings, state, frame);

            settings.ostr << settings.nl_or_ws;

            p.values->formatImpl(settings, state, frame);
        }
        else if (p.type == ASTAlterQuery::CHANNEL_ADD)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "ADD " << (settings.hilite ? hilite_none : "");

            p.values->formatImpl(settings, state, frame);
        }
        else if (p.type == ASTAlterQuery::CHANNEL_DROP)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "DROP " << (settings.hilite ? hilite_none : "");

            p.values->formatImpl(settings, state, frame);
        }
        else if (p.type == ASTAlterQuery::CHANNEL_MODIFY)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "MODIFY " << (settings.hilite ? hilite_none : "");

            p.values->formatImpl(settings, state, frame);
        }
        else if (p.type == ASTAlterQuery::CHANNEL_SUSPEND)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "SUSPEND " << (settings.hilite ? hilite_none : "");

            p.values->formatImpl(settings, state, frame);
        }
        else if (p.type == ASTAlterQuery::CHANNEL_RESUME)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "RESUME " << (settings.hilite ? hilite_none : "");

            p.values->formatImpl(settings, state, frame);
        }
        else if (p.type == ASTAlterQuery::CHANNEL_REFRESH)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "REFRESH " << (settings.hilite ? hilite_none : "");

            p.values->formatImpl(settings, state, frame);
        }
        else
            throw Exception("Unexpected type of ALTER", ErrorCodes::UNEXPECTED_AST_STRUCTURE);

        std::string comma = (i < (parameters.size() -1) ) ? "," : "";
        settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << comma << (settings.hilite ? hilite_none : "");

        settings.ostr << settings.nl_or_ws;
    }
}

}
