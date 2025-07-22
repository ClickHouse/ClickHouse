#include <Columns/IColumn.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeString.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/ReplaceQueryParameterVisitor.h>
#include <Interpreters/addTypeConversionToAST.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTQueryParameter.h>
#include <Parsers/ASTViewTargets.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/TablePropertiesQueriesASTs.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>
#include <Common/checkStackSize.h>
#include <Parsers/Access/ASTCreateUserQuery.h>
#include <Parsers/Access/ASTUserNameWithHost.h>
#include <Parsers/parseQuery.h>


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_query_size;
}

namespace ErrorCodes
{
    extern const int UNKNOWN_QUERY_PARAMETER;
    extern const int BAD_QUERY_PARAMETER;
}

/// It is important to keep in mind that in the case of ASTIdentifier, we are changing the shared object itself,
/// and all shared_ptr's that pointed to the original object will now point to the new replaced value.
/// However, with ASTQueryParameter, we are only assigning a new value to the passed shared_ptr, while
/// all other shared_ptr's still point to the old ASTQueryParameter.

void ReplaceQueryParameterVisitor::visit(ASTPtr & ast)
{
    checkStackSize();
    resolveParameterizedAlias(ast);

    if (ast->as<ASTQueryParameter>())
        visitQueryParameter(ast);
    else if (ast->as<ASTIdentifier>() || ast->as<ASTTableIdentifier>())
        visitIdentifier(ast);
    else if (ast->as<ASTSetQuery>())
        visitSetQuery(ast);
    else
    {
        if (auto * describe_query = dynamic_cast<ASTDescribeQuery *>(ast.get()); describe_query && describe_query->table_expression)
            visitChildren(describe_query->table_expression);
        else if (auto * create_user_query = dynamic_cast<ASTCreateUserQuery *>(ast.get()))
        {
            ASTPtr names = create_user_query->names;
            visitChildren(names);
        }
        else if (auto * create_query = dynamic_cast<ASTCreateQuery *>(ast.get());
                 create_query && create_query->targets && create_query->targets->hasTableASTWithQueryParams(ViewTarget::To))
        {
            auto to_table_ast = create_query->targets->getTableASTWithQueryParams(ViewTarget::To);

            visit(to_table_ast);

            create_query->targets->setTableID(ViewTarget::To, to_table_ast->as<ASTTableIdentifier>()->getTableId());
            create_query->targets->resetTableASTWithQueryParams(ViewTarget::To);

            visitChildren(ast);
        }
        else
            visitChildren(ast);
    }
}


void ReplaceQueryParameterVisitor::visitChildren(ASTPtr & ast)
{
    for (auto & child : ast->children)
    {
        void * old_ptr = child.get();
        visit(child);
        void * new_ptr = child.get();

        /// Some AST classes have naked pointers to children elements as members.
        /// We have to replace them if the child was replaced.
        if (new_ptr != old_ptr)
            ast->updatePointerToChild(old_ptr, new_ptr);
    }
}

const String & ReplaceQueryParameterVisitor::getParamValue(const String & name)
{
    auto search = query_parameters.find(name);
    if (search == query_parameters.end())
        throw Exception(ErrorCodes::UNKNOWN_QUERY_PARAMETER, "Substitution {} is not set", backQuote(name));

    ++num_replaced_parameters;
    return search->second;
}

void ReplaceQueryParameterVisitor::visitQueryParameter(ASTPtr & ast)
{
    const auto & ast_param = ast->as<ASTQueryParameter &>();
    const String & value = getParamValue(ast_param.name);
    const String & type_name = ast_param.type;
    String alias = ast_param.alias;

    const auto data_type = DataTypeFactory::instance().get(type_name);
    auto temp_column_ptr = data_type->createColumn();
    IColumn & temp_column = *temp_column_ptr;
    ReadBufferFromString read_buffer{value};
    FormatSettings format_settings;

    const SerializationPtr & serialization = data_type->getDefaultSerialization();
    try
    {
        if (ast_param.name == "_request_body")
            serialization->deserializeWholeText(temp_column, read_buffer, format_settings);
        else
            serialization->deserializeTextEscaped(temp_column, read_buffer, format_settings);
    }
    catch (Exception & e)
    {
        e.addMessage("value {} cannot be parsed as {} for query parameter '{}'", value, type_name, ast_param.name);
        throw;
    }

    if (!read_buffer.eof())
        throw Exception(ErrorCodes::BAD_QUERY_PARAMETER,
            "Value {} cannot be parsed as {} for query parameter '{}'"
            " because it isn't parsed completely: only {} of {} bytes was parsed: {}",
            value, type_name, ast_param.name, read_buffer.count(), value.size(), value.substr(0, read_buffer.count()));

    Field literal;
    /// If data type has custom serialization, we should use CAST from String,
    /// because CAST from field may not work correctly (for example for type IPv6).
    if (data_type->getCustomSerialization())
        literal = value;
    else
        literal = temp_column[0];

    /// If it's a String, substitute it in the form of a string literal without CAST
    /// to enable substitutions in simple queries that don't support expressions
    /// (such as CREATE USER).
    if (typeid_cast<const DataTypeString *>(data_type.get()))
        ast = std::make_shared<ASTLiteral>(literal);
    else
        ast = addTypeConversionToAST(std::make_shared<ASTLiteral>(literal), type_name);

    /// Keep the original alias.
    ast->setAlias(alias);
}

void ReplaceQueryParameterVisitor::visitIdentifier(ASTPtr & ast)
{
    auto ast_identifier = dynamic_pointer_cast<ASTIdentifier>(ast);
    if (ast_identifier->children.empty())
        return;

    bool replaced_parameter = false;
    auto & name_parts = ast_identifier->name_parts;
    for (size_t i = 0, j = 0, size = name_parts.size(); i < size; ++i)
    {
        if (name_parts[i].empty())
        {
            const auto & ast_param = ast_identifier->children[j++]->as<ASTQueryParameter &>();
            name_parts[i] = getParamValue(ast_param.name);
            replaced_parameter = true;
        }
    }

    /// Do not touch AST if there are no parameters
    if (!replaced_parameter)
        return;

    /// FIXME: what should this mean?
    if (!ast_identifier->semantic->special && name_parts.size() >= 2)
        ast_identifier->semantic->table = ast_identifier->name_parts.end()[-2];

    ast_identifier->resetFullName();
    ast_identifier->children.clear();
}

void ReplaceQueryParameterVisitor::resolveParameterizedAlias(ASTPtr & ast)
{
    auto ast_with_alias = std::dynamic_pointer_cast<ASTWithAlias>(ast);
    if (!ast_with_alias)
        return;

    if (ast_with_alias->parametrised_alias)
        setAlias(ast, getParamValue((*ast_with_alias->parametrised_alias)->name));
}

void ReplaceQueryParameterVisitor::visitSetQuery(ASTPtr & ast)
{
    auto & set_query = ast->as<ASTSetQuery &>();
    auto * additional_table_filters = set_query.changes.tryGet("additional_table_filters");

    if (set_query.is_standalone || !additional_table_filters)
        return;

    Map additional_filter_resolved;
    ASTPtr additional_filter_ast;

    for (const auto & additional_filter : additional_table_filters->safeGet<Map>())
    {
        const auto & tuple = additional_filter.safeGet<Tuple>();
        const auto & table = tuple.at(0).safeGet<String>();
        const auto & filter = tuple.at(1).safeGet<String>();

        if (!filter.contains('{'))
        {
            additional_filter_resolved.emplace_back(tuple);
            continue;
        }

        ParserExpression parser;
        additional_filter_ast = parseQuery(
            parser,
            filter.data(),
            filter.data() + filter.size(),
            "additional filter",
            DBMS_DEFAULT_MAX_QUERY_SIZE,
            DBMS_DEFAULT_MAX_PARSER_DEPTH,
            DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

        visit(additional_filter_ast);

        auto filter_with_applied_params = additional_filter_ast->formatWithSecretsOneLine();
        additional_filter_resolved.emplace_back(Tuple{table, filter_with_applied_params});
    }

    set_query.changes.setSetting("additional_table_filters", additional_filter_resolved);
}
}
