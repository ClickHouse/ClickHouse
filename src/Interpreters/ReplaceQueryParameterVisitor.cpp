#include <Common/typeid_cast.h>
#include <Common/quoteString.h>
#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeFactory.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromString.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTQueryParameter.h>
#include <Interpreters/ReplaceQueryParameterVisitor.h>
#include <Interpreters/addTypeConversionToAST.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_QUERY_PARAMETER;
    extern const int BAD_QUERY_PARAMETER;
}


void ReplaceQueryParameterVisitor::visit(ASTPtr & ast)
{
    if (ast->as<ASTQueryParameter>())
        visitQueryParameter(ast);
    else
        visitChildren(ast);
}


void ReplaceQueryParameterVisitor::visitChildren(ASTPtr & ast)
{
    for (auto & child : ast->children)
        visit(child);
}

const String & ReplaceQueryParameterVisitor::getParamValue(const String & name)
{
    auto search = query_parameters.find(name);
    if (search != query_parameters.end())
        return search->second;
    else
        throw Exception("Substitution " + backQuote(name) + " is not set", ErrorCodes::UNKNOWN_QUERY_PARAMETER);
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
    data_type->deserializeAsTextEscaped(temp_column, read_buffer, format_settings);

    if (!read_buffer.eof())
        throw Exception(ErrorCodes::BAD_QUERY_PARAMETER,
            "Value {} cannot be parsed as {} for query parameter '{}'"
            " because it isn't parsed completely: only {} of {} bytes was parsed: {}",
            value, type_name, ast_param.name, read_buffer.count(), value.size(), value.substr(0, read_buffer.count()));

    ast = addTypeConversionToAST(std::make_shared<ASTLiteral>(temp_column[0]), type_name);

    /// Keep the original alias.
    ast->setAlias(alias);
}

}
