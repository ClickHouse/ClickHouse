#include <stdlib.h>

#include <Common/typeid_cast.h>
#include <Common/StringUtils/StringUtils.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeFactory.h>
#include <Columns/IColumn.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromString.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTQueryParameter.h>
#include <Interpreters/ReplaceQueryParameterVisitor.h>

namespace DB
{

namespace ErrorCodes {
    extern const int UNKNOWN_IDENTIFIER;
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

void ReplaceQueryParameterVisitor::visit(ASTPtr & ast)
{
    for (auto & child : ast->children)
    {
        if (child->as<ASTQueryParameter>())
            visitQP(child);
        else
            visit(child);
    }
}

String ReplaceQueryParameterVisitor::getParamValue(const String & name)
{
    auto search = params_substitution.find(name);
    if (search != params_substitution.end())
        return search->second;
    else
        throw Exception("Expected same names in parameter field --param_{name}={value} and in query {name:type}", ErrorCodes::BAD_ARGUMENTS);
}

void ReplaceQueryParameterVisitor::visitQP(ASTPtr & ast)
{
    auto ast_param = ast->as<ASTQueryParameter>();
    String value = getParamValue(ast_param->name);
    const auto data_type = DataTypeFactory::instance().get(ast_param->type);

    auto temp_column_ptr = data_type->createColumn();
    IColumn &temp_column = *temp_column_ptr;
    ReadBufferFromString read_buffer{value};
    FormatSettings format_settings;
    data_type->deserializeAsWholeText(temp_column, read_buffer, format_settings);

    Field field = temp_column[0];
    ast = std::make_shared<ASTLiteral>(field);
}

}
