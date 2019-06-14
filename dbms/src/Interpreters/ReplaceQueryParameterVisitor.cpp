#include <boost/algorithm/string/replace.hpp>
#include <Common/typeid_cast.h>
#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeFactory.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromString.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTQueryParameter.h>
#include <Interpreters/ReplaceQueryParameterVisitor.h>

namespace DB
{

void ReplaceQueryParameterVisitor::visit(ASTPtr & ast)
{
    for (auto & child : ast->children)
    {
        if (child->as<ASTQueryParameter>())
            visitvisitQueryParameter(child);
        else
            visit(child);
    }
}

const String & ReplaceQueryParameterVisitor::getParamValue(const String & name)
{
    auto search = parameters_substitution.find(name);
    if (search != parameters_substitution.end())
        return search->second;
    else
        throw Exception("Expected name '" + name + "' in argument --param_{name}", ErrorCodes::BAD_ARGUMENTS);
}

void ReplaceQueryParameterVisitor::visitQueryParameter(ASTPtr & ast)
{
    const auto & ast_param = ast->as<ASTQueryParameter &>();
    const String & value = getParamValue(ast_param.name);
    const String & type = ast_param.type;

    /// Replacing all occurrences of types Date and DateTime with String.
    /// String comparison is used in "WHERE" conditions with this types.
    
    /// TODO: WTF, totally incorrect

    boost::replace_all(type, "DateTime", "String");
    boost::replace_all(type, "Date", "String");

    const auto data_type = DataTypeFactory::instance().get(type);
    auto temp_column_ptr = data_type->createColumn();
    IColumn & temp_column = *temp_column_ptr;
    ReadBufferFromString read_buffer{value};
    FormatSettings format_settings;
    data_type->deserializeAsWholeText(temp_column, read_buffer, format_settings);

    if (!read_buffer.eof())
        throw Exception("Expected correct value in parameter with name '" + ast_param->name + "'", ErrorCodes::BAD_ARGUMENTS);

    Field field = temp_column[0];
    ast = std::make_shared<ASTLiteral>(std::move(field));
}

}
