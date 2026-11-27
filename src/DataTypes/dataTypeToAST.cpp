#include <DataTypes/dataTypeToAST.h>

#include <DataTypes/IDataType.h>
#include <Core/Defines.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ParserDataType.h>
#include <Parsers/parseQuery.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

boost::intrusive_ptr<ASTDataType> dataTypeToAST(const DataTypePtr & data_type)
{
    ParserDataType parser;
    auto ast = parseQuery(parser, data_type->getName(), 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

    /// The dynamic cast is required here because ParserDataType can return
    /// instance of a derived class, for example ASTTupleDataType.
    auto ast_data_type = boost::dynamic_pointer_cast<ASTDataType>(ast);
    if (!ast_data_type)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "dataTypeToAST: unexpected AST node type for '{}'", data_type->getName());

    return ast_data_type;
}

}
