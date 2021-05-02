#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/InterpreterCreateDataTypeQuery.h>
#include <DataTypes/DataTypeFactory.h>
#include <Parsers/ASTCreateDataTypeQuery.h>

namespace DB
{

BlockIO InterpreterCreateDataTypeQuery::execute()
{
    FunctionNameNormalizer().visit(query_ptr.get());
    auto & create_data_type_query = query_ptr->as<ASTCreateDataTypeQuery &>();
    registerUserDefinedDataType(DataTypeFactory::instance(), create_data_type_query);
    return {};
}

}
