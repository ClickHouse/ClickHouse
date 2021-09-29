#include <Access/ContextAccess.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/InterpreterDropDataTypeQuery.h>
#include <Interpreters/UserDefinedSQLObjectsLoader.h>
#include <DataTypes/DataTypeFactory.h>
#include <Parsers/ASTDropDataTypeQuery.h>

namespace DB
{

BlockIO InterpreterDropDataTypeQuery::execute()
{
    auto current_context = getContext();
    current_context->checkAccess(AccessType::DROP_TYPE);

    FunctionNameNormalizer().visit(query_ptr.get());
    auto & drop_data_type_query = query_ptr->as<ASTDropDataTypeQuery &>();
    DataTypeFactory::instance().unregisterUserDefinedDataType(drop_data_type_query.type_name);
    UserDefinedSQLObjectsLoader::instance().removeObject(current_context, UserDefinedSQLObjectType::DataType, drop_data_type_query.type_name);
    return {};
}

}
