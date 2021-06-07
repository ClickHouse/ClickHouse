#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/InterpreterDropDataTypeQuery.h>
#include <Interpreters/UserDefinedObjectsOnDisk.h>
#include <DataTypes/DataTypeFactory.h>
#include <Parsers/ASTDropDataTypeQuery.h>

namespace DB
{

BlockIO InterpreterDropDataTypeQuery::execute()
{
    FunctionNameNormalizer().visit(query_ptr.get());
    auto & drop_data_type_query = query_ptr->as<ASTDropDataTypeQuery &>();
    DataTypeFactory::instance().unregisterUserDefinedDataType(drop_data_type_query.type_name);
    UserDefinedObjectsOnDisk::instance().removeUserDefinedDataType(getContext(), drop_data_type_query.type_name);
    return {};
}

}
