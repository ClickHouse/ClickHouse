#include <Access/ContextAccess.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/InterpreterCreateDataTypeQuery.h>
#include <Interpreters/UserDefinedSQLObjectsLoader.h>
#include <DataTypes/DataTypeFactory.h>
#include <Parsers/ASTCreateDataTypeQuery.h>
#include <Common/quoteString.h>

namespace DB
{

BlockIO InterpreterCreateDataTypeQuery::execute()
{
    auto current_context = getContext();
    current_context->checkAccess(AccessType::CREATE_TYPE);

    FunctionNameNormalizer().visit(query_ptr.get());
    auto & create_data_type_query = query_ptr->as<ASTCreateDataTypeQuery &>();
    registerUserDefinedDataType(DataTypeFactory::instance(), create_data_type_query, current_context);
    auto data_type_name = create_data_type_query.type_name;
    if (!is_internal)
    {
        try
        {
            UserDefinedSQLObjectsLoader::instance().storeObject(current_context, UserDefinedSQLObjectType::DataType, data_type_name, *query_ptr);
        }
        catch (Exception & e)
        {
            DataTypeFactory::instance().unregisterUserDefinedDataType(data_type_name);
            e.addMessage(fmt::format("while storing user defined type {} on disk", backQuote(data_type_name)));
            throw;
        }
    }
    return {};
}

void InterpreterCreateDataTypeQuery::setInternal(bool is_internal_)
{
    is_internal = is_internal_;
}

}
