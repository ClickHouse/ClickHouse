#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/InterpreterCreateDataTypeQuery.h>
#include <Interpreters/UserDefinedObjectsOnDisk.h>
#include <DataTypes/DataTypeFactory.h>
#include <Parsers/ASTCreateDataTypeQuery.h>
#include <Common/quoteString.h>

namespace DB
{

BlockIO InterpreterCreateDataTypeQuery::execute()
{
    FunctionNameNormalizer().visit(query_ptr.get());
    auto & create_data_type_query = query_ptr->as<ASTCreateDataTypeQuery &>();
    registerUserDefinedDataType(DataTypeFactory::instance(), create_data_type_query);
    if (!internal)
    {
        try
        {
            UserDefinedObjectsOnDisk::instance().storeUserDefinedDataType(getContext(), create_data_type_query);
        }
        catch (Exception & e)
        {
            DataTypeFactory::instance().unregisterUserDefinedDataType(create_data_type_query.type_name);
            e.addMessage(fmt::format("while storing user defined type {} on disk", backQuote(create_data_type_query.type_name)));
            throw;
        }
    }
    return {};
}

void InterpreterCreateDataTypeQuery::setInternal(bool internal_)
{
    internal = internal_;
}

}
