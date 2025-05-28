#include <Storages/System/StorageSystemUserDefinedTypes.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/UserDefinedTypeFactory.h>
#include <Interpreters/Context.h>
#include <Columns/IColumn.h>

namespace DB
{

ColumnsDescription StorageSystemUserDefinedTypes::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Name of the user-defined type."},
        {"base_type_ast_string", std::make_shared<DataTypeString>(), "AST representation of the base type."},
        {"type_parameters_ast_string", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "AST representation of type parameters, if any."},
        {"input_expression", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Input expression for type conversion."},
        {"output_expression", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Output expression for type conversion."},
        {"default_expression", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Default value expression."},
        {"create_query_string", std::make_shared<DataTypeString>(), "CREATE TYPE query that was used to create this type."}
    };
}

void StorageSystemUserDefinedTypes::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    auto & udt_factory = UserDefinedTypeFactory::instance();
    auto type_names = udt_factory.getAllTypeNames(context);

    for (const auto & type_name : type_names)
    {
        auto type_info = udt_factory.getTypeInfo(type_name, context);

        res_columns[0]->insert(type_name);
        res_columns[1]->insert(UserDefinedTypeFactory::astToString(type_info.base_type_ast));
        
        if (type_info.type_parameters)
            res_columns[2]->insert(UserDefinedTypeFactory::astToString(type_info.type_parameters));
        else
            res_columns[2]->insertDefault();
            
        if (!type_info.input_expression.empty())
            res_columns[3]->insert(type_info.input_expression);
        else
            res_columns[3]->insertDefault();
            
        if (!type_info.output_expression.empty())
            res_columns[4]->insert(type_info.output_expression);
        else
            res_columns[4]->insertDefault();
            
        if (!type_info.default_expression.empty())
            res_columns[5]->insert(type_info.default_expression);
        else
            res_columns[5]->insertDefault();
            
        res_columns[6]->insert(type_info.create_query_string);
    }
}

}
