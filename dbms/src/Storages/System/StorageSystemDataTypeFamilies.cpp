#include <Core/Field.h>
#include <DataTypes/DataTypeFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/System/StorageSystemDataTypeFamilies.h>
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/predicate.hpp>

#include <sstream>

namespace DB
{
namespace
{
    String getPropertiesAsString(const DataTypePtr data_type)
    {
        std::vector<std::string> properties;
        if (data_type->isParametric())
            properties.push_back("parametric");
        if (data_type->haveSubtypes())
            properties.push_back("have_subtypes");
        if (data_type->cannotBeStoredInTables())
            properties.push_back("cannot_be_stored_in_tables");
        if (data_type->isComparable())
            properties.push_back("comparable");
        if (data_type->canBeComparedWithCollation())
            properties.push_back("can_be_compared_with_collation");
        if (data_type->canBeUsedAsVersion())
            properties.push_back("can_be_used_as_version");
        if (data_type->isSummable())
            properties.push_back("summable");
        if (data_type->canBeUsedInBitOperations())
            properties.push_back("can_be_used_in_bit_operations");
        if (data_type->canBeUsedInBooleanContext())
            properties.push_back("can_be_used_in_boolean_context");
        if (data_type->isValueRepresentedByNumber())
            properties.push_back("value_represented_by_number");
        if (data_type->isCategorial())
            properties.push_back("categorial");
        if (data_type->isNullable())
            properties.push_back("nullable");
        if (data_type->onlyNull())
            properties.push_back("only_null");
        if (data_type->canBeInsideNullable())
            properties.push_back("can_be_inside_nullable");
        return boost::algorithm::join(properties, ",");
    }
    ASTPtr createFakeEnumCreationAst()
    {
        String fakename{"e"};
        ASTPtr name = std::make_shared<ASTLiteral>(Field(fakename.c_str(), fakename.size()));
        ASTPtr value = std::make_shared<ASTLiteral>(Field(UInt64(1)));
        ASTPtr ast_func = makeASTFunction("equals", name, value);
        ASTPtr clone = ast_func->clone();
        clone->children.clear();
        clone->children.push_back(ast_func);
        return clone;
    }
}

void StorageSystemDataTypeFamilies::fillData(MutableColumns & res_columns) const
{
    const auto & factory = DataTypeFactory::instance();
    const auto & data_types = factory.getAllDataTypes();
    for (const auto & pair : data_types)
    {
        res_columns[0]->insert(pair.first);

        try
        {
            DataTypePtr type_ptr;
            //special case with enum, because it has arguments but it's properties doesn't
            //depend on arguments
            if (boost::starts_with(pair.first, "Enum"))
            {
                type_ptr = factory.get(pair.first, createFakeEnumCreationAst());
            }
            else
            {
                type_ptr = factory.get(pair.first);
            }

            res_columns[1]->insert(getPropertiesAsString(type_ptr));
        }
        catch (Exception & ex)
        {
            res_columns[1]->insert(String{"depends_on_arguments"});
        }
    }
}
}
