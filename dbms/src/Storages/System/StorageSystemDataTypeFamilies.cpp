#include <Core/Field.h>
#include <DataTypes/DataTypeFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/System/StorageSystemDataTypeFamilies.h>
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/predicate.hpp>

namespace DB
{
namespace
{
    void setTypePropertries(const DataTypePtr data_type, MutableColumns & res_columns)
    {
        res_columns[4]->insert(UInt64(data_type->isParametric()));
        res_columns[5]->insert(UInt64(data_type->haveSubtypes()));
        res_columns[6]->insert(UInt64(data_type->cannotBeStoredInTables()));
        res_columns[7]->insert(UInt64(data_type->isComparable()));
        res_columns[8]->insert(UInt64(data_type->canBeComparedWithCollation()));
        res_columns[9]->insert(UInt64(data_type->canBeUsedAsVersion()));
        res_columns[10]->insert(UInt64(data_type->isSummable()));
        res_columns[11]->insert(UInt64(data_type->canBeUsedInBitOperations()));
        res_columns[12]->insert(UInt64(data_type->canBeUsedInBooleanContext()));
        res_columns[13]->insert(UInt64(data_type->isCategorial()));
        res_columns[14]->insert(UInt64(data_type->isNullable()));
        res_columns[15]->insert(UInt64(data_type->onlyNull()));
        res_columns[16]->insert(UInt64(data_type->canBeInsideNullable()));
    }

    void setComplexTypeProperties(const String & name, MutableColumns & res_columns)
    {
        res_columns[3]->insert(Null());
        res_columns[4]->insert(UInt64(1)); //complex types are always parametric
        if (name == "AggregateFunction")
            res_columns[5]->insert(UInt64(0));
        else if (name == "Tuple")
            res_columns[5]->insert(Null());
        else
            res_columns[5]->insert(UInt64(1));

        for (size_t i = 6; i < StorageSystemDataTypeFamilies::getNamesAndTypes().size(); ++i)
            res_columns[i]->insert(Null());
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

    ASTPtr createFakeFixedStringAst()
    {
        ASTPtr result = std::make_shared<ASTLiteral>(Field(UInt64(1)));
        auto clone = result->clone();
        clone->children.clear();
        clone->children.push_back(result);
        return clone;
    }
}

void StorageSystemDataTypeFamilies::fillData(MutableColumns & res_columns) const
{
    const auto & factory = DataTypeFactory::instance();
    auto names = factory.getAllDataTypeNames();
    for (const auto & name : names)
    {
        res_columns[0]->insert(name);
        res_columns[1]->insert(UInt64(factory.isCaseInsensitive(name)));

        if (factory.isAlias(name))
            res_columns[2]->insert(factory.aliasTo(name));
        else
            res_columns[2]->insert(String(""));

        try
        {
            DataTypePtr type_ptr;

            Field size = Null();
            // hardcoded cases for simple parametric types
            if (boost::starts_with(name, "Enum"))
            {
                type_ptr = factory.get(name, createFakeEnumCreationAst());
                size = type_ptr->getMaximumSizeOfValueInMemory();
            }
            else if (name == "FixedString" || name == "BINARY")
            {
                type_ptr = factory.get(name, createFakeFixedStringAst());
            }
            else
            {
                type_ptr = factory.get(name);
                if (type_ptr->haveMaximumSizeOfValue())
                    size = type_ptr->getMaximumSizeOfValueInMemory();
            }

            res_columns[3]->insert(size);

            setTypePropertries(type_ptr, res_columns);
        }
        catch (Exception & ex)
        {
            setComplexTypeProperties(name, res_columns);
        }
    }
}
}
