#include <DataTypes/DataTypeNested.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Common/quoteString.h>
#include <Parsers/ASTNameTypePair.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int EMPTY_DATA_PASSED;
    extern const int BAD_ARGUMENTS;
}

String DataTypeNestedCustomName::getName() const
{
    WriteBufferFromOwnString s;
    s << "Nested(";
    for (size_t i = 0; i < elems.size(); ++i)
    {
        if (i != 0)
            s << ", ";

        s << backQuoteIfNeed(names[i]) << ' ';
        s << elems[i]->getName();
    }
    s << ")";

    return s.str();
}

static std::pair<DataTypePtr, DataTypeCustomDescPtr> create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.empty())
        throw Exception("Nested cannot be empty", ErrorCodes::EMPTY_DATA_PASSED);

    DataTypes nested_types;
    Strings nested_names;
    nested_types.reserve(arguments->children.size());
    nested_names.reserve(arguments->children.size());

    for (const auto & child : arguments->children)
    {
        const auto * name_type = child->as<ASTNameTypePair>();
        if (!name_type)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Data type Nested accepts only pairs with name and type");

        auto nested_type = DataTypeFactory::instance().get(name_type->type);
        nested_types.push_back(std::move(nested_type));
        nested_names.push_back(name_type->name);
    }

    auto data_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(nested_types, nested_names));
    auto custom_name = std::make_unique<DataTypeNestedCustomName>(nested_types, nested_names);

    return std::make_pair(std::move(data_type), std::make_unique<DataTypeCustomDesc>(std::move(custom_name)));
}

void registerDataTypeNested(DataTypeFactory & factory)
{
    return factory.registerDataTypeCustom("Nested", create);
}

DataTypePtr createNested(const DataTypes & types, const Names & names)
{
    auto custom_desc = std::make_unique<DataTypeCustomDesc>(
        std::make_unique<DataTypeNestedCustomName>(types, names));

    return DataTypeFactory::instance().getCustom(std::move(custom_desc));
}

}
