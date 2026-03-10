#include <DataTypes/DataTypeSortedStringKV.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationSortedStringKV.h>
#include <DataTypes/Serializations/SerializationNamed.h>
#include <Parsers/IAST.h>

namespace DB
{

static DataTypePtr create(const ASTPtr & /* arguments */)
{
    auto key_type = std::make_shared<DataTypeString>();
    auto value_type = std::make_shared<DataTypeString>();

    DataTypePtr type = std::make_shared<DataTypeTuple>(
        DataTypes{key_type, value_type});

    auto key_ser = std::make_shared<SerializationNamed>(key_type->getDefaultSerialization(), "key", SubstreamType::TupleElement);
    auto val_ser = std::make_shared<SerializationNamed>(value_type->getDefaultSerialization(), "value", SubstreamType::TupleElement);
    SerializationSortedStringKV::ElementSerializations elems = {key_ser, val_ser};

    type->setCustomization(std::make_unique<DataTypeCustomDesc>(
        std::make_unique<DataTypeSortedStringKV>(),
        std::make_shared<SerializationSortedStringKV>(elems, true /* has_explicit_names */)));

    return type;
}

void registerDataTypeSortedStringKV(DataTypeFactory & factory)
{
    factory.registerDataType("SortedStringKV", create);
}

}
