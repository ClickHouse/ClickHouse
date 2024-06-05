#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeVariant.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnConst.h>
#include <Core/Field.h>
#include <Parsers/IAST.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


DataTypeNullable::DataTypeNullable(const DataTypePtr & nested_data_type_)
    : nested_data_type{nested_data_type_}
{
    if (!nested_data_type->canBeInsideNullable())
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Nested type {} cannot be inside Nullable type", nested_data_type->getName());
}


bool DataTypeNullable::onlyNull() const
{
    return typeid_cast<const DataTypeNothing *>(nested_data_type.get());
}


MutableColumnPtr DataTypeNullable::createColumn() const
{
    return ColumnNullable::create(nested_data_type->createColumn(), ColumnUInt8::create());
}

Field DataTypeNullable::getDefault() const
{
    return Null();
}

size_t DataTypeNullable::getSizeOfValueInMemory() const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Value of type {} in memory is not of fixed size.", getName());
}


bool DataTypeNullable::equals(const IDataType & rhs) const
{
    return rhs.isNullable() && nested_data_type->equals(*static_cast<const DataTypeNullable &>(rhs).nested_data_type);
}

ColumnPtr DataTypeNullable::createColumnConst(size_t size, const Field & field) const
{
    if (onlyNull())
    {
        auto column = createColumn();
        column->insert(field);
        return ColumnConst::create(std::move(column), size);
    }

    auto column = nested_data_type->createColumn();
    bool is_null = field.isNull();

    if (is_null)
        nested_data_type->insertDefaultInto(*column);
    else
        column->insert(field);

    auto null_mask = ColumnUInt8::create();
    null_mask->getData().push_back(is_null ? 1 : 0);

    auto res = ColumnNullable::create(std::move(column), std::move(null_mask));
    return ColumnConst::create(std::move(res), size);
}

SerializationPtr DataTypeNullable::doGetDefaultSerialization() const
{
    return std::make_shared<SerializationNullable>(nested_data_type->getDefaultSerialization());
}

void DataTypeNullable::forEachChild(const ChildCallback & callback) const
{
    callback(*nested_data_type);
    nested_data_type->forEachChild(callback);
}


static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Nullable data type family must have exactly one argument - nested type");

    DataTypePtr nested_type = DataTypeFactory::instance().get(arguments->children[0]);

    return std::make_shared<DataTypeNullable>(nested_type);
}


void registerDataTypeNullable(DataTypeFactory & factory)
{
    factory.registerDataType("Nullable", create);
}


DataTypePtr makeNullable(const DataTypePtr & type)
{
    if (type->isNullable())
        return type;
    return std::make_shared<DataTypeNullable>(type);
}

DataTypePtr makeNullableSafe(const DataTypePtr & type)
{
    if (type->canBeInsideNullable())
        return makeNullable(type);
    return type;
}

DataTypePtr removeNullable(const DataTypePtr & type)
{
    if (type->isNullable())
        return static_cast<const DataTypeNullable &>(*type).getNestedType();
    return type;
}

DataTypePtr makeNullableOrLowCardinalityNullable(const DataTypePtr & type)
{
    if (isNullableOrLowCardinalityNullable(type))
        return type;

    if (type->lowCardinality())
    {
        const auto & dictionary_type = assert_cast<const DataTypeLowCardinality &>(*type).getDictionaryType();
        return std::make_shared<DataTypeLowCardinality>(makeNullable(dictionary_type));
    }

    return std::make_shared<DataTypeNullable>(type);
}

DataTypePtr makeNullableOrLowCardinalityNullableSafe(const DataTypePtr & type)
{
    if (isNullableOrLowCardinalityNullable(type))
        return type;

    if (type->lowCardinality())
    {
        const auto & dictionary_type = assert_cast<const DataTypeLowCardinality &>(*type).getDictionaryType();
        return std::make_shared<DataTypeLowCardinality>(makeNullable(dictionary_type));
    }

    return makeNullableSafe(type);
}

DataTypePtr removeNullableOrLowCardinalityNullable(const DataTypePtr & type)
{
    if (type->isNullable())
        return static_cast<const DataTypeNullable &>(*type).getNestedType();

    if (type->isLowCardinalityNullable())
    {
        auto dict_type = removeNullable(static_cast<const DataTypeLowCardinality &>(*type).getDictionaryType());
        return std::make_shared<DataTypeLowCardinality>(dict_type);
    }

    return type;

}

bool canContainNull(const IDataType & type)
{
    return type.isNullable() || type.isLowCardinalityNullable() || isDynamic(type) || isVariant(type);
}

}
