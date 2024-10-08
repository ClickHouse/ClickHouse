#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnUnique.h>
#include <Columns/ColumnsCommon.h>
#include <Common/HashTable/HashMap.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <Core/Field.h>
#include <base/TypeLists.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/SerializationLowCardinality.h>
#include <Parsers/IAST.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

DataTypeLowCardinality::DataTypeLowCardinality(DataTypePtr dictionary_type_)
        : dictionary_type(std::move(dictionary_type_))
{
    auto inner_type = dictionary_type;
    if (dictionary_type->isNullable())
        inner_type = static_cast<const DataTypeNullable &>(*dictionary_type).getNestedType();

    if (!inner_type->canBeInsideLowCardinality())
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "DataTypeLowCardinality is supported only for numbers, strings, Date or DateTime, but got {}",
                        dictionary_type->getName());
}

namespace
{
    template <typename Creator>
    struct CreateColumnVector
    {
        MutableColumnUniquePtr & column;
        const IDataType & keys_type;
        const Creator & creator;

        CreateColumnVector(MutableColumnUniquePtr & column_, const IDataType & keys_type_, const Creator & creator_)
                : column(column_), keys_type(keys_type_), creator(creator_)
        {
        }

        template <typename T>
        void operator()(TypeList<T>)
        {
            if (typeid_cast<const DataTypeNumber<T> *>(&keys_type))
                column = creator(static_cast<ColumnVector<T> *>(nullptr));
        }
    };
}

template <typename Creator>
MutableColumnUniquePtr DataTypeLowCardinality::createColumnUniqueImpl(const IDataType & keys_type,
                                                                      const Creator & creator)
{
    const auto * type = &keys_type;
    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(&keys_type))
        type = nullable_type->getNestedType().get();

    WhichDataType which(type);

    if (which.isString())
        return creator(static_cast<ColumnString *>(nullptr));
    if (which.isFixedString())
        return creator(static_cast<ColumnFixedString *>(nullptr));
    if (which.isDate())
        return creator(static_cast<ColumnVector<UInt16> *>(nullptr));
    if (which.isDate32())
        return creator(static_cast<ColumnVector<Int32> *>(nullptr));
    if (which.isDateTime())
        return creator(static_cast<ColumnVector<UInt32> *>(nullptr));
    if (which.isUUID())
        return creator(static_cast<ColumnVector<UUID> *>(nullptr));
    if (which.isIPv4())
        return creator(static_cast<ColumnVector<IPv4> *>(nullptr));
    if (which.isIPv6())
        return creator(static_cast<ColumnVector<IPv6> *>(nullptr));
    if (which.isInterval())
        return creator(static_cast<DataTypeInterval::ColumnType *>(nullptr));
    if (which.isInt() || which.isUInt() || which.isFloat())
    {
        MutableColumnUniquePtr column;
        TypeListUtils::forEach(TypeListIntAndFloat{}, CreateColumnVector(column, *type, creator));

        if (!column)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected numeric type: {}", type->getName());

        return column;
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected dictionary type for DataTypeLowCardinality: {}",
                    type->getName());
}


MutableColumnUniquePtr DataTypeLowCardinality::createColumnUnique(const IDataType & keys_type)
{
    auto creator = [&](auto x)
    {
        using ColumnType = typename std::remove_pointer_t<decltype(x)>;
        return ColumnUnique<ColumnType>::create(keys_type);
    };
    return createColumnUniqueImpl(keys_type, creator);
}

MutableColumnUniquePtr DataTypeLowCardinality::createColumnUnique(const IDataType & keys_type, MutableColumnPtr && keys)
{
    auto creator = [&](auto x)
    {
        using ColumnType = typename std::remove_pointer_t<decltype(x)>;
        return ColumnUnique<ColumnType>::create(std::move(keys), keys_type.isNullable());
    };
    return createColumnUniqueImpl(keys_type, creator);
}

MutableColumnPtr DataTypeLowCardinality::createColumn() const
{
    MutableColumnPtr indexes = DataTypeUInt8().createColumn();
    MutableColumnPtr dictionary = createColumnUnique(*dictionary_type);
    return ColumnLowCardinality::create(std::move(dictionary), std::move(indexes));
}

Field DataTypeLowCardinality::getDefault() const
{
    return dictionary_type->getDefault();
}

bool DataTypeLowCardinality::equals(const IDataType & rhs) const
{
    if (typeid(rhs) != typeid(*this))
        return false;

    const auto & low_cardinality_rhs= static_cast<const DataTypeLowCardinality &>(rhs);
    return dictionary_type->equals(*low_cardinality_rhs.dictionary_type);
}

SerializationPtr DataTypeLowCardinality::doGetDefaultSerialization() const
{
    return std::make_shared<SerializationLowCardinality>(dictionary_type);
}

void DataTypeLowCardinality::forEachChild(const ChildCallback & callback) const
{
    callback(*dictionary_type);
    dictionary_type->forEachChild(callback);
}


static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "LowCardinality data type family must have single argument - type of elements");

    return std::make_shared<DataTypeLowCardinality>(DataTypeFactory::instance().get(arguments->children[0]));
}

void registerDataTypeLowCardinality(DataTypeFactory & factory)
{
    factory.registerDataType("LowCardinality", create);
}


DataTypePtr removeLowCardinality(const DataTypePtr & type)
{
    if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(type.get()))
        return low_cardinality_type->getDictionaryType();
    return type;
}

DataTypePtr removeLowCardinalityAndNullable(const DataTypePtr & type)
{
    return removeNullable(removeLowCardinality(type));
};
}
