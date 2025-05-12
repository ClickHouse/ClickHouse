#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnUnique.h>
#include <Columns/ColumnUniqueCompressed.h>
#include <Columns/ColumnsCommon.h>
#include <Common/CurrentThread.h>
#include <Common/HashTable/HashMap.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <base/TypeLists.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/SerializationLowCardinality.h>
#include <Interpreters/Context.h>
#include <Parsers/IAST.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace Setting
{
    extern const SettingsString low_cardinality_experimental_compression;
    extern const SettingsUInt64 low_cardinality_compression_fc_block_parameter;
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
    template <typename T, typename Creator>
    MutableColumnUniquePtr wrappedCreator(const Creator & creator, MutableColumnPtr && keys)
    {
        return creator(static_cast<T *>(nullptr), std::move(keys));
    }

    template <typename Creator>
    struct CreateColumnVector
    {
        MutableColumnUniquePtr & column;
        const IDataType & keys_type;
        const Creator & creator;
        MutableColumnPtr && keys;

        CreateColumnVector(MutableColumnUniquePtr & column_, const IDataType & keys_type_, const Creator & creator_, MutableColumnPtr && keys_)
                : column(column_), keys_type(keys_type_), creator(creator_), keys(std::move(keys_))
        {
        }

        template <typename T>
        void operator()(TypeList<T>)
        {
            if (typeid_cast<const DataTypeNumber<T> *>(&keys_type))
                column = wrappedCreator<ColumnVector<T>>(creator, std::move(keys));
        }
    };

    enum class ColumnUniqueCompressionType
    {
        NONE,
        FRONT_CODING_BLOCK_DF, 
    };

    struct ColumnUniqueCompressionParameters
    {
        ColumnUniqueCompressionType type = ColumnUniqueCompressionType::NONE;
        size_t fc_block_size = 0; /// For front coding block types of compression
    };

    ColumnUniqueCompressionParameters getColumnUniqueParameters()
    {
        const auto get_column_unique_parameters_from_context = [](const ContextPtr & context)
        {
            const auto compression_type = context->getSettingsRef()[Setting::low_cardinality_experimental_compression];
            const auto front_coding_block_size = context->getSettingsRef()[Setting::low_cardinality_compression_fc_block_parameter];
            ColumnUniqueCompressionType type;
            if (compression_type.value == "fcblockdf")
            {
                type = ColumnUniqueCompressionType::FRONT_CODING_BLOCK_DF;
            }
            else
            {
                type = ColumnUniqueCompressionType::NONE;
            }
            return ColumnUniqueCompressionParameters{.type = type, .fc_block_size = front_coding_block_size};
        };

        const auto query_context = CurrentThread::getQueryContext();
        if (query_context)
        {
            return get_column_unique_parameters_from_context(query_context);
        }

        const auto global_context = Context::getGlobalContextInstance();
        if (global_context)
        {
            return get_column_unique_parameters_from_context(global_context);
        }

        return ColumnUniqueCompressionParameters{.type = ColumnUniqueCompressionType::NONE, .fc_block_size = 0};
    }
}

template <typename Creator>
MutableColumnUniquePtr DataTypeLowCardinality::createColumnUniqueImpl(const IDataType & keys_type,
                                                                      const Creator & creator,
                                                                      MutableColumnPtr && keys)
{
    const auto * type = &keys_type;
    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(&keys_type))
        type = nullable_type->getNestedType().get();

    WhichDataType which(type);

    if (which.isString())
    {
        const auto column_unique_params = getColumnUniqueParameters();
        switch (column_unique_params.type)
        {
            case ColumnUniqueCompressionType::NONE:
            {
                return wrappedCreator<ColumnString>(creator, std::move(keys));
            }
            case ColumnUniqueCompressionType::FRONT_CODING_BLOCK_DF:
            {
                MutableColumnPtr ensured_keys;
                if (keys)
                {
                    ensured_keys = std::move(keys);
                }
                else
                {
                    ensured_keys = ColumnString::create();
                }
                return ColumnUniqueFCBlockDF::create(std::move(ensured_keys), column_unique_params.fc_block_size, keys_type.isNullable());
            }
        }
    }
    if (which.isFixedString())
        return wrappedCreator<ColumnFixedString>(creator, std::move(keys));
    if (which.isDate())
        return wrappedCreator<ColumnVector<UInt16>>(creator, std::move(keys));
    if (which.isDate32())
        return wrappedCreator<ColumnVector<Int32>>(creator, std::move(keys));
    if (which.isDateTime())
        return wrappedCreator<ColumnVector<UInt32>>(creator, std::move(keys));
    if (which.isUUID())
        return wrappedCreator<ColumnVector<UUID>>(creator, std::move(keys));
    if (which.isIPv4())
        return wrappedCreator<ColumnVector<IPv4>>(creator, std::move(keys));
    if (which.isIPv6())
        return wrappedCreator<ColumnVector<IPv6>>(creator, std::move(keys));
    if (which.isInterval())
        return wrappedCreator<DataTypeInterval::ColumnType>(creator, std::move(keys));
    if (which.isInt() || which.isUInt() || which.isFloat())
    {
        MutableColumnUniquePtr column;
        TypeListUtils::forEach(TypeListIntAndFloat{}, CreateColumnVector(column, *type, creator, std::move(keys)));

        if (!column)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected numeric type: {}", type->getName());

        return column;
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected dictionary type for DataTypeLowCardinality: {}",
                    type->getName());
}


MutableColumnUniquePtr DataTypeLowCardinality::createColumnUnique(const IDataType & keys_type)
{
    auto creator = [&](auto x, MutableColumnPtr &&)
    {
        using ColumnType = typename std::remove_pointer_t<decltype(x)>;
        return ColumnUnique<ColumnType>::create(keys_type);
    };
    return createColumnUniqueImpl(keys_type, creator, MutableColumnPtr{});
}

MutableColumnUniquePtr DataTypeLowCardinality::createColumnUnique(const IDataType & keys_type, MutableColumnPtr && keys)
{
    auto creator = [&](auto x, MutableColumnPtr && keys_column)
    {
        using ColumnType = typename std::remove_pointer_t<decltype(x)>;
        return ColumnUnique<ColumnType>::create(std::move(keys_column), keys_type.isNullable());
    };
    return createColumnUniqueImpl(keys_type, creator, std::move(keys));
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
