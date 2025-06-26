#include <DataTypes/Serializations/SerializationDynamicHelpers.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnsNumber.h>
#include <IO/ReadBufferFromMemory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Iterate over rows in Dynamic column and create an indexes column with indexes of each type in the flattened list.
template <typename IndexesColumn>
ColumnPtr createIndexes(
    const ColumnDynamic & dynamic_column,
    /// Map (discriminator -> index) for types that are stored separately as variants.
    const std::unordered_map<ColumnVariant::Discriminator, size_t> & discriminator_to_index,
    /// Map (type name -> index) for types that are stored together in shared variant.
    const std::unordered_map<String, size_t> & shared_variant_type_to_index,
    /// Index that should be used for Null values.
    size_t null_index)
{
    auto column = IndexesColumn::create();
    auto & data = column->getData();
    data.reserve(dynamic_column.size());
    const auto & variant_column = dynamic_column.getVariantColumn();
    const auto & local_discriminators = variant_column.getLocalDiscriminators();
    const auto & offsets = variant_column.getOffsets();
    auto shared_variant_discr = dynamic_column.getSharedVariantDiscriminator();
    const auto & shared_variant_column = dynamic_column.getSharedVariant();
    for (size_t i = 0; i != local_discriminators.size(); ++i)
    {
        auto global_discr = variant_column.globalDiscriminatorByLocal(local_discriminators[i]);
        if (global_discr == ColumnVariant::NULL_DISCRIMINATOR)
        {
            data.push_back(static_cast<IndexesColumn::ValueType>(null_index));
        }
        else if (global_discr == shared_variant_discr)
        {
            auto value = shared_variant_column.getDataAt(offsets[i]);
            ReadBufferFromMemory buf(value.data, value.size);
            auto type = decodeDataType(buf);
            data.push_back(static_cast<IndexesColumn::ValueType>(shared_variant_type_to_index.at(type->getName())));
        }
        else
        {
            data.push_back(static_cast<IndexesColumn::ValueType>(discriminator_to_index.at(global_discr)));
        }
    }

    return column;
}

}

DataTypePtr getIndexesTypeForFlattenedDynamicColumn(size_t max_index)
{
    if (max_index <= std::numeric_limits<UInt8>::max())
        return std::make_shared<DataTypeUInt8>();
    if (max_index <= std::numeric_limits<UInt16>::max())
        return std::make_shared<DataTypeUInt16>();
    if (max_index <= std::numeric_limits<UInt32>::max())
        return std::make_shared<DataTypeUInt32>();
    return std::make_shared<DataTypeUInt64>();
}


FlattenedDynamicColumn flattenDynamicColumn(const ColumnDynamic & dynamic_column)
{
    const auto & variant_info = dynamic_column.getVariantInfo();
    const auto & variant_types = assert_cast<const DataTypeVariant &>(*variant_info.variant_type).getVariants();
    const auto & variant_column = dynamic_column.getVariantColumn();
    auto shared_variant_discr = dynamic_column.getSharedVariantDiscriminator();
    FlattenedDynamicColumn flattened_dynamic_column;
    /// Mapping from the discriminator of a variant to an index of this type in flattened list.
    std::unordered_map<ColumnVariant::Discriminator, size_t> discriminator_to_index;
    for (size_t i = 0; i != variant_types.size(); ++i)
    {
        /// SharedVariant will be processed later.
        if (i == shared_variant_discr)
            continue;

        discriminator_to_index[i] = flattened_dynamic_column.types.size();
        flattened_dynamic_column.types.push_back(variant_types[i]);
        flattened_dynamic_column.columns.push_back(variant_column.getVariantPtrByGlobalDiscriminator(i));
    }

    /// Mapping from the type name from shared variant to an index of this type in flattened list.
    std::unordered_map<String, size_t> shared_variant_type_to_index;
    /// Collect all types from SharedVariant.
    const auto & shared_variant_column = dynamic_column.getSharedVariant();
    FormatSettings format_settings;
    for (size_t i = 0; i != shared_variant_column.size(); ++i)
    {
        auto value = shared_variant_column.getDataAt(i);
        ReadBufferFromMemory buf(value.data, value.size);
        auto type = decodeDataType(buf);
        auto type_name = type->getName();
        auto it = shared_variant_type_to_index.find(type_name);
        /// If we see this type for the first time, add it to the list and create a column for it.
        if (it == shared_variant_type_to_index.end())
        {
            it = shared_variant_type_to_index.emplace(type_name, flattened_dynamic_column.types.size()).first;
            flattened_dynamic_column.types.push_back(type);
            flattened_dynamic_column.columns.push_back(type->createColumn());
        }

        /// Deserialize value into the corresponding column.
        type->getDefaultSerialization()->deserializeBinary(*flattened_dynamic_column.columns[it->second]->assumeMutable(), buf, format_settings);
    }

    /// Now choose type for indexes column and create it.
    size_t max_index = flattened_dynamic_column.types.size(); /// This index will be used for NULL.
    flattened_dynamic_column.indexes_type = getIndexesTypeForFlattenedDynamicColumn(max_index);
    switch (flattened_dynamic_column.indexes_type->getTypeId())
    {
        case TypeIndex::UInt8:
            flattened_dynamic_column.indexes_column = createIndexes<ColumnUInt8>(dynamic_column, discriminator_to_index, shared_variant_type_to_index, max_index);
            break;
        case TypeIndex::UInt16:
            flattened_dynamic_column.indexes_column = createIndexes<ColumnUInt16>(dynamic_column, discriminator_to_index, shared_variant_type_to_index, max_index);
            break;
        case TypeIndex::UInt32:
            flattened_dynamic_column.indexes_column = createIndexes<ColumnUInt32>(dynamic_column, discriminator_to_index, shared_variant_type_to_index, max_index);
            break;
        case TypeIndex::UInt64:
            flattened_dynamic_column.indexes_column = createIndexes<ColumnUInt64>(dynamic_column, discriminator_to_index, shared_variant_type_to_index, max_index);
            break;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected type as type of indices column type: {}", flattened_dynamic_column.indexes_type->getName());
    }

    return flattened_dynamic_column;
}

namespace
{

/// Fill usual Dynamic column with the data of flattened Dynamic column representation.
template <typename IndexesColumn>
void fillDynamicColumn(
    FlattenedDynamicColumn && flattened_column,
    ColumnDynamic & dynamic_column,
    /// Map (index -> discriminator) for types that will be stored as separate variants.
    const std::unordered_map<size_t, ColumnVariant::Discriminator> & index_to_discriminator,
    /// All types with index starting from this should be inserted into shared variant.
    size_t first_index_for_shared_variant)
{
    const auto & indexes_data = assert_cast<const IndexesColumn &>(*flattened_column.indexes_column).getData();
    dynamic_column.reserve(dynamic_column.size() + indexes_data.size());
    auto & variant_column = dynamic_column.getVariantColumn();
    auto & local_discriminators = variant_column.getLocalDiscriminators();
    auto & offsets = variant_column.getOffsets();
    auto shared_variant_discr = dynamic_column.getSharedVariantDiscriminator();
    auto & shared_variant = dynamic_column.getSharedVariant();
    /// For NULL values we use index equal to the total number of types.
    size_t null_index = flattened_column.types.size();
    std::vector<size_t> flattened_columns_offsets(flattened_column.columns.size(), 0);
    for (size_t i = 0; i != indexes_data.size(); ++i)
    {
        auto index = indexes_data[i];
        if (index == null_index)
        {
            local_discriminators.push_back(ColumnVariant::NULL_DISCRIMINATOR);
            offsets.emplace_back();
        }
        else if (index < first_index_for_shared_variant)
        {
            auto global_discr = index_to_discriminator.at(index);
            local_discriminators.push_back(variant_column.localDiscriminatorByGlobal(global_discr));
            /// If we see this type for the first time, replace the corresponding variant column with the data of this type.
            if (flattened_columns_offsets[index] == 0)
                variant_column.getVariantPtrByGlobalDiscriminator(global_discr) = IColumn::mutate(std::move(flattened_column.columns[index]));
            offsets.push_back(flattened_columns_offsets[index]);
            ++flattened_columns_offsets[index];
        }
        else
        {
            /// Insert value of this type into shared variant.
            local_discriminators.push_back(variant_column.localDiscriminatorByGlobal(shared_variant_discr));
            offsets.push_back(shared_variant.size());
            ColumnDynamic::serializeValueIntoSharedVariant(
                shared_variant,
                *flattened_column.columns[index],
                flattened_column.types[index],
                flattened_column.types[index]->getDefaultSerialization(),
                flattened_columns_offsets[index]);
            ++flattened_columns_offsets[index];
        }
    }
}

}

void unflattenDynamicColumn(FlattenedDynamicColumn && flattened_column, ColumnDynamic & dynamic_column)
{
    /// Iterate over types and try to add them as new variants into Dynamic column until the limit is reached.
    size_t first_index_for_shared_variant = flattened_column.types.size();
    for (size_t i = 0; i != flattened_column.types.size(); ++i)
    {
        auto type_name = flattened_column.types[i]->getName();
        /// If a type cannot be added as a new variant, it means that the limit is reached
        /// and all remaining variants should be inserted into shared variant.
        if (!dynamic_column.addNewVariant(flattened_column.types[i], type_name))
        {
            first_index_for_shared_variant = i;
            break;
        }
    }

    /// Map (index -> discriminator) for types that were successfully added as variants.
    std::unordered_map<size_t, ColumnVariant::Discriminator> index_to_discriminator;
    for (size_t i = 0; i != first_index_for_shared_variant; ++i)
        index_to_discriminator[i] = dynamic_column.getVariantInfo().variant_name_to_discriminator.at(flattened_column.types[i]->getName());


    switch (flattened_column.indexes_type->getTypeId())
    {
        case TypeIndex::UInt8:
            fillDynamicColumn<ColumnUInt8>(std::move(flattened_column), dynamic_column, index_to_discriminator, first_index_for_shared_variant);
            break;
        case TypeIndex::UInt16:
            fillDynamicColumn<ColumnUInt16>(std::move(flattened_column), dynamic_column, index_to_discriminator, first_index_for_shared_variant);
            break;
        case TypeIndex::UInt32:
            fillDynamicColumn<ColumnUInt32>(std::move(flattened_column), dynamic_column, index_to_discriminator, first_index_for_shared_variant);
            break;
        case TypeIndex::UInt64:
            fillDynamicColumn<ColumnUInt64>(std::move(flattened_column), dynamic_column, index_to_discriminator, first_index_for_shared_variant);
            break;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected type of indexes in flattened Dynamic column: {}", flattened_column.indexes_type->getName());
    }
}

namespace
{

template <typename IndexesColumn>
std::vector<size_t> getLimitsImpl(const IColumn & indexes_column, size_t num_types)
{
    const auto & data = assert_cast<const IndexesColumn &>(indexes_column).getData();
    std::vector<size_t> limits(num_types);
    for (size_t i = 0; i != data.size(); ++i)
    {
        if (data[i] < num_types)
            ++limits[data[i]];
    }

    return limits;
}

}

std::vector<size_t> getLimitsForFlattenedDynamicColumn(const IColumn & indexes_column, size_t num_types)
{
    switch (indexes_column.getDataType())
    {
        case TypeIndex::UInt8:
            return getLimitsImpl<ColumnUInt8>(indexes_column, num_types);
        case TypeIndex::UInt16:
            return getLimitsImpl<ColumnUInt16>(indexes_column, num_types);
        case TypeIndex::UInt32:
            return getLimitsImpl<ColumnUInt32>(indexes_column, num_types);
        case TypeIndex::UInt64:
            return getLimitsImpl<ColumnUInt64>(indexes_column, num_types);
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected column type of indexes in flattened Dynamic column: {}", indexes_column.getName());
    }
}

}
