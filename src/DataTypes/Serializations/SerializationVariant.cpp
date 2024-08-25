#include <DataTypes/Serializations/SerializationVariant.h>
#include <DataTypes/Serializations/SerializationVariantElement.h>
#include <DataTypes/Serializations/SerializationNumber.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/Serializations/SerializationNamed.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Columns/ColumnVariant.h>

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_DATA;
}

void SerializationVariant::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    const auto * type_variant = data.type ? &assert_cast<const DataTypeVariant &>(*data.type) : nullptr;
    const auto * column_variant = data.column ? &assert_cast<const ColumnVariant &>(*data.column) : nullptr;

    auto discriminators_serialization = std::make_shared<SerializationNamed>(std::make_shared<SerializationNumber<ColumnVariant::Discriminator>>(), "discr", SubstreamType::NamedVariantDiscriminators);
    auto local_discriminators = column_variant ? column_variant->getLocalDiscriminatorsPtr() : nullptr;

    settings.path.push_back(Substream::VariantDiscriminators);
    auto discriminators_data = SubstreamData(discriminators_serialization)
                             .withType(type_variant ? std::make_shared<DataTypeNumber<ColumnVariant::Discriminator>>() : nullptr)
                             .withColumn(column_variant ? column_variant->getLocalDiscriminatorsPtr() : nullptr)
                             .withSerializationInfo(data.serialization_info);

    settings.path.back().data = discriminators_data;
    callback(settings.path);
    settings.path.pop_back();

    settings.path.push_back(Substream::VariantElements);
    settings.path.back().data = data;

    for (size_t i = 0; i < variants.size(); ++i)
    {
        settings.path.back().creator = std::make_shared<SerializationVariantElement::VariantSubcolumnCreator>(local_discriminators, variant_names[i], i, column_variant ? column_variant->localDiscriminatorByGlobal(i) : i);

        auto variant_data = SubstreamData(variants[i])
                             .withType(type_variant ? type_variant->getVariant(i) : nullptr)
                             .withColumn(column_variant ? column_variant->getVariantPtrByGlobalDiscriminator(i) : nullptr)
                             .withSerializationInfo(data.serialization_info);

        addVariantElementToPath(settings.path, i);
        settings.path.back().data = variant_data;
        variants[i]->enumerateStreams(settings, callback, variant_data);
        settings.path.pop_back();
    }

    settings.path.pop_back();
}

struct SerializeBinaryBulkStateVariant : public ISerialization::SerializeBinaryBulkState
{
    std::vector<ISerialization::SerializeBinaryBulkStatePtr> states;
};

struct DeserializeBinaryBulkStateVariant : public ISerialization::DeserializeBinaryBulkState
{
    std::vector<ISerialization::DeserializeBinaryBulkStatePtr> states;
};

void SerializationVariant::serializeBinaryBulkStatePrefix(
    const IColumn & column,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    const ColumnVariant & col = assert_cast<const ColumnVariant &>(column);

    auto variant_state = std::make_shared<SerializeBinaryBulkStateVariant>();
    variant_state->states.resize(variants.size());

    settings.path.push_back(Substream::VariantElements);

    for (size_t i = 0; i < variants.size(); ++i)
    {
        addVariantElementToPath(settings.path, i);
        variants[i]->serializeBinaryBulkStatePrefix(col.getVariantByGlobalDiscriminator(i), settings, variant_state->states[i]);
        settings.path.pop_back();
    }

    settings.path.pop_back();
    state = std::move(variant_state);
}


void SerializationVariant::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    auto * variant_state = checkAndGetState<SerializeBinaryBulkStateVariant>(state);

    settings.path.push_back(Substream::VariantElements);
    for (size_t i = 0; i < variants.size(); ++i)
    {
        addVariantElementToPath(settings.path, i);
        variants[i]->serializeBinaryBulkStateSuffix(settings, variant_state->states[i]);
        settings.path.pop_back();
    }
    settings.path.pop_back();
}


void SerializationVariant::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state) const
{
    auto variant_state = std::make_shared<DeserializeBinaryBulkStateVariant>();
    variant_state->states.resize(variants.size());

    settings.path.push_back(Substream::VariantElements);
    for (size_t i = 0; i < variants.size(); ++i)
    {
        addVariantElementToPath(settings.path, i);
        variants[i]->deserializeBinaryBulkStatePrefix(settings, variant_state->states[i]);
        settings.path.pop_back();
    }

    settings.path.pop_back();
    state = std::move(variant_state);
}


void SerializationVariant::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    const ColumnVariant & col = assert_cast<const ColumnVariant &>(column);
    if (const size_t size = col.size(); limit == 0 || offset + limit > size)
        limit = size - offset;

    settings.path.push_back(Substream::VariantDiscriminators);
    auto * discriminators_stream = settings.getter(settings.path);
    settings.path.pop_back();

    if (!discriminators_stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for VariantDiscriminators in SerializationVariant::serializeBinaryBulkWithMultipleStreams");

    auto * variant_state = checkAndGetState<SerializeBinaryBulkStateVariant>(state);

    /// If offset = 0 and limit == col.size() or we have only NULLs, we don't need to calculate
    /// offsets and limits for variants and need to just serialize whole columns.
    if ((offset == 0 && limit == col.size()) || col.hasOnlyNulls())
    {
        /// First, serialize discriminators.
        /// If we have only NULLs or local and global discriminators are the same, just serialize the column as is.
        if (col.hasOnlyNulls() || col.hasGlobalVariantsOrder())
        {
            SerializationNumber<ColumnVariant::Discriminator>().serializeBinaryBulk(col.getLocalDiscriminatorsColumn(), *discriminators_stream, offset, limit);
        }
        /// If local and global discriminators are different, we should convert local to global before serializing (because we don't serialize the mapping).
        else
        {
            const auto & local_discriminators = col.getLocalDiscriminators();
            for (size_t i = offset; i != offset + limit; ++i)
                writeBinaryLittleEndian(col.globalDiscriminatorByLocal(local_discriminators[i]), *discriminators_stream);
        }

        /// Second, serialize variants in global order.
        settings.path.push_back(Substream::VariantElements);
        for (size_t i = 0; i != variants.size(); ++i)
        {
            addVariantElementToPath(settings.path, i);
            variants[i]->serializeBinaryBulkWithMultipleStreams(col.getVariantByGlobalDiscriminator(i), 0, 0, settings, variant_state->states[i]);
            settings.path.pop_back();
        }
        settings.path.pop_back();
        return;
    }

    /// If we have only one non empty variant and no NULLs, we can use the same limit offset for this variant.
    if (auto non_empty_local_discr = col.getLocalDiscriminatorOfOneNoneEmptyVariantNoNulls())
    {
        /// First, serialize discriminators.
        /// We know that all discriminators are the same, so we just need to serialize this discriminator limit times.
        auto non_empty_global_discr = col.globalDiscriminatorByLocal(*non_empty_local_discr);
        for (size_t i = 0; i != limit; ++i)
            writeBinaryLittleEndian(non_empty_global_discr, *discriminators_stream);

        /// Second, serialize non-empty variant (other variants are empty and we can skip their serialization).
        settings.path.push_back(Substream::VariantElements);
        addVariantElementToPath(settings.path, non_empty_global_discr);
        /// We can use the same offset/limit as for whole Variant column
        variants[non_empty_global_discr]->serializeBinaryBulkWithMultipleStreams(col.getVariantByGlobalDiscriminator(non_empty_global_discr), offset, limit, settings, variant_state->states[non_empty_global_discr]);
        settings.path.pop_back();
        settings.path.pop_back();
        return;
    }

    /// In general case we should iterate through local discriminators in range [offset, offset + limit] to serialize global discriminators and calculate offset/limit pair for each variant.
    const auto & local_discriminators = col.getLocalDiscriminators();
    const auto & offsets = col.getOffsets();
    std::vector<std::pair<size_t, size_t>> variant_offsets_and_limits(variants.size(), {0, 0});
    size_t end = offset + limit;
    for (size_t i = offset; i < end; ++i)
    {
        auto global_discr = col.globalDiscriminatorByLocal(local_discriminators[i]);
        writeBinaryLittleEndian(global_discr, *discriminators_stream);

        if (global_discr != ColumnVariant::NULL_DISCRIMINATOR)
        {
            /// If we see this discriminator for the first time, update offset
            if (!variant_offsets_and_limits[global_discr].second)
                variant_offsets_and_limits[global_discr].first = offsets[i];
            /// Update limit for this discriminator.
            ++variant_offsets_and_limits[global_discr].second;
        }
    }

    /// Serialize variants in global order.
    settings.path.push_back(Substream::VariantElements);
    for (size_t i = 0; i != variants.size(); ++i)
    {
        /// Serialize variant only if we have its discriminator in the range.
        if (variant_offsets_and_limits[i].second)
        {
            addVariantElementToPath(settings.path, i);
            variants[i]->serializeBinaryBulkWithMultipleStreams(
                col.getVariantByGlobalDiscriminator(i),
                variant_offsets_and_limits[i].first,
                variant_offsets_and_limits[i].second,
                settings,
                variant_state->states[i]);
            settings.path.pop_back();
        }
    }
    settings.path.pop_back();
}


void SerializationVariant::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    auto mutable_column = column->assumeMutable();
    ColumnVariant & col = assert_cast<ColumnVariant &>(*mutable_column);
    /// We always serialize Variant column with global variants order,
    /// so while deserialization column should be always with global variants order.
    if (!col.hasGlobalVariantsOrder())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to deserialize data into Variant column with not global variants order");

    /// First, deserialize discriminators.
    settings.path.push_back(Substream::VariantDiscriminators);
    if (auto cached_discriminators = getFromSubstreamsCache(cache, settings.path))
    {
        col.getLocalDiscriminatorsPtr() = cached_discriminators;
    }
    else
    {
        auto * discriminators_stream = settings.getter(settings.path);
        if (!discriminators_stream)
            return;

        SerializationNumber<ColumnVariant::Discriminator>().deserializeBinaryBulk(*col.getLocalDiscriminatorsPtr()->assumeMutable(), *discriminators_stream, limit, 0);
        addToSubstreamsCache(cache, settings.path, col.getLocalDiscriminatorsPtr());
    }
    settings.path.pop_back();

    /// Second, calculate limits for each variant by iterating through new discriminators.
    std::vector<size_t> variant_limits(variants.size(), 0);
    auto & discriminators_data = col.getLocalDiscriminators();
    size_t discriminators_offset = discriminators_data.size() - limit;
    for (size_t i = discriminators_offset; i != discriminators_data.size(); ++i)
    {
        ColumnVariant::Discriminator discr = discriminators_data[i];
        if (discr != ColumnVariant::NULL_DISCRIMINATOR)
            ++variant_limits[discr];
    }

    /// Now we can deserialize variants according to their limits.
    auto * variant_state = checkAndGetState<DeserializeBinaryBulkStateVariant>(state);
    settings.path.push_back(Substream::VariantElements);
    for (size_t i = 0; i != variants.size(); ++i)
    {
        addVariantElementToPath(settings.path, i);
        variants[i]->deserializeBinaryBulkWithMultipleStreams(col.getVariantPtrByLocalDiscriminator(i), variant_limits[i], settings, variant_state->states[i], cache);
        settings.path.pop_back();
    }
    settings.path.pop_back();

    /// Fill offsets column.
    /// It's important to do it after deserialization of all variants, because to fill offsets we need
    /// initial variants sizes without values in current range, but some variants can be shared with
    /// other columns via substream cache and they can already contain values from this range even
    /// before we call deserialize for them. So, before deserialize we cannot know for sure if
    /// variant columns already contain values from current range or not. But after calling deserialize
    /// we know for sure that they contain these values, so we can use valiant limits and their
    /// new sizes to calculate correct offsets.
    settings.path.push_back(Substream::VariantOffsets);
    if (auto cached_offsets = getFromSubstreamsCache(cache, settings.path))
    {
        col.getOffsetsPtr() = cached_offsets;
    }
    else
    {
        auto & offsets = col.getOffsets();
        offsets.reserve(offsets.size() + limit);
        std::vector<size_t> variant_offsets;
        variant_offsets.reserve(variants.size());
        for (size_t i = 0; i != variants.size(); ++i)
            variant_offsets.push_back(col.getVariantByLocalDiscriminator(i).size() - variant_limits[i]);

        for (size_t i = discriminators_offset; i != discriminators_data.size(); ++i)
        {
            ColumnVariant::Discriminator discr = discriminators_data[i];
            if (discr == ColumnVariant::NULL_DISCRIMINATOR)
                offsets.emplace_back();
            else
                offsets.push_back(variant_offsets[discr]++);
        }

        addToSubstreamsCache(cache, settings.path, col.getOffsetsPtr());
    }
    settings.path.pop_back();
}

void SerializationVariant::addVariantElementToPath(DB::ISerialization::SubstreamPath & path, size_t i) const
{
    path.push_back(Substream::VariantElement);
    path.back().variant_element_name = variant_names[i];
}

void SerializationVariant::serializeBinary(const Field & /*field*/, WriteBuffer & /*ostr*/, const FormatSettings & /*settings*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinary from a field is not implemented for SerializationVariant");
}

void SerializationVariant::deserializeBinary(Field & /*field*/, ReadBuffer & /*istr*/, const FormatSettings & /*settings*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method deserializeBinary to a field is not implemented for SerializationVariant");
}

void SerializationVariant::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnVariant & col = assert_cast<const ColumnVariant &>(column);
    auto global_discr = col.globalDiscriminatorAt(row_num);
    writeBinaryLittleEndian(global_discr, ostr);
    if (global_discr != ColumnVariant::NULL_DISCRIMINATOR)
        variants[global_discr]->serializeBinary(col.getVariantByGlobalDiscriminator(global_discr), col.offsetAt(row_num), ostr, settings);
}

void SerializationVariant::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ColumnVariant & col = assert_cast<ColumnVariant &>(column);
    ColumnVariant::Discriminator global_discr;
    readBinaryLittleEndian(global_discr, istr);
    if (global_discr == ColumnVariant::NULL_DISCRIMINATOR)
    {
        col.insertDefault();
    }
    else
    {
        auto & variant_column = col.getVariantByGlobalDiscriminator(global_discr);
        variants[global_discr]->deserializeBinary(variant_column, istr, settings);
        col.getLocalDiscriminators().push_back(col.localDiscriminatorByGlobal(global_discr));
        col.getOffsets().push_back(variant_column.size() - 1);
    }
}

namespace
{

const std::unordered_map<TypeIndex, size_t> & getTypesTextDeserializePriorityMap()
{
    static std::unordered_map<TypeIndex, size_t> priority_map = []
    {
        static constexpr std::array priorities = {
            /// Complex types have highest priority.
            TypeIndex::Array,
            TypeIndex::Tuple,
            TypeIndex::Map,
            TypeIndex::AggregateFunction,

            /// Enums can be parsed both from strings and numbers.
            /// So they have high enough priority.
            TypeIndex::Enum8,
            TypeIndex::Enum16,

            /// Types that can be parsed from strings.
            TypeIndex::UUID,
            TypeIndex::IPv4,
            TypeIndex::IPv6,

            /// Types that can be parsed from numbers.
            /// The order:
            ///    1) Integers
            ///    2) Big Integers
            ///    3) Decimals
            ///    4) Floats
            /// In each group small types have higher priority.
            TypeIndex::Int8,
            TypeIndex::UInt8,
            TypeIndex::Int16,
            TypeIndex::UInt16,
            TypeIndex::Int32,
            TypeIndex::UInt32,
            TypeIndex::Int64,
            TypeIndex::UInt64,
            TypeIndex::Int128,
            TypeIndex::UInt128,
            TypeIndex::Int256,
            TypeIndex::UInt256,
            TypeIndex::Decimal32,
            TypeIndex::Decimal64,
            TypeIndex::Decimal128,
            TypeIndex::Decimal256,
            TypeIndex::Float32,
            TypeIndex::Float64,

            /// Dates and DateTimes. More simple Date types have higher priority.
            /// They have lower priority as numbers as some DateTimes sometimes can
            /// be also parsed from numbers, but we don't want it usually.
            TypeIndex::Date,
            TypeIndex::Date32,
            TypeIndex::DateTime,
            TypeIndex::DateTime64,

            /// String types have almost the lowest priority,
            /// as in text formats almost all data can
            /// be deserialized into String type.
            TypeIndex::FixedString,
            TypeIndex::String,
        };

        std::unordered_map<TypeIndex, size_t> pm;

        pm.reserve(priorities.size());
        for (size_t i = 0; i != priorities.size(); ++i)
            pm[priorities[i]] = priorities.size() - i;
        return pm;
    }();

    return priority_map;
}

/// We want to create more or less optimal order of types in which we will try text deserializations.
/// To do it, for each type we calculate a priority and then sort them by this priority.
/// Above we defined priority of each data type, but types can be nested and also we can have LowCardinality and Nullable.
/// To sort any nested types we create a priority that is a tuple of 3 elements:
/// 1) The maximum depth of nested types like Array/Map/Tuple.
/// 2) The combination of simple and complex types priorities.
/// 3) The depth of nested types LowCardinality/Nullable.
/// So, when we will sort types, first we will sort by the maximum depth of nested types, so more nested types are deserialized first,
/// then for types with the same depth we sort by the types priority, and last we sort by the depth of LowCardinality/Nullable types,
/// so if we have types with the same level of nesting and the same priority, we will first try to deserialize LowCardinality/Nullable types
/// (for example if we have types Array(Array(String)) and Array(Array(Nullable(String))).
/// This is just a batch of heuristics.
std::tuple<size_t, size_t, size_t> getTypeTextDeserializePriority(const DataTypePtr & type, size_t nested_depth, size_t simple_nested_depth, const std::unordered_map<TypeIndex, size_t> & priority_map)
{
    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type.get()))
        return getTypeTextDeserializePriority(nullable_type->getNestedType(), nested_depth, simple_nested_depth + 1, priority_map);

    if (const auto * lc_type = typeid_cast<const DataTypeLowCardinality *>(type.get()))
        return getTypeTextDeserializePriority(lc_type->getDictionaryType(), nested_depth, simple_nested_depth + 1, priority_map);

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
    {
        auto [elements_nested_depth, elements_priority, elements_simple_nested_depth] = getTypeTextDeserializePriority(array_type->getNestedType(), nested_depth + 1, simple_nested_depth, priority_map);
        return {elements_nested_depth, elements_priority + priority_map.at(TypeIndex::Array), elements_simple_nested_depth};
    }

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        size_t max_nested_depth = 0;
        size_t sum_priority = 0;
        size_t max_simple_nested_depth = 0;
        for (const auto & elem : tuple_type->getElements())
        {
            auto [elem_nested_depth, elem_priority, elem_simple_nested_depth] = getTypeTextDeserializePriority(elem, nested_depth + 1, simple_nested_depth, priority_map);
            sum_priority += elem_priority;
            if (elem_nested_depth > max_nested_depth)
                max_nested_depth = elem_nested_depth;
            if (elem_simple_nested_depth > max_simple_nested_depth)
                max_simple_nested_depth = elem_simple_nested_depth;
        }

        return {max_nested_depth, sum_priority + priority_map.at(TypeIndex::Tuple), max_simple_nested_depth};
    }

    if (const auto * map_type = typeid_cast<const DataTypeMap *>(type.get()))
    {
        auto [key_max_depth, key_priority, key_simple_nested_depth] = getTypeTextDeserializePriority(map_type->getKeyType(), nested_depth + 1, simple_nested_depth, priority_map);
        auto [value_max_depth, value_priority, value_simple_nested_depth] = getTypeTextDeserializePriority(map_type->getValueType(), nested_depth + 1, simple_nested_depth, priority_map);
        return {std::max(key_max_depth, value_max_depth), key_priority + value_priority + priority_map.at(TypeIndex::Map), std::max(key_simple_nested_depth, value_simple_nested_depth)};
    }

    if (const auto * variant_type = typeid_cast<const DataTypeVariant *>(type.get()))
    {
        size_t max_priority = 0;
        size_t max_depth = 0;
        size_t max_simple_nested_depth = 0;
        for (const auto & variant : variant_type->getVariants())
        {
            auto [variant_max_depth, variant_priority, variant_simple_nested_depth] = getTypeTextDeserializePriority(variant, nested_depth, simple_nested_depth, priority_map);
            if (variant_priority > max_priority)
                max_priority = variant_priority;
            if (variant_max_depth > max_depth)
                max_depth = variant_max_depth;
            if (variant_simple_nested_depth > max_simple_nested_depth)
                max_simple_nested_depth = variant_simple_nested_depth;
        }

        return {max_depth, max_priority, max_simple_nested_depth};
    }

    /// Bool type should have priority higher then all integers.
    if (isBool(type))
        return {nested_depth, priority_map.at(TypeIndex::Int8) + 1, simple_nested_depth};

    auto it = priority_map.find(type->getTypeId());
    return {nested_depth, it == priority_map.end() ? 0 : it->second, simple_nested_depth};
}

}

std::vector<size_t> SerializationVariant::getVariantsDeserializeTextOrder(const DB::DataTypes & variant_types)
{
    std::vector<std::tuple<size_t, size_t, size_t>> priorities;
    priorities.reserve(variant_types.size());
    std::vector<size_t> order;
    order.reserve(variant_types.size());
    const auto & priority_map = getTypesTextDeserializePriorityMap();
    for (size_t i = 0; i != variant_types.size(); ++i)
    {
        priorities.push_back(getTypeTextDeserializePriority(variant_types[i], 0, 0, priority_map));
        order.push_back(i);
    }

    std::sort(order.begin(), order.end(), [&](size_t left, size_t right) { return priorities[left] > priorities[right]; });
    return order;
}


bool SerializationVariant::tryDeserializeImpl(
    IColumn & column,
    const String & field,
    std::function<bool(ReadBuffer &)> check_for_null,
    std::function<bool(IColumn & variant_column, const SerializationPtr & variant_serialization, ReadBuffer &)> try_deserialize_nested) const
{
    auto & column_variant = assert_cast<ColumnVariant &>(column);
    ReadBufferFromString null_buf(field);
    if (check_for_null(null_buf) && null_buf.eof())
    {
        column_variant.insertDefault();
        return true;
    }

    for (size_t global_discr : deserialize_text_order)
    {
        ReadBufferFromString variant_buf(field);
        auto & variant_column = column_variant.getVariantByGlobalDiscriminator(global_discr);
        size_t prev_size = variant_column.size();
        if (try_deserialize_nested(variant_column, variants[global_discr], variant_buf) && variant_buf.eof())
        {
            column_variant.getLocalDiscriminators().push_back(column_variant.localDiscriminatorByGlobal(global_discr));
            column_variant.getOffsets().push_back(prev_size);
            return true;
        }
        else if (variant_column.size() > prev_size)
        {
            variant_column.popBack(1);
        }
    }

    return false;
}

void SerializationVariant::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnVariant & col = assert_cast<const ColumnVariant &>(column);
    auto global_discr = col.globalDiscriminatorAt(row_num);
    if (global_discr == ColumnVariant::NULL_DISCRIMINATOR)
        SerializationNullable::serializeNullEscaped(ostr, settings);
    else
        variants[global_discr]->serializeTextEscaped(col.getVariantByGlobalDiscriminator(global_discr), col.offsetAt(row_num), ostr, settings);
}

bool SerializationVariant::tryDeserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String field;
    readEscapedString(field, istr);
    return tryDeserializeTextEscapedImpl(column, field, settings);
}

void SerializationVariant::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String field;
    readEscapedString(field, istr);
    if (!tryDeserializeTextEscapedImpl(column, field, settings))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse escaped value of type {} here: {}", variant_name, field);
}

bool SerializationVariant::tryDeserializeTextEscapedImpl(DB::IColumn & column, const String & field, const DB::FormatSettings & settings) const
{
    auto check_for_null = [&](ReadBuffer & buf)
    {
        return SerializationNullable::tryDeserializeNullEscaped(buf, settings);
    };
    auto try_deserialize_variant =[&](IColumn & variant_column, const SerializationPtr & variant_serialization, ReadBuffer & buf)
    {
        return variant_serialization->tryDeserializeTextEscaped(variant_column, buf, settings);
    };

    return tryDeserializeImpl(column, field, check_for_null, try_deserialize_variant);
}

void SerializationVariant::serializeTextRaw(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnVariant & col = assert_cast<const ColumnVariant &>(column);
    auto global_discr = col.globalDiscriminatorAt(row_num);
    if (global_discr == ColumnVariant::NULL_DISCRIMINATOR)
        SerializationNullable::serializeNullRaw(ostr, settings);
    else
        variants[global_discr]->serializeTextRaw(col.getVariantByGlobalDiscriminator(global_discr), col.offsetAt(row_num), ostr, settings);
}

bool SerializationVariant::tryDeserializeTextRaw(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String field;
    readString(field, istr);
    return tryDeserializeTextRawImpl(column, field, settings);
}

void SerializationVariant::deserializeTextRaw(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String field;
    readString(field, istr);
    if (!tryDeserializeTextRawImpl(column, field, settings))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse raw value of type {} here: {}", variant_name, field);
}

bool SerializationVariant::tryDeserializeTextRawImpl(DB::IColumn & column, const String & field, const DB::FormatSettings & settings) const
{
    auto check_for_null = [&](ReadBuffer & buf)
    {
        return SerializationNullable::tryDeserializeNullRaw(buf, settings);
    };
    auto try_deserialize_variant =[&](IColumn & variant_column, const SerializationPtr & variant_serialization, ReadBuffer & buf)
    {
        return variant_serialization->tryDeserializeTextRaw(variant_column, buf, settings);
    };

    return tryDeserializeImpl(column, field, check_for_null, try_deserialize_variant);
}

void SerializationVariant::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnVariant & col = assert_cast<const ColumnVariant &>(column);
    auto global_discr = col.globalDiscriminatorAt(row_num);
    if (global_discr == ColumnVariant::NULL_DISCRIMINATOR)
        SerializationNullable::serializeNullQuoted(ostr);
    else
        variants[global_discr]->serializeTextQuoted(col.getVariantByGlobalDiscriminator(global_discr), col.offsetAt(row_num), ostr, settings);
}

bool SerializationVariant::tryDeserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String field;
    if (!tryReadQuotedField(field, istr))
        return false;
    return tryDeserializeTextQuotedImpl(column, field, settings);
}

void SerializationVariant::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String field;
    readQuotedField(field, istr);
    if (!tryDeserializeTextQuotedImpl(column, field, settings))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse quoted value of type {} here: {}", variant_name, field);
}

bool SerializationVariant::tryDeserializeTextQuotedImpl(DB::IColumn & column, const String & field, const DB::FormatSettings & settings) const
{
    auto check_for_null = [&](ReadBuffer & buf)
    {
        return SerializationNullable::tryDeserializeNullQuoted(buf);
    };
    auto try_deserialize_variant =[&](IColumn & variant_column, const SerializationPtr & variant_serialization, ReadBuffer & buf)
    {
        return variant_serialization->tryDeserializeTextQuoted(variant_column, buf, settings);
    };

    return tryDeserializeImpl(column, field, check_for_null, try_deserialize_variant);
}

void SerializationVariant::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnVariant & col = assert_cast<const ColumnVariant &>(column);
    auto global_discr = col.globalDiscriminatorAt(row_num);
    if (global_discr == ColumnVariant::NULL_DISCRIMINATOR)
        SerializationNullable::serializeNullCSV(ostr, settings);
    else
        variants[global_discr]->serializeTextCSV(col.getVariantByGlobalDiscriminator(global_discr), col.offsetAt(row_num), ostr, settings);
}

bool SerializationVariant::tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String field;
    readCSVStringInto<String, true, false>(field, istr, settings.csv);
    return tryDeserializeTextCSVImpl(column, field, settings);
}

void SerializationVariant::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String field;
    readCSVField(field, istr, settings.csv);
    if (!tryDeserializeTextCSVImpl(column, field, settings))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse CSV value of type {} here: {}", variant_name, field);
}

bool SerializationVariant::tryDeserializeTextCSVImpl(DB::IColumn & column, const String & field, const DB::FormatSettings & settings) const
{
    auto check_for_null = [&](ReadBuffer & buf)
    {
        return SerializationNullable::tryDeserializeNullCSV(buf, settings);
    };
    auto try_deserialize_variant =[&](IColumn & variant_column, const SerializationPtr & variant_serialization, ReadBuffer & buf)
    {
        return variant_serialization->tryDeserializeTextCSV(variant_column, buf, settings);
    };

    return tryDeserializeImpl(column, field, check_for_null, try_deserialize_variant);
}

void SerializationVariant::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnVariant & col = assert_cast<const ColumnVariant &>(column);
    auto global_discr = col.globalDiscriminatorAt(row_num);
    if (global_discr == ColumnVariant::NULL_DISCRIMINATOR)
        SerializationNullable::serializeNullText(ostr, settings);
    else
        variants[global_discr]->serializeText(col.getVariantByGlobalDiscriminator(global_discr), col.offsetAt(row_num), ostr, settings);
}

bool SerializationVariant::tryDeserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String field;
    readStringUntilEOF(field, istr);
    return tryDeserializeWholeTextImpl(column, field, settings);
}

void SerializationVariant::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String field;
    readStringUntilEOF(field, istr);
    if (!tryDeserializeWholeTextImpl(column, field, settings))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse text value of type {} here: {}", variant_name, field);
}

bool SerializationVariant::tryDeserializeWholeTextImpl(DB::IColumn & column, const String & field, const DB::FormatSettings & settings) const
{
    auto check_for_null = [&](ReadBuffer & buf)
    {
        return SerializationNullable::tryDeserializeNullText(buf);
    };
    auto try_deserialize_variant =[&](IColumn & variant_column, const SerializationPtr & variant_serialization, ReadBuffer & buf)
    {
        return variant_serialization->tryDeserializeWholeText(variant_column, buf, settings);
    };

    return tryDeserializeImpl(column, field, check_for_null, try_deserialize_variant);
}

void SerializationVariant::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnVariant & col = assert_cast<const ColumnVariant &>(column);
    auto global_discr = col.globalDiscriminatorAt(row_num);
    if (global_discr == ColumnVariant::NULL_DISCRIMINATOR)
        SerializationNullable::serializeNullJSON(ostr);
    else
        variants[global_discr]->serializeTextJSON(col.getVariantByGlobalDiscriminator(global_discr), col.offsetAt(row_num), ostr, settings);
}

bool SerializationVariant::tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String field;
    if (!tryReadJSONField(field, istr, settings.json))
        return false;
    return tryDeserializeTextJSONImpl(column, field, settings);
}

void SerializationVariant::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String field;
    readJSONField(field, istr, settings.json);
    if (!tryDeserializeTextJSONImpl(column, field, settings))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse JSON value of type {} here: {}", variant_name, field);
}

bool SerializationVariant::tryDeserializeTextJSONImpl(DB::IColumn & column, const String & field, const DB::FormatSettings & settings) const
{
    auto check_for_null = [&](ReadBuffer & buf)
    {
        return SerializationNullable::tryDeserializeNullJSON(buf);
    };
    auto try_deserialize_variant =[&](IColumn & variant_column, const SerializationPtr & variant_serialization, ReadBuffer & buf)
    {
        return variant_serialization->tryDeserializeTextJSON(variant_column, buf, settings);
    };

    return tryDeserializeImpl(column, field, check_for_null, try_deserialize_variant);
}

void SerializationVariant::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnVariant & col = assert_cast<const ColumnVariant &>(column);
    auto global_discr = col.globalDiscriminatorAt(row_num);
    if (global_discr == ColumnVariant::NULL_DISCRIMINATOR)
        SerializationNullable::serializeNullXML(ostr);
    else
        variants[global_discr]->serializeTextXML(col.getVariantByGlobalDiscriminator(global_discr), col.offsetAt(row_num), ostr, settings);
}

}
