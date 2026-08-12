#include <Columns/ColumnObject.h>
#include <Common/SipHash.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/Serializations/SerializationObjectCombinedPath.h>
#include <Interpreters/castColumn.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

SerializationObjectCombinedPath::SerializationObjectCombinedPath(
    const SerializationPtr & literal_serialization_,
    const SerializationPtr & sub_object_serialization_,
    const DataTypePtr & dynamic_type_,
    const DataTypePtr & sub_object_type_)
    : literal_serialization(literal_serialization_)
    , sub_object_serialization(sub_object_serialization_)
    , dynamic_type(dynamic_type_)
    , sub_object_type(sub_object_type_)
{
}

UInt128 SerializationObjectCombinedPath::getHash(
    const SerializationPtr & literal_serialization_,
    const SerializationPtr & sub_object_serialization_,
    const DataTypePtr & dynamic_type_,
    const DataTypePtr & sub_object_type_)
{
    SipHash hash;
    hash.update("ObjectCombinedPath");
    hash.update(literal_serialization_->getHash());
    hash.update(sub_object_serialization_->getHash());
    auto dynamic_type_name = dynamic_type_->getName();
    hash.update(dynamic_type_name.size());
    hash.update(dynamic_type_name);
    auto sub_object_type_name = sub_object_type_->getName();
    hash.update(sub_object_type_name.size());
    hash.update(sub_object_type_name);
    return hash.get128();
}

SerializationPtr SerializationObjectCombinedPath::create(
    const SerializationPtr & literal_serialization_,
    const SerializationPtr & sub_object_serialization_,
    const DataTypePtr & dynamic_type_,
    const DataTypePtr & sub_object_type_)
{
    if (!literal_serialization_->supportsPooling() || !sub_object_serialization_->supportsPooling())
        return std::shared_ptr<ISerialization>(new SerializationObjectCombinedPath(literal_serialization_, sub_object_serialization_, dynamic_type_, sub_object_type_));
    return ISerialization::pooled(
        getHash(literal_serialization_, sub_object_serialization_, dynamic_type_, sub_object_type_),
        [&] { return new SerializationObjectCombinedPath(literal_serialization_, sub_object_serialization_, dynamic_type_, sub_object_type_); });
}

size_t SerializationObjectCombinedPath::allocatedBytes() const
{
    return sizeof(*this);
}

bool SerializationObjectCombinedPath::supportsPooling() const
{
    return literal_serialization->supportsPooling() && sub_object_serialization->supportsPooling();
}

struct DeserializeBinaryBulkStateObjectCombinedPath : public ISerialization::DeserializeBinaryBulkState
{
    ISerialization::DeserializeBinaryBulkStatePtr literal_state;
    ISerialization::DeserializeBinaryBulkStatePtr sub_object_state;

    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        auto new_state = std::make_shared<DeserializeBinaryBulkStateObjectCombinedPath>(*this);
        new_state->literal_state = literal_state ? literal_state->clone() : nullptr;
        new_state->sub_object_state = sub_object_state ? sub_object_state->clone() : nullptr;
        return new_state;
    }
};

void SerializationObjectCombinedPath::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    const auto * deserialize_state = data.deserialize_state
        ? checkAndGetState<DeserializeBinaryBulkStateObjectCombinedPath>(data.deserialize_state)
        : nullptr;

    auto literal_data = SubstreamData(literal_serialization)
                            .withType(data.type)
                            .withColumn(data.column)
                            .withSerializationInfo(data.serialization_info)
                            .withDeserializeState(deserialize_state ? deserialize_state->literal_state : nullptr);
    literal_serialization->enumerateStreams(settings, callback, literal_data);

    auto sub_object_data = SubstreamData(sub_object_serialization)
                               .withType(sub_object_type)
                               .withColumn(nullptr)
                               .withSerializationInfo(data.serialization_info)
                               .withDeserializeState(deserialize_state ? deserialize_state->sub_object_state : nullptr);
    sub_object_serialization->enumerateStreams(settings, callback, sub_object_data);
}

void SerializationObjectCombinedPath::serializeBinaryBulkStatePrefix(const IColumn &, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStatePrefix is not implemented for SerializationObjectCombinedPath");
}

void SerializationObjectCombinedPath::serializeBinaryBulkStateSuffix(SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStateSuffix is not implemented for SerializationObjectCombinedPath");
}

void SerializationObjectCombinedPath::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsDeserializeStatesCache * cache) const
{
    auto combined_state = std::make_shared<DeserializeBinaryBulkStateObjectCombinedPath>();
    literal_serialization->deserializeBinaryBulkStatePrefix(settings, combined_state->literal_state, cache);
    sub_object_serialization->deserializeBinaryBulkStatePrefix(settings, combined_state->sub_object_state, cache);
    state = std::move(combined_state);
}

void SerializationObjectCombinedPath::serializeBinaryBulkWithMultipleStreams(const IColumn &, size_t, size_t, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkWithMultipleStreams is not implemented for SerializationObjectCombinedPath");
}

void SerializationObjectCombinedPath::throwNoSerialization()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Text/binary serialization is not implemented for object combined path subcolumn");
}

void SerializationObjectCombinedPath::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & result_column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    if (!state)
        return;

    auto * combined_state = checkAndGetState<DeserializeBinaryBulkStateObjectCombinedPath>(state);

    /// The literal and sub-object streams may already be cached from reading other subcolumns in the
    /// same block. Cached columns span the whole block, but we only need rows from the current range.
    /// Setting this flag makes the cache insert only current-range rows into our temporary columns.
    auto nested_settings = settings;
    nested_settings.insert_only_rows_in_current_range_from_substreams_cache = true;

    /// Deserialize literal path into a temporary column.
    ColumnPtr literal_column = dynamic_type->createColumn();
    literal_serialization->deserializeBinaryBulkWithMultipleStreams(
        literal_column, rows_offset, limit, nested_settings, combined_state->literal_state, cache);

    /// Deserialize sub-object into a temporary column.
    ColumnPtr sub_object_column = sub_object_type->createColumn();
    sub_object_serialization->deserializeBinaryBulkWithMultipleStreams(
        sub_object_column, rows_offset, limit, nested_settings, combined_state->sub_object_state, cache);

    size_t rows = literal_column->size();
    auto & result = result_column->assumeMutableRef();

    /// If sub-object is all defaults, append literal column directly.
    if (sub_object_column->getNumberOfDefaultRows() == rows)
    {
        result.insertRangeFrom(*literal_column, 0, rows);
        return;
    }

    /// Cast sub-object to Dynamic.
    auto casted_sub_object = castColumn({sub_object_column, sub_object_type, ""}, dynamic_type);

    /// Merge row-by-row into result: prefer literal, then sub-object, then NULL.
    result.reserve(result.size() + rows);
    for (size_t i = 0; i < rows; ++i)
    {
        if (!literal_column->isDefaultAt(i))
            result.insertFrom(*literal_column, i);
        else if (!sub_object_column->isDefaultAt(i))
            result.insertFrom(*casted_sub_object, i);
        else
            result.insertDefault();
    }
}

}
