#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnVariant.h>
#include <Common/SipHash.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/Serializations/SerializationObject.h>
#include <DataTypes/Serializations/SerializationObjectDynamicPath.h>
#include <DataTypes/Serializations/SerializationObjectSharedDataPath.h>
#include <DataTypes/Serializations/SerializationObjectHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

SerializationObjectDynamicPath::SerializationObjectDynamicPath(
    const DB::SerializationPtr & nested_,
    const String & path_,
    const String & path_subcolumn_,
    const DataTypePtr & dynamic_type_,
    const SerializationPtr & dynamic_serialization_,
    const DataTypePtr & subcolumn_type_,
    const DataTypePtr & default_path_type_)
    : SerializationWrapper(nested_)
    , path(path_)
    , path_subcolumn(path_subcolumn_)
    , dynamic_serialization(dynamic_serialization_)
    , dynamic_type(dynamic_type_)
    , subcolumn_type(subcolumn_type_)
    , default_path_type(default_path_type_)
    , runtime_path_type(default_path_type ? DataTypeObject::getTypeOfRuntimePaths(default_path_type) : nullptr)
    , runtime_path_serialization(default_path_type ? runtime_path_type->getDefaultSerialization() : nested_)
{
}

struct DeserializeBinaryBulkStateObjectDynamicPath : public ISerialization::DeserializeBinaryBulkState
{
    ISerialization::DeserializeBinaryBulkStatePtr structure_state;
    ISerialization::DeserializeBinaryBulkStatePtr nested_state;
    SerializationPtr shared_data_path_serialization;
    bool read_from_shared_data{};

    ISerialization::DeserializeBinaryBulkStatePtr cloneImpl(ISerialization::DeserializeBinaryBulkStateCloneContext & context) const override
    {
        auto new_state = std::make_shared<DeserializeBinaryBulkStateObjectDynamicPath>(*this);
        new_state->structure_state = structure_state ? structure_state->clone(context) : nullptr;
        new_state->nested_state = nested_state ? nested_state->clone(context) : nullptr;
        return new_state;
    }
};


UInt128 SerializationObjectDynamicPath::getHash(const SerializationPtr & nested_, const String & path_, const String & path_subcolumn_, const DataTypePtr & dynamic_type_, const SerializationPtr & dynamic_serialization_, const DataTypePtr & subcolumn_type_, const DataTypePtr & default_path_type_)
{
    SipHash hash;
    hash.update("ObjectDynamicPath");
    hash.update(nested_->getHash());
    hash.update(path_.size());
    hash.update(path_);
    hash.update(path_subcolumn_.size());
    hash.update(path_subcolumn_);
    hash.update(dynamic_serialization_->getHash());
    auto dynamic_type_name = dynamic_type_->getName();
    hash.update(dynamic_type_name.size());
    hash.update(dynamic_type_name);
    auto subcolumn_type_name = subcolumn_type_->getName();
    hash.update(subcolumn_type_name.size());
    hash.update(subcolumn_type_name);
    auto default_path_type_name = default_path_type_ ? default_path_type_->getName() : "";
    hash.update(default_path_type_name.size());
    hash.update(default_path_type_name);
    return hash.get128();
}

SerializationPtr SerializationObjectDynamicPath::create(const SerializationPtr & nested_, const String & path_, const String & path_subcolumn_, const DataTypePtr & dynamic_type_, const SerializationPtr & dynamic_serialization_, const DataTypePtr & subcolumn_type_, const DataTypePtr & default_path_type_)
{
    if (!nested_->supportsPooling() || !dynamic_serialization_->supportsPooling())
        return std::shared_ptr<ISerialization>(new SerializationObjectDynamicPath(nested_, path_, path_subcolumn_, dynamic_type_, dynamic_serialization_, subcolumn_type_, default_path_type_));
    return ISerialization::pooled(getHash(nested_, path_, path_subcolumn_, dynamic_type_, dynamic_serialization_, subcolumn_type_, default_path_type_), [&] { return new SerializationObjectDynamicPath(nested_, path_, path_subcolumn_, dynamic_type_, dynamic_serialization_, subcolumn_type_, default_path_type_); });
}

void SerializationObjectDynamicPath::enumerateStreams(
    ISerialization::EnumerateStreamsSettings & settings,
    const ISerialization::StreamCallback & callback,
    const ISerialization::SubstreamData & data) const
{
    settings.path.push_back(Substream::ObjectStructure);
    callback(settings.path);
    settings.path.pop_back();

    const auto * deserialize_state = data.deserialize_state ? checkAndGetState<DeserializeBinaryBulkStateObjectDynamicPath>(data.deserialize_state) : nullptr;

    /// We cannot enumerate anything if we don't have deserialization state, as we don't know the dynamic structure.
    if (!deserialize_state)
        return;

    settings.path.push_back(Substream::ObjectData);
    const auto * structure_state = checkAndGetState<SerializationObject::DeserializeBinaryBulkStateObjectStructure>(deserialize_state->structure_state);
    /// Check if we have our path in dynamic paths.
    if (structure_state->dynamic_paths.contains(path))
    {
        settings.path.push_back(Substream::ObjectDynamicPath);
        settings.path.back().object_path_name = path;
        /// With `DEFAULT PATH TYPE` the dynamic path stream has type `Variant(T)`.
        auto path_data = SubstreamData(runtime_path_serialization)
                             .withType(default_path_type ? runtime_path_type : data.type)
                             .withColumn(default_path_type ? nullptr : data.column)
                             .withSerializationInfo(data.serialization_info)
                             .withDeserializeState(deserialize_state->nested_state);
        settings.path.back().data = path_data;
        runtime_path_serialization->enumerateStreams(settings, callback, path_data);
        settings.path.pop_back();
    }
    /// Otherwise we will have to read all shared data and try to find our path there.
    else
    {
        settings.path.push_back(Substream::ObjectSharedData);
        auto shared_data_path_substream_data = data;
        shared_data_path_substream_data.deserialize_state = deserialize_state->nested_state;
        settings.path.back().data = shared_data_path_substream_data;
        deserialize_state->shared_data_path_serialization->enumerateStreams(settings, callback, shared_data_path_substream_data);
        settings.path.pop_back();
    }

    settings.path.pop_back();
}

void SerializationObjectDynamicPath::serializeBinaryBulkStatePrefix(const IColumn &, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStatePrefix is not implemented for SerializationObjectDynamicPath");
}

void SerializationObjectDynamicPath::serializeBinaryBulkStateSuffix(SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStateSuffix is not implemented for SerializationObjectDynamicPath");
}

void SerializationObjectDynamicPath::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsDeserializeStatesCache * cache) const
{
    auto structure_state = SerializationObject::deserializeObjectStructureStatePrefix(settings, cache);
    if (!structure_state)
        return;

    auto dynamic_path_state = std::make_shared<DeserializeBinaryBulkStateObjectDynamicPath>();
    dynamic_path_state->structure_state = std::move(structure_state);
    /// Remember if we need to read from shared data or we have this path in dynamic paths.
    auto * object_structure_state = checkAndGetState<SerializationObject::DeserializeBinaryBulkStateObjectStructure>(dynamic_path_state->structure_state);
    dynamic_path_state->read_from_shared_data = !object_structure_state->dynamic_paths.contains(path);
    settings.path.push_back(Substream::ObjectData);
    if (dynamic_path_state->read_from_shared_data)
    {
        settings.path.push_back(Substream::ObjectSharedData);
        /// The shared data stores each path as a separate column, so its subcolumns are resolved
        /// from that root, while `nested_serialization` was resolved inside the enclosing types.
        /// With `DEFAULT PATH TYPE T` the shared data stores values of type `T`.
        auto shared_data_subcolumn_serialization = path_subcolumn.empty()
            ? nested_serialization
            : default_path_type ? default_path_type->getSubcolumnSerialization(path_subcolumn, default_path_type->getDefaultSerialization())
                                : dynamic_type->getSubcolumnSerialization(path_subcolumn, dynamic_serialization);
        dynamic_path_state->shared_data_path_serialization = SerializationObjectSharedDataPath::create(
            shared_data_subcolumn_serialization,
            object_structure_state->shared_data_serialization_version,
            path,
            path_subcolumn,
            dynamic_type,
            dynamic_serialization,
            subcolumn_type,
            getSharedDataPathBucket(path, object_structure_state->shared_data_buckets),
            default_path_type);
        dynamic_path_state->shared_data_path_serialization->deserializeBinaryBulkStatePrefix(settings, dynamic_path_state->nested_state, cache);
        settings.path.pop_back();
    }
    else
    {
        settings.path.push_back(Substream::ObjectDynamicPath);
        settings.path.back().object_path_name = path;
        runtime_path_serialization->deserializeBinaryBulkStatePrefix(settings, dynamic_path_state->nested_state, cache);
        settings.path.pop_back();
    }

    settings.path.pop_back();
    state = std::move(dynamic_path_state);
}

void SerializationObjectDynamicPath::serializeBinaryBulkWithMultipleStreams(const IColumn &, size_t, size_t, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkWithMultipleStreams is not implemented for SerializationObjectDynamicPath");
}

void SerializationObjectDynamicPath::deserializeBinaryBulkWithMultipleStreams(
    IColumn & result_column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    if (!state)
        return;

    auto * dynamic_path_state = checkAndGetState<DeserializeBinaryBulkStateObjectDynamicPath>(state);
    settings.path.push_back(Substream::ObjectData);
    /// Check if we don't need to read shared data. In this case just read data from dynamic path.
    if (!dynamic_path_state->read_from_shared_data)
    {
        settings.path.push_back(Substream::ObjectDynamicPath);
        settings.path.back().object_path_name = path;
        /// With `DEFAULT PATH TYPE T` the stream has type `Variant(T)`. `SerializationObjectCombinedPath` reads it
        /// as is (NULL means a missing path), otherwise it is densified into `T` (a missing path is `default(T)`).
        if (default_path_type && !(path_subcolumn.empty() && typeid_cast<ColumnVariant *>(&result_column)))
        {
            auto variant_column = runtime_path_type->createColumn();
            runtime_path_serialization->deserializeBinaryBulkWithMultipleStreams(*variant_column, limit, settings, dynamic_path_state->nested_state, cache);
            const auto & variant = assert_cast<const ColumnVariant &>(*variant_column);
            auto dense_column = default_path_type->createColumn();
            dense_column->reserve(variant.size());
            for (size_t i = 0; i != variant.size(); ++i)
            {
                if (variant.isNullAt(i))
                    dense_column->insertDefault();
                else
                    dense_column->insertFrom(variant.getVariantByGlobalDiscriminator(0), variant.offsetAt(i));
            }
            ColumnPtr path_column = std::move(dense_column);
            if (!path_subcolumn.empty())
                path_column = default_path_type->getSubcolumn(path_subcolumn, path_column);
            result_column.insertRangeFrom(*path_column, 0, path_column->size());
        }
        else
        {
            runtime_path_serialization->deserializeBinaryBulkWithMultipleStreams(result_column, limit, settings, dynamic_path_state->nested_state, cache);
        }
        settings.path.pop_back();
    }
    else
    {
        settings.path.push_back(Substream::ObjectSharedData);
        dynamic_path_state->shared_data_path_serialization->deserializeBinaryBulkWithMultipleStreams(result_column, limit, settings, dynamic_path_state->nested_state, cache);
        settings.path.pop_back();
    }

    settings.path.pop_back();
}

size_t SerializationObjectDynamicPath::allocatedBytes() const
{
    return sizeof(*this) + path.capacity() + path_subcolumn.capacity();
}

}
