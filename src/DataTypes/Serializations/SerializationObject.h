#pragma once

#include <Columns/ColumnObject.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/Serializations/SerializationObjectSharedData.h>
#include <list>

namespace DB
{

class SerializationObjectDynamicPath;
class SerializationSubObject;

/// Class for binary serialization/deserialization of an Object type (currently only JSON).
class SerializationObject : public ISerialization
{
public:
    /// Serialization can change in future. Let's introduce serialization version.
    struct SerializationVersion
    {
        enum Value
        {
            /// V1 serialization:
            /// - ObjectStructure stream:
            ///     <max_dynamic_paths parameter>
            ///     <actual number of dynamic paths>
            ///     <sorted list of dynamic paths>
            ///     <statistics with number of non-null values for dynamic paths> (only in MergeTree serialization)
            ///     <statistics with number of non-null values for some paths in shared data> (only in MergeTree serialization)
            /// - ObjectData stream:
            ///   - ObjectTypedPath stream for each column in typed paths
            ///   - ObjectDynamicPath stream for each column in dynamic paths
            ///   - ObjectSharedData stream shared data column.
            V1 = 0,
            /// V2 serialization: the same as V1 but without max_dynamic_paths parameter in ObjectStructure stream.
            V2 = 2,
            /// V3 serialization: the same as V2 but with 2 additions:
            ///   - additional information about shared data serialization version that goes after list of dynamic paths.
            ///   - additional bool flag before statistics that indicates if there are any statistics serialized.
            V3 = 4,

            /// Serializations used only in Native format:
            /// String serialization:
            ///  - ObjectData stream with single String column containing serialized JSON.
            STRING = 1,
            /// FLATTENED serialization:
            /// - ObjectStructure stream:
            ///     <list of all paths stored in Object column (except typed paths)>
            /// - ObjectData stream:
            ///   - ObjectTypedPath stream for each column in typed paths
            ///   - ObjectDynamicPath stream for each column in dynamic paths and flattened shared data
            /// This serialization is used in Native format only for easier support for Object type in clients.
            FLATTENED = 3,
        };

        Value value;

        static void checkVersion(UInt64 version);

        explicit SerializationVersion(UInt64 version);
        explicit SerializationVersion(MergeTreeObjectSerializationVersion version);
        explicit SerializationVersion(Value value_) : value(value_) {}
    };

    SerializationObject(
        const std::unordered_map<String, DataTypePtr> & typed_paths_types_,
        const std::unordered_set<String> & paths_to_skip_,
        const std::vector<String> & path_regexps_to_skip_,
        const DataTypePtr & dynamic_type_);

    void enumerateStreams(
        EnumerateStreamsSettings & settings,
        const StreamCallback & callback,
        const SubstreamData & data) const override;

    void serializeBinaryBulkStatePrefix(
        const IColumn & column,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void serializeBinaryBulkStateSuffix(
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void deserializeBinaryBulkStatePrefix(
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsDeserializeStatesCache * cache) const override;

    void serializeBinaryBulkWithMultipleStreams(
        const IColumn & column,
        size_t offset,
        size_t limit,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void deserializeBinaryBulkWithMultipleStreams(
        ColumnPtr & column,
        size_t rows_offset,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const override;

    void serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    void serializeForHashCalculation(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;

    virtual void deserializeObject(IColumn & column, std::string_view object, const FormatSettings & settings) const = 0;

    static void restoreColumnObject(ColumnObject & column_object, size_t prev_size);

private:
    friend SerializationObjectDynamicPath;
    friend SerializationSubObject;

    /// State of an Object structure. Can be also used during deserializing of Object subcolumns.
    struct DeserializeBinaryBulkStateObjectStructure : public ISerialization::DeserializeBinaryBulkState
    {
        SerializationVersion serialization_version;
        std::shared_ptr<std::vector<String>> sorted_dynamic_paths; /// Use shared_ptr to avoid copying during state clone.
        std::unordered_set<std::string_view> dynamic_paths;
        SerializationObjectSharedData::SerializationVersion shared_data_serialization_version;
        size_t shared_data_buckets = 1;
        /// Paths statistics. Map (dynamic path) -> (number of non-null values in this path).
        ColumnObject::StatisticsPtr statistics;

        /// For flattened serialization only.
        std::vector<String> flattened_paths;

        explicit DeserializeBinaryBulkStateObjectStructure(UInt64 serialization_version_)
            : serialization_version(serialization_version_)
            , shared_data_serialization_version(SerializationObjectSharedData::SerializationVersion::MAP)
        {
        }

        DeserializeBinaryBulkStatePtr clone() const override
        {
            return std::make_shared<DeserializeBinaryBulkStateObjectStructure>(*this);
        }
    };

    static DeserializeBinaryBulkStatePtr deserializeObjectStructureStatePrefix(
        DeserializeBinaryBulkSettings & settings,
        SubstreamsDeserializeStatesCache * cache);

    struct TypedPathSubcolumnCreator : public ISubcolumnCreator
    {
        String path;

        explicit TypedPathSubcolumnCreator(const String & path_) : path(path_) {}

        DataTypePtr create(const DataTypePtr & prev) const override { return prev; }
        ColumnPtr create(const ColumnPtr & prev) const override { return prev; }
        SerializationPtr create(const SerializationPtr & prev, const DataTypePtr &) const override;
    };

protected:
    bool shouldSkipPath(const String & path) const;

    std::unordered_map<String, DataTypePtr> typed_paths_types;
    std::unordered_map<std::string_view, SerializationPtr> typed_paths_serializations;
    std::unordered_set<String> paths_to_skip;
    std::vector<String> sorted_paths_to_skip;
    std::list<re2::RE2> path_regexps_to_skip;
    DataTypePtr dynamic_type;
    SerializationPtr dynamic_serialization;

private:
    std::vector<String> sorted_typed_paths;
};

}
