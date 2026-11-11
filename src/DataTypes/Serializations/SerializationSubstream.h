#pragma once

#include <Columns/IColumn_fwd.h>
#include <base/types.h>

#include <functional>
#include <memory>
#include <optional>
#include <set>
#include <vector>

/// The parts of ISerialization that IDataType.h needs. ISerialization.h includes this header and keeps the
/// original nested names (ISerialization::Substream, ISerialization::Kind, ...) as member aliases.

namespace DB
{

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;
class ISerialization;
using SerializationPtr = std::shared_ptr<const ISerialization>;
class SerializationInfo;
using SerializationInfoPtr = std::shared_ptr<const SerializationInfo>;
using SerializationInfoMutablePtr = std::shared_ptr<SerializationInfo>;

/// ISerialization::DeserializeBinaryBulkState and ISerialization::ISubcolumnCreator, defined in ISerialization.h.
struct SerializationDeserializeBinaryBulkState;
using SerializationDeserializeBinaryBulkStatePtr = std::shared_ptr<SerializationDeserializeBinaryBulkState>;
struct ISerializationSubcolumnCreator;

enum class SerializationKind : UInt8
{
    DEFAULT = 0,
    SPARSE = 1,
    DETACHED = 2,
    REPLICATED = 3,
    /// Automatic LowCardinality serialization: a column whose data type is NOT
    /// LowCardinality is stored on disk in dictionary-encoded form. Such a column
    /// flows through the pipeline as a (non-native) ColumnLowCardinality and is
    /// materialized to a full column at boundaries that require it.
    LOW_CARDINALITY = 4,
};

/// We can have multiple serialization kinds created over each other.
/// For example:
///  - Detached over Sparse over Default
///  - Detached over Replicated over Default
///  - etc
using SerializationKindStack = std::vector<SerializationKind>;

struct SerializationSubstreamData
{
    SerializationSubstreamData() = default;
    explicit SerializationSubstreamData(SerializationPtr serialization_)
        : serialization(std::move(serialization_))
    {
    }

    SerializationSubstreamData & withType(DataTypePtr type_)
    {
        type = std::move(type_);
        return *this;
    }

    SerializationSubstreamData & withColumn(ColumnPtr column_)
    {
        column = std::move(column_);
        return *this;
    }

    SerializationSubstreamData & withSerializationInfo(SerializationInfoPtr serialization_info_)
    {
        serialization_info = std::move(serialization_info_);
        return *this;
    }

    SerializationSubstreamData & withDeserializeState(SerializationDeserializeBinaryBulkStatePtr deserialize_state_)
    {
        deserialize_state = std::move(deserialize_state_);
        return *this;
    }

    SerializationSubstreamData & withLazyColumnCreator(std::function<ColumnPtr()> lazy_column_creator_)
    {
        lazy_column_creator = std::move(lazy_column_creator_);
        return *this;
    }

    SerializationPtr serialization;
    DataTypePtr type;
    ColumnPtr column;
    SerializationInfoPtr serialization_info;

    /// For types with dynamic subcolumns deserialize state contains information
    /// about current dynamic structure. And this information can be useful
    /// when we call enumerateStreams after deserializeBinaryBulkStatePrefix
    /// to enumerate dynamic streams.
    SerializationDeserializeBinaryBulkStatePtr deserialize_state;

    /// When column is null, this optional creator can materialize it on demand.
    /// Used for derived subcolumns (e.g. String `.size`) whose data can be
    /// computed from a parent column without being stored separately.
    std::function<ColumnPtr()> lazy_column_creator;
};

struct SerializationSubstream
{
    enum Type
    {
        ArrayElements,
        ArraySizes,

        StringSizes,
        InlinedStringSizes,

        NullableElements,
        NullMap,
        NullMapHidden,
        SparseNullMap,

        TupleElement,
        NamedOffsets,
        NamedNullMap,

        DictionaryKeys,
        DictionaryKeysPrefix,
        DictionaryIndexes,

        SparseElements,
        SparseOffsets,

        ReplicatedElements,
        ReplicatedIndexes,

        DeprecatedObjectStructure,
        DeprecatedObjectData,

        VariantDiscriminators,
        VariantDiscriminatorsPrefix,
        NamedVariantDiscriminators,
        VariantOffsets,
        VariantElements,
        VariantElement,
        VariantElementNullMap,

        DynamicData,
        DynamicStructure,

        ObjectData,
        ObjectTypedPath,
        ObjectDynamicPath,
        ObjectSharedData,
        ObjectSharedDataStructure,
        ObjectSharedDataStructurePrefix,
        ObjectSharedDataStructureSuffix,
        ObjectSharedDataData,
        ObjectSharedDataSubstreams,
        ObjectSharedDataPathsMarks,
        ObjectSharedDataSubstreamsMarks,
        ObjectSharedDataPathsSubstreamsMetadata,
        ObjectSharedDataPathsInfos,
        ObjectSharedDataCopy,
        ObjectSharedDataCopySizes,
        ObjectSharedDataCopyPathsIndexes,
        ObjectSharedDataCopyValues,
        ObjectTypedSharedData,
        ObjectTypedSharedDataHeader,
        ObjectTypedSharedDataPathsData,
        ObjectTypedSharedDataPathsMarks,
        /// A SHARED typed path of Object: the identity element of the resolved path, with no file of its own.
        ObjectTypedSharedDataPath,
        ObjectStructure,

        MapKeyValue,
        ObjectDistinctPaths,
        ObjectSubObject,
        ObjectCombinedPath,

        Bucket,
        MapBucketsInfo,
        MapBucketIndexes,

        QuantizedCodes,
        ProductQuantizationCodebook,

        Regular,
    };

    /// Types of substreams that can have arbitrary name.
    static const std::set<Type> named_types;

    Type type = Type::Regular;

    /// The name of a variant element type.
    String variant_element_name;

    /// Name of substream for type from 'named_types'.
    String name_of_substream;

    /// Path name for Object type elements.
    String object_path_name;

    /// Index of a bucket in Object shared data serialization or Map serialization with buckets.
    size_t bucket = 0;

    /// For a stream shared with other subcolumns: this subcolumn's bytes of it, set by the serialization that knows them.
    struct SizeHint
    {
        size_t compressed = 0;
        size_t uncompressed = 0;
    };
    std::optional<SizeHint> size_hint;

    /// Data for current substream.
    SerializationSubstreamData data;

    /// Creator of subcolumn for current substream.
    std::shared_ptr<const ISerializationSubcolumnCreator> creator = nullptr;

    /// Flag, that may help to traverse substream paths.
    mutable bool visited = false;

    SerializationSubstream() = default;
    SerializationSubstream(Type type_) : type(type_) {} /// NOLINT
    String toString() const;
};

struct SerializationSubstreamPath : public std::vector<SerializationSubstream>
{
    String toString() const;
};

using SubstreamType = SerializationSubstream::Type;

}
