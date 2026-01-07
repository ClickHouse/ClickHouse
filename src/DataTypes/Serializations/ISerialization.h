#pragma once

#include <Columns/IColumn_fwd.h>
#include <Core/Types_fwd.h>
#include <Core/SettingsEnums.h>
#include <base/demangle.h>
#include <Common/typeid_cast.h>
#include <Common/ThreadPool_fwd.h>
#include <Formats/MarkInCompressedFile.h>
#include <Storages/MergeTree/MergeTreeDataPartType.h>

#include <boost/noncopyable.hpp>
#include <unordered_map>
#include <memory>
#include <set>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class IDataType;

class ReadBuffer;
class WriteBuffer;
class ProtobufReader;
class ProtobufWriter;

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;

class ISerialization;
using SerializationPtr = std::shared_ptr<const ISerialization>;

class SerializationInfo;
using SerializationInfoPtr = std::shared_ptr<const SerializationInfo>;
using SerializationInfoMutablePtr = std::shared_ptr<SerializationInfo>;

using ValueSizeMap = std::map<std::string, double>;

class Field;

struct FormatSettings;
struct NameAndTypePair;

struct MergeTreeSettings;

/** Represents serialization of data type.
 *  Has methods to serialize/deserialize column in binary and several text formats.
 *  Every data type has default serialization, but can be serialized in different representations.
 *  Default serialization can be wrapped to one of the special kind of serializations.
 *  Currently there is only one special serialization: Sparse.
 *  Each serialization has its own implementation of IColumn as its in-memory representation.
 */
class ISerialization : private boost::noncopyable, public std::enable_shared_from_this<ISerialization>
{
public:
    ISerialization() = default;
    virtual ~ISerialization() = default;

    enum class Kind : UInt8
    {
        DEFAULT = 0,
        SPARSE = 1,
        DETACHED = 2,
        REPLICATED = 3,
    };

    /// We can have multiple serialization kinds created over each other.
    /// For example:
    ///  - Detached over Sparse over Default
    ///  - Detached over Replicated over Default
    ///  - etc
    using KindStack = std::vector<Kind>;

    virtual KindStack getKindStack() const { return {Kind::DEFAULT}; }
    SerializationPtr getPtr() const { return shared_from_this(); }

    static KindStack getKindStack(const IColumn & column);
    static String kindStackToString(const KindStack & kind);
    static KindStack stringToKindStack(const String & str);
    /// Check if provided kind stack contains specific kind.
    static bool hasKind(const KindStack & kind_stack, Kind kind);

    /** Binary serialization for range of values in column - for writing to disk/network, etc.
      *
      * Some data types are represented in multiple streams while being serialized.
      * Example:
      * - Arrays are represented as stream of all elements and stream of array sizes.
      * - Nullable types are represented as stream of values (with unspecified values in place of NULLs) and stream of NULL flags.
      *
      * Different streams are identified by "path".
      * If the data type require single stream (it's true for most of data types), the stream will have empty path.
      * Otherwise, the path can have components like "array elements", "array sizes", etc.
      *
      * For multidimensional arrays, path can have arbitrary length.
      * As an example, for 2-dimensional arrays of numbers we have at least three streams:
      * - array sizes;                      (sizes of top level arrays)
      * - array elements / array sizes;     (sizes of second level (nested) arrays)
      * - array elements / array elements;  (the most deep elements, placed contiguously)
      *
      * Descendants must override either serializeBinaryBulk, deserializeBinaryBulk methods (for simple cases with single stream)
      *  or serializeBinaryBulkWithMultipleStreams, deserializeBinaryBulkWithMultipleStreams, enumerateStreams methods (for cases with multiple streams).
      *
      * Default implementations of ...WithMultipleStreams methods will call serializeBinaryBulk, deserializeBinaryBulk for single stream.
      */

    struct ISubcolumnCreator
    {
        virtual DataTypePtr create(const DataTypePtr & prev) const = 0;
        virtual SerializationPtr create(const SerializationPtr & prev_serialization, const DataTypePtr & prev_type) const = 0;
        virtual ColumnPtr create(const ColumnPtr & prev) const = 0;
        virtual ~ISubcolumnCreator() = default;
    };

    using SubcolumnCreatorPtr = std::shared_ptr<const ISubcolumnCreator>;

    struct SerializeBinaryBulkState
    {
        virtual ~SerializeBinaryBulkState() = default;
    };

    struct DeserializeBinaryBulkState
    {
        DeserializeBinaryBulkState() = default;
        DeserializeBinaryBulkState(const DeserializeBinaryBulkState &) = default;

        virtual ~DeserializeBinaryBulkState() = default;

        virtual std::shared_ptr<DeserializeBinaryBulkState> clone() const { return std::make_shared<DeserializeBinaryBulkState>(); }
    };

    using SerializeBinaryBulkStatePtr = std::shared_ptr<SerializeBinaryBulkState>;
    using DeserializeBinaryBulkStatePtr = std::shared_ptr<DeserializeBinaryBulkState>;

    struct SubstreamData
    {
        SubstreamData() = default;
        explicit SubstreamData(SerializationPtr serialization_)
            : serialization(std::move(serialization_))
        {
        }

        SubstreamData & withType(DataTypePtr type_)
        {
            type = std::move(type_);
            return *this;
        }

        SubstreamData & withColumn(ColumnPtr column_)
        {
            column = std::move(column_);
            return *this;
        }

        SubstreamData & withSerializationInfo(SerializationInfoPtr serialization_info_)
        {
            serialization_info = std::move(serialization_info_);
            return *this;
        }

        SubstreamData & withDeserializeState(DeserializeBinaryBulkStatePtr deserialize_state_)
        {
            deserialize_state = std::move(deserialize_state_);
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
        DeserializeBinaryBulkStatePtr deserialize_state;
    };

    struct Substream
    {
        enum Type
        {
            ArrayElements,
            ArraySizes,

            StringSizes,
            InlinedStringSizes,

            NullableElements,
            NullMap,
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
            ObjectSharedDataBucket,
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
            ObjectStructure,

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

        /// Index of a bucket in Object shared data serialization.
        size_t object_shared_data_bucket = 0;

        /// Data for current substream.
        SubstreamData data;

        /// Creator of subcolumn for current substream.
        SubcolumnCreatorPtr creator = nullptr;

        /// Flag, that may help to traverse substream paths.
        mutable bool visited = false;

        Substream() = default;
        Substream(Type type_) : type(type_) {} /// NOLINT
        String toString() const;
    };

    struct SubstreamPath : public std::vector<Substream>
    {
        String toString() const;
    };

    /// Cache for common substreams of one type, but possible different its subcolumns.
    /// E.g. sizes of arrays of Nested data type.
    struct ISubstreamsCacheElement
    {
        virtual ~ISubstreamsCacheElement() = default;
    };

    using SubstreamsCache = std::unordered_map<String, std::unique_ptr<ISubstreamsCacheElement>>;

    using StreamCallback = std::function<void(const SubstreamPath &)>;

    struct EnumerateStreamsSettings
    {
        SubstreamPath path;
        bool position_independent_encoding = true;
        /// If set to false, don't enumerate dynamic subcolumns
        /// (such as dynamic types in Dynamic column or dynamic paths in JSON column).
        /// It may be needed when dynamic subcolumns are processed separately.
        bool enumerate_dynamic_streams = true;

        /// If set to false, don't enumerate virtual subcolumns
        /// (such as .size subcolumn in String column).
        bool enumerate_virtual_streams = false;

        /// If set to true, enumerate also specialized substreams for prefixes and suffixes.
        /// For example for discriminators in Variant column we should enumerate a separate
        /// substream VariantDiscriminatorsPrefix together with substream VariantDiscriminators that is
        /// used for discriminators data.
        /// It's needed in compact parts when we write mark per each substream, because
        /// all prefixes are serialized before the data (suffixes - after) and we need to separate streams
        /// for prefixes/suffixes and for data to be able to seek to them separately.
        bool use_specialized_prefixes_and_suffixes_substreams = false;

        /// Serialization version that should be used for Object column.
        MergeTreeObjectSerializationVersion object_serialization_version = MergeTreeObjectSerializationVersion::V2;
        /// Serialization version that should be used for shared data inside Object column.
        MergeTreeObjectSharedDataSerializationVersion object_shared_data_serialization_version = MergeTreeObjectSharedDataSerializationVersion::MAP;
        /// Number of buckets that should be used for Object shared data serialization.
        size_t object_shared_data_buckets = 1;
        /// Type of MergeTree data part we serialize/deserialize data from if any.
        MergeTreeDataPartType data_part_type = MergeTreeDataPartType::Unknown;
    };

    virtual void enumerateStreams(
        EnumerateStreamsSettings & settings,
        const StreamCallback & callback,
        const SubstreamData & data) const;

    /// Enumerate streams with default settings.
    void enumerateStreams(
        const StreamCallback & callback,
        const DataTypePtr & type = nullptr,
        const ColumnPtr & column = nullptr) const;

    /// Similar to enumerateStreams, but also includes virtual substreams.
    /// For example, DataTypeString has a virtual `.size` substream, which is included here.
    void enumerateAllStreams(
        const StreamCallback & callback,
        const DataTypePtr & type = nullptr,
        const ColumnPtr & column = nullptr) const;

    using OutputStreamGetter = std::function<WriteBuffer*(const SubstreamPath &)>;
    using InputStreamGetter = std::function<ReadBuffer*(const SubstreamPath &)>;
    using StreamMarkGetter = std::function<MarkInCompressedFile(const SubstreamPath &)>;

    struct SerializeBinaryBulkSettings
    {
        OutputStreamGetter getter;
        SubstreamPath path;

        size_t low_cardinality_max_dictionary_size = 0;
        bool low_cardinality_use_single_dictionary_for_part = true;

        bool position_independent_encoding = true;

        bool use_compact_variant_discriminators_serialization = false;

        enum class ObjectAndDynamicStatisticsMode
        {
            NONE,   /// Don't write statistics.
            PREFIX, /// Write statistics in prefix.
            PREFIX_EMPTY, /// Write empty statistics in prefix.
            SUFFIX, /// Write statistics in suffix.
        };
        ObjectAndDynamicStatisticsMode object_and_dynamic_write_statistics = ObjectAndDynamicStatisticsMode::NONE;

        /// Serialization versions that should be used for Dynamic and Object columns.
        MergeTreeDynamicSerializationVersion dynamic_serialization_version = MergeTreeDynamicSerializationVersion::V2;
        MergeTreeObjectSerializationVersion object_serialization_version = MergeTreeObjectSerializationVersion::V2;
        /// Serialization version that should be used for shared data inside Object column.
        MergeTreeObjectSharedDataSerializationVersion object_shared_data_serialization_version = MergeTreeObjectSharedDataSerializationVersion::MAP;

        /// Number of buckets to use in Object shared data serialization if corresponding version supports it.
        size_t object_shared_data_buckets = 1;

        bool native_format = false;
        const FormatSettings * format_settings = nullptr;

        /// If set to true, all prefixes and suffixes should be written to separate specialized substreams.
        /// For example prefix for discriminators in Variant column should be written in a separate
        /// substream VariantDiscriminatorsPrefix instead of substream VariantDiscriminators that is
        /// used for discriminators data.
        /// It's needed in compact parts when we write mark per each substream, because
        /// all prefixes are serialized before the data (suffixes - after) and we need to separate streams
        /// for prefixes/suffixes and for data to be able to seek to them separately.
        bool use_specialized_prefixes_and_suffixes_substreams = false;

        /// Callback to get current mark of the specific stream.
        /// Used only in MergeTree for Object shared data serialization.
        StreamMarkGetter stream_mark_getter;

        /// Type of MergeTree data part we serialize data from if any.
        /// Some serializations may differ from type part for more optimal deserialization.
        MergeTreeDataPartType data_part_type = MergeTreeDataPartType::Unknown;
    };

    struct DeserializeBinaryBulkSettings
    {
        InputStreamGetter getter;
        SubstreamPath path;

        /// True if continue reading from previous positions in file. False if made fseek to the start of new granule.
        bool continuous_reading = true;

        bool position_independent_encoding = true;

        bool native_format = false;
        const FormatSettings * format_settings;

        bool object_and_dynamic_read_statistics = false;

        /// Callback that should be called when new dynamic subcolumns are discovered during prefix deserialization.
        StreamCallback dynamic_subcolumns_callback;
        /// Callback to start prefetches for specific substreams during prefixes deserialization.
        StreamCallback prefixes_prefetch_callback;
        /// ThreadPool that can be used to read prefixes of subcolumns in parallel.
        ThreadPool * prefixes_deserialization_thread_pool = nullptr;

        /// If set to true, all prefixes and suffixes should be read from separate specialized substreams.
        /// For example prefix for discriminators in Variant column should be read from a separate
        /// substream VariantDiscriminatorsPrefix instead of substream VariantDiscriminators that is
        /// used for discriminators data.
        /// It's needed in compact parts when we write mark per each substream, because
        /// all prefixes are serialized before the data (suffixes - after) and we need to separate streams
        /// for prefixes/suffixes and for data to be able to seek to them separately.
        bool use_specialized_prefixes_and_suffixes_substreams = false;

        /// Callback to seek specific stream to a specific mark.
        /// Used only in MergeTree for Object shared data deserialization.
        std::function<void(const SubstreamPath &, const MarkInCompressedFile &)> seek_stream_to_mark_callback;
        /// Callback to seek specific stream to a current mark that we read from.
        /// Used only in MergeTree and Compact part for Object shared data deserialization.
        std::function<void(const SubstreamPath &)> seek_stream_to_current_mark_callback;

        /// Callback used to get avg_value_size_hint for each substream.
        std::function<double(const SubstreamPath &)> get_avg_value_size_hint_callback;

        /// Callback used to update avg_value_size_hint for each substream.
        std::function<void(const SubstreamPath &, const IColumn &)> update_avg_value_size_hint_callback;

        /// Type of MergeTree data part we deserialize data from if any.
        /// Some serializations may differ from type part for more optimal deserialization.
        MergeTreeDataPartType data_part_type = MergeTreeDataPartType::Unknown;

        /// Usually substreams cache contains the whole column with rows from
        /// multiple ranges. But sometimes we need to read a separate column
        /// with rows only from current range. If this flag is true and
        /// there is a column in cache, insert only rows from current range from it.
        bool insert_only_rows_in_current_range_from_substreams_cache = false;
    };

    /// Call before serializeBinaryBulkWithMultipleStreams chain to write something before first mark.
    /// Column may be used only to retrieve the structure.
    virtual void serializeBinaryBulkStatePrefix(
        const IColumn & /*column*/,
        SerializeBinaryBulkSettings & /*settings*/,
        SerializeBinaryBulkStatePtr & /*state*/) const {}

    /// Call after serializeBinaryBulkWithMultipleStreams chain to finish serialization.
    virtual void serializeBinaryBulkStateSuffix(
        SerializeBinaryBulkSettings & /*settings*/,
        SerializeBinaryBulkStatePtr & /*state*/) const {}

    using SubstreamsDeserializeStatesCache = std::unordered_map<String, DeserializeBinaryBulkStatePtr>;

    /// Call before before deserializeBinaryBulkWithMultipleStreams chain to get DeserializeBinaryBulkStatePtr.
    virtual void deserializeBinaryBulkStatePrefix(
        DeserializeBinaryBulkSettings & /*settings*/,
        DeserializeBinaryBulkStatePtr & /*state*/,
        SubstreamsDeserializeStatesCache * /*cache*/) const {}

    /** 'offset' and 'limit' are used to specify range.
      * limit = 0 - means no limit.
      * offset must be not greater than size of column.
      * offset + limit could be greater than size of column
      *  - in that case, column is serialized till the end.
      */
    virtual void serializeBinaryBulkWithMultipleStreams(
        const IColumn & column,
        size_t offset,
        size_t limit,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const;

    /// Read no more than limit values and append them into column.
    /// If rows_offset is not 0, the deserialization process will skip the first rows_offset rows.
    virtual void deserializeBinaryBulkWithMultipleStreams(
        ColumnPtr & column,
        size_t rows_offset,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const;

    /** Override these methods for data types that require just single stream (most of data types).
      */
    virtual void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const;
    /// If rows_offset is not 0, the deserialization process will skip the first rows_offset rows.
    virtual void deserializeBinaryBulk(
        IColumn & column,
        ReadBuffer & istr,
        size_t rows_offset,
        size_t limit,
        double avg_value_size_hint) const;

    /** Serialization/deserialization of individual values.
      *
      * These are helper methods for implementation of various formats to input/output for user (like CSV, JSON, etc.).
      * There is no one-to-one correspondence between formats and these methods.
      * For example, TabSeparated and Pretty formats could use same helper method serializeTextEscaped.
      *
      * For complex data types (like arrays) binary serde for individual values may differ from bulk serde.
      * For example, if you serialize single array, it will be represented as its size and elements in single contiguous stream,
      *  but if you bulk serialize column with arrays, then sizes and elements will be written to separate streams.
      */

    /// There is two variants for binary serde. First variant work with Field.
    virtual void serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings &) const = 0;
    virtual void deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings &) const = 0;

    /// Other variants takes a column, to avoid creating temporary Field object.
    /// Column must be non-constant.

    /// Serialize one value of a column at specified row number.
    virtual void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const = 0;
    /// Deserialize one value and insert into a column.
    /// If method will throw an exception, then column will be in same state as before call to method.
    virtual void deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings &) const = 0;

    /// Method that is used to serialize value for generic hash calculation of a value in the column.
    /// Note that this method should respect compatibility.
    virtual void serializeForHashCalculation(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
    {
        serializeBinary(column, row_num, ostr, {});
    }

    /** Text serialization with escaping but without quoting.
      */
    virtual void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const = 0;

    virtual void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const = 0;
    virtual bool tryDeserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const;

    /** Text serialization as a literal that may be inserted into a query.
      */
    virtual void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const = 0;

    virtual void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const = 0;
    virtual bool tryDeserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const;

    /** Text serialization for the CSV format.
      */
    virtual void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const = 0;
    virtual void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const = 0;
    virtual bool tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const;

    /** Text serialization for displaying on a terminal or saving into a text file, and the like.
      * Without escaping or quoting.
      */
    virtual void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const = 0;

    /** Text deserialization in case when buffer contains only one value, without any escaping and delimiters.
      */
    virtual void deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const = 0;
    virtual bool tryDeserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const;

    /** Text serialization intended for using in JSON format.
      */
    virtual void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const = 0;
    virtual void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const = 0;
    virtual bool tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const;
    virtual void serializeTextJSONPretty(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings, size_t /*indent*/) const
    {
        serializeTextJSON(column, row_num, ostr, settings);
    }


    /** Text serialization for putting into the XML format.
      */
    virtual void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
    {
        serializeText(column, row_num, ostr, settings);
    }

    /** Text deserialization without escaping and quoting. Reads all data until first \n or \t
     *  into a temporary string and then call deserializeWholeText. It was implemented this way
     *  because this function is rarely used and because proper implementation requires a lot of
     *  additional code in data types serialization and ReadHelpers.
     */
    virtual void deserializeTextRaw(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const;
    virtual bool tryDeserializeTextRaw(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const;
    virtual void serializeTextRaw(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const;

    virtual void serializeTextMarkdown(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const;

    struct StreamFileNameSettings
    {
        StreamFileNameSettings() = default;
        explicit StreamFileNameSettings(const MergeTreeSettings & merge_tree_settings);

        bool escape_variant_substreams = true;
    };

    static String getFileNameForStream(const NameAndTypePair & column, const SubstreamPath & path, const StreamFileNameSettings & settings);
    static String getFileNameForStream(const String & name_in_storage, const SubstreamPath & path, const StreamFileNameSettings & settings);
    static String getFileNameForRenamedColumnStream(const NameAndTypePair & column_from, const NameAndTypePair & column_to, const String & file_name);
    static String getFileNameForRenamedColumnStream(const String & name_from, const String & name_to, const String & file_name);

    static String getSubcolumnNameForStream(const SubstreamPath & path, bool encode_sparse_stream = false);
    static String getSubcolumnNameForStream(const SubstreamPath & path, size_t prefix_len, bool encode_sparse_stream = false);

    static void addColumnWithNumReadRowsToSubstreamsCache(SubstreamsCache * cache, const SubstreamPath & path, ColumnPtr column, size_t num_read_rows);
    static std::optional<std::pair<ColumnPtr, size_t>> getColumnWithNumReadRowsFromSubstreamsCache(SubstreamsCache * cache, const SubstreamPath & path);
    static void addElementToSubstreamsCache(SubstreamsCache * cache, const SubstreamPath & path, std::unique_ptr<ISubstreamsCacheElement> && element);
    static ISubstreamsCacheElement * getElementFromSubstreamsCache(SubstreamsCache * cache, const SubstreamPath & path);

    static void addToSubstreamsDeserializeStatesCache(SubstreamsDeserializeStatesCache * cache, const SubstreamPath & path, DeserializeBinaryBulkStatePtr state);
    static DeserializeBinaryBulkStatePtr getFromSubstreamsDeserializeStatesCache(SubstreamsDeserializeStatesCache * cache, const SubstreamPath & path);

    static bool isSpecialCompressionAllowed(const SubstreamPath & path);

    static size_t getArrayLevel(const SubstreamPath & path);
    static bool hasSubcolumnForPath(const SubstreamPath & path, size_t prefix_len);
    static SubstreamData createFromPath(const SubstreamPath & path, size_t prefix_len);

    /// Returns true if subcolumn doesn't actually stores any data in column and doesn't require a separate stream
    /// for writing/reading data. For example, it's a null-map subcolumn of Variant type (it's always constructed from discriminators);.
    static bool isEphemeralSubcolumn(const SubstreamPath & path, size_t prefix_len);

    /// Returns true if stream with specified path corresponds to dynamic subcolumn.
    static bool isDynamicSubcolumn(const SubstreamPath & path, size_t prefix_len);

    static bool isLowCardinalityDictionarySubcolumn(const SubstreamPath & path);
    static bool isDynamicOrObjectStructureSubcolumn(const SubstreamPath & path);

    /// Returns true if stream with specified path corresponds to Variant subcolumn.
    static bool isVariantSubcolumn(const SubstreamPath & path);

    /// In old versions we could escape file names for some specific substreams differently and it can lead
    /// to not found stream file names in new versions. To keep compatibility, if we can't find stream file name
    /// we are trying to change escaping (via StreamFileNameSettings) and try to find stream file name again.
    static bool tryToChangeStreamFileNameSettingsForNotFoundStream(const SubstreamPath & substream_path, StreamFileNameSettings & stream_file_name_settings);

    /// Return true if the specified path contains prefix that should be deserialized in deserializeBinaryBulkStatePrefix.
    static bool hasPrefix(const SubstreamPath & path, bool use_specialized_prefixes_and_suffixes_substreams = false);

    /// If we have data in substreams cache for substream path from settings insert it
    /// into resulting column and return true, otherwise do nothing and return false.
    static bool insertDataFromSubstreamsCacheIfAny(SubstreamsCache * cache, const DeserializeBinaryBulkSettings & settings, ColumnPtr & result_column);
    /// Perform insertion from column found in substreams cache.
    static void insertDataFromCachedColumn(const DeserializeBinaryBulkSettings & settings, ColumnPtr & result_column, const ColumnPtr & cached_column, size_t num_read_rows, SubstreamsCache * cache);

protected:
    void addSubstreamAndCallCallback(SubstreamPath & path, const StreamCallback & callback, Substream substream) const;

    template <typename State, typename StatePtr>
    State * checkAndGetState(const StatePtr & state) const;

    template <typename State, typename StatePtr>
    static State * checkAndGetState(const StatePtr & state, const ISerialization * serialization);

    [[noreturn]] void throwUnexpectedDataAfterParsedValue(IColumn & column, ReadBuffer & istr, const FormatSettings &, const String & type_name) const;
};

using SerializationPtr = std::shared_ptr<const ISerialization>;
using Serializations = std::vector<SerializationPtr>;
using SerializationByName = std::unordered_map<String, SerializationPtr>;
using SubstreamType = ISerialization::Substream::Type;

template <typename State, typename StatePtr>
State * ISerialization::checkAndGetState(const StatePtr & state) const
{
    return checkAndGetState<State, StatePtr>(state, this);
}

template <typename State, typename StatePtr>
State * ISerialization::checkAndGetState(const StatePtr & state, const ISerialization * serialization)
{
    if (!state)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Got empty state for {}", demangle(typeid(*serialization).name()));

    auto * state_concrete = typeid_cast<State *>(state.get());
    if (!state_concrete)
    {
        auto & state_ref = *state;
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Invalid State for {}. Expected: {}, got {}",
                demangle(typeid(*serialization).name()),
                demangle(typeid(State).name()),
                demangle(typeid(state_ref).name()));
    }

    return state_concrete;
}

}
