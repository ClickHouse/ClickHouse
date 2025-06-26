#pragma once

#include <DataTypes/Serializations/SimpleTextSerialization.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class SerializationObjectSharedDataPath;

/// Class for binary serialization/deserialization of a shared data structure inside Object type.
class SerializationObjectSharedData final : public SimpleTextSerialization
{
public:
    enum class Mode
    {
        /// MAP serialization: serialize as Array(Tuple(paths String, values String)) - the same as in-memory shared data representation.
        MAP = 0,
        /// SEPARATE_PATHS: TBD
        SEPARATE_PATHS = 1,
        /// SEPARATE_SUBSTREAMS: TBD
        SEPARATE_SUBSTREAMS = 2,
    };

    SerializationObjectSharedData(Mode mode_, const DataTypePtr & dynamic_type_, size_t buckets_);

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

    void serializeBinary(const Field &, WriteBuffer &, const FormatSettings &) const override { throwNoSerialization(); }
    void deserializeBinary(Field &, ReadBuffer &, const FormatSettings &) const override { throwNoSerialization(); }
    void serializeBinary(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override { throwNoSerialization(); }
    void deserializeBinary(IColumn &, ReadBuffer &, const FormatSettings &) const override { throwNoSerialization(); }
    void serializeText(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override { throwNoSerialization(); }
    void deserializeText(IColumn &, ReadBuffer &, const FormatSettings &, bool) const override { throwNoSerialization(); }
    bool tryDeserializeText(IColumn &, ReadBuffer &, const FormatSettings &, bool) const override { throwNoSerialization(); }

private:
    struct StructureGranule
    {
        /// List of requested paths that can be read from this granule.
        std::vector<String> paths;
        std::unordered_set<std::string_view> paths_set;
        /// Map from the position of the path in the list of paths stored in
        /// the granule to the position in the list of requested paths above.
        std::unordered_map<size_t, size_t> original_position_to_local_position;
        /// Number of rows in this granule.
        size_t num_rows = 0;
        /// How much rows should be read from this granule. Can be less than num_rows if we read only a part of the granule.
        size_t limit = 0;
        /// How much rows should be skipped in this granule before reading.
        size_t offset = 0;
        /// Total number of paths in this granule, not only requested ones.
        size_t num_paths = 0;
        /// Mark of the data stream for this granule.
        MarkInCompressedFile data_stream_mark;
        /// Mark of the paths marks stream for this granule.
        MarkInCompressedFile paths_marks_stream_mark;
        /// Mark of the paths substreams metadata stream for this granule.
        MarkInCompressedFile paths_substreams_metadata_stream_mark;

        StructureGranule() {}

        StructureGranule(const StructureGranule & other)
        {
            *this = other;
        }

        StructureGranule & operator=(const StructureGranule & other)
        {
            paths = other.paths;
            paths_set.insert(paths.begin(), paths.end());
            original_position_to_local_position = other.original_position_to_local_position;
            num_rows = other.num_rows;
            limit = other.limit;
            offset = other.offset;
            num_paths = other.num_paths;
            data_stream_mark = other.data_stream_mark;
            paths_marks_stream_mark = other.paths_marks_stream_mark;
            paths_substreams_metadata_stream_mark = other.paths_substreams_metadata_stream_mark;
            return *this;
        }

        void clear()
        {
            paths.clear();
            paths_set.clear();
            original_position_to_local_position.clear();
            num_rows = 0;
            limit = 0;
            offset = 0;
            num_paths = 0;
        }
    };

    using StructureGranules = std::vector<StructureGranule>;

    struct SubstreamsCacheStructureElement : public ISerialization::ISubstreamsCacheElement
    {
        SubstreamsCacheStructureElement(std::shared_ptr<StructureGranules> structure_granules_) : structure_granules(structure_granules_) {}

        std::shared_ptr<StructureGranules> structure_granules;
    };

    struct DeserializeBinaryBulkStateObjectSharedDataStructure : public ISerialization::DeserializeBinaryBulkState
    {
        std::unordered_set<String> requested_paths;
        bool read_all_paths = false;

        /// We can read only a part of the granule, so we need to remember the state of the last read granule.
        StructureGranule last_granule_structure;

        ISerialization::DeserializeBinaryBulkStatePtr clone() const override
        {
            return std::make_shared<DeserializeBinaryBulkStateObjectSharedDataStructure>(*this);
        }
    };

    static DeserializeBinaryBulkStatePtr deserializeStructureStatePrefix(DeserializeBinaryBulkSettings & settings, SubstreamsDeserializeStatesCache * cache);
    static std::shared_ptr<StructureGranules> deserializeStructure(size_t rows_offset, size_t limit, DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStateObjectSharedDataStructure & structure_state, SubstreamsCache * cache, Mode mode);
    static void deserializeStructureGranulePrefix(ReadBuffer & buf, StructureGranule & structure_granule, const std::unordered_set<String> & requested_paths, bool read_all_paths);
    static void deserializeStructureGranuleSuffix(ReadBuffer & buf, StructureGranule & structure_granule, Mode mode);

    [[noreturn]] static void throwNoSerialization()
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Text/binary serialization is not implemented for object shared data");
    }

    friend SerializationObjectSharedDataPath;

    Mode mode;
    SerializationPtr serialization_map;
    DataTypePtr dynamic_type;
    SerializationPtr dynamic_serialization;
    size_t buckets;
};

}
