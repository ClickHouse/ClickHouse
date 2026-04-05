#pragma once

#include <DataTypes/Serializations/SimpleTextSerialization.h>
#include <Columns/ColumnMap.h>

namespace DB
{

class SerializationMapSize;
class SerializationMapKeysOrValues;
class SerializationMapKeyValue;

class SerializationMap final : public SimpleTextSerialization
{
private:
    SerializationPtr key_serialization;
    SerializationPtr value_serialization;

    /// 'nested' is an Array(Tuple(key_type, value_type))
    SerializationPtr nested_serialization;
    MergeTreeMapSerializationVersion serialization_version;

    /// Version of the on-disk format for the Map buckets info stream.
    /// This stream stores the number of buckets and optional per-column
    /// statistics (average map size, row count) used to choose the bucket count.
    enum class BucketsInfoSerializationVersion
    {
        V1 = 0,
    };

    SerializationMap(
        const SerializationPtr & key_serialization_,
        const SerializationPtr & value_serialization_,
        const SerializationPtr & nested_serialization_,
        MergeTreeMapSerializationVersion serialization_version_);

public:
    static UInt128 getHash(const SerializationPtr & nested_, MergeTreeMapSerializationVersion serialization_version_);
    static SerializationPtr create(
        const SerializationPtr & key_serialization_,
        const SerializationPtr & value_serialization_,
        const SerializationPtr & nested_serialization_,
        MergeTreeMapSerializationVersion serialization_version_);

    bool supportsPooling() const override { return nested_serialization->supportsPooling(); }

    void serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const override;
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeForHashCalculation(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings &, bool whole) const override;
    bool tryDeserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings &, bool whole) const override;
    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    bool tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeTextJSONPretty(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings, size_t indent) const override;
    void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;

    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    bool tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

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

    static void readMapSafe(DB::IColumn & column, std::function<void()> && read_func);

    SerializationPtr getNestedSerialization() const { return nested_serialization; }
    SerializationPtr getValueSerialization() const { return value_serialization; }
    MergeTreeMapSerializationVersion getMapSerializationVersion() const { return serialization_version; }
    static size_t getBucketForKey(const ColumnPtr & key_column, size_t row, size_t buckets);
    static size_t calculateNumberOfBuckets(const ColumnMap::StatisticsPtr & statistics, size_t max_buckets, MergeTreeMapBucketsStrategy strategy, double coefficient, size_t min_avg_size = 0);

private:
    friend SerializationMapSize;
    friend SerializationMapKeysOrValues;
    friend SerializationMapKeyValue;

    /// Small shared state cached at the current substream path so that both
    /// `SerializationMap` and `SerializationMapKeyValue` can coordinate.
    /// `SerializationMap` sets `reading_full_map = true` during prefix deserialization;
    /// `SerializationMapKeyValue` reads the flag to decide whether to keep the
    /// intermediate nested column (needed for cache sharing) or discard it after extraction.
    struct DeserializeBinaryBulkStateMapReadingInfo : public DeserializeBinaryBulkState
    {
        bool reading_full_map = false;

        DeserializeBinaryBulkStatePtr clone() const override
        {
            return std::make_shared<DeserializeBinaryBulkStateMapReadingInfo>(*this);
        }
    };

    /// State read from the buckets info stream during deserialization prefix.
    /// Contains the bucket count and optional statistics that were written
    /// when the data part was created.
    struct DeserializeBinaryBulkStateBucketsInfo : public DeserializeBinaryBulkState
    {
        /// Number of buckets the Map column was split into during serialization.
        UInt64 buckets;
        /// Per-column statistics (average map size, element count) read from the stream;
        ColumnMap::StatisticsPtr statistics;

        explicit DeserializeBinaryBulkStateBucketsInfo(UInt64 buckets_, const ColumnMap::StatisticsPtr & statistics_)
            : buckets(buckets_), statistics(statistics_)
        {
        }

        DeserializeBinaryBulkStatePtr clone() const override
        {
            return std::make_shared<DeserializeBinaryBulkStateBucketsInfo>(*this);
        }
    };

    static DeserializeBinaryBulkStatePtr deserializeBucketsInfoStatePrefix(DeserializeBinaryBulkSettings & settings, SubstreamsDeserializeStatesCache * cache);
    static DeserializeBinaryBulkStatePtr deserializeMapReadingInfoStatePrefix(SubstreamsDeserializeStatesCache * cache, const ISerialization::SubstreamPath & path);

    template <typename KeyWriter, typename ValueWriter>
    void serializeTextImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr, KeyWriter && key_writer, ValueWriter && value_writer) const;

    template <typename ReturnType = void, typename Reader>
    ReturnType deserializeTextImpl(IColumn & column, ReadBuffer & istr, Reader && reader) const;

    template <typename ReturnType>
    ReturnType deserializeTextJSONImpl(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const;

    VectorWithMemoryTracking<ColumnPtr> splitMapToBuckets(const IColumn & map_column, size_t start, size_t end, size_t buckets) const;
    void collectMapFromBuckets(const VectorWithMemoryTracking<ColumnPtr> & map_buckets, IColumn & map_column) const;
};

}
