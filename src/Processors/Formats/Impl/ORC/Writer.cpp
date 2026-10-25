#include <Processors/Formats/Impl/ORC/Writer.h>
#include <Processors/Formats/Impl/ORC/RLE.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <Common/DateLUT.h>
#include <Common/intExp.h>
#include <Common/assert_cast.h>
#include <base/unaligned.h>
#include <Common/DateLUTImpl.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Common/checkStackSize.h>

#include <algorithm>
#include <bit>
#include <optional>
#include <unordered_map>
#include <unordered_set>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}
}

namespace DB::ORC
{

struct WriterContext
{
    Compressor * compressor = nullptr;
    size_t block_size = 0;
    bool enable_index = false;
    double dictionary_key_size_threshold = 0;
    const DateLUTImpl * timezone = nullptr;
    bool timezone_is_utc = true;
    /// 2015-01-01 00:00:00 in the time zone of the writer, as seconds since the epoch.
    Int64 epoch_offset = 0;
};

namespace
{

struct StreamData
{
    Proto::StreamKind kind;
    UInt32 column;
    std::string data;
};

std::string compressWhole(const WriterContext & context, const std::string & data)
{
    OutputByteStream stream(context.compressor, context.block_size);
    stream.write(data.data(), data.size());
    stream.finish();
    return stream.getOutput();
}

}

class ColumnWriter
{
public:
    ColumnWriter(const Type & type, WriterContext & context_)
        : column_id(type.getColumnId())
        , context(context_)
        , present_stream(context.compressor, context.block_size)
        , present_encoder(present_stream)
        , index_statistics(type.getKind())
        , stripe_statistics(type.getKind())
        , file_statistics(type.getKind())
    {
    }

    virtual ~ColumnWriter() = default;

    /// Must be called by the most derived constructor, after the streams are created.
    void init()
    {
        if (context.enable_index)
            recordPositions();
    }

    /// Adds the values `[offset, offset + num_values)` of `column`, which is not `Nullable`.
    /// `null_map` (1 for a null) and `parent_null_map` (1 for a null in the parent struct, for which
    /// no value is written at all) are for these values only, so they start at the first value.
    virtual void add(const IColumn & /*column*/, const UInt8 * null_map, size_t /*offset*/, size_t num_values, const UInt8 * parent_null_map)
    {
        addPresent(null_map, num_values, parent_null_map);
    }

    /// Writes the PRESENT stream for the values.
    void addPresent(const UInt8 * null_map, size_t num_values, const UInt8 * parent_null_map)
    {
        for (size_t i = 0; i < num_values; ++i)
        {
            if (parent_null_map && parent_null_map[i])
                continue;
            present_encoder.write(!null_map || !null_map[i]);
        }
        if (null_map)
        {
            for (size_t i = 0; i < num_values && !has_null_value; ++i)
                if (null_map[i])
                    has_null_value = true;
        }
    }

    virtual void createRowIndexEntry()
    {
        entries.push_back(Entry{std::move(current_present_positions), std::move(current_data_positions), index_statistics.toProto()});
        current_present_positions.clear();
        current_data_positions.clear();
        stripe_statistics.merge(index_statistics);
        index_statistics.reset();
        recordPositions();
    }

    virtual void mergeRowGroupStatsIntoStripeStats()
    {
        stripe_statistics.merge(index_statistics);
        index_statistics.reset();
    }

    /// Encodes the deferred data of the stripe.
    virtual void finalizeStripe() {}

    virtual void writeIndex(std::vector<StreamData> & streams)
    {
        Proto::RowIndex index;
        for (const auto & entry : entries)
        {
            auto & proto_entry = index.entry.emplace_back();
            /// The positions of the PRESENT stream are written only if the stream is written.
            if (has_null_value)
                proto_entry.positions = entry.present_positions;
            proto_entry.positions.insert(proto_entry.positions.end(), entry.data_positions.begin(), entry.data_positions.end());
            proto_entry.statistics = entry.statistics;
        }
        streams.push_back({Proto::StreamKind::ROW_INDEX, static_cast<UInt32>(column_id), compressWhole(context, index.serialize())});
    }

    virtual void flush(std::vector<StreamData> & streams)
    {
        present_encoder.flush();
        present_stream.finish();
        if (has_null_value)
            streams.push_back({Proto::StreamKind::PRESENT, static_cast<UInt32>(column_id), present_stream.getOutput()});
    }

    virtual void getColumnEncodings(std::vector<Proto::ColumnEncoding> & encodings) const
    {
        encodings.push_back({Proto::ColumnEncodingKind::DIRECT, 0, {}});
    }

    virtual void getStripeStatistics(std::vector<Proto::ColumnStatistics> & stats) const
    {
        stats.push_back(stripe_statistics.toProto());
    }

    virtual void getFileStatistics(std::vector<Proto::ColumnStatistics> & stats) const
    {
        stats.push_back(file_statistics.toProto());
    }

    virtual void mergeStripeStatsIntoFileStats()
    {
        file_statistics.merge(stripe_statistics);
        stripe_statistics.reset();
    }

    virtual size_t estimateSize() const
    {
        return present_stream.estimateSize();
    }

    /// Starts a new stripe.
    virtual void reset()
    {
        present_stream.reset();
        has_null_value = false;
        entries.clear();
        current_present_positions.clear();
        current_data_positions.clear();
        resetStreams();
        if (context.enable_index)
            recordPositions();
    }

protected:
    /// Appends the positions of the data streams, in the order the reader seeks them.
    virtual void recordDataPositions(std::vector<UInt64> & positions) = 0;
    /// Clears the data streams for a new stripe.
    virtual void resetStreams() = 0;

    /// The number of the non-null values, also updates the statistics with the count.
    size_t countValues(const UInt8 * null_map, size_t num_values)
    {
        size_t count = num_values;
        if (null_map)
        {
            count = 0;
            for (size_t i = 0; i < num_values; ++i)
                count += !null_map[i];
        }
        index_statistics.increase(count);
        if (count < num_values)
            index_statistics.setHasNull();
        return count;
    }

    struct Entry
    {
        std::vector<UInt64> present_positions;
        std::vector<UInt64> data_positions;
        Proto::ColumnStatistics statistics;
    };

    const UInt64 column_id;
    WriterContext & context;
    OutputByteStream present_stream;
    BooleanRleEncoder present_encoder;
    bool has_null_value = false;
    StatisticsBuilder index_statistics;
    StatisticsBuilder stripe_statistics;
    StatisticsBuilder file_statistics;
    std::vector<Entry> entries;

private:
    void recordPositions()
    {
        present_encoder.recordPosition(current_present_positions);
        recordDataPositions(current_data_positions);
    }

    std::vector<UInt64> current_present_positions;
    std::vector<UInt64> current_data_positions;
};

namespace
{

std::unique_ptr<ColumnWriter> buildColumnWriter(const Type & type, const DataTypePtr & data_type, WriterContext & context);

/// Adds the values of a column that can be `Nullable`. The values that are null in the parent
/// (`outer_null_map`, for the fields of a struct) are null as well.
void addColumn(
    ColumnWriter & writer, const IColumn & column, const UInt8 * outer_null_map, size_t offset, size_t num_values, const UInt8 * parent_null_map)
{
    if (const auto * nullable = typeid_cast<const ColumnNullable *>(&column))
    {
        const UInt8 * null_map = nullable->getNullMapData().data() + offset;
        if (!outer_null_map)
        {
            writer.add(nullable->getNestedColumn(), null_map, offset, num_values, parent_null_map);
            return;
        }
        PaddedPODArray<UInt8> merged(num_values);
        for (size_t i = 0; i < num_values; ++i)
            merged[i] = null_map[i] | outer_null_map[i];
        writer.add(nullable->getNestedColumn(), merged.data(), offset, num_values, parent_null_map);
        return;
    }
    writer.add(column, outer_null_map, offset, num_values, parent_null_map);
}

/// The raw values of a column with fixed-size values.
template <typename T>
const T * getRawValues(const IColumn & column)
{
    auto raw = column.getRawData();
    if (column.sizeOfValueIfFixed() != sizeof(T))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected column {} with values of size {} for ORC (expected {})",
            column.getName(), column.sizeOfValueIfFixed(), sizeof(T));
    return reinterpret_cast<const T *>(raw.data());
}

/// The value of the integer column (of any width up to 8 bytes) as a signed number of the same width,
/// as ORC does not have unsigned types.
Int64 readSignedValue(const char * data, size_t width, size_t row)
{
    switch (width)
    {
        case 1: return unalignedLoad<Int8>(data + row);
        case 2: return unalignedLoad<Int16>(data + row * 2);
        case 4: return unalignedLoad<Int32>(data + row * 4);
        default: return unalignedLoad<Int64>(data + row * 8);
    }
}

/// The writers that have children call the same methods for them.
class CompoundColumnWriter : public ColumnWriter
{
public:
    using ColumnWriter::ColumnWriter;

    void createRowIndexEntry() override
    {
        ColumnWriter::createRowIndexEntry();
        for (auto & child : children)
            child->createRowIndexEntry();
    }

    void mergeRowGroupStatsIntoStripeStats() override
    {
        ColumnWriter::mergeRowGroupStatsIntoStripeStats();
        for (auto & child : children)
            child->mergeRowGroupStatsIntoStripeStats();
    }

    void finalizeStripe() override
    {
        for (auto & child : children)
            child->finalizeStripe();
    }

    void writeIndex(std::vector<StreamData> & streams) override
    {
        ColumnWriter::writeIndex(streams);
        for (auto & child : children)
            child->writeIndex(streams);
    }

    void flush(std::vector<StreamData> & streams) override
    {
        ColumnWriter::flush(streams);
        flushOwnStreams(streams);
        for (auto & child : children)
            child->flush(streams);
    }

    void getColumnEncodings(std::vector<Proto::ColumnEncoding> & encodings) const override
    {
        getOwnColumnEncoding(encodings);
        for (const auto & child : children)
            child->getColumnEncodings(encodings);
    }

    void getStripeStatistics(std::vector<Proto::ColumnStatistics> & stats) const override
    {
        ColumnWriter::getStripeStatistics(stats);
        for (const auto & child : children)
            child->getStripeStatistics(stats);
    }

    void getFileStatistics(std::vector<Proto::ColumnStatistics> & stats) const override
    {
        ColumnWriter::getFileStatistics(stats);
        for (const auto & child : children)
            child->getFileStatistics(stats);
    }

    void mergeStripeStatsIntoFileStats() override
    {
        ColumnWriter::mergeStripeStatsIntoFileStats();
        for (auto & child : children)
            child->mergeStripeStatsIntoFileStats();
    }

    size_t estimateSize() const override
    {
        size_t size = ColumnWriter::estimateSize() + estimateOwnSize();
        for (const auto & child : children)
            size += child->estimateSize();
        return size;
    }

    void reset() override
    {
        ColumnWriter::reset();
        for (auto & child : children)
            (*child).reset();
    }

protected:
    virtual void flushOwnStreams(std::vector<StreamData> &) {}
    virtual void getOwnColumnEncoding(std::vector<Proto::ColumnEncoding> & encodings) const
    {
        encodings.push_back({Proto::ColumnEncodingKind::DIRECT, 0, {}});
    }
    virtual size_t estimateOwnSize() const { return 0; }

    std::vector<std::unique_ptr<ColumnWriter>> children;
};

class StructColumnWriter final : public CompoundColumnWriter
{
public:
    StructColumnWriter(const Type & type, const DataTypes & field_types, WriterContext & context_)
        : CompoundColumnWriter(type, context_)
    {
        if (field_types.size() != type.getSubtypeCount())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ORC struct {} has {} fields, the ClickHouse type has {}",
                type.toString(), type.getSubtypeCount(), field_types.size());
        for (UInt64 i = 0; i < type.getSubtypeCount(); ++i)
            children.push_back(buildColumnWriter(*type.getSubtype(i), field_types[i], context));
        init();
    }

    void add(const IColumn & column, const UInt8 * null_map, size_t offset, size_t num_values, const UInt8 * parent_null_map) override
    {
        const auto & tuple = assert_cast<const ColumnTuple &>(column);
        ColumnWriter::add(column, null_map, offset, num_values, parent_null_map);
        /// The fields of the null values are null, and they are not written.
        for (size_t i = 0; i < children.size(); ++i)
            addColumn(*children[i], tuple.getColumn(i), null_map, offset, num_values, null_map);
        countValues(null_map, num_values);
    }

    /// Adds the rows of the root struct, which are the columns of the file.
    void addRoot(const Columns & columns, size_t offset, size_t num_values)
    {
        if (columns.size() != children.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected {} columns for ORC, got {}", children.size(), columns.size());
        addPresent(nullptr, num_values, nullptr);
        for (size_t i = 0; i < children.size(); ++i)
            addColumn(*children[i], *columns[i], nullptr, offset, num_values, nullptr);
        countValues(nullptr, num_values);
    }

protected:
    void recordDataPositions(std::vector<UInt64> &) override {}
    void resetStreams() override {}
};

template <bool is_map>
class CollectionColumnWriter final : public CompoundColumnWriter
{
public:
    CollectionColumnWriter(const Type & type, const DataTypes & child_types, WriterContext & context_)
        : CompoundColumnWriter(type, context_)
        , length_stream(context.compressor, context.block_size)
        , length_encoder(length_stream, false)
    {
        if (child_types.size() != type.getSubtypeCount())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ORC type {} does not match the ClickHouse type", type.toString());
        for (UInt64 i = 0; i < type.getSubtypeCount(); ++i)
            children.push_back(buildColumnWriter(*type.getSubtype(i), child_types[i], context));
        init();
    }

    void add(const IColumn & column, const UInt8 * null_map, size_t offset, size_t num_values, const UInt8 * parent_null_map) override
    {
        const ColumnArray & array = is_map ? assert_cast<const ColumnMap &>(column).getNestedColumn() : assert_cast<const ColumnArray &>(column);
        /// The offsets of `ColumnArray` are the end offsets, and the one before the first is zero.
        const auto & offsets = array.getOffsets();

        ColumnWriter::add(column, null_map, offset, num_values, parent_null_map);

        UInt64 element_offset = offsets[static_cast<ssize_t>(offset) - 1];
        UInt64 total_elements = offsets[offset + num_values - 1] - element_offset;

        if constexpr (is_map)
        {
            const auto & key_value = assert_cast<const ColumnTuple &>(array.getData());
            addColumn(*children[0], key_value.getColumn(0), nullptr, element_offset, total_elements, nullptr);
            addColumn(*children[1], key_value.getColumn(1), nullptr, element_offset, total_elements, nullptr);
        }
        else
        {
            addColumn(*children[0], array.getData(), nullptr, element_offset, total_elements, nullptr);
        }

        size_t count = 0;
        for (size_t i = 0; i < num_values; ++i)
        {
            if (null_map && null_map[i])
                continue;
            size_t row = offset + i;
            UInt64 length = offsets[row] - offsets[static_cast<ssize_t>(row) - 1];
            length_encoder.write(static_cast<Int64>(length));
            index_statistics.updateCollection(length);
            ++count;
        }
        index_statistics.increase(count);
        if (count < num_values)
            index_statistics.setHasNull();
    }

protected:
    void recordDataPositions(std::vector<UInt64> & positions) override
    {
        length_encoder.recordPosition(positions);
    }

    void resetStreams() override
    {
        length_stream.reset();
    }

    void flushOwnStreams(std::vector<StreamData> & streams) override
    {
        length_encoder.flush();
        length_stream.finish();
        streams.push_back({Proto::StreamKind::LENGTH, static_cast<UInt32>(column_id), length_stream.getOutput()});
    }

    void getOwnColumnEncoding(std::vector<Proto::ColumnEncoding> & encodings) const override
    {
        encodings.push_back({Proto::ColumnEncodingKind::DIRECT_V2, 0, {}});
    }

    size_t estimateOwnSize() const override { return length_stream.estimateSize(); }

private:
    OutputByteStream length_stream;
    IntRleEncoderV2 length_encoder;
};

class UnionColumnWriter final : public CompoundColumnWriter
{
public:
    /// The branches of the union are in the same order as the variants of the `Variant` type,
    /// so the global discriminator of a value is its tag.
    UnionColumnWriter(const Type & type, const DataTypes & variant_types, WriterContext & context_)
        : CompoundColumnWriter(type, context_)
        , tag_stream(context.compressor, context.block_size)
        , tag_encoder(tag_stream)
    {
        if (variant_types.size() != type.getSubtypeCount())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ORC union {} does not match the ClickHouse type", type.toString());
        for (UInt64 i = 0; i < type.getSubtypeCount(); ++i)
            children.push_back(buildColumnWriter(*type.getSubtype(i), variant_types[i], context));
        init();
    }

    void add(const IColumn & column, const UInt8 * null_map, size_t offset, size_t num_values, const UInt8 * parent_null_map) override
    {
        const auto & variant = assert_cast<const ColumnVariant &>(column);

        /// A `NULL` of `Variant` is a null value of the union.
        PaddedPODArray<UInt8> union_null_map(num_values);
        bool has_nulls = false;
        for (size_t i = 0; i < num_values; ++i)
        {
            union_null_map[i] = (null_map && null_map[i]) || variant.globalDiscriminatorAt(offset + i) == ColumnVariant::NULL_DISCRIMINATOR;
            has_nulls |= union_null_map[i];
        }
        const UInt8 * union_nulls = has_nulls ? union_null_map.data() : nullptr;

        ColumnWriter::add(column, union_nulls, offset, num_values, parent_null_map);

        std::vector<Int64> child_offset(children.size(), -1);
        std::vector<UInt64> child_length(children.size(), 0);
        size_t count = 0;
        for (size_t i = 0; i < num_values; ++i)
        {
            if (union_nulls && union_nulls[i])
                continue;
            size_t row = offset + i;
            auto tag = variant.globalDiscriminatorAt(row);
            if (tag >= children.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid union tag {} for union with {} children", static_cast<UInt32>(tag), children.size());
            if (child_offset[tag] == -1)
                child_offset[tag] = static_cast<Int64>(variant.offsetAt(row));
            ++child_length[tag];
            tag_encoder.write(static_cast<char>(tag));
            ++count;
        }

        /// The values of each branch are consecutive in its column.
        for (size_t i = 0; i < children.size(); ++i)
            if (child_length[i])
                addColumn(*children[i], variant.getVariantByGlobalDiscriminator(i), nullptr, static_cast<size_t>(child_offset[i]), child_length[i], nullptr);

        index_statistics.increase(count);
        if (count < num_values)
            index_statistics.setHasNull();
    }

protected:
    void recordDataPositions(std::vector<UInt64> & positions) override
    {
        tag_encoder.recordPosition(positions);
    }

    void resetStreams() override
    {
        tag_stream.reset();
    }

    void flushOwnStreams(std::vector<StreamData> & streams) override
    {
        tag_encoder.flush();
        tag_stream.finish();
        streams.push_back({Proto::StreamKind::DATA, static_cast<UInt32>(column_id), tag_stream.getOutput()});
    }

    size_t estimateOwnSize() const override { return tag_stream.estimateSize(); }

private:
    OutputByteStream tag_stream;
    ByteRleEncoder tag_encoder;
};


/// The writers of the primitive columns.
class PrimitiveColumnWriter : public ColumnWriter
{
public:
    using ColumnWriter::ColumnWriter;

    void getColumnEncodings(std::vector<Proto::ColumnEncoding> & encodings) const override
    {
        encodings.push_back({encoding_kind, dictionary_size, {}});
    }

protected:
    Proto::ColumnEncodingKind encoding_kind = Proto::ColumnEncodingKind::DIRECT;
    UInt32 dictionary_size = 0;
};

class BooleanColumnWriter final : public PrimitiveColumnWriter
{
public:
    BooleanColumnWriter(const Type & type, WriterContext & context_)
        : PrimitiveColumnWriter(type, context_)
        , data_stream(context.compressor, context.block_size)
        , encoder(data_stream)
    {
        init();
    }

    void add(const IColumn & column, const UInt8 * null_map, size_t offset, size_t num_values, const UInt8 * parent_null_map) override
    {
        ColumnWriter::add(column, null_map, offset, num_values, parent_null_map);
        const UInt8 * data = getRawValues<UInt8>(column) + offset;
        for (size_t i = 0; i < num_values; ++i)
        {
            if (null_map && null_map[i])
                continue;
            encoder.write(data[i] != 0);
            index_statistics.updateBoolean(data[i] != 0);
        }
        countValues(null_map, num_values);
    }

    void flush(std::vector<StreamData> & streams) override
    {
        ColumnWriter::flush(streams);
        encoder.flush();
        data_stream.finish();
        streams.push_back({Proto::StreamKind::DATA, static_cast<UInt32>(column_id), data_stream.getOutput()});
    }

    size_t estimateSize() const override { return ColumnWriter::estimateSize() + data_stream.estimateSize(); }

protected:
    void recordDataPositions(std::vector<UInt64> & positions) override { encoder.recordPosition(positions); }
    void resetStreams() override { data_stream.reset(); }

private:
    OutputByteStream data_stream;
    BooleanRleEncoder encoder;
};

class ByteColumnWriter final : public PrimitiveColumnWriter
{
public:
    ByteColumnWriter(const Type & type, WriterContext & context_)
        : PrimitiveColumnWriter(type, context_)
        , data_stream(context.compressor, context.block_size)
        , encoder(data_stream)
    {
        init();
    }

    void add(const IColumn & column, const UInt8 * null_map, size_t offset, size_t num_values, const UInt8 * parent_null_map) override
    {
        ColumnWriter::add(column, null_map, offset, num_values, parent_null_map);
        /// The unsigned values are written as signed ones of the same width.
        const Int8 * data = getRawValues<Int8>(column) + offset;
        for (size_t i = 0; i < num_values; ++i)
        {
            if (null_map && null_map[i])
                continue;
            encoder.write(static_cast<char>(data[i]));
            index_statistics.updateInteger(data[i]);
        }
        countValues(null_map, num_values);
    }

    void flush(std::vector<StreamData> & streams) override
    {
        ColumnWriter::flush(streams);
        encoder.flush();
        data_stream.finish();
        streams.push_back({Proto::StreamKind::DATA, static_cast<UInt32>(column_id), data_stream.getOutput()});
    }

    size_t estimateSize() const override { return ColumnWriter::estimateSize() + data_stream.estimateSize(); }

protected:
    void recordDataPositions(std::vector<UInt64> & positions) override { encoder.recordPosition(positions); }
    void resetStreams() override { data_stream.reset(); }

private:
    OutputByteStream data_stream;
    ByteRleEncoder encoder;
};

/// SHORT, INT, LONG and DATE.
class IntegerColumnWriter final : public PrimitiveColumnWriter
{
public:
    IntegerColumnWriter(const Type & type, WriterContext & context_)
        : PrimitiveColumnWriter(type, context_)
        , data_stream(context.compressor, context.block_size)
        , encoder(data_stream, true)
        , is_date(type.getKind() == DATE)
    {
        encoding_kind = Proto::ColumnEncodingKind::DIRECT_V2;
        init();
    }

    void add(const IColumn & column, const UInt8 * null_map, size_t offset, size_t num_values, const UInt8 * parent_null_map) override
    {
        ColumnWriter::add(column, null_map, offset, num_values, parent_null_map);
        /// The unsigned values are written as signed ones of the same width (except `Date`).
        const size_t width = column.sizeOfValueIfFixed();
        if (width != 2 && width != 4 && width != 8)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected column {} for ORC integer", column.getName());
        const char * data = column.getRawData().data();
        for (size_t i = 0; i < num_values; ++i)
        {
            if (null_map && null_map[i])
                continue;
            /// `Date` is the unsigned number of days (up to 2149), which must not wrap around
            /// to the negative ones after 2059. `Date32` is signed, as the other integers.
            Int64 value = (is_date && width == 2)
                ? unalignedLoad<UInt16>(data + (offset + i) * sizeof(UInt16))
                : readSignedValue(data, width, offset + i);
            encoder.write(value);
            if (is_date)
                index_statistics.updateDate(static_cast<Int32>(value));
            else
                index_statistics.updateInteger(value);
        }
        countValues(null_map, num_values);
    }

    void flush(std::vector<StreamData> & streams) override
    {
        ColumnWriter::flush(streams);
        encoder.flush();
        data_stream.finish();
        streams.push_back({Proto::StreamKind::DATA, static_cast<UInt32>(column_id), data_stream.getOutput()});
    }

    size_t estimateSize() const override { return ColumnWriter::estimateSize() + data_stream.estimateSize(); }

protected:
    void recordDataPositions(std::vector<UInt64> & positions) override { encoder.recordPosition(positions); }
    void resetStreams() override { data_stream.reset(); }

private:
    OutputByteStream data_stream;
    IntRleEncoderV2 encoder;
    const bool is_date;
};

template <typename ItemType>
class FloatingColumnWriter final : public PrimitiveColumnWriter
{
public:
    FloatingColumnWriter(const Type & type, WriterContext & context_)
        : PrimitiveColumnWriter(type, context_)
        , data_stream(context.compressor, context.block_size)
    {
        init();
    }

    void add(const IColumn & column, const UInt8 * null_map, size_t offset, size_t num_values, const UInt8 * parent_null_map) override
    {
        ColumnWriter::add(column, null_map, offset, num_values, parent_null_map);
        const ItemType * data = getRawValues<ItemType>(column) + offset;
        if (!null_map && std::endian::native == std::endian::little)
        {
            data_stream.write(reinterpret_cast<const char *>(data), num_values * sizeof(ItemType));
            for (size_t i = 0; i < num_values; ++i)
                index_statistics.updateDouble(static_cast<double>(data[i]));
        }
        else
        {
            for (size_t i = 0; i < num_values; ++i)
            {
                if (null_map && null_map[i])
                    continue;
                ItemType value = data[i];
                if constexpr (std::endian::native == std::endian::big)
                    value = std::bit_cast<ItemType>(std::byteswap(std::bit_cast<std::conditional_t<sizeof(ItemType) == 8, UInt64, UInt32>>(value)));
                data_stream.write(reinterpret_cast<const char *>(&value), sizeof(value));
                index_statistics.updateDouble(static_cast<double>(data[i]));
            }
        }
        countValues(null_map, num_values);
    }

    void flush(std::vector<StreamData> & streams) override
    {
        ColumnWriter::flush(streams);
        data_stream.finish();
        streams.push_back({Proto::StreamKind::DATA, static_cast<UInt32>(column_id), data_stream.getOutput()});
    }

    size_t estimateSize() const override { return ColumnWriter::estimateSize() + data_stream.estimateSize(); }

protected:
    void recordDataPositions(std::vector<UInt64> & positions) override { data_stream.recordPosition(positions); }
    void resetStreams() override { data_stream.reset(); }

private:
    OutputByteStream data_stream;
};

/// STRING, VARCHAR, CHAR and BINARY. The values of a stripe are kept in memory and encoded at the
/// end of the stripe, either directly or with a dictionary.
class StringColumnWriter final : public PrimitiveColumnWriter
{
public:
    StringColumnWriter(const Type & type, WriterContext & context_)
        : PrimitiveColumnWriter(type, context_)
        , is_binary(type.getKind() == BINARY)
        , data_stream(context.compressor, context.block_size)
        , length_stream(context.compressor, context.block_size)
        , dictionary_data_stream(context.compressor, context.block_size)
        , length_encoder(length_stream, false)
        , index_encoder(data_stream, false)
    {
        /// The binary columns never use dictionaries, as in the reference implementation.
        if (is_binary || context.dictionary_key_size_threshold <= 0)
            use_dictionary = false;
        encoding_kind = Proto::ColumnEncodingKind::DIRECT_V2;
        init();
    }

    void add(const IColumn & column, const UInt8 * null_map, size_t offset, size_t num_values, const UInt8 * parent_null_map) override
    {
        ColumnWriter::add(column, null_map, offset, num_values, parent_null_map);
        /// `String`, or the raw bytes of the values of fixed size (`FixedString`, `IPv6`, big integers).
        auto add_value = [&](std::string_view value)
        {
            blob.append(value);
            value_ends.push_back(blob.size());
            if (is_binary)
                index_statistics.updateBinary(value.size());
            else
                index_statistics.updateString(value);
        };

        if (const auto * strings = typeid_cast<const ColumnString *>(&column))
        {
            const auto & offsets = strings->getOffsets();
            const auto * chars = reinterpret_cast<const char *>(strings->getChars().data());
            for (size_t i = 0; i < num_values; ++i)
            {
                if (null_map && null_map[i])
                    continue;
                size_t row = offset + i;
                add_value(std::string_view(chars + offsets[row - 1], offsets[row] - offsets[row - 1]));
            }
        }
        else
        {
            for (size_t i = 0; i < num_values; ++i)
            {
                if (null_map && null_map[i])
                    continue;
                add_value(column.getDataAt(offset + i));
            }
        }
        countValues(null_map, num_values);
    }

    void createRowIndexEntry() override
    {
        if (!use_dictionary)
            decideDictionary();
        ColumnWriter::createRowIndexEntry();
    }

    void finalizeStripe() override
    {
        if (!use_dictionary)
            decideDictionary();

        /// The start of the group without an entry is not needed.
        group_starts.resize(entries.size());

        if (*use_dictionary)
            encodeWithDictionary();
        else
            encodeDirect();
    }

    void flush(std::vector<StreamData> & streams) override
    {
        ColumnWriter::flush(streams);
        if (*use_dictionary)
        {
            index_encoder.flush();
            data_stream.finish();
            length_encoder.flush();
            length_stream.finish();
            dictionary_data_stream.finish();
            streams.push_back({Proto::StreamKind::DATA, static_cast<UInt32>(column_id), data_stream.getOutput()});
            streams.push_back({Proto::StreamKind::DICTIONARY_DATA, static_cast<UInt32>(column_id), dictionary_data_stream.getOutput()});
            streams.push_back({Proto::StreamKind::LENGTH, static_cast<UInt32>(column_id), length_stream.getOutput()});
        }
        else
        {
            data_stream.finish();
            length_encoder.flush();
            length_stream.finish();
            streams.push_back({Proto::StreamKind::DATA, static_cast<UInt32>(column_id), data_stream.getOutput()});
            streams.push_back({Proto::StreamKind::LENGTH, static_cast<UInt32>(column_id), length_stream.getOutput()});
        }
    }

    size_t estimateSize() const override
    {
        return ColumnWriter::estimateSize() + blob.size() + value_ends.size() * sizeof(UInt64);
    }

protected:
    void recordDataPositions(std::vector<UInt64> &) override
    {
        /// The positions are known only after the encoding, at the end of the stripe.
        group_starts.push_back(value_ends.size());
    }

    void resetStreams() override
    {
        data_stream.reset();
        length_stream.reset();
        dictionary_data_stream.reset();
        blob.clear();
        value_ends.clear();
        group_starts.clear();
        dictionary_size = 0;
    }

private:
    std::string_view getValue(size_t i) const
    {
        size_t begin = i ? value_ends[i - 1] : 0;
        return std::string_view(blob.data() + begin, value_ends[i] - begin);
    }

    /// The decision is made once for the file, by the values of the first row group (or the first
    /// stripe without the row index), as in the reference implementation.
    void decideDictionary()
    {
        std::unordered_set<std::string_view> distinct;
        for (size_t i = 0; i < value_ends.size(); ++i)
            distinct.insert(getValue(i));
        use_dictionary = distinct.size() <= static_cast<size_t>(static_cast<double>(value_ends.size()) * context.dictionary_key_size_threshold);
    }

    void encodeDirect()
    {
        encoding_kind = Proto::ColumnEncodingKind::DIRECT_V2;
        size_t group = 0;
        for (size_t i = 0; i <= value_ends.size(); ++i)
        {
            while (group < group_starts.size() && group_starts[group] == i)
            {
                auto & positions = entries[group].data_positions;
                data_stream.recordPosition(positions);
                length_encoder.recordPosition(positions);
                ++group;
            }
            if (i == value_ends.size())
                break;
            auto value = getValue(i);
            data_stream.write(value.data(), value.size());
            length_encoder.write(static_cast<Int64>(value.size()));
        }
    }

    void encodeWithDictionary()
    {
        encoding_kind = Proto::ColumnEncodingKind::DICTIONARY_V2;

        /// The dictionary is sorted, as in the reference implementation.
        std::unordered_map<std::string_view, UInt32> index_of;
        std::vector<std::string_view> keys;
        for (size_t i = 0; i < value_ends.size(); ++i)
        {
            auto value = getValue(i);
            if (index_of.emplace(value, 0).second)
                keys.push_back(value);
        }
        std::sort(keys.begin(), keys.end());
        for (size_t i = 0; i < keys.size(); ++i)
            index_of[keys[i]] = static_cast<UInt32>(i);
        dictionary_size = static_cast<UInt32>(keys.size());

        size_t group = 0;
        for (size_t i = 0; i <= value_ends.size(); ++i)
        {
            while (group < group_starts.size() && group_starts[group] == i)
            {
                index_encoder.recordPosition(entries[group].data_positions);
                ++group;
            }
            if (i == value_ends.size())
                break;
            index_encoder.write(index_of[getValue(i)]);
        }

        for (const auto & key : keys)
        {
            dictionary_data_stream.write(key.data(), key.size());
            length_encoder.write(static_cast<Int64>(key.size()));
        }
    }

    const bool is_binary;
    std::optional<bool> use_dictionary;
    OutputByteStream data_stream;
    OutputByteStream length_stream;
    OutputByteStream dictionary_data_stream;
    IntRleEncoderV2 length_encoder;
    IntRleEncoderV2 index_encoder;

    std::string blob;
    std::vector<UInt64> value_ends;
    /// The index of the first value of each row group.
    std::vector<UInt64> group_starts;
};

class TimestampColumnWriter final : public PrimitiveColumnWriter
{
public:
    /// `scale` is of `DateTime64`, or nullopt for `DateTime` (`UInt32` seconds).
    TimestampColumnWriter(const Type & type, std::optional<UInt32> scale_, WriterContext & context_)
        : PrimitiveColumnWriter(type, context_)
        , scale(scale_)
        , seconds_stream(context.compressor, context.block_size)
        , nanos_stream(context.compressor, context.block_size)
        , seconds_encoder(seconds_stream, true)
        , nanos_encoder(nanos_stream, false)
        , is_utc(type.getKind() == TIMESTAMP_INSTANT || context.timezone_is_utc)
        , epoch_offset(type.getKind() == TIMESTAMP_INSTANT ? 1420070400 : context.epoch_offset)
    {
        encoding_kind = Proto::ColumnEncodingKind::DIRECT_V2;
        init();
    }

    void add(const IColumn & column, const UInt8 * null_map, size_t offset, size_t num_values, const UInt8 * parent_null_map) override
    {
        ColumnWriter::add(column, null_map, offset, num_values, parent_null_map);
        const char * data = column.getRawData().data();
        const Int64 multiplier = scale ? common::exp10_i64(static_cast<int>(*scale)) : 1;
        const Int64 nanos_multiplier = scale ? common::exp10_i64(9 - static_cast<int>(*scale)) : 0;
        for (size_t i = 0; i < num_values; ++i)
        {
            if (null_map && null_map[i])
                continue;
            Int64 secs = 0;
            Int64 nano = 0;
            if (scale)
            {
                /// For the values before the epoch, the nanoseconds are negative (as in the Arrow ORC adapter).
                Int64 value = unalignedLoad<Int64>(data + (offset + i) * sizeof(Int64));
                secs = value / multiplier;
                nano = (value % multiplier) * nanos_multiplier;
            }
            else
            {
                secs = unalignedLoad<UInt32>(data + (offset + i) * sizeof(UInt32));
                nano = 0;
            }

            Int64 utc_seconds = is_utc ? secs : secs + context.timezone->timezoneOffset(secs);
            index_statistics.updateTimestamp(utc_seconds * 1000 + nano / 1000000, static_cast<Int32>(nano % 1000000));

            if (secs < 0 && nano > 999999)
                secs += 1;
            seconds_encoder.write(secs - epoch_offset);
            nanos_encoder.write(formatNanos(nano));
        }
        countValues(null_map, num_values);
    }

    void flush(std::vector<StreamData> & streams) override
    {
        ColumnWriter::flush(streams);
        seconds_encoder.flush();
        seconds_stream.finish();
        nanos_encoder.flush();
        nanos_stream.finish();
        streams.push_back({Proto::StreamKind::DATA, static_cast<UInt32>(column_id), seconds_stream.getOutput()});
        streams.push_back({Proto::StreamKind::SECONDARY, static_cast<UInt32>(column_id), nanos_stream.getOutput()});
    }

    size_t estimateSize() const override
    {
        return ColumnWriter::estimateSize() + seconds_stream.estimateSize() + nanos_stream.estimateSize();
    }

protected:
    void recordDataPositions(std::vector<UInt64> & positions) override
    {
        seconds_encoder.recordPosition(positions);
        nanos_encoder.recordPosition(positions);
    }

    void resetStreams() override
    {
        seconds_stream.reset();
        nanos_stream.reset();
    }

private:
    /// The trailing zeros are removed and their number is kept in the lowest 3 bits.
    static Int64 formatNanos(Int64 nanos)
    {
        if (nanos == 0)
            return 0;
        if (nanos % 100 != 0)
            return nanos << 3;
        nanos /= 100;
        Int64 trailing_zeros = 1;
        while (nanos % 10 == 0 && trailing_zeros < 7)
        {
            nanos /= 10;
            ++trailing_zeros;
        }
        return (nanos << 3) | trailing_zeros;
    }

    const std::optional<UInt32> scale;
    OutputByteStream seconds_stream;
    OutputByteStream nanos_stream;
    IntRleEncoderV2 seconds_encoder;
    IntRleEncoderV2 nanos_encoder;
    const bool is_utc;
    const Int64 epoch_offset;
};

class DecimalColumnWriter final : public PrimitiveColumnWriter
{
public:
    DecimalColumnWriter(const Type & type, WriterContext & context_)
        : PrimitiveColumnWriter(type, context_)
        , scale(static_cast<Int32>(type.getScale()))
        , value_stream(context.compressor, context.block_size)
        , scale_stream(context.compressor, context.block_size)
        , scale_encoder(scale_stream, true)
    {
        encoding_kind = Proto::ColumnEncodingKind::DIRECT_V2;
        init();
    }

    void add(const IColumn & column, const UInt8 * null_map, size_t offset, size_t num_values, const UInt8 * parent_null_map) override
    {
        ColumnWriter::add(column, null_map, offset, num_values, parent_null_map);
        /// `Decimal32`, `Decimal64` or `Decimal128`.
        const size_t width = column.sizeOfValueIfFixed();
        const char * data = column.getRawData().data();
        for (size_t i = 0; i < num_values; ++i)
        {
            if (null_map && null_map[i])
                continue;
            size_t row = offset + i;
            Int128 value;
            if (width == 16)
                value = unalignedLoad<Int128>(data + row * 16);
            else if (width == 8)
                value = unalignedLoad<Int64>(data + row * 8);
            else if (width == 4)
                value = unalignedLoad<Int32>(data + row * 4);
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected column {} for ORC decimal", column.getName());

            /// Zigzag-encoded base 128 varint.
            UInt128 encoded = (static_cast<UInt128>(value) << 1) ^ static_cast<UInt128>(value >> 127);
            while (encoded >= 0x80)
            {
                char byte = static_cast<char>(static_cast<UInt8>(encoded & 0x7F) | 0x80);
                value_stream.write(&byte, 1);
                encoded >>= 7;
            }
            char last = static_cast<char>(static_cast<UInt8>(encoded));
            value_stream.write(&last, 1);

            scale_encoder.write(scale);
            index_statistics.updateDecimal(value, scale);
        }
        countValues(null_map, num_values);
    }

    void flush(std::vector<StreamData> & streams) override
    {
        ColumnWriter::flush(streams);
        value_stream.finish();
        scale_encoder.flush();
        scale_stream.finish();
        streams.push_back({Proto::StreamKind::DATA, static_cast<UInt32>(column_id), value_stream.getOutput()});
        streams.push_back({Proto::StreamKind::SECONDARY, static_cast<UInt32>(column_id), scale_stream.getOutput()});
    }

    size_t estimateSize() const override
    {
        return ColumnWriter::estimateSize() + value_stream.estimateSize() + scale_stream.estimateSize();
    }

protected:
    void recordDataPositions(std::vector<UInt64> & positions) override
    {
        value_stream.recordPosition(positions);
        scale_encoder.recordPosition(positions);
    }

    void resetStreams() override
    {
        value_stream.reset();
        scale_stream.reset();
    }

private:
    const Int32 scale;
    OutputByteStream value_stream;
    OutputByteStream scale_stream;
    IntRleEncoderV2 scale_encoder;
};

std::unique_ptr<ColumnWriter> buildColumnWriter(const Type & type, const DataTypePtr & data_type_with_nulls, WriterContext & context)
{
    checkStackSize();

    /// The nulls are handled by `addColumn`.
    DataTypePtr data_type = removeNullable(data_type_with_nulls);

    switch (type.getKind())
    {
        case BOOLEAN:
            return std::make_unique<BooleanColumnWriter>(type, context);
        case BYTE:
            return std::make_unique<ByteColumnWriter>(type, context);
        case SHORT:
        case INT:
        case LONG:
        case DATE:
            return std::make_unique<IntegerColumnWriter>(type, context);
        case FLOAT:
            return std::make_unique<FloatingColumnWriter<float>>(type, context);
        case DOUBLE:
            return std::make_unique<FloatingColumnWriter<double>>(type, context);
        case STRING:
        case BINARY:
        case CHAR:
        case VARCHAR:
            return std::make_unique<StringColumnWriter>(type, context);
        case TIMESTAMP:
        case TIMESTAMP_INSTANT:
        {
            std::optional<UInt32> scale;
            if (const auto * datetime64 = typeid_cast<const DataTypeDateTime64 *>(data_type.get()))
                scale = datetime64->getScale();
            return std::make_unique<TimestampColumnWriter>(type, scale, context);
        }
        case DECIMAL:
            return std::make_unique<DecimalColumnWriter>(type, context);
        case STRUCT:
            return std::make_unique<StructColumnWriter>(type, assert_cast<const DataTypeTuple &>(*data_type).getElements(), context);
        case LIST:
            return std::make_unique<CollectionColumnWriter<false>>(
                type, DataTypes{assert_cast<const DataTypeArray &>(*data_type).getNestedType()}, context);
        case MAP:
        {
            const auto & map_type = assert_cast<const DataTypeMap &>(*data_type);
            return std::make_unique<CollectionColumnWriter<true>>(type, DataTypes{map_type.getKeyType(), map_type.getValueType()}, context);
        }
        case UNION:
            return std::make_unique<UnionColumnWriter>(type, assert_cast<const DataTypeVariant &>(*data_type).getVariants(), context);
        case GEOMETRY:
        case GEOGRAPHY:
            break;
    }
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Writing ORC type {} is not supported", type.toString());
}

}


Writer::Writer(const Type & schema_, const DataTypes & column_types, OutputStream & out_, const WriterOptions & options_)
    : out(out_), options(options_)
{
    if (options.compression_block_size == 0 || options.compression_block_size >= (1ULL << 23))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "The ORC compression block size must be positive and less than 8 MiB, got {}", options.compression_block_size);

    schema = Type::buildSelectedType(schema_, std::vector<bool>(schema_.getMaximumColumnId() + 1, true));
    schema->assignIds(0);

    compressor = createCompressor(options.compression);

    context = std::make_unique<WriterContext>();
    context->compressor = compressor.get();
    context->block_size = options.compression_block_size;
    context->enable_index = options.row_index_stride > 0;
    context->dictionary_key_size_threshold = options.dictionary_key_size_threshold;
    context->timezone = &DateLUT::instance(options.timezone_name);
    context->timezone_is_utc = options.timezone_name == "GMT" || options.timezone_name == "UTC";
    static constexpr Int64 UTC_2015_01_01 = 1420070400;
    context->epoch_offset = UTC_2015_01_01 - context->timezone->timezoneOffset(UTC_2015_01_01);

    if (schema->getKind() != STRUCT)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The root type of ORC file must be a struct");
    column_writer = std::make_unique<StructColumnWriter>(*schema, column_types, *context);

    write("ORC");
    footer.header_length = current_offset;
    footer.row_index_stride = static_cast<UInt32>(options.row_index_stride);
    /// The identifier of the C++ writer of the reference implementation, which this writer follows.
    footer.writer = 1;
    footer.software_version = "ClickHouse";
    schema->toProto(footer.types);
}

Writer::~Writer() = default;

void Writer::write(const std::string & data)
{
    out.write(data.data(), data.size());
    current_offset += data.size();
}

void Writer::add(const Columns & columns, size_t num_rows)
{
    auto & root = typeid_cast<StructColumnWriter &>(*column_writer);

    if (context->enable_index)
    {
        size_t position = 0;
        while (position < num_rows)
        {
            size_t chunk = std::min<size_t>(num_rows - position, options.row_index_stride - index_rows);
            root.addRoot(columns, position, chunk);
            position += chunk;
            index_rows += chunk;
            stripe_rows += chunk;
            if (index_rows >= options.row_index_stride)
            {
                column_writer->createRowIndexEntry();
                index_rows = 0;
            }
        }
    }
    else
    {
        stripe_rows += num_rows;
        root.addRoot(columns, 0, num_rows);
    }

    if (column_writer->estimateSize() >= options.stripe_size)
        writeStripe();
}

void Writer::writeStripe()
{
    if (context->enable_index && index_rows != 0)
    {
        column_writer->createRowIndexEntry();
        index_rows = 0;
    }
    else
        column_writer->mergeRowGroupStatsIntoStripeStats();

    column_writer->finalizeStripe();

    std::vector<StreamData> index_streams;
    std::vector<StreamData> data_streams;
    if (context->enable_index)
        column_writer->writeIndex(index_streams);
    column_writer->flush(data_streams);

    Proto::StripeFooter stripe_footer;
    UInt64 index_length = 0;
    UInt64 data_length = 0;
    for (const auto & s : index_streams)
    {
        stripe_footer.streams.push_back({static_cast<UInt32>(s.kind), s.column, s.data.size()});
        index_length += s.data.size();
    }
    for (const auto & s : data_streams)
    {
        stripe_footer.streams.push_back({static_cast<UInt32>(s.kind), s.column, s.data.size()});
        data_length += s.data.size();
    }
    column_writer->getColumnEncodings(stripe_footer.columns);
    stripe_footer.writer_timezone = options.timezone_name;

    auto & stripe_statistics = metadata.stripe_stats.emplace_back();
    column_writer->getStripeStatistics(stripe_statistics.col_stats);
    column_writer->mergeStripeStatsIntoFileStats();

    Proto::StripeInformation stripe;
    stripe.offset = current_offset;

    for (const auto & s : index_streams)
        write(s.data);
    for (const auto & s : data_streams)
        write(s.data);

    std::string footer_data = compressWhole(*context, stripe_footer.serialize());
    write(footer_data);

    stripe.index_length = index_length;
    stripe.data_length = data_length;
    stripe.footer_length = footer_data.size();
    stripe.number_of_rows = stripe_rows;
    footer.stripes.push_back(stripe);

    total_rows += stripe_rows;
    stripe_rows = 0;
    index_rows = 0;
    (*column_writer).reset();
}

void Writer::close()
{
    if (stripe_rows > 0)
        writeStripe();

    Proto::PostScript postscript;

    std::string metadata_data = compressWhole(*context, metadata.serialize());
    write(metadata_data);
    postscript.metadata_length = metadata_data.size();

    footer.content_length = current_offset - footer.header_length;
    footer.number_of_rows = total_rows;
    footer.statistics.clear();
    column_writer->getFileStatistics(footer.statistics);
    std::string footer_data = compressWhole(*context, footer.serialize());
    write(footer_data);

    postscript.footer_length = footer_data.size();
    postscript.compression = options.compression;
    postscript.compression_block_size = options.compression_block_size;
    postscript.version = {0, 12};
    /// ORC-135: the timestamp statistics are in UTC.
    postscript.writer_version = 6;
    postscript.magic = "ORC";
    std::string postscript_data = postscript.serialize();
    if (postscript_data.size() > 255)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ORC postscript is too large: {} bytes", postscript_data.size());
    write(postscript_data);
    write(std::string(1, static_cast<char>(postscript_data.size())));
}

}
