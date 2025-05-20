#pragma once

#include <Processors/Formats/Impl/Parquet/ReadCommon.h>

#include <IO/VarInt.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>

namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_DATA;
    extern const int CANNOT_DECOMPRESS;
    extern const int FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME;
}

namespace DB::Parquet
{

struct PageDecoderInfo;

struct Dictionary
{
    enum class Mode
    {
        Uninitialized,

        FixedSize,
        /// `data` strings with 4-byte length prefixes, `offsets` points to the end of each string.
        StringPlain,
    };

    Mode mode = Mode::Uninitialized;
    size_t value_size = 0; // if fixed_size
    PaddedPODArray<UInt32> offsets; // if !fixed_size
    size_t count = 0;

    /// Points into `col`, or `decompressed_buf`, or into Prefetcher's memory (kept alive by dictionary_page_prefetch).
    std::span<const char> data;

    PaddedPODArray<char> decompressed_buf;
    ColumnPtr col;

    void reset();

    bool isInitialized() const;

    double getAverageValueSize() const;

    void index(const PaddedPODArray<UInt32> & indexes, IColumn & out);

    void decode(parq::Encoding::type encoding, const PageDecoderInfo & info, size_t num_values, std::span<const char> data_, const IDataType & raw_decoded_type);
};

struct PageDecoder
{
    virtual void skip(size_t num_values) = 0;
    virtual void decode(size_t num_values, IColumn &) = 0;

    explicit PageDecoder(std::span<const char> data_) : data(data_.data()), end(data_.data() + data_.size()) {}
    virtual ~PageDecoder() = default;

    const char * data = nullptr;
    const char * end = nullptr;

    void requireRemainingBytes(size_t n)
    {
        if (size_t(end - data) < n)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpectd end of page data");
    }
};

/// We choose PageDecoder implementation in two steps:
///  1. during schema conversion we create PageDecoderInfo (this should be in schema conversion because
///    that's where column type and data type are decided, and they should match the decoder type);
///  2. after reading page header, the Encoding becomes known, and we create a PageDecoder.
struct PageDecoderInfo
{
    enum class Kind
    {
        FixedSize,
        String,
        ShortInt, // convert Int32 to UInt8 or UInt16
        Boolean,
    };

    Kind kind = Kind::FixedSize;
    size_t value_size = 0; // if FixedSize or ShortInt

    /// True if we can decompress the whole page directly into IColumn's memory.
    bool canReadDirectlyIntoColumn(parq::Encoding::type, size_t /*num_values*/, IColumn &, std::span<char> & out) const;

    /// [data, end) must be padded, i.e. have at least PADDING_FOR_SIMD bytes of readable memory
    /// before `data` and after `end`.
    std::unique_ptr<PageDecoder> makeDecoder(parq::Encoding::type, std::span<const char> data) const;
};

void decodeRepOrDefLevels(parq::Encoding::type encoding, UInt8 max, size_t num_values, std::span<const char> data, PaddedPODArray<UInt8> & out);

std::unique_ptr<PageDecoder> makeDictionaryIndicesDecoder(parq::Encoding::type encoding, size_t dictionary_size, std::span<const char> data);

}
