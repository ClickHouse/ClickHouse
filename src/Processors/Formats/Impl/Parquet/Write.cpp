#include <Processors/Formats/Impl/Parquet/Write.h>
#include <Processors/Formats/Impl/Parquet/ThriftUtil.h>
#include <lz4.h>
#include <zlib.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <xxhash.h>
#include <DataTypes/DataTypeObject.h>
#include <Columns/MaskOperations.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnObject.h>
#include <IO/WriteHelpers.h>
#include <IO/Libdeflate.h>
#include <Common/WKB.h>
#include <Common/config_version.h>
#include <base/arithmeticOverflow.h>
#include <Common/formatReadable.h>
#include <Common/Arena.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <base/unaligned.h>
#include <bit>
#include <cmath>
#include <limits>
#include <set>
#include <DataTypes/DataTypeEnum.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeCustom.h>

#if USE_SNAPPY
#include <snappy.h>
#endif

namespace DB::ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int LIMIT_EXCEEDED;
    extern const int LOGICAL_ERROR;
    extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
}

namespace DB::Parquet
{

namespace
{

/// Values of Parquet physical types, as the converters below produce them.

struct ByteArray
{
    UInt32 len = 0;
    const uint8_t * ptr = nullptr;

    ByteArray() = default;
    ByteArray(UInt32 len_, const uint8_t * ptr_) : len(len_), ptr(ptr_) {}
};

/// The length is the same for all values of a column and is known from the converter.
struct FixedLenByteArray
{
    const uint8_t * ptr = nullptr;
};

/// Parquet physical types, by the type of their values.
struct BooleanType { using c_type = bool; };
struct Int32Type { using c_type = Int32; };
struct Int64Type { using c_type = Int64; };
struct FloatType { using c_type = Float32; };
struct DoubleType { using c_type = Float64; };
struct ByteArrayType { using c_type = ByteArray; };
struct FLBAType { using c_type = FixedLenByteArray; };

template <typename T, typename SourceType>
struct StatisticsNumeric
{
    T min = std::numeric_limits<T>::has_infinity
        ? std::numeric_limits<T>::infinity() : std::numeric_limits<T>::max();
    T max = std::numeric_limits<T>::has_infinity
        ? -std::numeric_limits<T>::infinity() : std::numeric_limits<T>::lowest();

    void add(SourceType x)
    {
        min = std::min(min, static_cast<T>(x));
        max = std::max(max, static_cast<T>(x));
    }

    void merge(const StatisticsNumeric & s)
    {
        min = std::min(min, s.min);
        max = std::max(max, s.max);
    }

    void clear() { *this = {}; }

    parq::Statistics get(const WriteOptions &)
    {
        parq::Statistics s;
        if (min > max) // empty
            return s;
        s.__isset.min_value = s.__isset.max_value = true;
        s.min_value.resize(sizeof(T));
        s.max_value.resize(sizeof(T));
        memcpy(s.min_value.data(), &min, sizeof(T));
        memcpy(s.max_value.data(), &max, sizeof(T));
        s.__set_is_min_value_exact(true);
        s.__set_is_max_value_exact(true);

        if constexpr (std::is_signed_v<T>)
        {
            s.__set_min(s.min_value);
            s.__set_max(s.max_value);
        }
        return s;
    }
};

struct StatisticsFixedStringRef
{
    size_t fixed_string_size = UINT64_MAX;
    const uint8_t * min = nullptr;
    const uint8_t * max = nullptr;

    void add(FixedLenByteArray a)
    {
        chassert(fixed_string_size != UINT64_MAX);
        addMin(a.ptr);
        addMax(a.ptr);
    }

    void merge(const StatisticsFixedStringRef & s)
    {
        chassert(fixed_string_size == UINT64_MAX || fixed_string_size == s.fixed_string_size);
        fixed_string_size = s.fixed_string_size;
        if (s.min == nullptr)
            return;
        addMin(s.min);
        addMax(s.max);
    }

    void clear() { min = max = nullptr; }

    parq::Statistics get(const WriteOptions & options) const
    {
        parq::Statistics s;
        if (min == nullptr || fixed_string_size > options.max_statistics_size)
            return s;
        s.__set_min_value(std::string(reinterpret_cast<const char *>(min), fixed_string_size));
        s.__set_max_value(std::string(reinterpret_cast<const char *>(max), fixed_string_size));
        s.__set_is_min_value_exact(true);
        s.__set_is_max_value_exact(true);
        return s;
    }

    void addMin(const uint8_t * p)
    {
        if (min == nullptr || memcmp(p, min, fixed_string_size) < 0)
            min = p;
    }
    void addMax(const uint8_t * p)
    {
        if (max == nullptr || memcmp(p, max, fixed_string_size) > 0)
            max = p;
    }
};

/// If SIGNED, compare as signed big endian integers.
template<size_t S, bool SIGNED>
struct StatisticsFixedStringCopy
{
    bool empty = true;
    std::array<uint8_t, S> min {};
    std::array<uint8_t, S> max {};

    void add(FixedLenByteArray a)
    {
        addMin(a.ptr);
        addMax(a.ptr);
        empty = false;
    }

    void merge(const StatisticsFixedStringCopy<S, SIGNED> & s)
    {
        if (s.empty)
            return;
        addMin(s.min.data());
        addMax(s.max.data());
        empty = false;
    }

    void clear() { empty = true; }

    parq::Statistics get(const WriteOptions &) const
    {
        parq::Statistics s;
        if (empty)
            return s;
        s.__set_min_value(std::string(reinterpret_cast<const char *>(min.data()), S));
        s.__set_max_value(std::string(reinterpret_cast<const char *>(max.data()), S));
        s.__set_is_min_value_exact(true);
        s.__set_is_max_value_exact(true);
        return s;
    }

    inline static int compare(const uint8_t * lhs, const uint8_t * rhs)
    {
        if constexpr (SIGNED)
            /// Comparing the first byte as signed is sufficient.
            if (*lhs != *rhs) return int(int8_t(*lhs)) - int(int8_t(*rhs));
        return memcmp(lhs, rhs, S);
    }

    void addMin(const uint8_t * p)
    {
        if (empty || compare(p, min.data()) < 0)
            memcpy(min.data(), p, S);
    }
    void addMax(const uint8_t * p)
    {
        if (empty || compare(p, max.data()) > 0)
            memcpy(max.data(), p, S);
    }
};

struct StatisticsStringRef
{
    ByteArray min;
    ByteArray max;

    void add(ByteArray x)
    {
        addMin(x);
        addMax(x);
    }

    void merge(const StatisticsStringRef & s)
    {
        if (s.min.ptr == nullptr)
            return;
        addMin(s.min);
        addMax(s.max);
    }

    void clear() { *this = {}; }

    parq::Statistics get(const WriteOptions & options) const
    {
        parq::Statistics s;
        if (min.ptr == nullptr)
            return s;
        if (static_cast<size_t>(min.len) <= options.max_statistics_size
            && static_cast<size_t>(max.len) <= options.max_statistics_size)
        {
            s.__set_min_value(std::string(reinterpret_cast<const char *>(min.ptr), static_cast<size_t>(min.len)));
            s.__set_max_value(std::string(reinterpret_cast<const char *>(max.ptr), static_cast<size_t>(max.len)));
            s.__set_is_min_value_exact(true);
            s.__set_is_max_value_exact(true);
        }
        return s;
    }

    void addMin(ByteArray x)
    {
        if (min.ptr == nullptr || compare(x, min) < 0)
            min = x;
    }

    void addMax(ByteArray x)
    {
        if (max.ptr == nullptr || compare(x, max) > 0)
            max = x;
    }

    static int compare(ByteArray a, ByteArray b)
    {
        int t = memcmp(a.ptr, b.ptr, std::min(a.len, b.len));
        if (t != 0)
            return t;
        return int(a.len) - int(b.len);
    }
};

struct StatisticsStringCopy
{
    bool empty = true;
    String min;
    String max;

    void add(ByteArray x)
    {
        addMin(x);
        addMax(x);
        empty = false;
    }

    void merge(const StatisticsStringCopy & s)
    {
        if (s.empty)
            return;
        addMin(ByteArray(static_cast<UInt32>(s.min.size()), reinterpret_cast<const uint8_t *>(s.min.data())));
        addMax(ByteArray(static_cast<UInt32>(s.max.size()), reinterpret_cast<const uint8_t *>(s.max.data())));
        empty = false;
    }

    void clear() { *this = {}; }

    parq::Statistics get(const WriteOptions & options) const
    {
        parq::Statistics s;
        if (empty)
            return s;
        if (min.size() <= options.max_statistics_size
            && max.size() <= options.max_statistics_size)
        {
            s.__set_min_value(std::string(min.data(), min.size()));
            s.__set_max_value(std::string(max.data(), max.size()));
            s.__set_is_min_value_exact(true);
            s.__set_is_max_value_exact(true);
        }
        return s;
    }

    void addMin(ByteArray x)
    {
        if (empty || compare(x, min) < 0)
        {
            // assign to String to make a copy only when we update min
            min.assign(reinterpret_cast<const char *>(x.ptr), x.len);
        }
    }

    void addMax(ByteArray x)
    {
        if (empty || compare(x, max) > 0)
        {
            // assign to String to make a copy only when we update max
            max.assign(reinterpret_cast<const char *>(x.ptr), x.len);
        }
    }

    static int compare(ByteArray a, const String & b)
    {
        int t = memcmp(a.ptr, b.data(), std::min(a.len, static_cast<UInt32>(b.size())));
        if (t != 0)
            return t;
        return int(a.len) - int(b.size());
    }
};

/// For WKB values of a `GEOMETRY` column: the sort order of this logical type is undefined, so no
/// min/max are produced. Instead, the bounding box and the set of geometry types are accumulated
/// for `GeospatialStatistics` of the column chunk.
/// The semantics follow `parquet::geospatial::GeoStatistics` of Arrow: ISO WKB with Z, M and ZM
/// variants of the types is accepted, and if any value is not a valid WKB, no statistics are written.
struct StatisticsGeometry
{
    static constexpr double inf = std::numeric_limits<double>::infinity();

    /// X, Y, Z, M.
    std::array<double, 4> min {inf, inf, inf, inf};
    std::array<double, 4> max {-inf, -inf, -inf, -inf};
    /// WKB geometry type codes of the top-level values, e.g. 1 for `POINT`, 3002 for `LINESTRING ZM`.
    std::set<Int32> geometry_types;
    bool is_valid = true;

    void add(ByteArray x)
    {
        if (!is_valid)
            return;
        const uint8_t * pos = x.ptr;
        const uint8_t * end = x.ptr + x.len;
        if (!addGeometry(pos, end, /*record_type=*/ true, /*depth=*/ 0) || pos != end)
            is_valid = false;
    }

    void merge(const StatisticsGeometry & s)
    {
        is_valid = is_valid && s.is_valid;
        if (!is_valid)
            return;
        for (size_t i = 0; i < 4; ++i)
        {
            if (std::isnan(min[i]) || std::isnan(max[i]) || std::isnan(s.min[i]) || std::isnan(s.max[i]))
            {
                min[i] = std::numeric_limits<double>::quiet_NaN();
                max[i] = std::numeric_limits<double>::quiet_NaN();
            }
            else
            {
                min[i] = std::min(min[i], s.min[i]);
                max[i] = std::max(max[i], s.max[i]);
            }
        }
        geometry_types.insert(s.geometry_types.begin(), s.geometry_types.end());
    }

    void clear()
    {
        *this = StatisticsGeometry();
    }

    parq::Statistics get(const WriteOptions &) const
    {
        return {};
    }

    /// Returns nullopt if a value could not be parsed as WKB, then no statistics are written.
    std::optional<parq::GeospatialStatistics> getGeospatial() const
    {
        if (!is_valid)
            return std::nullopt;

        parq::GeospatialStatistics res;
        /// An empty list means that the types are unknown, this is the case only if all values are NULL.
        res.__set_geospatial_types(std::vector<Int32>(geometry_types.begin(), geometry_types.end()));

        const bool write_x = hasBound(0);
        const bool write_y = hasBound(1);
        if (write_x && write_y)
        {
            parq::BoundingBox bbox;
            bbox.__set_xmin(min[0]);
            bbox.__set_xmax(max[0]);
            bbox.__set_ymin(min[1]);
            bbox.__set_ymax(max[1]);
            if (hasBound(2))
            {
                bbox.__set_zmin(min[2]);
                bbox.__set_zmax(max[2]);
            }
            if (hasBound(3))
            {
                bbox.__set_mmin(min[3]);
                bbox.__set_mmax(max[3]);
            }
            res.__set_bbox(bbox);
        }
        return res;
    }

private:
    /// The bound is neither empty (no coordinates seen) nor invalid (NaN).
    bool hasBound(size_t i) const
    {
        return !std::isinf(min[i] - max[i]) && !std::isnan(min[i]) && !std::isnan(max[i]);
    }

    template <typename T>
    static bool read(const uint8_t *& pos, const uint8_t * end, bool little_endian, T & out)
    {
        if (static_cast<size_t>(end - pos) < sizeof(T))
            return false;
        out = little_endian ? unalignedLoadEndian<std::endian::little, T>(pos) : unalignedLoadEndian<std::endian::big, T>(pos);
        pos += sizeof(T);
        return true;
    }

    /// `dimensions`: 0 - XY, 1 - XYZ, 2 - XYM, 3 - XYZM.
    bool addCoordinates(const uint8_t *& pos, const uint8_t * end, bool little_endian, UInt32 dimensions, UInt32 count)
    {
        const size_t num_values = (dimensions == 0) ? 2 : (dimensions == 3 ? 4 : 3);
        if (static_cast<UInt64>(end - pos) < static_cast<UInt64>(count) * num_values * sizeof(Float64))
            return false;
        for (UInt32 i = 0; i < count; ++i)
        {
            Float64 coord[4];
            for (size_t j = 0; j < num_values; ++j)
                read(pos, end, little_endian, coord[j]);
            /// For XYM, the third value is M.
            if (dimensions == 2)
            {
                coord[3] = coord[2];
                updateBound(0, coord[0]);
                updateBound(1, coord[1]);
                updateBound(3, coord[3]);
            }
            else
            {
                for (size_t j = 0; j < num_values; ++j)
                    updateBound(j, coord[j]);
            }
        }
        return true;
    }

    void updateBound(size_t i, Float64 value)
    {
        /// The order of the arguments matters for NaN: a NaN coordinate does not change the bounds.
        min[i] = std::min(min[i], value);
        max[i] = std::max(max[i], value);
    }

    bool addGeometry(const uint8_t *& pos, const uint8_t * end, bool record_type, size_t depth)
    {
        if (depth > 128)
            return false;

        UInt8 endian = 0;
        if (!read(pos, end, true, endian))
            return false;
        const bool little_endian = endian == 0x01;

        UInt32 wkb_type = 0;
        if (!read(pos, end, little_endian, wkb_type))
            return false;
        const UInt32 geometry_type = wkb_type % 1000;
        const UInt32 dimensions = wkb_type / 1000;
        if (geometry_type < 1 || geometry_type > 7 || dimensions > 3)
            return false;

        if (record_type)
            geometry_types.insert(static_cast<Int32>(wkb_type));

        switch (geometry_type)
        {
            case 1: /// POINT
                return addCoordinates(pos, end, little_endian, dimensions, 1);
            case 2: /// LINESTRING
            {
                UInt32 count = 0;
                return read(pos, end, little_endian, count) && addCoordinates(pos, end, little_endian, dimensions, count);
            }
            case 3: /// POLYGON
            {
                UInt32 num_rings = 0;
                if (!read(pos, end, little_endian, num_rings))
                    return false;
                for (UInt32 i = 0; i < num_rings; ++i)
                {
                    UInt32 count = 0;
                    if (!read(pos, end, little_endian, count) || !addCoordinates(pos, end, little_endian, dimensions, count))
                        return false;
                }
                return true;
            }
            default: /// MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, GEOMETRYCOLLECTION
            {
                UInt32 num_parts = 0;
                if (!read(pos, end, little_endian, num_parts))
                    return false;
                /// Only the type of the top-level geometry is recorded, e.g. `MULTIPOINT`, not `POINT`.
                for (UInt32 i = 0; i < num_parts; ++i)
                    if (!addGeometry(pos, end, /*record_type=*/ false, depth + 1))
                        return false;
                return true;
            }
        }
    }
};

/// The column usually needs to be converted to one of Parquet physical types, e.g. UInt16 -> Int32
/// or [element of ColumnString] -> std::string_view.
/// We do this conversion in small batches rather than all at once, just before encoding the batch,
/// in hopes of getting better performance through cache locality.
/// The Converter* structs below are responsible for that.
/// When conversion is not needed, getBatch() will just return pointer into original data.

template <typename Col, typename To, typename MinMaxType = typename std::conditional_t<
        std::is_signed_v<typename Col::Container::value_type>,
        To,
        typename std::make_unsigned_t<To>>>
struct ConverterNumeric
{
    using Statistics = StatisticsNumeric<MinMaxType, To>;

    const Col & column;
    PODArray<To> buf;

    explicit ConverterNumeric(const ColumnPtr & c) : column(assert_cast<const Col &>(*c)) {}

    const To * getBatch(size_t offset, size_t count)
    {
        if constexpr (sizeof(*column.getData().data()) == sizeof(To) && !std::is_same_v<To, bool>)
            return reinterpret_cast<const To *>(column.getData().data() + offset);
        else
        {
            buf.resize(count);
            for (size_t i = 0; i < count; ++i)
                buf[i] = static_cast<To>(column.getData()[offset + i]); // NOLINT
            return buf.data();
        }
    }
};

struct ConverterDateTime64WithMultiplier
{
    using Statistics = StatisticsNumeric<Int64, Int64>;

    using Col = ColumnDecimal<DateTime64>;
    const Col & column;
    Int64 multiplier;
    PODArray<Int64> buf;

    ConverterDateTime64WithMultiplier(const ColumnPtr & c, Int64 multiplier_) : column(assert_cast<const Col &>(*c)), multiplier(multiplier_) {}

    const Int64 * getBatch(size_t offset, size_t count)
    {
        buf.resize(count);
        for (size_t i = 0; i < count; ++i)
        {
            Int64 value = column.getData()[offset + i].value;
            if (common::mulOverflow(value, multiplier, buf[i]))
                throw Exception(
                    ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE,
                    "DateTime64 value {} is out of range for Parquet timestamp (multiplier {})",
                    value, multiplier);
        }
        return buf.data();
    }
};

/// Multiply DateTime by 1000 to get milliseconds (because Parquet doesn't support seconds).
struct ConverterDateTime
{
    using Statistics = StatisticsNumeric<Int64, Int64>;

    using Col = ColumnVector<UInt32>;
    const Col & column;
    PODArray<Int64> buf;

    explicit ConverterDateTime(const ColumnPtr & c) : column(assert_cast<const Col &>(*c)) {}

    const Int64 * getBatch(size_t offset, size_t count)
    {
        buf.resize(count);
        for (size_t i = 0; i < count; ++i)
            buf[i] = static_cast<Int64>(column.getData()[offset + i]) * 1000;
        return buf.data();
    }
};

struct ConverterString
{
    using Statistics = StatisticsStringRef;

    const ColumnString & column;
    PODArray<ByteArray> buf;

    explicit ConverterString(const ColumnPtr & c) : column(assert_cast<const ColumnString &>(*c)) {}

    const ByteArray * getBatch(size_t offset, size_t count)
    {
        buf.resize(count);
        for (size_t i = 0; i < count; ++i)
        {
            std::string_view s = column.getDataAt(offset + i);
            buf[i] = ByteArray(static_cast<UInt32>(s.size()), reinterpret_cast<const uint8_t *>(s.data()));
        }
        return buf.data();
    }
};

/// WKB values of a `GEOMETRY` column.
struct ConverterGeometry : ConverterString
{
    using Statistics = StatisticsGeometry;

    using ConverterString::ConverterString;
};

template <typename T>
struct ConverterEnumAsString
{
    using Statistics = StatisticsStringRef;

    explicit ConverterEnumAsString(const ColumnPtr & c, const DataTypePtr & enum_type_)
    : column(assert_cast<const ColumnVector<T> &>(*c)), enum_type(assert_cast<const DataTypeEnum<T> *>(enum_type_.get())) {}

    const ColumnVector<T> & column;
    const DataTypeEnum<T> * enum_type;
    PODArray<ByteArray> buf;

    const ByteArray * getBatch(size_t offset, size_t count)
    {
        buf.resize(count);

        const auto & data = column.getData();

        for (size_t i = 0; i < count; ++i)
        {
            const T value = data[offset + i];
            const std::string_view s = enum_type->getNameForValue(value);
            buf[i] = ByteArray(static_cast<UInt32>(s.size()), reinterpret_cast<const uint8_t *>(s.data()));
        }
        return buf.data();
    }
};

struct ConverterUUID
{
    /// Use ...Copy, not ...Ref: each batch reuses `swapped_buf` storage which can be reallocated
    /// between successive `getBatch` calls (when `data_count` exceeds the current capacity, e.g.
    /// after a low-density null run). Statistics that hold raw pointers into `swapped_buf` would
    /// then dereference freed memory when merging the next page's stats — a heap-use-after-free.
    /// `StatisticsFixedStringCopy` keeps min/max as inline 16-byte arrays, so no dangling refs.
    using Statistics = StatisticsFixedStringCopy<sizeof(UUID), /*SIGNED=*/ false>;

    const ColumnVector<UUID> & column;
    PODArray<FixedLenByteArray> buf;
    PODArray<UUID> swapped_buf;

    explicit ConverterUUID(const ColumnPtr & c) : column(assert_cast<const ColumnVector<UUID> &>(*c)) {}

    const FixedLenByteArray * getBatch(size_t offset, size_t count)
    {
        buf.resize(count);
        swapped_buf.resize(count);

        for (size_t i = 0; i < count; ++i)
        {
            UUID res = column.getData()[offset + i];
            auto * bytes = reinterpret_cast<uint8_t *>(&res);

            if constexpr (std::endian::native == std::endian::little)
            {
                std::reverse(bytes, bytes + 8);
                std::reverse(bytes + 8, bytes + 16);
            }
            else
            {
                std::swap_ranges(bytes, bytes + 8, bytes + 8);
            }

            swapped_buf[i] = res;
            buf[i].ptr = reinterpret_cast<const uint8_t *>(&swapped_buf[i]);
        }
        return buf.data();
    }

    size_t fixedStringSize() { return 16; }
};

struct ConverterFixedString
{
    using Statistics = StatisticsFixedStringRef;

    const ColumnFixedString & column;
    PODArray<FixedLenByteArray> buf;

    explicit ConverterFixedString(const ColumnPtr & c) : column(assert_cast<const ColumnFixedString &>(*c)) {}

    const FixedLenByteArray * getBatch(size_t offset, size_t count)
    {
        buf.resize(count);
        for (size_t i = 0; i < count; ++i)
            buf[i].ptr = reinterpret_cast<const uint8_t *>(column.getChars().data() + (offset + i) * column.getN());
        return buf.data();
    }

    size_t fixedStringSize() { return column.getN(); }
};

struct ConverterFixedStringAsString
{
    using Statistics = StatisticsStringRef;

    const ColumnFixedString & column;
    PODArray<ByteArray> buf;

    explicit ConverterFixedStringAsString(const ColumnPtr & c) : column(assert_cast<const ColumnFixedString &>(*c)) {}

    const ByteArray * getBatch(size_t offset, size_t count)
    {
        buf.resize(count);
        for (size_t i = 0; i < count; ++i)
            buf[i] = ByteArray(static_cast<UInt32>(column.getN()), reinterpret_cast<const uint8_t *>(column.getChars().data() + (offset + i) * column.getN()));
        return buf.data();
    }
};

template <typename T>
struct ConverterNumberAsFixedString
{
    /// Calculate min/max statistics for little-endian fixed strings, not numbers, because parquet
    /// doesn't know it's numbers.
    using Statistics = StatisticsFixedStringCopy<sizeof(T), /*SIGNED=*/ false>;

    const ColumnVector<T> & column;
    PODArray<FixedLenByteArray> buf;

    explicit ConverterNumberAsFixedString(const ColumnPtr & c) : column(assert_cast<const ColumnVector<T> &>(*c)) {}

    const FixedLenByteArray * getBatch(size_t offset, size_t count)
    {
        buf.resize(count);
        for (size_t i = 0; i < count; ++i)
            buf[i].ptr = reinterpret_cast<const uint8_t *>(column.getData().data() + offset + i);
        return buf.data();
    }

    size_t fixedStringSize() { return sizeof(T); }
};

/// Serialize a wide integer as a Parquet fixed-length `DECIMAL`. The extra leading byte preserves
/// the full unsigned range and sign-extends signed values. Building the result byte by byte makes
/// the conversion independent of host endianness.
template <typename T>
struct ConverterWideIntegerAsDecimal
{
    static constexpr size_t encoded_size = sizeof(T) + 1;
    static constexpr size_t num_limbs = sizeof(T) / sizeof(UInt64);
    static_assert(sizeof(T) % sizeof(UInt64) == 0);
    using Statistics = StatisticsFixedStringCopy<encoded_size, /*SIGNED=*/ true>;

    const ColumnVector<T> & column;
    PODArray<uint8_t> data_buf;
    PODArray<FixedLenByteArray> ptr_buf;

    explicit ConverterWideIntegerAsDecimal(const ColumnPtr & c) : column(assert_cast<const ColumnVector<T> &>(*c)) {}

    const FixedLenByteArray * getBatch(size_t offset, size_t count)
    {
        data_buf.resize(count * encoded_size);
        ptr_buf.resize(count);
        for (size_t i = 0; i < count; ++i)
        {
            uint8_t * out = data_buf.data() + i * encoded_size;
            const T & source = column.getData()[offset + i];
            if constexpr (std::numeric_limits<T>::is_signed)
                out[0] = source < 0 ? 0xff : 0;
            else
                out[0] = 0;

            /// `wide::integer` stores limbs in native order. Visit them from least to most
            /// significant and emit each limb from least to most significant byte into the output
            /// buffer backwards, producing big-endian bytes without repeatedly shifting `T`.
            for (size_t limb_idx = 0; limb_idx < num_limbs; ++limb_idx)
            {
                const size_t native_limb_idx = std::endian::native == std::endian::little
                    ? limb_idx
                    : num_limbs - limb_idx - 1;
                const UInt64 limb = source.items[native_limb_idx];
                for (size_t byte_idx = 0; byte_idx < sizeof(UInt64); ++byte_idx)
                {
                    const size_t output_idx = encoded_size - 1 - limb_idx * sizeof(UInt64) - byte_idx;
                    out[output_idx] = static_cast<uint8_t>(limb >> (byte_idx * 8));
                }
            }
            ptr_buf[i].ptr = out;
        }
        return ptr_buf.data();
    }

    size_t fixedStringSize() { return encoded_size; }
};

struct ConverterJSON
{
    using Statistics = StatisticsStringCopy;

    const ColumnObject & column;
    DataTypePtr data_type;
    /// Created once: for `Object` it is not a cheap accessor, and `valueSize` is called per value.
    SerializationPtr serialization;
    PODArray<ByteArray> buf;
    std::vector<String> stash;
    const FormatSettings & format_settings;

    explicit ConverterJSON(const ColumnPtr & c, const DataTypePtr & data_type_, const FormatSettings & format_settings_)
        : column(assert_cast<const ColumnObject &>(*c))
        , data_type(data_type_)
        , serialization(data_type->getDefaultSerialization())
        , format_settings(format_settings_)
    {
    }

    size_t batch_offset = 0;

    /// The values are serialized one at a time, so that a batch can be cut by bytes before all of
    /// its values are materialized: `startBatch` prepares up to `count` values, `valueSize` serializes
    /// the values up to the `i`-th, and `finishBatch` serializes the rest of the first `count` values.
    void startBatch(size_t offset, size_t count)
    {
        batch_offset = offset;
        buf.resize(count);
        stash.clear();
        /// The strings must not move: `buf` points into them.
        stash.reserve(count);
    }

    size_t valueSize(size_t i)
    {
        materializeUpTo(i + 1);
        return buf[i].len;
    }

    const ByteArray * finishBatch(size_t count)
    {
        materializeUpTo(count);
        return buf.data();
    }

private:
    void materializeUpTo(size_t count)
    {
        chassert(count <= buf.size());

        for (size_t i = stash.size(); i < count; ++i)
        {
            WriteBufferFromOwnString wb;
            serialization->serializeTextJSON(column, batch_offset + i, wb, format_settings);

            stash.emplace_back(std::move(wb.str()));
            const String & s = stash.back();

            buf[i] = ByteArray(static_cast<UInt32>(s.size()), reinterpret_cast<const uint8_t *>(s.data()));
        }
    }
};

/// Like ConverterNumberAsFixedString, but converts to big-endian. (Parquet uses little-endian
/// for INT32 and INT64, but big-endian for decimals represented as FIXED_LEN_BYTE_ARRAY.)
template <typename T>
struct ConverterDecimal
{
    using Statistics = StatisticsFixedStringCopy<sizeof(T), /*SIGNED=*/ true>;

    const ColumnDecimal<T> & column;
    PODArray<uint8_t> data_buf;
    PODArray<FixedLenByteArray> ptr_buf;

    explicit ConverterDecimal(const ColumnPtr & c) : column(assert_cast<const ColumnDecimal<T> &>(*c)) {}

    const FixedLenByteArray * getBatch(size_t offset, size_t count)
    {
        data_buf.resize(count * sizeof(T));
        ptr_buf.resize(count);
        memcpy(data_buf.data(), reinterpret_cast<const char *>(column.getData().data() + offset), count * sizeof(T));
        for (size_t i = 0; i < count; ++i)
        {
            std::reverse(data_buf.data() + i * sizeof(T), data_buf.data() + (i + 1) * sizeof(T));
            ptr_buf[i].ptr = data_buf.data() + i * sizeof(T);
        }
        return ptr_buf.data();
    }

    size_t fixedStringSize() { return sizeof(T); }
};

/// Returns either `source` or `scratch`.
PODArray<char> & compress(PODArray<char> & source, PODArray<char> & scratch, CompressionMethod method, int level)
{
    /// We could use wrapWriteBufferWithCompressionMethod() for everything, but I worry about the
    /// overhead of creating a bunch of WriteBuffers on each page (thousands of values).
#if USE_LIBDEFLATE
    /// One-shot libdeflate for gzip: the page is already fully in memory, and libdeflate is faster
    /// and compresses better than the streaming zlib path. Levels outside libdeflate's [1, 12]
    /// range (e.g. level 0 = store) keep using the streaming path below.
    if (method == CompressionMethod::Gzip && level >= 1 && level <= 12)
    {
        scratch.resize(Libdeflate::compressBound(method, level, source.size()));
        size_t compressed_size = Libdeflate::compress(method, level, source.data(), source.size(), scratch.data(), scratch.size());
        scratch.resize(compressed_size);
        return scratch;
    }
#endif

    switch (method)
    {
        case CompressionMethod::None:
            return source;

        case CompressionMethod::Lz4:
        {
            #pragma clang diagnostic push
            #pragma clang diagnostic ignored "-Wold-style-cast"

            size_t max_dest_size = LZ4_COMPRESSBOUND(source.size());

            #pragma clang diagnostic pop

            if (max_dest_size > std::numeric_limits<int>::max())
                throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress column of size {}", ReadableSize(source.size()));

            scratch.resize(max_dest_size);

            int compressed_size = LZ4_compress_default(
                source.data(),
                scratch.data(),
                static_cast<int>(source.size()),
                static_cast<int>(max_dest_size));

            scratch.resize(static_cast<size_t>(compressed_size));
            return scratch;
        }

#if USE_SNAPPY
        case CompressionMethod::Snappy:
        {
            size_t max_dest_size = snappy::MaxCompressedLength(source.size());

            if (max_dest_size > std::numeric_limits<int>::max())
                throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress column of size {}", formatReadableSizeWithBinarySuffix(source.size()));

            scratch.resize(max_dest_size);

            size_t compressed_size = 0;
            snappy::RawCompress(source.data(), source.size(), scratch.data(), &compressed_size);

            scratch.resize(compressed_size);
            return scratch;
        }
#endif

        default:
        {
            auto dest_buf = std::make_unique<WriteBufferFromVector<PODArray<char>>>(scratch);
            auto compressed_buf = wrapWriteBufferWithCompressionMethod(
                std::move(dest_buf),
                method,
                level,
                /*zstd_window_log*/ 0,
                /// Parquet's `SNAPPY` codec is raw block compression and is special-cased above —
                /// this dispatch never sees it, so the snappy mode here is irrelevant.
                SnappyMode::Basic,
                source.size(),
                /*existing_memory*/ source.data());
            chassert(compressed_buf->position() == source.data());
            chassert(compressed_buf->available() == source.size());
            compressed_buf->position() += source.size();
            compressed_buf->finalize();
            return scratch;
        }
    }
}

/// The most bytes the RLE / bit-packed hybrid encoding of `size` values of `bit_width` bits can take,
/// with some slack: the size of the longest single run on top.
size_t maxRLESize(size_t bit_width, size_t size)
{
    const size_t num_groups = (size + 7) / 8;
    const size_t value_bytes = (bit_width + 7) / 8;
    /// In the worst case, each group of 8 values is either bit-packed with an indicator byte of its own,
    /// or is a repeated run of its own: an indicator byte and the value.
    const size_t max_runs_size = num_groups * (1 + std::max(bit_width, value_bytes));
    /// A bit-packed run of the most groups an indicator byte can count, or a repeated run with the longest indicator.
    const size_t max_run_size = std::max(1 + 64 * bit_width, 5 + value_bytes);
    return max_runs_size + max_run_size;
}

/// The RLE / bit-packed hybrid encoding:
/// https://parquet.apache.org/docs/file-format/data-pages/encodings/#run-length-encoding--bit-packing-hybrid-rle--3
/// Values are taken in groups of 8. A value repeated at least 8 times, starting at the beginning of a
/// group, becomes a repeated run, everything else is bit-packed. This produces the same bytes as the
/// encoder of Apache Arrow.
class RLEEncoder
{
public:
    /// Appends the encoded values to `out_`.
    RLEEncoder(PODArray<char> & out_, size_t bit_width_) : out(out_), bit_width(bit_width_)
    {
        /// Bit-packing assumes this, see `flushLiteralRun`.
        chassert(bit_width <= 32);
    }

    void put(UInt64 value)
    {
        chassert(bit_width == 64 || value < (UInt64(1) << bit_width));

        if (value == current_value)
        {
            ++repeat_count;
            /// Continuation of a repeated run, the fast path for long runs.
            if (repeat_count > 8)
                return;
        }
        else
        {
            /// A repeated run, which was long enough, has ended.
            if (repeat_count >= 8)
            {
                chassert(literal_count == 0);
                flushRepeatedRun();
            }
            repeat_count = 1;
            current_value = value;
        }

        buffered_values[num_buffered_values] = value;
        ++num_buffered_values;
        if (num_buffered_values == 8)
            flushBufferedValues();
    }

    /// Writes out everything that is buffered. The encoder must not be used after that.
    void flush()
    {
        if (literal_count == 0 && repeat_count == 0 && num_buffered_values == 0)
            return;

        bool all_repeat = literal_count == 0 && (repeat_count == num_buffered_values || num_buffered_values == 0);
        if (repeat_count > 0 && all_repeat)
        {
            flushRepeatedRun();
        }
        else
        {
            /// Pad the last group with zeros: the reader knows the number of values.
            if (num_buffered_values != 0)
            {
                for (; num_buffered_values < 8; ++num_buffered_values)
                    buffered_values[num_buffered_values] = 0;
            }
            literal_count += num_buffered_values;
            flushLiteralRun(/*update_indicator_byte*/ true);
            repeat_count = 0;
        }
    }

private:
    /// Called when a group of 8 values is buffered.
    void flushBufferedValues()
    {
        if (repeat_count >= 8)
        {
            /// The buffered values are a part of a repeated run now, it will be written when it ends.
            num_buffered_values = 0;
            /// The values of the preceding bit-packed run are written out, but its indicator byte is not.
            if (literal_count != 0)
            {
                chassert(repeat_count == 8);
                flushLiteralRun(/*update_indicator_byte*/ true);
            }
            return;
        }

        literal_count += num_buffered_values;
        /// The indicator byte of a bit-packed run counts at most 63 groups.
        flushLiteralRun(/*update_indicator_byte*/ literal_count / 8 + 1 >= 64);
        repeat_count = 0;
    }

    void flushLiteralRun(bool update_indicator_byte)
    {
        /// The indicator byte is written in place when the run ends, so that the values could be written
        /// out as they come.
        if (!literal_indicator_byte_pos)
        {
            literal_indicator_byte_pos = out.size();
            out.push_back(char(0));
        }

        chassert(num_buffered_values == 0 || num_buffered_values == 8);
        if (num_buffered_values == 8)
        {
            /// 8 values of `bit_width` bits take exactly `bit_width` bytes, least significant bits first.
            size_t pos = out.size();
            out.resize(pos + bit_width);
            char * dst = out.data() + pos;
            /// At most 7 + 32 bits are buffered at a time.
            UInt64 bits = 0;
            size_t num_bits = 0;
            for (UInt64 value : buffered_values)
            {
                bits |= value << num_bits;
                num_bits += bit_width;
                for (; num_bits >= 8; num_bits -= 8)
                {
                    *dst = static_cast<char>(bits & 0xff);
                    ++dst;
                    bits >>= 8;
                }
            }
            chassert(num_bits == 0);
            num_buffered_values = 0;
        }

        if (update_indicator_byte)
        {
            chassert(literal_count % 8 == 0);
            size_t num_groups = literal_count / 8;
            chassert(num_groups < 64);
            out[*literal_indicator_byte_pos] = static_cast<char>((num_groups << 1) | 1);
            literal_indicator_byte_pos.reset();
            literal_count = 0;
        }
    }

    void flushRepeatedRun()
    {
        chassert(repeat_count > 0);
        /// The indicator is a ULEB128 number, with the least significant bit 0 for a repeated run.
        UInt64 indicator = UInt64(repeat_count) << 1;
        while (indicator >= 0x80)
        {
            out.push_back(static_cast<char>((indicator & 0x7f) | 0x80));
            indicator >>= 7;
        }
        out.push_back(static_cast<char>(indicator));
        /// The value takes the least number of whole bytes, little endian.
        for (size_t i = 0; i < (bit_width + 7) / 8; ++i)
            out.push_back(static_cast<char>((current_value >> (i * 8)) & 0xff));
        num_buffered_values = 0;
        repeat_count = 0;
    }

    PODArray<char> & out;
    const size_t bit_width;

    UInt64 buffered_values[8] {};
    size_t num_buffered_values = 0;

    /// The last value and the number of times in a row it was seen. Maintained in bit-packed runs too,
    /// to switch to a repeated run once it is repeated enough.
    UInt64 current_value = 0;
    size_t repeat_count = 0;

    /// The number of values in the current bit-packed run, excluding `buffered_values`.
    size_t literal_count = 0;
    /// Position in `out` of the indicator byte of the current bit-packed run, if any.
    std::optional<size_t> literal_indicator_byte_pos;
};

/// The most bytes `encodeRepDefLevelsRLE` can write for `size` levels, including the length prefix.
size_t maxEncodedRepDefLevelsSize(size_t size, UInt8 max_level)
{
    size_t bit_width = bitScanReverse(max_level) + 1;
    return sizeof(Int32) + maxRLESize(bit_width, size);
}

void encodeRepDefLevelsRLE(const UInt8 * data, size_t size, UInt8 max_level, PODArray<char> & out)
{
    chassert(max_level > 0);
    size_t offset = out.size();
    size_t prefix_size = sizeof(Int32);

    size_t bit_width = bitScanReverse(max_level) + 1;
    out.reserve(offset + maxEncodedRepDefLevelsSize(size, max_level));
    out.resize(offset + prefix_size);

    RLEEncoder encoder(out, bit_width);
    for (size_t i = 0; i < size; ++i)
        encoder.put(data[i]);
    encoder.flush();

    size_t len = out.size() - offset - prefix_size;
    if (len > INT32_MAX)
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Encoded levels are too big: {}", len);
    unalignedStoreLittleEndian<Int32>(out.data() + offset, static_cast<Int32>(len));
}

/// Appends the PLAIN encoding of the values to `out`.
template <typename ParquetDType>
void encodePlain(const typename ParquetDType::c_type * values, size_t count, size_t fixed_string_size, PODArray<char> & out)
{
    using T = typename ParquetDType::c_type;
    static_assert(!std::is_same_v<ParquetDType, BooleanType>, "booleans are bit-packed, see PlainEncoder");

    if constexpr (std::is_same_v<ParquetDType, ByteArrayType>)
    {
        /// The length as 4 bytes little endian, followed by the bytes.
        size_t total_size = 0;
        for (size_t i = 0; i < count; ++i)
            total_size += sizeof(UInt32) + values[i].len;
        size_t pos = out.size();
        out.resize(pos + total_size);
        char * dst = out.data() + pos;
        for (size_t i = 0; i < count; ++i)
        {
            unalignedStoreLittleEndian<UInt32>(dst, values[i].len);
            dst += sizeof(UInt32);
            /// An empty string may have a null pointer, which `memcpy` does not accept.
            if (values[i].len)
                memcpy(dst, values[i].ptr, values[i].len);
            dst += values[i].len;
        }
    }
    else if constexpr (std::is_same_v<ParquetDType, FLBAType>)
    {
        size_t pos = out.size();
        out.resize(pos + count * fixed_string_size);
        char * dst = out.data() + pos;
        for (size_t i = 0; i < count; ++i)
        {
            memcpy(dst, values[i].ptr, fixed_string_size);
            dst += fixed_string_size;
        }
    }
    else
    {
        /// Numbers, little endian.
        size_t pos = out.size();
        out.resize(pos + count * sizeof(T));
        char * dst = out.data() + pos;
        if constexpr (std::endian::native == std::endian::little)
        {
            if (count)
                memcpy(dst, values, count * sizeof(T));
        }
        else
        {
            for (size_t i = 0; i < count; ++i)
                unalignedStoreLittleEndian<T>(dst + i * sizeof(T), values[i]);
        }
    }
}

/// Accumulates the PLAIN encoding of the values of a page.
template <typename ParquetDType>
class PlainEncoder
{
public:
    using T = typename ParquetDType::c_type;

    explicit PlainEncoder(size_t fixed_string_size_) : fixed_string_size(fixed_string_size_) {}

    void put(const T * values, size_t count)
    {
        if constexpr (std::is_same_v<ParquetDType, BooleanType>)
        {
            /// Bit-packed, least significant bit first.
            buf.resize_fill((num_booleans + count + 7) / 8, 0);
            for (size_t i = 0; i < count; ++i, ++num_booleans)
                buf[num_booleans / 8] |= static_cast<char>(UInt8(values[i]) << (num_booleans % 8));
        }
        else
        {
            encodePlain<ParquetDType>(values, count, fixed_string_size, buf);
        }
    }

    size_t estimatedDataEncodedSize() const { return buf.size(); }

    /// Appends the encoded values to `out` and starts a new page.
    void flush(PODArray<char> & out)
    {
        out.insert(buf.begin(), buf.end());
        buf.clear();
        num_booleans = 0;
    }

private:
    const size_t fixed_string_size;
    PODArray<char> buf;
    size_t num_booleans = 0;
};

/// Collects distinct values into a dictionary, and accumulates the indexes of the values of a page in
/// the dictionary. The dictionary is in the order of the first occurrence of each value.
/// Not used for booleans.
template <typename ParquetDType>
class DictionaryEncoder
{
public:
    using T = typename ParquetDType::c_type;

    explicit DictionaryEncoder(size_t fixed_string_size_) : fixed_string_size(fixed_string_size_) {}

    void put(const T * values, size_t count)
    {
        size_t pos = indexes.size();
        indexes.resize(pos + count);
        for (size_t i = 0; i < count; ++i)
            indexes[pos + i] = getOrInsert(values[i]);
    }

    size_t numEntries() const { return dictionary.size(); }

    /// The size of the PLAIN-encoded dictionary, which `writeDictionary` writes.
    size_t dictionaryEncodedSize() const { return dictionary_encoded_size; }

    /// An upper bound of the size of what `flush` writes.
    size_t estimatedDataEncodedSize() const { return 1 + maxRLESize(bitWidth(), indexes.size()); }

    /// Appends the encoded indexes to `out` and starts a new page: the bit width as a byte, and the
    /// RLE / bit-packed hybrid encoding of the indexes.
    void flush(PODArray<char> & out)
    {
        size_t bit_width = bitWidth();
        out.reserve(out.size() + estimatedDataEncodedSize());
        out.push_back(static_cast<char>(bit_width));
        RLEEncoder encoder(out, bit_width);
        for (UInt32 index : indexes)
            encoder.put(index);
        encoder.flush();
        indexes.clear();
    }

    /// Appends the PLAIN-encoded dictionary to `out`.
    void writeDictionary(PODArray<char> & out) const
    {
        out.reserve(out.size() + dictionary_encoded_size);
        if constexpr (std::is_same_v<ParquetDType, ByteArrayType> || std::is_same_v<ParquetDType, FLBAType>)
        {
            for (std::string_view value : dictionary)
            {
                if constexpr (std::is_same_v<ParquetDType, ByteArrayType>)
                {
                    ByteArray x(static_cast<UInt32>(value.size()), reinterpret_cast<const uint8_t *>(value.data()));
                    encodePlain<ParquetDType>(&x, 1, fixed_string_size, out);
                }
                else
                {
                    FixedLenByteArray x{reinterpret_cast<const uint8_t *>(value.data())};
                    encodePlain<ParquetDType>(&x, 1, fixed_string_size, out);
                }
            }
        }
        else
        {
            encodePlain<ParquetDType>(dictionary.data(), dictionary.size(), fixed_string_size, out);
        }
    }

private:
    static constexpr bool is_byte_array = std::is_same_v<ParquetDType, ByteArrayType> || std::is_same_v<ParquetDType, FLBAType>;

    /// Numbers are compared by their bits, so that e.g. 0 and -0, or different NaNs, are kept apart.
    using NumberBits = std::conditional_t<sizeof(T) == 4, UInt32, UInt64>;
    using Map = std::conditional_t<is_byte_array, HashMapWithSavedHash<std::string_view, UInt32>, HashMap<NumberBits, UInt32>>;
    /// Points into `arena` for byte arrays.
    using DictionaryValue = std::conditional_t<is_byte_array, std::string_view, T>;

    /// Ceiling of log2 of the number of entries, but at least 1 for a single entry.
    size_t bitWidth() const
    {
        size_t num_entries = dictionary.size();
        if (num_entries <= 1)
            return num_entries;
        return 64 - std::countl_zero(UInt64(num_entries - 1));
    }

    UInt32 getOrInsert(const T & value)
    {
        typename Map::LookupResult it = nullptr;
        bool inserted = false;

        if constexpr (is_byte_array)
        {
            std::string_view key;
            if constexpr (std::is_same_v<ParquetDType, ByteArrayType>)
                key = std::string_view(reinterpret_cast<const char *>(value.ptr), value.len);
            else
                key = std::string_view(reinterpret_cast<const char *>(value.ptr), fixed_string_size);

            map.emplace(ArenaKeyHolder{key, arena}, it, inserted);
            if (inserted)
            {
                dictionary.push_back(it->getKey());
                dictionary_encoded_size += std::is_same_v<ParquetDType, ByteArrayType> ? sizeof(UInt32) + key.size() : fixed_string_size;
            }
        }
        else
        {
            map.emplace(std::bit_cast<NumberBits>(value), it, inserted);
            if (inserted)
            {
                dictionary.push_back(value);
                dictionary_encoded_size += sizeof(T);
            }
        }

        if (inserted)
        {
            if (dictionary.size() > std::numeric_limits<UInt32>::max())
                throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Too many distinct values for a Parquet dictionary");
            it->getMapped() = static_cast<UInt32>(dictionary.size() - 1);
        }
        return it->getMapped();
    }

    const size_t fixed_string_size;
    Arena arena;
    Map map;
    PODArray<DictionaryValue> dictionary;
    size_t dictionary_encoded_size = 0;
    /// Indexes of the values of the current page.
    PODArray<UInt32> indexes;
};

void addToEncodingsUsed(ColumnChunkWriteState & s, parq::Encoding::type e)
{
    if (!std::count(s.column_chunk.meta_data.encodings.begin(), s.column_chunk.meta_data.encodings.end(), e))
        s.column_chunk.meta_data.encodings.push_back(e);
}

/// Maintain PageEncodingStats as we write pages. Readers use it to tell whether a column chunk is
/// fully dictionary-encoded (so the dictionary holds the complete set of values), which enables
/// dictionary-based row group filtering.
void addToEncodingStats(ColumnChunkWriteState & s, const parq::PageHeader & header)
{
    parq::Encoding::type encoding{};
    if (header.__isset.dictionary_page_header)
        encoding = header.dictionary_page_header.encoding;
    else if (header.__isset.data_page_header)
        encoding = header.data_page_header.encoding;
    else if (header.__isset.data_page_header_v2)
        encoding = header.data_page_header_v2.encoding;
    else
        return;

    auto & stats = s.column_chunk.meta_data.encoding_stats;
    for (parq::PageEncodingStats & st : stats)
    {
        if (st.page_type == header.type && st.encoding == encoding)
        {
            st.__set_count(st.count + 1);
            return;
        }
    }
    parq::PageEncodingStats st;
    st.__set_page_type(header.type);
    st.__set_encoding(encoding);
    st.__set_count(1);
    stats.push_back(std::move(st));
    s.column_chunk.meta_data.__isset.encoding_stats = true;
}

void writePage(const parq::PageHeader & header, const PODArray<char> & compressed, ColumnChunkWriteState & s, bool add_to_offset_index, size_t first_row_index, WriteBuffer & out)
{
    size_t header_size = serializeThriftStruct(header, out);
    out.write(compressed.data(), compressed.size());
    size_t compressed_page_size = header.compressed_page_size + header_size;

    /// Remember first data page and first dictionary page.
    if (header.__isset.data_page_header && s.column_chunk.meta_data.data_page_offset == -1)
        s.column_chunk.meta_data.__set_data_page_offset(s.column_chunk.meta_data.total_compressed_size);
    if (header.__isset.dictionary_page_header && !s.column_chunk.meta_data.__isset.dictionary_page_offset)
        s.column_chunk.meta_data.__set_dictionary_page_offset(s.column_chunk.meta_data.total_compressed_size);

    if (add_to_offset_index)
    {
        parq::PageLocation location;
        /// Offset relative to column chunk. finalizeColumnChunkAndWriteFooter later adjusts it to global offset.
        location.__set_offset(s.column_chunk.meta_data.total_compressed_size);
        location.__set_compressed_page_size(static_cast<int32_t>(compressed_page_size));
        location.__set_first_row_index(first_row_index);
        s.indexes.offset_index.page_locations.emplace_back(location);
    }

    s.column_chunk.meta_data.total_uncompressed_size += header.uncompressed_page_size + header_size;
    s.column_chunk.meta_data.total_compressed_size += compressed_page_size;

    addToEncodingStats(s, header);
}

/// Folds the oversized bloom filter built by `writeColumnImpl` down to the smallest size that still meets the requested
/// false positive probability, and fills in its header. See the comment at the construction of the filter in
/// `writeColumnImpl` for why it starts oversized.
void foldBloomFilter(ColumnChunkIndexes & indexes, BloomFilterData && unfolded_data, const WriteOptions & options)
{
    /// Format documentation: https://parquet.apache.org/docs/file-format/bloomfilter/
    const size_t num_blocks = unfolded_data.size() / 8;

    if (num_blocks == 0)
        return;

    /// The number of blocks is a power of two by construction (see `writeColumnImpl`), so the filter can be halved
    /// exactly `countr_zero(num_blocks)` times and every fold below leaves a power of two.
    chassert(std::has_single_bit(num_blocks));

    /// The false positive probability (fpp) that the bloom filter parameters ask for. A split block bloom filter sets
    /// k = 8 bits per inserted value (one in each of the 8 words of its block), and the filter is sized to spend
    /// c = `bits_per_value` bits per value, so its expected fpp is given by the classic bloom filter approximation
    ///     f = (1 - e^(-k / c))^k
    /// (the probability that a bit is still unset after inserting n values into m bits is about e^(-k * n / m), and an
    /// absent value passes only if all k bits it checks are set), see http://tfk.mit.edu/pdf/bloom.pdf, section 2,
    /// and https://parquet.apache.org/docs/file-format/bloomfilter/#sizing-an-sbbf.
    const double fpp = std::pow(1 - std::exp(-8 / options.bloom_filter_bits_per_value), 8);

    /// The fpp of the concrete filter follows from the fraction of set bits (the fill rate): a membership check tests
    /// 8 bits, so an absent value passes with probability about fill_rate^8.
    size_t total_set_bits = 0;
    for (size_t i = 0; i < num_blocks * 8; ++i)
        total_set_bits += std::popcount(unfolded_data[i]);
    if (total_set_bits == 0)
        return;

    const double fill_rate = static_cast<double>(total_set_bits) / (static_cast<double>(num_blocks) * 256);

    indexes.bloom_filter_data = std::move(unfolded_data);
    BloomFilterData & data = indexes.bloom_filter_data;

    /// Folding merges neighboring blocks by OR-ing them, which halves the filter and yields exactly the filter that
    /// would have been built with half the blocks (see the merge loop below for why). Every fold increases the fill
    /// rate and with it the fpp: OR-ing two blocks with fill rate p leaves a bit unset with probability (1 - p)^2, so
    /// the fill rate of the merged block is 1 - (1 - p)^2. Fold as often as the fpp estimated that way stays within
    /// the requested one. A filter sized for all values of the column chunk is at or below the requested fpp before
    /// the first fold, so at least the unfolded filter is always kept.
    const int max_folds = std::countr_zero(num_blocks);
    double one_minus_fill_rate = 1.0 - fill_rate;
    UInt32 folds = 0;
    for (int i = 0; i < max_folds; ++i)
    {
        one_minus_fill_rate = one_minus_fill_rate * one_minus_fill_rate;
        const double folded_fill_rate = 1.0 - one_minus_fill_rate;
        if (std::pow(folded_fill_rate, 8) > fpp)
            break;
        ++folds;
    }

    if (folds > 0)
    {
        /// Merge each group of 2^folds consecutive blocks into one block. A value goes to block
        /// `((h >> 32) * num_blocks) >> 32` (see the hashing in `writeColumnImpl`), i.e. the block index is taken from
        /// the top bits of the hash, so a filter with half the blocks puts a value into block `i / 2` where this filter
        /// put it into block `i`: blocks `2 * i` and `2 * i + 1` together hold exactly the values that block `i` of the
        /// smaller filter would hold, and OR-ing them gives that block bit for bit. The same holds for 2^folds
        /// consecutive blocks after several halvings. The reader derives the block index from the number of blocks in
        /// the file and thus finds every value where the folded filter put it.
        /// The merge is done in place: group `i` lands in block `i`, which is at or before the first block of the
        /// group, so no block that is still to be read is overwritten.
        const size_t group_size = size_t(1) << folds;
        const size_t new_num_blocks = num_blocks >> folds;
        for (size_t i = 0; i < new_num_blocks; ++i)
        {
            UInt32 * dst = &data[i * 8];
            const UInt32 * src = &data[i * group_size * 8];
            /// For i = 0 the destination is the first block of the group itself, so there is nothing to move
            /// (and `memcpy` is not allowed on identical or overlapping regions).
            if (dst != src)
                memcpy(dst, src, 8 * sizeof(UInt32));
            for (size_t j = 1; j < group_size; ++j)
            {
                const UInt32 * block = src + j * 8;
                for (size_t w = 0; w < 8; ++w)
                    dst[w] |= block[w];
            }
        }
        /// `resize` only moves the logical end; also release the capacity of the unfolded filter, because the folded
        /// filters of completed row groups stay in memory until `flushBloomFilters`.
        data.resize(new_num_blocks * 8);
        data.shrink_to_fit();
    }

    /// Fill out the paperwork.
    auto & header = indexes.bloom_filter_header;
    header.__set_numBytes(Int32(data.size() * sizeof(data[0])));
    parq::BloomFilterAlgorithm alg;
    alg.__set_BLOCK(parq::SplitBlockAlgorithm());
    header.__set_algorithm(alg);
    parq::BloomFilterHash hash;
    hash.__set_XXHASH(parq::XxHash());
    header.__set_hash(hash);
    parq::BloomFilterCompression comp;
    comp.__set_UNCOMPRESSED(parq::Uncompressed());
    header.__set_compression(comp);
}

template <typename ParquetDType, typename Converter>
void writeColumnImpl(
    ColumnChunkWriteState & s, const WriteOptions & options, WriteBuffer & out, Converter && converter)
{
    size_t num_values = s.max_def > 0 ? s.def.size() : s.primitive_column->size();
    auto encoding = options.encoding;

    typename Converter::Statistics page_statistics;
    typename Converter::Statistics total_statistics;

    static constexpr bool can_use_dictionary = !std::is_same_v<ParquetDType, BooleanType>;
    bool use_dictionary = can_use_dictionary && options.use_dictionary_encoding && !s.is_bool;

    /// For some readers filter pushdown implementations it's inconvenient if a row straddles pages,
    /// so let's be nice to them and avoid that. This matches arrow's logic.
    /// If we ever use DataPageV2, enable this flag in that case too:
    /// bool pages_change_on_record_boundaries = options.write_page_index || options.data_page_v2;
    bool pages_change_on_record_boundaries = options.write_page_index;
    /// If there are no arrays, record boundaries are everywhere.
    pages_change_on_record_boundaries &= s.max_rep > 0;

    size_t fixed_string_size = 0;
    if constexpr (std::is_same_v<ParquetDType, FLBAType>)
    {
        fixed_string_size = converter.fixedStringSize();

        if constexpr (std::is_same_v<typename Converter::Statistics, StatisticsFixedStringRef>)
            page_statistics.fixed_string_size = converter.fixedStringSize();
    }

    s.column_chunk.meta_data.__isset.size_statistics = true;
    auto reset_size_statistics = [&]
    {
        if constexpr (std::is_same_v<ParquetDType, ByteArrayType>)
            s.column_chunk.meta_data.size_statistics.__set_unencoded_byte_array_data_bytes(0);
        if (s.max_rep > 0)
            s.column_chunk.meta_data.size_statistics.__set_repetition_level_histogram(std::vector<Int64>(s.max_rep + 1));
        if (s.max_def > 0)
            s.column_chunk.meta_data.size_statistics.__set_definition_level_histogram(std::vector<Int64>(s.max_def + 1));
    };
    reset_size_statistics();

    /// The only non-dictionary encoding we write is PLAIN.
    if (encoding != parq::Encoding::PLAIN)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported Parquet encoding for writing: {}", static_cast<int>(encoding));

    /// Exactly one of the two is used at a time, depending on `use_dictionary`.
    std::optional<DictionaryEncoder<ParquetDType>> dict_encoder;
    std::optional<PlainEncoder<ParquetDType>> plain_encoder;
    if (use_dictionary)
        dict_encoder.emplace(fixed_string_size);
    else
        plain_encoder.emplace(fixed_string_size);

    /// Calls `f` with the encoder in use. Both have the same interface for the values of a page.
    auto with_encoder = [&](auto && f)
    {
        if constexpr (can_use_dictionary)
            if (use_dictionary)
                return f(*dict_encoder);
        return f(*plain_encoder);
    };

    auto estimated_data_encoded_size = [&]
    {
        return with_encoder([](const auto & encoder) { return encoder.estimatedDataEncodedSize(); });
    };

    struct PageData
    {
        parq::PageHeader header;
        PODArray<char> data;
        size_t first_row_index = 0;
    };
    std::vector<PageData> dict_encoded_pages; // can't write them out until we have full dictionary

    /// Reused across pages to reduce number of allocations and improve locality.
    PODArray<char> encoded;
    PODArray<char> compressed_maybe;

    /// Bloom filter of this column chunk, if requested.
    ///
    /// Why the filter starts sized for all values and is folded afterwards: a bloom filter has to be sized for the
    /// number of distinct values it will hold, which is not known when the column chunk is started. The writer used
    /// to find it by deduplicating the hashes of all values in a hash set first, which costs a hash set the size of
    /// the column chunk and, for unlucky value distributions, degenerates into an excessive number of collisions
    /// (https://github.com/ClickHouse/ClickHouse/issues/105295). Instead, the filter is sized under the assumption
    /// that all values are distinct, the largest size it can ever need, so it meets the requested false positive
    /// probability for any data. Once all values are hashed into it, its fill rate reveals how many distinct values
    /// it actually received, and `foldBloomFilter` halves it as long as the requested false positive probability holds.
    /// Folding a split block bloom filter yields exactly the filter that would have been built with fewer blocks,
    /// so nothing is lost compared to knowing the right size upfront, and the cost is a larger temporary buffer
    /// instead of a hash set. The same approach was adopted by arrow-rs: https://github.com/apache/arrow-rs/pull/9628
    ///
    /// Possible future optimization: if using dictionary encoding, take already-deduplicated values
    /// from the dictionary instead.
    std::optional<BloomFilterData> bloom_data;
    if (options.write_bloom_filter)
    {
        /// There appear to be undocumented requirements:
        ///  * number of blocks must be a power of two,
        ///  * bloom filter size must be at most 128 MiB.
        /// At least arrow's `parquet::BlockSplitBloomFilter::Init` requires this.
        /// A column chunk that would need a bigger filter gets none.
        /// Only the entries at the maximum definition level are hashed into the filter, so size it for the number of
        /// leaf values in the primitive column, not for `num_values`, which also counts the null and empty-array
        /// placeholders of a repeated or nullable leaf (a sparse `Array(Nullable(T))` may have hundreds of
        /// placeholders per value).
        const double requested_num_blocks
            = static_cast<double>(s.primitive_column->size()) * options.bloom_filter_bits_per_value / 256;
        size_t num_blocks = 1;
        bool too_many_blocks = false;
        while (static_cast<double>(num_blocks) < requested_num_blocks)
        {
            if (num_blocks >= 4 * 1024 * 1024)
            {
                too_many_blocks = true;
                break;
            }
            num_blocks *= 2;
        }
        if (!too_many_blocks)
        {
            bloom_data.emplace();
            bloom_data->reserve_exact(num_blocks * 8);
            /// `BloomFilterData` zeroes freshly allocated memory in the allocator, so plain `resize` is
            /// enough here; `resize_fill` would `memset` the whole buffer on top of that.
            bloom_data->resize(num_blocks * 8);
        }
    }

    /// Start of current page.
    size_t def_offset = 0; // index in def and rep
    size_t data_offset = 0; // index in primitive_column
    size_t row_idx = 0;

    auto flush_page = [&](size_t def_count, size_t data_count)
    {
        encoded.clear();

        /// Unless the next value starts a record, the record is split, and pages no longer start
        /// where the page index says they do.
        if (pages_change_on_record_boundaries && def_offset + def_count < num_values && s.rep[def_offset + def_count] != 0)
        {
            s.indexes.column_index_valid = false;
            s.indexes.offset_index_valid = false;
        }

        /// Concatenate encoded rep, def, and data.

        size_t row_count = def_count;
        if (s.max_rep > 0)
        {
            encodeRepDefLevelsRLE(s.rep.data() + def_offset, def_count, s.max_rep, encoded);

            row_count = 0;
            for (size_t i = def_offset; i < def_offset + def_count; ++i)
            {
                ++s.column_chunk.meta_data.size_statistics.repetition_level_histogram[s.rep[i]];
                row_count += s.rep[i] == 0;
            }
        }

        if (s.max_def > 0)
        {
            encodeRepDefLevelsRLE(s.def.data() + def_offset, def_count, s.max_def, encoded);

            for (size_t i = def_offset; i < def_offset + def_count; ++i)
                ++s.column_chunk.meta_data.size_statistics.definition_level_histogram[s.def[i]];
        }

        /// Append the values, and reset the encoder for the next page.
        with_encoder([&](auto & encoder) { encoder.flush(encoded); });

        if (encoded.size() > INT32_MAX)
            throw Exception(ErrorCodes::CANNOT_COMPRESS, "Uncompressed page is too big: {}", encoded.size());

        size_t uncompressed_size = encoded.size();
        auto & compressed = compress(encoded, compressed_maybe, s.compression, s.compression_level);

        if (compressed.size() > INT32_MAX)
            throw Exception(ErrorCodes::CANNOT_COMPRESS, "Compressed page is too big: {}", compressed.size());

        parq::PageHeader header;
        header.__set_type(parq::PageType::DATA_PAGE);
        header.__set_uncompressed_page_size(static_cast<int>(uncompressed_size));
        header.__set_compressed_page_size(static_cast<int>(compressed.size()));
        header.__isset.data_page_header = true;
        auto & d = header.data_page_header;
        d.__set_num_values(static_cast<Int32>(def_count));
        d.__set_encoding(use_dictionary ? parq::Encoding::RLE_DICTIONARY : encoding);
        d.__set_definition_level_encoding(parq::Encoding::RLE);
        d.__set_repetition_level_encoding(parq::Encoding::RLE);

        if (options.write_checksums)
        {
            uint32_t crc = static_cast<uint32_t>(crc32_z(0, reinterpret_cast<const Bytef *>(compressed.data()), compressed.size()));
            header.__set_crc(crc);
        }

        parq::Statistics page_stats = page_statistics.get(options);
        bool has_null_count = s.max_def == 1 && s.max_rep == 0;
        if (has_null_count)
            page_stats.__set_null_count(static_cast<Int64>(def_count - data_count));

        if (options.write_page_statistics)
            d.__set_statistics(page_stats);

        if (options.write_page_index)
        {
            bool all_null_page = data_count == 0;
            bool has_stats = page_stats.__isset.min_value && page_stats.__isset.max_value;
            if (!all_null_page && !has_stats)
                s.indexes.column_index_valid = false;

            s.indexes.column_index.min_values.push_back(page_stats.min_value);
            s.indexes.column_index.max_values.push_back(page_stats.max_value);
            if (has_null_count)
            {
                /// Note: has_null_count is always equal across all pages of the same column.
                s.indexes.column_index.__isset.null_counts = true;
                s.indexes.column_index.null_counts.emplace_back(page_stats.null_count);
            }
            s.indexes.column_index.null_pages.push_back(all_null_page);
        }

        total_statistics.merge(page_statistics);
        page_statistics.clear();

        if (use_dictionary)
        {
            dict_encoded_pages.push_back({.header = std::move(header), .data = {}, .first_row_index = row_idx});
            std::swap(dict_encoded_pages.back().data, compressed);
        }
        else
        {
            writePage(header, compressed, s, options.write_page_index, row_idx, out);
        }
        def_offset += def_count;
        data_offset += data_count;
        row_idx += row_count;
    };

    auto flush_dict = [&]
    {
        if constexpr (can_use_dictionary)
        {
            encoded.clear();
            dict_encoder->writeDictionary(encoded);
            size_t dict_size = encoded.size();
            chassert(dict_size == dict_encoder->dictionaryEncodedSize());

            if (dict_size > INT32_MAX)
                throw Exception(ErrorCodes::CANNOT_COMPRESS, "Uncompressed dictionary page is too big: {}", dict_size);

            auto & compressed = compress(encoded, compressed_maybe, s.compression, s.compression_level);

            if (compressed.size() > INT32_MAX)
                throw Exception(ErrorCodes::CANNOT_COMPRESS, "Compressed dictionary page is too big: {}", compressed.size());

            parq::PageHeader header;
            header.__set_type(parq::PageType::DICTIONARY_PAGE);
            header.__set_uncompressed_page_size(static_cast<int>(dict_size));
            header.__set_compressed_page_size(static_cast<int>(compressed.size()));
            header.__isset.dictionary_page_header = true;
            header.dictionary_page_header.__set_num_values(static_cast<Int32>(dict_encoder->numEntries()));
            header.dictionary_page_header.__set_encoding(parq::Encoding::PLAIN);

            if (options.write_checksums)
            {
                uint32_t crc = static_cast<uint32_t>(crc32_z(0, reinterpret_cast<const Bytef *>(compressed.data()), compressed.size()));
                header.__set_crc(crc);
            }

            writePage(header, compressed, s, /*add_to_offset_index*/ false, /*first_row_index*/ 0, out);

            for (auto & p : dict_encoded_pages)
                writePage(p.header, p.data, s, options.write_page_index, p.first_row_index, out);

            dict_encoded_pages.clear();
            dict_encoder.reset();
        }
    };

    auto dict_encoded_size = [&]
    {
        if constexpr (can_use_dictionary)
            return dict_encoder->dictionaryEncodedSize();
        else
            return 0uz;
    };

    auto is_dict_too_big = [&]
    {
        return dict_encoded_size() >= options.max_dictionary_size;
    };

    /// The size of the dictionary page must fit in Int32. The batch can add at most `batch_encoded_bytes`
    /// to the dictionary.
    auto would_overflow_dict = [&](size_t batch_encoded_bytes)
    {
        return dict_encoded_size() + batch_encoded_bytes > INT32_MAX;
    };

    /// Fallback to non-dictionary encoding.
    ///
    /// Discard encoded data and start over.
    /// This is different from what arrow does: arrow writes out the dictionary-encoded
    /// data, then uses non-dictionary encoding for later pages.
    /// Starting over seems better: it produces slightly smaller files (I saw 1-4%) in
    /// exchange for slight decrease in speed (I saw < 5%). This seems like a good
    /// trade because encoding speed is less important than decoding (as evidenced
    /// by arrow not supporting parallel encoding, even though it's easy to support).
    auto restart_without_dictionary = [&]
    {
        def_offset = 0;
        data_offset = 0;
        row_idx = 0;
        dict_encoded_pages.clear();
        use_dictionary = false;

        s.indexes = {};
        /// Everything the discarded pass accumulated is about to be accumulated again.
        /// (no need to clear hashes_for_bloom_filter: the same values hash to the same set)
        reset_size_statistics();
        page_statistics.clear();
        total_statistics.clear();

        dict_encoder.reset();
        plain_encoder.emplace(fixed_string_size);
    };

    /// A batch is bounded by bytes as well as by rows. `write_batch_size` values of a wide column
    /// otherwise carry gigabytes into a single page, which overruns both the page's own 32-bit size
    /// and the 32-bit size of the dictionary page, and stages the whole column chunk in memory on
    /// the way.
    ///
    /// The budget only has to keep those 32-bit quantities out of reach, so it sits far above any
    /// page a caller would ask for: a batch of 1024 values stays under it unless the values average
    /// more than 64 KiB, and pages of ordinary data come out exactly as they did before.
    ///
    /// Cuts the batch at the first record boundary at or after the budget. A record is kept whole
    /// unless it would not fit a page at all: then the batch ends before the value that would carry
    /// the page past `max_record_bytes`, wherever that is.
    ///
    /// The cut is measured by what the page holds, which is more than the values: `overhead_per_value`
    /// covers the 4-byte length prefix plain `BYTE_ARRAY` puts in front of every value, and the rep and
    /// def levels of a nested column of narrow values can weigh as much as the values themselves. With
    /// a dictionary, the page holds indexes, so the values are budgeted for both, as they are staged
    /// for the dictionary as well. All of these are worst cases, which only matter for huge records.
    static constexpr size_t max_batch_bytes = 64uz << 20;

    static constexpr size_t max_record_bytes = (2uz << 30) - (64uz << 20);

    /// The converter materializes every value it is given, so a long record is handed to it in
    /// slices of this many values. The slices of a record go into the same page, unless the page
    /// would grow past `max_record_bytes`.
    const size_t max_batch_values = std::max(options.write_batch_size, 64uz << 10);

    auto max_levels_size = [&](size_t count)
    {
        size_t res = 0;
        if (s.max_rep > 0)
            res += maxEncodedRepDefLevelsSize(count, s.max_rep);
        if (s.max_def > 0)
            res += maxEncodedRepDefLevelsSize(count, s.max_def);
        return res;
    };

    auto dictionary_entries = [&]
    {
        if constexpr (can_use_dictionary)
            return use_dictionary ? dict_encoder->numEntries() : 0uz;
        else
            return 0uz;
    };

    /// Like `estimatedDataEncodedSize` of a dictionary encoder, for `count` indexes into a dictionary
    /// of up to `max_entries`: a byte of bit width and the RLE of the indexes.
    auto max_dictionary_indexes_size = [&](size_t count, size_t max_entries)
    {
        if (!use_dictionary)
            return 0uz;
        size_t bit_width = std::min(32uz, static_cast<size_t>(bitScanReverse(std::max(max_entries, 1uz))) + 1);
        return 1 + maxRLESize(bit_width, count);
    };

    struct BatchSize
    {
        /// What `unencoded_byte_array_data_bytes` reports.
        size_t payload = 0;
        /// What the plain encoding of the values writes.
        size_t encoded = 0;
    };

    auto limit_batch_by_bytes
        = [&](size_t batch_def_offset, size_t & def_count, size_t & data_count, size_t overhead_per_value, auto && value_size)
    {
        BatchSize size;
        size_t data_idx = 0;
        /// Every value of the batch may add an entry to the dictionary.
        const size_t batch_dictionary_entries = dictionary_entries();

        for (size_t i = 0; i < def_count; ++i)
        {
            const bool has_value = s.max_def == 0 || s.def[batch_def_offset + i] == s.max_def;
            const size_t value_bytes = has_value ? value_size(data_idx) : 0;
            const size_t values_encoded = size.encoded + (has_value ? value_bytes + overhead_per_value : 0);
            const size_t values = data_idx + has_value;
            const size_t page_bytes = values_encoded + max_levels_size(i + 1)
                + max_dictionary_indexes_size(values, batch_dictionary_entries + values);

            if (i > 0 && page_bytes > max_record_bytes)
            {
                def_count = i;
                data_count = data_idx;
                break;
            }

            size.payload += value_bytes;
            size.encoded = values_encoded;
            data_idx += has_value;

            const bool record_ends = !pages_change_on_record_boundaries
                || batch_def_offset + i + 1 == num_values
                || s.rep[batch_def_offset + i + 1] == 0;

            if (record_ends && page_bytes >= max_batch_bytes)
            {
                def_count = i + 1;
                data_count = data_idx;
                break;
            }
        }

        return size;
    };

    while (def_offset < num_values)
    {
        /// Pick enough data for a page.
        size_t next_def_offset = def_offset;
        size_t next_data_offset = data_offset;
        while (true)
        {
            /// Bite off a batch of defs and corresponding data values.
            size_t def_count = std::min(options.write_batch_size, num_values - next_def_offset);
            size_t data_count = 0;
            if (s.max_def == 0) // no arrays or nullables
                data_count = def_count;
            else
                for (size_t i = 0; i < def_count; ++i)
                    data_count += s.def[next_def_offset + i] == s.max_def;

            if (pages_change_on_record_boundaries)
            {
                /// Each record (table row) starts with a value with rep = 0.
                while (next_def_offset + def_count < num_values && s.rep[next_def_offset + def_count] != 0
                    && def_count < max_batch_values)
                {
                    data_count += s.def[next_def_offset + def_count] == s.max_def;
                    ++def_count;
                }
            }

            /// Only the size of a byte array has to be known from the converted value, so the batch
            /// of any other type is cut before it is converted.
            const typename ParquetDType::c_type * converted = nullptr;

            BatchSize batch_size;
            if constexpr (std::is_same_v<ParquetDType, ByteArrayType>)
            {
                if constexpr (requires { converter.startBatch(next_data_offset, data_count); })
                {
                    /// The converter serializes values, so only the values that make it into the
                    /// batch are serialized.
                    converter.startBatch(next_data_offset, data_count);
                    batch_size = limit_batch_by_bytes(
                        next_def_offset, def_count, data_count, sizeof(uint32_t),
                        [&](size_t i) { return converter.valueSize(i); });
                    converted = converter.finishBatch(data_count);
                }
                else
                {
                    converted = converter.getBatch(next_data_offset, data_count);
                    batch_size = limit_batch_by_bytes(
                        next_def_offset, def_count, data_count, sizeof(uint32_t),
                        [&](size_t i) { return static_cast<size_t>(converted[i].len); });
                }
            }
            else if constexpr (std::is_same_v<ParquetDType, FLBAType>)
            {
                batch_size = limit_batch_by_bytes(
                    next_def_offset, def_count, data_count, 0, [&](size_t) { return converter.fixedStringSize(); });
            }
            else
            {
                /// Fixed-width values are written back to back (booleans are bit-packed, so this only
                /// over-counts). A batch of `write_batch_size` of them is far below the budget, so only
                /// a very long repeated record needs to be walked.
                static constexpr size_t value_bytes = sizeof(typename ParquetDType::c_type);
                if (def_count * value_bytes + max_levels_size(def_count)
                        + max_dictionary_indexes_size(def_count, dictionary_entries() + def_count) < max_batch_bytes)
                    batch_size = {.payload = data_count * value_bytes, .encoded = data_count * value_bytes};
                else
                    batch_size = limit_batch_by_bytes(
                        next_def_offset, def_count, data_count, 0, [](size_t) { return value_bytes; });
            }

            if (next_def_offset > def_offset)
            {
                const size_t page_values = next_data_offset - data_offset + data_count;
                const size_t page_values_bytes = use_dictionary
                    ? max_dictionary_indexes_size(page_values, dictionary_entries() + data_count)
                    : estimated_data_encoded_size() + batch_size.encoded;

                if (page_values_bytes + max_levels_size(next_def_offset - def_offset + def_count) > max_record_bytes)
                {
                    flush_page(next_def_offset - def_offset, next_data_offset - data_offset);
                    break;
                }
            }

            if constexpr (!std::is_same_v<ParquetDType, ByteArrayType>)
                converted = converter.getBatch(next_data_offset, data_count);

            if (options.write_page_statistics || options.write_column_chunk_statistics)
                for (size_t i = 0; i < data_count; ++i)
                    page_statistics.add(converted[i]);

            if (bloom_data.has_value())
            {
/// With XXH_INLINE_ALL (from contrib/xxHash) every XXH function is marked as unused,
/// so any actual use triggers this warning.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wused-but-marked-unused"
                /// Hash the value into its block of the split block bloom filter: 8 bits per value, one in each
                /// of the 8 words of the block, at positions derived from the lower 32 bits of the hash and the salt.
                /// Format documentation: https://parquet.apache.org/docs/file-format/bloomfilter/
                auto & bd = *bloom_data;
                const size_t num_blocks = bd.size() / 8;
                static constexpr UInt32 salt[8] = {
                    0x47b6137bU, 0x44974d91U, 0x8824ad5bU, 0xa2b7289dU, 0x705495c7U, 0x2df1424bU, 0x9efc4947U, 0x5c6bfb31U};

                for (size_t i = 0; i < data_count; ++i)
                {
                    UInt64 h = 0;
                    constexpr UInt64 seed = 0;
                    if constexpr (std::is_same_v<ParquetDType, FLBAType>)
                        h = XXH_INLINE_XXH64(converted[i].ptr, converter.fixedStringSize(), seed);
                    else if constexpr (std::is_same_v<ParquetDType, ByteArrayType>)
                        h = XXH_INLINE_XXH64(converted[i].ptr, converted[i].len, seed);
                    else
                    {
                        static_assert(sizeof(converted[i]) <= 12, "unexpected non-primitive type");
                        h = XXH_INLINE_XXH64(reinterpret_cast<const void*>(&converted[i]), sizeof(converted[i]), seed);
                    }
                    const size_t block_idx = ((h >> 32) * num_blocks) >> 32;
                    chassert(block_idx < num_blocks);
                    const UInt32 x = UInt32(h); // overflow to take the lower 32 bits
                    for (size_t word_idx = 0; word_idx < 8; ++word_idx)
                    {
                        const UInt32 y = x * salt[word_idx]; // overflow to take the lower 32 bits
                        const size_t bit_idx = y >> 27;
                        bd[block_idx * 8 + word_idx] |= 1u << bit_idx;
                    }
                }
#pragma clang diagnostic pop
            }

            if (use_dictionary && would_overflow_dict(batch_size.encoded))
            {
                restart_without_dictionary();
                break;
            }

            if constexpr (std::is_same_v<ParquetDType, ByteArrayType>)
                s.column_chunk.meta_data.size_statistics.unencoded_byte_array_data_bytes += batch_size.payload;

            with_encoder([&](auto & encoder) { encoder.put(converted, data_count); });

            next_def_offset += def_count;
            next_data_offset += data_count;

            if (use_dictionary && is_dict_too_big())
            {
                restart_without_dictionary();
                break;
            }

            /// A page ends with a record, unless the record does not fit a page at all.
            const bool at_record_boundary = !pages_change_on_record_boundaries || next_def_offset == num_values
                || s.rep[next_def_offset] == 0;
            const size_t page_target = std::min(options.data_page_size, max_record_bytes);
            if (next_def_offset == num_values ||
                (at_record_boundary && estimated_data_encoded_size() >= page_target))
            {
                flush_page(next_def_offset - def_offset, next_data_offset - data_offset);
                break;
            }
        }
    }

    if (use_dictionary)
        flush_dict();

    chassert(data_offset == s.primitive_column->size());

    if (options.write_column_chunk_statistics)
    {
        s.column_chunk.meta_data.__set_statistics(total_statistics.get(options));

        if (s.max_def == 1 && s.max_rep == 0)
            s.column_chunk.meta_data.statistics.__set_null_count(static_cast<Int64>(def_offset - data_offset));

        if constexpr (std::is_same_v<typename Converter::Statistics, StatisticsGeometry>)
        {
            if (auto geospatial_statistics = total_statistics.getGeospatial())
                s.column_chunk.meta_data.__set_geospatial_statistics(*std::move(geospatial_statistics));
        }
    }

    /// Report which encodings we've used.
    if (s.max_rep > 0 || s.max_def > 0)
        addToEncodingsUsed(s, parq::Encoding::RLE); // levels
    if (use_dictionary)
    {
        addToEncodingsUsed(s, parq::Encoding::PLAIN); // dictionary itself
        addToEncodingsUsed(s, parq::Encoding::RLE_DICTIONARY); // ids
    }
    else
    {
        addToEncodingsUsed(s, encoding);
    }

    if (bloom_data.has_value())
        foldBloomFilter(s.indexes, *std::move(bloom_data), options);
}

}

void writeColumnChunkBody(
    ColumnChunkWriteState & s, const WriteOptions & options, const FormatSettings & format_settings, WriteBuffer & out)
{
    s.column_chunk.meta_data.__set_num_values(s.max_def > 0 ? s.def.size() : s.primitive_column->size());

    /// We'll be updating these as we go.
    s.column_chunk.meta_data.__set_encodings({});
    s.column_chunk.meta_data.encoding_stats.clear();
    s.column_chunk.meta_data.__isset.encoding_stats = false;
    s.column_chunk.meta_data.__set_total_compressed_size(0);
    s.column_chunk.meta_data.__set_total_uncompressed_size(0);
    s.column_chunk.meta_data.__set_data_page_offset(-1);

    s.primitive_column = s.primitive_column->convertToFullColumnIfLowCardinality();

    switch (s.primitive_column->getDataType())
    {
        /// Numeric conversion to Int32 or Int64.
        #define N(source_type, parquet_dtype) \
            writeColumnImpl<parquet_dtype>(s, options, out, \
                ConverterNumeric<ColumnVector<source_type>, parquet_dtype::c_type>( \
                    s.primitive_column))

        case TypeIndex::UInt8:
            if (s.is_bool)
                writeColumnImpl<BooleanType>(s, options, out,
                    ConverterNumeric<ColumnVector<UInt8>, bool, bool>(s.primitive_column));
            else
                N(UInt8, Int32Type);
            break;
        case TypeIndex::UInt16 : N(UInt16, Int32Type); break;
        case TypeIndex::UInt64 : N(UInt64, Int64Type); break;
        case TypeIndex::Int8:
        {
            if (options.output_enum_as_byte_array && isEnum8(s.type))
                writeColumnImpl<ByteArrayType>(
                    s, options, out, ConverterEnumAsString<Int8>(s.primitive_column, s.type));
            else
                N(Int8, Int32Type);
         break;
        }
        case TypeIndex::Int16:
        {
            if (options.output_enum_as_byte_array && isEnum16(s.type))
                writeColumnImpl<ByteArrayType>(
                    s, options, out, ConverterEnumAsString<Int16>(s.primitive_column, s.type));
            else
                N(Int16, Int32Type);
            break;
        }
        case TypeIndex::Int32  : N(Int32,  Int32Type); break;
        case TypeIndex::Int64  : N(Int64,  Int64Type); break;

        case TypeIndex::UInt32:
            if (s.datetime_multiplier == 1)
                N(UInt32, Int32Type);
            else
            {
                /// It's actually a DateTime that needs to be converted to milliseconds.
                chassert(s.datetime_multiplier == 1000);
                writeColumnImpl<Int64Type>(s, options, out, ConverterDateTime(s.primitive_column));
            }
            break;

        #undef N

        case TypeIndex::Float32:
            writeColumnImpl<FloatType>(
                s, options, out, ConverterNumeric<ColumnVector<Float32>, Float32, Float32>(
                    s.primitive_column));
            break;

        case TypeIndex::Float64:
            writeColumnImpl<DoubleType>(
                s, options, out, ConverterNumeric<ColumnVector<Float64>, Float64, Float64>(
                    s.primitive_column));
            break;

        case TypeIndex::DateTime64:
            if (s.datetime_multiplier == 1)
                writeColumnImpl<Int64Type>(
                    s, options, out, ConverterNumeric<ColumnDecimal<DateTime64>, Int64, Int64>(
                        s.primitive_column));
            else
                writeColumnImpl<Int64Type>(
                    s, options, out, ConverterDateTime64WithMultiplier(
                        s.primitive_column, s.datetime_multiplier));
            break;

        case TypeIndex::IPv4:
            writeColumnImpl<Int32Type>(
                s, options, out, ConverterNumeric<ColumnVector<IPv4>, Int32, UInt32>(
                    s.primitive_column));
            break;

        case TypeIndex::String:
            if (s.is_geometry)
                writeColumnImpl<ByteArrayType>(
                    s, options, out, ConverterGeometry(s.primitive_column));
            else
                writeColumnImpl<ByteArrayType>(
                    s, options, out, ConverterString(s.primitive_column));
            break;

        case TypeIndex::FixedString:
            if (options.output_fixed_string_as_fixed_byte_array)
                writeColumnImpl<FLBAType>(
                s, options, out, ConverterFixedString(s.primitive_column));
            else
                writeColumnImpl<ByteArrayType>(
                s, options, out, ConverterFixedStringAsString(s.primitive_column));
            break;
        case TypeIndex::Object:
            writeColumnImpl<ByteArrayType>(s, options, out, ConverterJSON(s.primitive_column, s.type, format_settings));
            break;

        #define F(source_type) \
            if (options.output_wide_integer_as_decimal) \
                writeColumnImpl<FLBAType>( \
                    s, options, out, ConverterWideIntegerAsDecimal<source_type>(s.primitive_column)); \
            else \
                writeColumnImpl<FLBAType>( \
                    s, options, out, ConverterNumberAsFixedString<source_type>(s.primitive_column))
        case TypeIndex::UInt128: F(UInt128); break;
        case TypeIndex::UInt256: F(UInt256); break;
        case TypeIndex::Int128:  F(Int128); break;
        case TypeIndex::Int256:  F(Int256); break;
        #undef F

        case TypeIndex::IPv6:
            writeColumnImpl<FLBAType>(
                s, options, out, ConverterNumberAsFixedString<IPv6>(s.primitive_column));
            break;

        case TypeIndex::UUID:
            writeColumnImpl<FLBAType>(s,
                options,
                out,
                ConverterUUID(s.primitive_column));
        break;

        #define D(source_type) \
            writeColumnImpl<FLBAType>( \
                s, options, out, ConverterDecimal<source_type>(s.primitive_column))
        case TypeIndex::Decimal32:  D(Decimal32); break;
        case TypeIndex::Decimal64:  D(Decimal64); break;
        case TypeIndex::Decimal128: D(Decimal128); break;
        case TypeIndex::Decimal256: D(Decimal256); break;
        #undef D

        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected column type: {}", s.primitive_column->getFamilyName());
    }

    /// Free some memory.
    s.primitive_column = {};
    s.def = {};
    s.rep = {};
}

void writeFileHeader(FileWriteState & file, WriteBuffer & out)
{
    chassert(file.offset == 0);
    /// Write the magic bytes. We're a wizard now.
    out.write("PAR1", 4);
    file.offset = 4;
}

void finalizeColumnChunkAndWriteFooter(ColumnChunkWriteState s, FileWriteState & file, WriteBuffer & out)
{
    for (auto & location : s.indexes.offset_index.page_locations)
        location.offset += file.offset;

    if (s.column_chunk.meta_data.data_page_offset != -1)
        s.column_chunk.meta_data.data_page_offset += file.offset;
    if (s.column_chunk.meta_data.__isset.dictionary_page_offset)
        s.column_chunk.meta_data.dictionary_page_offset += file.offset;

    file.offset += s.column_chunk.meta_data.total_compressed_size;
    s.column_chunk.file_offset = file.offset;

    size_t footer_size = serializeThriftStruct(s.column_chunk, out);
    file.offset += footer_size;

    file.unflushed_bloom_filter_bytes += s.indexes.bloom_filter_data.size() * sizeof(s.indexes.bloom_filter_data[0]);
    file.current_row_group.column_indexes.push_back(std::move(s.indexes));
    file.current_row_group.row_group.columns.push_back(std::move(s.column_chunk));
}

static void flushBloomFilters(FileWriteState & file, WriteBuffer & out)
{
    while (file.row_groups_with_flushed_bloom_filter < file.completed_row_groups.size())
    {
        auto & rg = file.completed_row_groups[file.row_groups_with_flushed_bloom_filter];
        chassert(rg.column_indexes.size() == rg.row_group.columns.size());
        for (size_t col_idx = 0; col_idx < rg.column_indexes.size(); ++col_idx)
        {
            auto & indexes = rg.column_indexes[col_idx];
            if (indexes.bloom_filter_data.empty())
                continue;
            auto & column = rg.row_group.columns[col_idx];
            column.meta_data.__set_bloom_filter_offset(file.offset);

            size_t header_size = serializeThriftStruct(indexes.bloom_filter_header, out);
            size_t data_size = indexes.bloom_filter_data.size() * sizeof(indexes.bloom_filter_data[0]);
            out.write(indexes.bloom_filter_data.raw_data(), data_size);

            column.meta_data.__set_bloom_filter_length(Int32(header_size + data_size));
            file.offset += column.meta_data.bloom_filter_length;

            indexes.bloom_filter_data = {}; // free the memory
        }
        file.row_groups_with_flushed_bloom_filter += 1;
    }
    file.unflushed_bloom_filter_bytes = 0;
}

void finalizeRowGroup(FileWriteState & file, size_t num_rows, const WriteOptions & options, WriteBuffer & out)
{
    auto & r = file.current_row_group.row_group;
    if (!file.completed_row_groups.empty())
        chassert(r.columns.size() == file.completed_row_groups[0].row_group.columns.size());
    r.__set_num_rows(num_rows);
    r.__set_total_compressed_size(0);
    for (auto & c : r.columns)
    {
        r.total_byte_size += c.meta_data.total_uncompressed_size;
        r.total_compressed_size += c.meta_data.total_compressed_size;
    }

    if (r.columns.empty())
    {
        /// All columns are empty tuples, there are no pages.
        r.__set_file_offset(file.offset);
    }
    else
    {
        auto & m = r.columns[0].meta_data;
        r.__set_file_offset(m.__isset.dictionary_page_offset ? m.dictionary_page_offset : m.data_page_offset);
    }

    file.completed_row_groups.push_back(std::move(file.current_row_group));
    file.current_row_group = {};

    if (options.write_bloom_filter && file.unflushed_bloom_filter_bytes >= options.bloom_filter_flush_threshold_bytes)
        flushBloomFilters(file, out);
}

static void writePageIndex(FileWriteState & file, WriteBuffer & out)
{
    // write column index
    for (auto & rg : file.completed_row_groups)
    {
        chassert(rg.column_indexes.size() == rg.row_group.columns.size());
        for (size_t j = 0; j < rg.column_indexes.size(); ++j)
        {
            if (!rg.column_indexes.at(j).column_index_valid)
                continue;

            auto & column = rg.row_group.columns.at(j);
            column.__set_column_index_offset(file.offset);
            size_t length = serializeThriftStruct(rg.column_indexes.at(j).column_index, out);
            column.__set_column_index_length(static_cast<int32_t>(length));
            file.offset += length;
        }
    }

    // write offset index
    for (auto & rg : file.completed_row_groups)
    {
        for (size_t j = 0; j < rg.column_indexes.size(); ++j)
        {
            if (!rg.column_indexes.at(j).offset_index_valid)
                continue;

            auto & column = rg.row_group.columns.at(j);
            column.__set_offset_index_offset(file.offset);
            size_t length = serializeThriftStruct(rg.column_indexes.at(j).offset_index, out);
            column.__set_offset_index_length(static_cast<int32_t>(length));
            file.offset += length;
        }
    }
}

void writeFileFooter(FileWriteState & file,
    SchemaElements schema,
    const WriteOptions & options,
    WriteBuffer & out,
    const Block & header)
{
    chassert(file.offset != 0);
    chassert(file.current_row_group.row_group.columns.empty());

    if (options.write_bloom_filter)
        flushBloomFilters(file, out);

    if (options.write_page_index)
        writePageIndex(file, out);

    parq::FileMetaData meta;
    meta.version = 2;
    meta.schema = std::move(schema);
    meta.row_groups.reserve(file.completed_row_groups.size());
    for (auto & rg : file.completed_row_groups)
    {
        meta.num_rows += rg.row_group.num_rows;
        meta.row_groups.push_back(std::move(rg.row_group));
    }

    /// parquet.thrift sayeth:
    ///  >  This should be in the format
    ///  >  <Application> version <App Version> (build <App Build Hash>).
    ///  >  e.g. impala version 1.0 (build 6cf94d29b2b7115df4de2c06e2ab4326d721eb55)
    meta.__set_created_by(fmt::format("ClickHouse version {}.{}.{} (build {})", VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH, VERSION_GITHASH));

    if (options.write_page_statistics || options.write_column_chunk_statistics)
    {
        meta.__set_column_orders({});
        for (auto & s : meta.schema)
            if (!s.__isset.num_children)
                meta.column_orders.emplace_back();
        for (auto & c : meta.column_orders)
            c.__set_TYPE_ORDER({});
    }

    /// Documentation about geoparquet metadata: https://geoparquet.org/releases/v1.0.0-beta.1/
    if (options.write_geometadata)
    {
        std::vector<std::pair<std::string, Poco::JSON::Object::Ptr>> geo_columns_metadata;
        for (const auto & [column_name, type] : header.getNamesAndTypesList())
        {
            if (type->getCustomName() &&
                (type->getCustomName()->getName() == WKBPointTransform::name ||
                type->getCustomName()->getName() == WKBMultiPointTransform::name ||
                type->getCustomName()->getName() == WKBLineStringTransform::name ||
                type->getCustomName()->getName() == WKBPolygonTransform::name ||
                type->getCustomName()->getName() == WKBMultiLineStringTransform::name ||
                type->getCustomName()->getName() == WKBMultiPolygonTransform::name))
            {
                Poco::JSON::Object::Ptr geom_meta = new Poco::JSON::Object;
                geom_meta->set("encoding", "WKB");

                Poco::JSON::Array::Ptr geom_types = new Poco::JSON::Array;
                geom_types->add(type->getCustomName()->getName());
                geom_meta->set("geometry_types", geom_types);
                /// With the `GEOMETRY` logical type, the column has the default CRS of the logical
                /// type (`OGC:CRS84`). Omit `crs` here, which means the same default in GeoParquet,
                /// so that both kinds of metadata describe the same CRS.
                if (!options.write_geometry_logical_type)
                    geom_meta->set("crs", "EPSG:4326");

                if (type->getCustomName()->getName() == WKBPolygonTransform::name ||
                    type->getCustomName()->getName() == WKBMultiPolygonTransform::name)
                {
                    geom_meta->set("edges", "planar");
                }
                geo_columns_metadata.push_back({column_name, geom_meta});
            }
            else if (options.write_geometry_logical_type && type->getCustomName() && type->getCustomName()->getName() == "Geometry")
            {
                /// An empty list of geometry types means that any type can be present. Without the
                /// metadata, readers (including older versions of ClickHouse) that rely only on the
                /// GeoParquet metadata would read the WKB column as a plain binary one, and readers
                /// that use `GeospatialStatistics` of the `GEOMETRY` logical type could narrow the
                /// type to the one that happens to be present in the data.
                Poco::JSON::Object::Ptr geom_meta = new Poco::JSON::Object;
                geom_meta->set("encoding", "WKB");
                geom_meta->set("geometry_types", Poco::JSON::Array::Ptr(new Poco::JSON::Array));
                geo_columns_metadata.push_back({column_name, geom_meta});
            }
        }

        if (!geo_columns_metadata.empty())
        {
            Poco::JSON::Object::Ptr columns = new Poco::JSON::Object;
            for (const auto & [column_name, column_type] : geo_columns_metadata)
            {
                columns->set(column_name, column_type);
            }

            Poco::JSON::Object::Ptr geo = new Poco::JSON::Object;
            geo->set("version", "1.0.0");
            geo->set("columns", columns);
            geo->set("primary_column", geo_columns_metadata[0].first);

            std::ostringstream // STYLE_CHECK_ALLOW_STD_STRING_STREAM
                oss;
            Poco::JSON::Stringifier::stringify(geo, oss, 4);

            parq::KeyValue key_value;
            key_value.__set_key("geo");
            key_value.__set_value(oss.str());

            meta.key_value_metadata.push_back(std::move(key_value));
            meta.__isset.key_value_metadata = true;
        }
    }

    size_t footer_size = serializeThriftStruct(meta, out);

    if (footer_size > INT32_MAX)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Parquet file metadata too big: {}", footer_size);

    writeIntBinary(static_cast<int>(footer_size), out);
    out.write("PAR1", 4);
    file.offset += footer_size + 8;
}

size_t ColumnChunkWriteState::allocatedBytes() const
{
    size_t r = def.allocated_bytes() + rep.allocated_bytes();
    if (primitive_column)
        r += primitive_column->allocatedBytes();
    return r;
}

}
