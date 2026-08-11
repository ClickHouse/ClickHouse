#include <Formats/PNGSerializer.h>

#include <cstring>
#include <algorithm>
#include <limits>
#include <map>
#include <numeric>
#include <optional>
#include <string>

#include <Columns/ColumnNullable.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Formats/PNGWriter.h>
#include <Common/Exception.h>
#include <Common/PODArray.h>
#include <Common/assert_cast.h>
#include <Common/NaNUtils.h>
#include <Common/StringUtils.h>
#include <base/arithmeticOverflow.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_NUMBER_OF_COLUMNS;
    extern const int LOGICAL_ERROR;
    extern const int TOO_MANY_ROWS;
}

namespace
{
    /// Both parts of the `fcTL` frame delay are 16-bit, which bounds the time scale settings and the
    /// longest delay a single frame can express.
    constexpr UInt64 MAX_DELAY_PART = std::numeric_limits<UInt16>::max();

    /// Half of the `UInt64` domain: shifting a signed `t` by it turns the signed order into the unsigned one.
    constexpr UInt64 SIGNED_TIME_BIAS = UInt64(1) << 63;

    /// How to interpret a value column when converting it to an 8-bit pixel component.
    /// Determined once from the column type, so the per-row path does not re-dispatch on the data type.
    enum class ValueKind : uint8_t
    {
        UInt,   /// Integer clamped to [0, 255].
        Int,    /// Integer clamped to [0, 255].
        Float,  /// Clamped to [0, 1] and scaled to [0, 255].
        Bool,   /// 0 or 255.
    };

    /// Convert a single value of a column to an 8-bit pixel component, using the kind precomputed from its type.
    UInt8 extractByte(const IColumn & column, size_t row_num, ValueKind kind, bool nullable)
    {
        const IColumn * data_column = &column;
        if (nullable)
        {
            const auto & nullable_column = assert_cast<const ColumnNullable &>(column);
            if (nullable_column.isNullAt(row_num))
                return 0;
            data_column = &nullable_column.getNestedColumn();
        }

        switch (kind)
        {
            case ValueKind::UInt:
                return static_cast<UInt8>(std::min<UInt64>(data_column->getUInt(row_num), 255));
            case ValueKind::Int:
                return static_cast<UInt8>(std::clamp<Int64>(data_column->getInt(row_num), 0, 255));
            case ValueKind::Float:
            {
                Float64 value = data_column->getFloat64(row_num);
                if (!isFinite(value))
                    return 0;
                value = std::clamp(value, 0.0, 1.0);
                return static_cast<UInt8>(std::lround(value * 255.0));
            }
            case ValueKind::Bool:
                return data_column->getBool(row_num) ? 255 : 0;
        }
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected value kind for PNG pixel component");
    }

    /// Strip a `LowCardinality` wrapper, if present.
    const IDataType * removeLowCardinalityType(const IDataType * type)
    {
        if (const auto * low_cardinality = typeid_cast<const DataTypeLowCardinality *>(type))
            return low_cardinality->getDictionaryType().get();
        return type;
    }

    /// Strip `LowCardinality` and `Nullable` wrappers, returning the innermost value type.
    const IDataType * unwrapType(const IDataType * type)
    {
        type = removeLowCardinalityType(type);
        if (type->isNullable())
            return typeid_cast<const DataTypeNullable &>(*type).getNestedType().get();
        return type;
    }

    /// Whether the materialized column for this type is a `ColumnNullable` (after `LowCardinality` is removed).
    bool isNullableType(const IDataType & type)
    {
        return removeLowCardinalityType(&type)->isNullable();
    }

    bool isAllowedPixelType(const IDataType & type)
    {
        WhichDataType which(*unwrapType(&type));
        return which.isNativeInteger() || which.isNativeFloat();
    }

    bool isAllowedBoolType(const IDataType & type)
    {
        return unwrapType(&type)->getName() == "Bool";
    }

    bool isAllowedCoordinateType(const IDataType & type)
    {
        return WhichDataType(*unwrapType(&type)).isNativeInteger();
    }

    /// Classify a value column type into a `ValueKind`. The type must have already been validated.
    ValueKind classifyValueKind(const IDataType & type)
    {
        WhichDataType which(*unwrapType(&type));
        if (which.isNativeUInt())
            return ValueKind::UInt;
        if (which.isNativeInt())
            return ValueKind::Int;
        if (which.isNativeFloat())
            return ValueKind::Float;
        return ValueKind::Bool;
    }

    /// Identify a column by its lower-cased name.
    String lowerName(const String & name)
    {
        String result;
        result.reserve(name.size());
        for (char c : name)
            result.push_back(toLowerIfAlphaASCII(c));
        return result;
    }
}

class PNGSerializer::Impl
{
public:
    Impl(const Block & header, const FormatSettings & format_settings);

    bool isAnimated() const { return animated; }
    bool isStreamingAnimation() const { return streaming_animation; }
    void setFrameCallback(FrameCallback callback) { frame_callback = std::move(callback); }

    void setColumns(const ColumnPtr * columns, size_t num_columns);
    void writeRow(size_t row_num);
    void finalizeFrames();
    void reset();

    UInt32 getDeclaredFrameCount() const;

    size_t getWidth() const { return width; }
    size_t getHeight() const { return height; }
    size_t getChannels() const { return channels; }
    const UInt8 * getPixels() const { return single_frame.pixels.data(); }

private:
    enum class Mode : uint8_t
    {
        RGB,
        RGBA,
        Grayscale,
        Binary,
    };

    /// One image of the result: the pixels plus the position of the implicit coordinate cursor within it.
    /// Every frame of an animation is filled independently, so the cursor belongs to the frame and not to
    /// the serializer.
    struct Frame
    {
        /// The image buffer can be large (its size is controlled by user settings), so it uses a
        /// `PODArray` backed by the ClickHouse allocator. This way its memory is accounted by the
        /// memory tracker and respects the per-query memory limits.
        PaddedPODArray<UInt8> pixels;
        size_t implicit_x = 0;
        size_t implicit_y = 0;
    };

    size_t width = 0;
    size_t height = 0;
    Mode mode = Mode::RGB;
    size_t channels = 0;
    size_t frame_bytes = 0;

    /// Column indices in the input header. nullopt if absent.
    std::optional<size_t> x_idx;
    std::optional<size_t> y_idx;
    std::optional<size_t> r_idx;
    std::optional<size_t> g_idx;
    std::optional<size_t> b_idx;
    std::optional<size_t> a_idx;
    std::optional<size_t> v_idx;
    std::optional<size_t> t_idx;

    bool explicit_coords = false;
    bool x_nullable = false;
    bool y_nullable = false;

    /// Animation state. `animated` is set by the presence of the `t` column.
    bool animated = false;
    bool t_nullable = false;
    /// `t` of an unsigned type covers the whole `UInt64` range, which does not fit into `Int64`, so the
    /// signedness is remembered and the value is read through the matching accessor.
    bool t_unsigned = false;
    bool streaming_animation = false;
    UInt64 time_multiplier = 1;
    UInt64 time_divisor = 60;

    /// The still image, or, in the streaming animated mode, the frame currently being filled.
    Frame single_frame;
    /// All frames of the animation in the buffered mode, ordered by `t`.
    std::map<UInt64, Frame> buffered_frames;
    /// The frame the current row is written into.
    Frame * active_frame = nullptr;

    /// The value of `t` of the frame being filled, as an order-preserving key (see `timeKey`), and the length
    /// of the last frame handed over, which the final frame reuses because there is no following `t` to derive
    /// its duration from.
    std::optional<UInt64> current_time;
    std::optional<UInt64> last_delay_units;

    /// How many frames have already been handed over to the callback, and whether the result has been read to
    /// the end. Together they tell the streaming mode that the frame it is about to emit is the only one, so
    /// that `acTL` can declare the exact count instead of an upper bound.
    size_t emitted_frames = 0;
    bool finalizing = false;

    FrameCallback frame_callback;

    /// How to extract one pixel component, precomputed once from the column types so that
    /// the per-row path does not re-dispatch on the data type. One entry per output channel,
    /// in the order the channels are written (R, G, B[, A] or the single grayscale/binary value).
    struct ChannelExtractor
    {
        size_t column_index = 0;
        ValueKind kind = ValueKind::UInt;
        bool nullable = false;
    };
    std::vector<ChannelExtractor> channel_extractors;

    std::vector<ColumnPtr> src_columns;

    void writePixel(size_t x, size_t y, const UInt8 * components);
    void switchFrame(size_t row_num);
    void emitFrame(const Frame & frame, UInt64 delay_units);
    void clearFrame(Frame & frame) const;
    std::pair<UInt16, UInt16> delayFromUnits(UInt64 units) const;
    UInt64 timeKey(size_t row_num) const;
    String timeToString(UInt64 key) const;
};

PNGSerializer::Impl::Impl(const Block & header, const FormatSettings & format_settings)
    : width(format_settings.image.width)
    , height(format_settings.image.height)
{
    if (width == 0 || height == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Image width and height must be greater than zero (got {}x{})", width, height);

    /// Reject absurdly large dimensions up front, before allocating the image buffer, with a clear message
    /// naming the settings. This bounds the buffer size and keeps the width and height well within the
    /// 4-byte range that the PNG header stores them in.
    static constexpr size_t MAX_IMAGE_DIMENSION = 1000000;
    if (width > MAX_IMAGE_DIMENSION || height > MAX_IMAGE_DIMENSION)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Image width and height must not exceed {} (got {}x{}). "
            "Reduce 'output_format_image_width'/'output_format_image_height'.",
            MAX_IMAGE_DIMENSION, width, height);

    const size_t num_cols = header.columns();
    if (num_cols == 0)
        throw Exception(ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS,
            "PNG format requires at least one column");

    for (size_t i = 0; i < num_cols; ++i)
    {
        const auto & col = header.getByPosition(i);
        const String key = lowerName(col.name);

        auto assign_unique = [&](std::optional<size_t> & target, const char * role)
        {
            if (target.has_value())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Duplicate column for the role '{}' in PNG format input", role);
            target = i;
        };

        if (key == "x")
            assign_unique(x_idx, "x");
        else if (key == "y")
            assign_unique(y_idx, "y");
        else if (key == "r")
            assign_unique(r_idx, "r");
        else if (key == "g")
            assign_unique(g_idx, "g");
        else if (key == "b")
            assign_unique(b_idx, "b");
        else if (key == "a")
            assign_unique(a_idx, "a");
        else if (key == "v")
            assign_unique(v_idx, "v");
        else if (key == "t")
            assign_unique(t_idx, "t");
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Column '{}' is not recognized by the PNG format. "
                "Expected one of: x, y, r, g, b, a, v, t (case-insensitive)", col.name);
    }

    const bool has_x = x_idx.has_value();
    const bool has_y = y_idx.has_value();
    if (has_x != has_y)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "PNG format requires either both 'x' and 'y' columns for explicit coordinates, or neither");
    explicit_coords = has_x && has_y;

    const bool has_rgb = r_idx.has_value() && g_idx.has_value() && b_idx.has_value();
    const bool has_rgba = has_rgb && a_idx.has_value();
    const bool has_v = v_idx.has_value();
    const bool has_any_rgb = r_idx.has_value() || g_idx.has_value() || b_idx.has_value() || a_idx.has_value();

    if (has_v && has_any_rgb)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "PNG format does not allow mixing the 'v' column with 'r', 'g', 'b', or 'a' columns");

    if (has_v)
    {
        const auto & v_type = *header.getByPosition(*v_idx).type;
        if (isAllowedBoolType(v_type))
            mode = Mode::Binary;
        else if (isAllowedPixelType(v_type))
            mode = Mode::Grayscale;
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Column 'v' must have a numeric or Bool type, got '{}'", v_type.getName());
        channels = 1;
    }
    else if (has_rgba)
    {
        mode = Mode::RGBA;
        channels = 4;
    }
    else if (has_rgb)
    {
        mode = Mode::RGB;
        channels = 3;
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Cannot determine PNG color mode: expected 'r', 'g', 'b' (RGB), "
            "'r', 'g', 'b', 'a' (RGBA), or 'v' (grayscale/binary)");
    }

    /// Validate types of pixel and coordinate columns.
    auto check_pixel_type = [&](size_t idx, const char * role)
    {
        const auto & type = *header.getByPosition(idx).type;
        if (!isAllowedPixelType(type))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Column '{}' must have a numeric type, got '{}'", role, type.getName());
    };

    if (mode == Mode::RGB || mode == Mode::RGBA)
    {
        check_pixel_type(*r_idx, "r");
        check_pixel_type(*g_idx, "g");
        check_pixel_type(*b_idx, "b");
    }
    if (mode == Mode::RGBA)
        check_pixel_type(*a_idx, "a");

    if (explicit_coords)
    {
        const auto & x_type = *header.getByPosition(*x_idx).type;
        const auto & y_type = *header.getByPosition(*y_idx).type;
        if (!isAllowedCoordinateType(x_type) || !isAllowedCoordinateType(y_type))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Columns 'x' and 'y' must have an integer type, got '{}' and '{}'",
                x_type.getName(), y_type.getName());
        x_nullable = isNullableType(x_type);
        y_nullable = isNullableType(y_type);
    }

    animated = t_idx.has_value();
    if (animated)
    {
        const auto & t_type = *header.getByPosition(*t_idx).type;
        if (!isAllowedCoordinateType(t_type))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Column 't' must have an integer type, got '{}'", t_type.getName());
        t_nullable = isNullableType(t_type);
        t_unsigned = WhichDataType(*unwrapType(&t_type)).isNativeUInt();

        streaming_animation = format_settings.image.streaming_animation;

        /// The time scale is the fraction `multiplier / divisor`, and only its reduced form matters:
        /// a scale of 100000/60 is 5000/3, which the 16-bit parts of the `fcTL` frame delay represent
        /// exactly even though the raw multiplier does not fit into them. So the fraction is normalized
        /// first, and only the reduced denominator has to fit: it goes into `delay_den` verbatim, while
        /// a numerator over the 16-bit limit merely means that delays past the longest expressible one
        /// are clamped, which `delayFromUnits` does for long delays anyway.
        const UInt64 multiplier_setting = format_settings.image.time_multiplier_seconds;
        const UInt64 divisor_setting = format_settings.image.time_divisor_seconds;
        if (multiplier_setting == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "'output_format_image_time_multiplier_seconds' must not be zero");
        if (divisor_setting == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "'output_format_image_time_divisor_seconds' must not be zero");
        const UInt64 scale_gcd = std::gcd(multiplier_setting, divisor_setting);
        time_multiplier = multiplier_setting / scale_gcd;
        time_divisor = divisor_setting / scale_gcd;
        if (time_divisor > MAX_DELAY_PART)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "The time unit {}/{} of the 't' column reduces to {}/{} seconds, and its denominator "
                "does not fit the 16-bit frame delay of an animated PNG (at most {})",
                multiplier_setting, divisor_setting, time_multiplier, time_divisor, MAX_DELAY_PART);
    }

    /// Precompute the per-channel extraction plan once, in the order the channels are written.
    /// `Bool` is backed by `UInt8`, so the kind must be forced for binary mode rather than inferred from the type.
    auto add_channel = [&](size_t idx, ValueKind kind)
    {
        const auto & type = *header.getByPosition(idx).type;
        channel_extractors.push_back({idx, kind, isNullableType(type)});
    };

    if (mode == Mode::RGB || mode == Mode::RGBA)
    {
        add_channel(*r_idx, classifyValueKind(*header.getByPosition(*r_idx).type));
        add_channel(*g_idx, classifyValueKind(*header.getByPosition(*g_idx).type));
        add_channel(*b_idx, classifyValueKind(*header.getByPosition(*b_idx).type));
    }
    if (mode == Mode::RGBA)
        add_channel(*a_idx, classifyValueKind(*header.getByPosition(*a_idx).type));
    if (mode == Mode::Grayscale)
        add_channel(*v_idx, classifyValueKind(*header.getByPosition(*v_idx).type));
    if (mode == Mode::Binary)
        add_channel(*v_idx, ValueKind::Bool);

    /// Size of one image. For RGBA an empty buffer is transparent;
    /// for RGB / grayscale / binary it is black.
    frame_bytes = 0;
    if (common::mulOverflow(width, height, frame_bytes) || common::mulOverflow(frame_bytes, channels, frame_bytes))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Image dimensions {}x{} with {} channel(s) overflow the maximum buffer size",
            width, height, channels);

    /// In the buffered animated mode the frames are allocated as their values of `t` are encountered;
    /// otherwise there is exactly one image to fill.
    if (!animated || streaming_animation)
    {
        single_frame.pixels.resize_fill(frame_bytes, 0);
        active_frame = &single_frame;
    }
}

void PNGSerializer::Impl::setColumns(const ColumnPtr * columns, size_t num_columns)
{
    /// Materialize the columns so that the per-row reading path works uniformly. This unwraps
    /// `Const`, `Sparse`, `LowCardinality`, etc., into plain columns; in particular a
    /// `LowCardinality(Nullable(...))` becomes a `ColumnNullable`, matching the precomputed `nullable` flags.
    src_columns.clear();
    src_columns.reserve(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        src_columns.push_back(columns[i]->convertToFullIfWrapped()->convertToFullColumnIfLowCardinality());
}

void PNGSerializer::Impl::writePixel(size_t x, size_t y, const UInt8 * components)
{
    UInt8 * ptr = active_frame->pixels.data() + (y * width + x) * channels;
    std::memcpy(ptr, components, channels);
}

void PNGSerializer::Impl::clearFrame(Frame & frame) const
{
    std::fill(frame.pixels.begin(), frame.pixels.end(), UInt8(0));
    frame.implicit_x = 0;
    frame.implicit_y = 0;
}

std::pair<UInt16, UInt16> PNGSerializer::Impl::delayFromUnits(UInt64 units) const
{
    /// The frame is displayed for `units * multiplier / divisor` seconds, which `fcTL` stores as the
    /// fraction `delay_num / delay_den`.
    UInt64 num = 0;
    if (common::mulOverflow(units, time_multiplier, num))
        num = std::numeric_limits<UInt64>::max();
    UInt64 den = time_divisor;

    if (const UInt64 common_divisor = std::gcd(num, den); common_divisor > 1)
    {
        num /= common_divisor;
        den /= common_divisor;
    }

    /// Both parts are 16-bit; the denominator always fits, because the reduced divisor is validated
    /// against `MAX_DELAY_PART`, but the numerator can exceed it. A delay of `MAX_DELAY_PART` seconds
    /// or more is longer than any that `fcTL` can express, and is clamped to the longest one, in the
    /// same spirit as the clamping applied to out-of-range pixel values. The comparison is written with
    /// a division, because a numerator close to the maximum of `UInt64` (two frames at the two ends of
    /// the `t` domain) makes `MAX_DELAY_PART * den` the wrong thing to compute directly.
    if (num > MAX_DELAY_PART)
    {
        if (num / MAX_DELAY_PART >= den)
        {
            num = MAX_DELAY_PART;
            den = 1;
        }
        else
        {
            /// The delay itself fits, only its representation does not. Take the largest denominator that
            /// brings the numerator into 16 bits and round the numerator to the nearest integer, so the
            /// ratio is preserved as precisely as the chunk allows; a floor-division of both parts by a
            /// common factor would distort a ratio whose parts the factor does not divide.
            /// Here `num < MAX_DELAY_PART * den <= MAX_DELAY_PART^2`, so none of this overflows.
            const UInt64 new_den = MAX_DELAY_PART * den / num;
            num = (num * new_den + den / 2) / den;
            den = new_den;
        }
    }

    return {static_cast<UInt16>(num), static_cast<UInt16>(den)};
}

void PNGSerializer::Impl::emitFrame(const Frame & frame, UInt64 delay_units)
{
    if (!frame_callback)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No frame callback is installed on the PNG serializer");

    /// `acTL` declares at most `MAX_DECLARED_FRAMES` frames (in the streaming mode that many are declared
    /// up front as an upper bound), so a frame past that limit would make the declared count wrong and the
    /// datastream invalid. Fail before writing it.
    if (emitted_frames >= PNGWriter::MAX_DECLARED_FRAMES)
        throw Exception(ErrorCodes::TOO_MANY_ROWS,
            "The animation has more than {} frames (distinct values of 't'), which is the largest "
            "frame count the 'acTL' chunk of an animated PNG can declare",
            PNGWriter::MAX_DECLARED_FRAMES);

    last_delay_units = delay_units;
    const auto [delay_num, delay_den] = delayFromUnits(delay_units);
    frame_callback(frame.pixels.data(), delay_num, delay_den);
    ++emitted_frames;
}

/// The value of `t` of one row, mapped to a `UInt64` that keeps the order of the original values. A signed
/// `t` is shifted by half of the domain, which is exactly what flipping the sign bit does. The mapping is
/// affine, so a difference of two keys is the difference of the two values, and the frame delays and the
/// frame order can be computed on the keys alone, without narrowing a `UInt64` `t` to `Int64`.
UInt64 PNGSerializer::Impl::timeKey(size_t row_num) const
{
    const IColumn * t_col = src_columns[*t_idx].get();
    if (t_nullable)
    {
        const auto & nullable = assert_cast<const ColumnNullable &>(*t_col);
        if (nullable.isNullAt(row_num))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Column 't' of the PNG format must not contain NULL, because it determines "
                "which frame of the animation a record belongs to");
        t_col = &nullable.getNestedColumn();
    }

    if (t_unsigned)
        return t_col->getUInt(row_num);
    return static_cast<UInt64>(t_col->getInt(row_num)) ^ SIGNED_TIME_BIAS;
}

/// The original value of `t` behind a key, for error messages.
String PNGSerializer::Impl::timeToString(UInt64 key) const
{
    if (t_unsigned)
        return std::to_string(key);
    return std::to_string(static_cast<Int64>(key ^ SIGNED_TIME_BIAS));
}

void PNGSerializer::Impl::switchFrame(size_t row_num)
{
    const UInt64 time = timeKey(row_num);

    if (current_time.has_value() && time == *current_time)
        return;

    if (streaming_animation)
    {
        if (current_time.has_value())
        {
            if (time < *current_time)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "The 't' column of the PNG format must be non-decreasing when "
                    "'output_format_image_streaming_animation' is enabled, because every frame is written out "
                    "as soon as the next value of 't' is seen, but {} follows {}. "
                    "Add 'ORDER BY t' to the query, or set 'output_format_image_streaming_animation = 0' "
                    "to buffer all frames in memory instead.",
                    timeToString(time), timeToString(*current_time));

            /// Subtraction of two ordered keys cannot overflow, and gives the difference of the values.
            emitFrame(single_frame, time - *current_time);
            clearFrame(single_frame);
        }
    }
    else
    {
        auto [it, inserted] = buffered_frames.try_emplace(time);
        if (inserted)
        {
            /// Fail as soon as the limit of `acTL` is exceeded, instead of buffering frames that could
            /// never be written out; this also keeps the frame count within `UInt32` for
            /// `getDeclaredFrameCount`.
            if (buffered_frames.size() > PNGWriter::MAX_DECLARED_FRAMES)
                throw Exception(ErrorCodes::TOO_MANY_ROWS,
                    "The animation has more than {} frames (distinct values of 't'), which is the largest "
                    "frame count the 'acTL' chunk of an animated PNG can declare",
                    PNGWriter::MAX_DECLARED_FRAMES);
            it->second.pixels.resize_fill(frame_bytes, 0);
        }
        active_frame = &it->second;
    }

    current_time = time;
}

void PNGSerializer::Impl::writeRow(size_t row_num)
{
    if (animated)
        switchFrame(row_num);

    UInt8 components[4] = {0, 0, 0, 255};

    for (size_t channel = 0; channel < channel_extractors.size(); ++channel)
    {
        const auto & extractor = channel_extractors[channel];
        components[channel] = extractByte(*src_columns[extractor.column_index], row_num, extractor.kind, extractor.nullable);
    }

    if (explicit_coords)
    {
        const IColumn * x_col = src_columns[*x_idx].get();
        const IColumn * y_col = src_columns[*y_idx].get();

        if (x_nullable)
        {
            const auto & nullable = assert_cast<const ColumnNullable &>(*x_col);
            if (nullable.isNullAt(row_num))
                return;
            x_col = &nullable.getNestedColumn();
        }
        if (y_nullable)
        {
            const auto & nullable = assert_cast<const ColumnNullable &>(*y_col);
            if (nullable.isNullAt(row_num))
                return;
            y_col = &nullable.getNestedColumn();
        }

        const Int64 x_val = x_col->getInt(row_num);
        const Int64 y_val = y_col->getInt(row_num);

        /// Out-of-range coordinates are silently ignored.
        if (x_val < 0 || y_val < 0)
            return;
        const auto ux = static_cast<UInt64>(x_val);
        const auto uy = static_cast<UInt64>(y_val);
        if (ux >= width || uy >= height)
            return;

        writePixel(ux, uy, components);
    }
    else
    {
        /// The image is filled in scanline order; advance x and y incrementally. Each frame of an animation
        /// is a separate image, so the cursor belongs to the frame and restarts for every value of `t`.
        if (active_frame->implicit_y >= height)
        {
            if (animated)
                throw Exception(ErrorCodes::TOO_MANY_ROWS,
                    "The frame at t = {} has more rows than the {}x{} PNG image can hold ({} pixels). "
                    "Use explicit 'x' and 'y' coordinate columns, or increase "
                    "'output_format_image_width'/'output_format_image_height'.",
                    timeToString(*current_time), width, height, width * height);

            throw Exception(ErrorCodes::TOO_MANY_ROWS,
                "The result has more rows than the {}x{} PNG image can hold ({} pixels). "
                "Use explicit 'x' and 'y' coordinate columns, or increase "
                "'output_format_image_width'/'output_format_image_height'.",
                width, height, width * height);
        }

        writePixel(active_frame->implicit_x, active_frame->implicit_y, components);

        if (++active_frame->implicit_x == width)
        {
            active_frame->implicit_x = 0;
            ++active_frame->implicit_y;
        }
    }
}

void PNGSerializer::Impl::finalizeFrames()
{
    if (!animated)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The PNG result is not an animation");

    /// Every frame handed over from here on is emitted after the whole result has been seen, so the exact
    /// frame count is known even in the streaming mode.
    finalizing = true;

    if (streaming_animation)
    {
        /// The frame being filled is the last one, and there is no following value of `t` to derive its
        /// duration from, so it is displayed for as long as the previous frame was. If the result turned out
        /// to be empty, this hands over the untouched empty image, so that the animation still has a frame.
        emitFrame(single_frame, last_delay_units.value_or(1));
        return;
    }

    /// An animation must have at least one frame, so an empty result becomes a single empty image.
    if (buffered_frames.empty())
        buffered_frames[0].pixels.resize_fill(frame_bytes, 0);

    for (auto it = buffered_frames.begin(); it != buffered_frames.end(); ++it)
    {
        const auto next_frame = std::next(it);
        /// Every frame is displayed until the next one begins; the last one reuses the previous duration.
        const UInt64 delay_units = next_frame != buffered_frames.end()
            ? static_cast<UInt64>(next_frame->first) - static_cast<UInt64>(it->first)
            : last_delay_units.value_or(1);
        emitFrame(it->second, delay_units);
    }
}

UInt32 PNGSerializer::Impl::getDeclaredFrameCount() const
{
    /// In the streaming mode the frames are written out before the result has been read to the end, so the
    /// real count is not known when `acTL` has to be written and an upper bound is declared instead.
    /// The one exception is an animation whose very first frame is handed over from `finalizeFrames`: the
    /// result has been read to the end by then and that frame is the only one, so the count is exact. This
    /// covers an empty result and the common case of a single distinct value of `t`, which would otherwise
    /// produce a spec-conforming file that looks truncated to a decoder that trusts `acTL`.
    if (streaming_animation)
        return (finalizing && emitted_frames == 0) ? 1 : PNGWriter::MAX_DECLARED_FRAMES;

    return buffered_frames.empty() ? 1 : static_cast<UInt32>(buffered_frames.size());
}

void PNGSerializer::Impl::reset()
{
    clearFrame(single_frame);
    buffered_frames.clear();
    src_columns.clear();
    current_time.reset();
    last_delay_units.reset();
    emitted_frames = 0;
    finalizing = false;
    if (!animated || streaming_animation)
        active_frame = &single_frame;
    else
        active_frame = nullptr;
}

PNGSerializer::PNGSerializer(const Block & header, const FormatSettings & settings)
    : impl(std::make_unique<Impl>(header, settings))
{
}

PNGSerializer::~PNGSerializer() = default;

bool PNGSerializer::isAnimated() const
{
    return impl->isAnimated();
}

bool PNGSerializer::isStreamingAnimation() const
{
    return impl->isStreamingAnimation();
}

void PNGSerializer::setFrameCallback(FrameCallback callback)
{
    impl->setFrameCallback(std::move(callback));
}

void PNGSerializer::setColumns(const ColumnPtr * columns, size_t num_columns)
{
    impl->setColumns(columns, num_columns);
}

void PNGSerializer::writeRow(size_t row_num)
{
    impl->writeRow(row_num);
}

void PNGSerializer::finalizeFrames()
{
    impl->finalizeFrames();
}

void PNGSerializer::reset()
{
    (*impl).reset();
}

UInt32 PNGSerializer::getDeclaredFrameCount() const
{
    return impl->getDeclaredFrameCount();
}

size_t PNGSerializer::getWidth() const
{
    return impl->getWidth();
}

size_t PNGSerializer::getHeight() const
{
    return impl->getHeight();
}

size_t PNGSerializer::getChannels() const
{
    return impl->getChannels();
}

const UInt8 * PNGSerializer::getPixels() const
{
    return impl->getPixels();
}

}
