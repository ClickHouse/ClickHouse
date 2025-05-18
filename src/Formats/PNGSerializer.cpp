#include "base/types.h"

#include <string_view>
#include <array>
#include <optional>
#include <cstring>
#include <absl/container/flat_hash_map.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Formats/FormatSettings.h>
#include <DataTypes/IDataType.h>
#include <Formats/PNGSerializer.h>
#include <Formats/PNGWriter.h>
#include <Common/Exception.h>

namespace DB
{


namespace ErrorCodes
{
extern const int INCORRECT_NUMBER_OF_COLUMNS;
extern const int LOGICAL_ERROR;
extern const int TOO_MANY_ROWS;
extern const int CANNOT_CONVERT_TYPE;
extern const int NOT_IMPLEMENTED;
}

namespace
{

/* Extracts a boolean value specifically for the binary pixel format
 * Handles Nullable, Const wrappers (TODO), ensures the underlying type is Bool */
inline bool extractBool(const IColumn & col, size_t row_num)
{
    if (const auto * null_col = typeid_cast<const ColumnNullable *>(&col))
    {
        if (null_col->isNullAt(row_num))
            return false;

        const auto & nested = null_col->getNestedColumn();
        return nested.getBool(row_num);
    }

    return col.getBool(row_num);
}

inline UInt16 extractPixelComponentImpl(const IColumn & data_col, size_t row_num, int bit_depth)
{
    auto type_id = data_col.getDataType();
    auto max_val_u16 = (bit_depth == 16) ? std::numeric_limits<uint16_t>::max() : std::numeric_limits<uint8_t>::max();
    Float64 max_val_float = static_cast<Float64>(max_val_u16);
    switch (type_id)
    {
        case TypeIndex::UInt8:
        case TypeIndex::UInt16:
        case TypeIndex::UInt32:
        case TypeIndex::UInt64: {
            UInt64 val = data_col.getUInt(row_num);
            return static_cast<UInt16>(std::min(val, static_cast<UInt64>(max_val_u16)));
        }

        case TypeIndex::Int8:
        case TypeIndex::Int16:
        case TypeIndex::Int32:
        case TypeIndex::Int64: {
            Int64 val = data_col.getInt(row_num);
            return static_cast<UInt16>(std::clamp(val, INT64_C(0), static_cast<Int64>(max_val_u16)));
        }

        case TypeIndex::Float32:
        case TypeIndex::Float64: {
            Float64 val = data_col.getFloat64(row_num);
            /* IMPORTANT: Input floats are expected to be directly in the target range
             * For 8 bit is [0, 255], or [0, 65535] for 16
             * Values outside this range will be clamped. No automatic scaling */
            val = std::round(std::clamp(val, 0.0, max_val_float));
            return static_cast<UInt16>(val);
        }
        default:
            throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Unsupported type for pixel component");
    }
}

/* Handles wrapper columns like Nullable, Const, and LowCardinality (TODO),
* then delegates the actual data conversion */
inline UInt16 extractPixelComponent(const IColumn & col, size_t row_num, int bit_depth)
{
    if (const auto * nullable_col = typeid_cast<const ColumnNullable *>(&col)) [[unlikely]]
    {
        if (nullable_col->isNullAt(row_num))
            return 0; ///< Assume default color for NULL values is black

        return extractPixelComponent(nullable_col->getNestedColumn(), row_num, bit_depth);
    }

    /// TODO
    if ([[maybe_unused]] const auto * lc_col = typeid_cast<const ColumnLowCardinality *>(&col)) [[unlikely]]
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "LowCardinality column support is not implemented yet for PNG format");
    }


    if ([[maybe_unused]] const auto * const_col = typeid_cast<const ColumnConst *>(&col)) [[unlikely]]
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Const column support is not implemented yet for PNG format");
    }

    return extractPixelComponentImpl(col, row_num, bit_depth);
}

}

class PNGSerializer::SerializerImpl
{
public:
    SerializerImpl(const Block & header_, const FormatSettings & format_settings_, PNGWriter & writer_, size_t color_channels_)
        : format_settings(format_settings_)
        , header(header_)
        , writer(writer_)
        , max_width(format_settings.png_image.max_width)
        , max_height(format_settings.png_image.max_height)
        , bit_depth(format_settings.png_image.bit_depth)
        , bytes_per_component((bit_depth == 16) ? 2 : 1)
        , channels(color_channels_)
    {
        if (bit_depth != 8 && bit_depth != 16)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "PNG format currently only supports 8 or 16 bit depth, got {}", bit_depth);

        pixels.resize(channels * bytes_per_component * max_width * max_height);
        std::fill(pixels.begin(), pixels.end(), std::byte(0));
    }

    virtual ~SerializerImpl() = default;
    virtual void commonSetColumns(const ColumnPtr * columns, size_t num_columns) = 0;
    virtual void commonWriteRowImpl(size_t row_num) = 0;

    void commonFinalizeWrite()
    {
        try
        {
            writer.startImage();
            writer.writeRows(reinterpret_cast<const unsigned char *>(pixels.data()), pixels.size());
            writer.finalize();
        }
        catch (...)
        {
            clear();
            throw;
        }
    }

    void clear()
    {
        pixels.clear();
        /// std::fill(pixels.begin(), pixels.end(), std::byte(0));
        processed_input_rows = 0;
        src_columns.clear();
    }

    void commonReset() { clear(); }

protected:
    friend class PNGSerializer;

    const FormatSettings & format_settings;
    const Block & header;
    PNGWriter & writer;
    
    size_t max_width;
    size_t max_height;
    size_t processed_input_rows = 0;

    int bit_depth;
    size_t bytes_per_component;
    size_t channels;

    std::vector<std::byte> pixels;
    std::vector<ColumnPtr> src_columns;
};

/* Template parameters:
 * - CoordinatesFormatPolicy: Controls data configuration (implicit/explicit coordinates)
 * - PixelsFormatPolicy: Controls output color format (RGBA/RGB/binary/grayscale)
*/
template <typename CoordinatesFormatPolicy, typename PixelsFormatPolicy>
class SerializerImplTemplated final : public PNGSerializer::SerializerImpl
{
public:
    SerializerImplTemplated(const Block & header_, const FormatSettings & format_settings_, PNGWriter & writer_, size_t color_channels_)
        : PNGSerializer::SerializerImpl(header_, format_settings_, writer_, color_channels_)
    {
        if constexpr (PixelsFormatPolicy::Format == PixelOutputFormat::BINARY)
        {
            if (bit_depth != 8)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Binary pixel format only supports 8-bit depth");
        }
        if constexpr (CoordinatesFormatPolicy::Format == CoordinatesFormat::EXPLICIT)
        {
            /// We explicitly require to specify columns with coordinates as 'x' and 'y' (case-insensitive)
            coord_x_col_idx = header.getPositionByName("x", /* case_insensitive */ true);
            coord_y_col_idx = header.getPositionByName("y", /* case_insensitive */ true);
        }

        constexpr auto expected_channel_names = 
                    getExpectedChannelNames<PixelsFormatPolicy::Format, PixelsFormatPolicy::Channels>();
        
        for (size_t c = 0; c < PixelsFormatPolicy::Channels; ++c)
        {
            const auto & name = expected_channel_names[c];
            if (!header.has(String(name)))
            {
                throw Exception(ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS,
                    "Required channel column '{}' is missing in the header for pixel format '{}'",
                    name, PixelsFormatPolicy::Format);
            }

            channel_col_indices[c] = header.getPositionByName(String(name));

            const auto & channel_type = header.getByPosition(channel_col_indices[c]).type;
            if (!isInteger(channel_type) && !isFloat(channel_type) && !(PixelsFormatPolicy::Format == PixelOutputFormat::BINARY && isBool(channel_type)))
            {
                throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE,
                    "Channel column '{}' must be a numeric type (or Bool for BINARY), but got '{}'",
                    name, channel_type->getName());
            }
        }
    }

    ~SerializerImplTemplated() override = default;

    void commonSetColumns(const ColumnPtr * columns, size_t num_columns) override
    {
        if (num_columns != CoordinatesFormatPolicy::CoordinateColumns + channels)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Expected {} columns, got {}", CoordinatesFormatPolicy::CoordinateColumns + channels, num_columns);

        src_columns.assign(columns, columns + num_columns);
    }

    void commonWritePixelDataAt(size_t x, size_t y, const UInt16 * components)
    {
        if (x >= max_width || y >= max_height)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Attempted to write pixel at ({}, {}) out of bounds {}x{}", x, y, max_width, max_height);
        }

        size_t pixel_buffer_offset = (y * max_width + x);
        size_t write_pos = pixel_buffer_offset * channels * bytes_per_component;

        std::byte * ptr = pixels.data() + write_pos;

        if (bit_depth == 16)
        {
            std::memcpy(ptr, components, channels * bytes_per_component);
        }
        else /// 8-bit
        {
            for (size_t c = 0; c < channels; ++c)
            {
                ptr[c] = static_cast<std::byte>(static_cast<UInt8>(components[c]));
            }
        }
    }

    void commonWriteRowImpl(size_t row_num) override
    {
        std::array<UInt16, PixelsFormatPolicy::Channels> components;
        if constexpr (PixelsFormatPolicy::Format == PixelOutputFormat::BINARY)
        {
            bool v = extractBool(*src_columns[channel_col_indices[0]], row_num);
            components[0] = static_cast<UInt16>(v ? 255 : 0);
        }
        else
        {
            for (size_t c = 0; c < PixelsFormatPolicy::Channels; ++c)
            {
                components[c] = extractPixelComponent(*src_columns[channel_col_indices[c]], row_num, bit_depth);
            }
        }

        /// In row-per-pixel approach we sequentially write the pixels until the buffer is full
        if constexpr (CoordinatesFormatPolicy::Format == CoordinatesFormat::IMPLICIT)
        {
            /// Alternative approach: instead of throwing exception
            /// We can just ignore extra pixels
            if (processed_input_rows >= max_height * max_width)
                throw Exception(ErrorCodes::TOO_MANY_ROWS, "Exceeded maximum image resolution: {}x{}", max_width, max_height);

            size_t current_pixel_index = processed_input_rows;
            size_t x = current_pixel_index % max_width;
            size_t y = current_pixel_index / max_width;

            commonWritePixelDataAt(x, y, components.data());
        }
        else if constexpr (CoordinatesFormatPolicy::Format == CoordinatesFormat::EXPLICIT)
        {
            auto x_val = src_columns[coord_x_col_idx.value()]->getInt(row_num);
            auto y_val = src_columns[coord_y_col_idx.value()]->getInt(row_num);

            if (x_val < 0 || static_cast<size_t>(x_val) >= max_width || y_val < 0 || static_cast<size_t>(y_val) >= max_height)
            {
                /// Alternative approach: ignore pixels that are out of bounds
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Coordinates ({}, {}) are out of bounds for image size {}x{}",
                    x_val, y_val, max_width, max_height);
            }

            size_t x = static_cast<size_t>(x_val);
            size_t y = static_cast<size_t>(y_val);

            commonWritePixelDataAt(x, y, components.data());
        }

        ++processed_input_rows;
    }

private:
    template <PixelOutputFormat TFormat, size_t TChannels>
    static constexpr std::array<std::string_view, TChannels> getExpectedChannelNames()
    {
        if constexpr (TFormat == PixelOutputFormat::BINARY || TFormat == PixelOutputFormat::GRAYSCALE)
            return { "v" };
        else if constexpr (TFormat == PixelOutputFormat::RGB)
            return { "r", "g", "b" };
        else if constexpr (TFormat == PixelOutputFormat::RGBA)
            return { "r", "g", "b", "a" };
    }

    static constexpr std::string_view coord_x_name_sv = "x";
    static constexpr std::string_view coord_y_name_sv = "y";

    std::optional<size_t> coord_x_col_idx;
    std::optional<size_t> coord_y_col_idx;

    std::array<size_t, PixelsFormatPolicy::Channels> channel_col_indices;
};

namespace
{
    using PixelFormatDispatchMap = absl::flat_hash_map<std::string_view, PixelOutputFormat>;
    using CoordFormatDispatchMap = absl::flat_hash_map<std::string_view, CoordinatesFormat>;

    const PixelFormatDispatchMap & getPixelFormatDispatchMap()
    {
        static const PixelFormatDispatchMap map = {
            {"BINARY",    PixelOutputFormat::BINARY},
            {"GRAYSCALE", PixelOutputFormat::GRAYSCALE},
            {"RGB",       PixelOutputFormat::RGB},
            {"RGBA",      PixelOutputFormat::RGBA}
        };
        return map;
    }

    const CoordFormatDispatchMap & getCoordFormatDispatchMap()
    {
        static const CoordFormatDispatchMap map = {
            {"EXPLICIT", CoordinatesFormat::EXPLICIT},
            {"IMPLICIT", CoordinatesFormat::IMPLICIT}
        };
        return map;
    }
} /// anonymous namespace

PNGSerializer::PNGSerializer(const Block & header, const FormatSettings & settings, PNGWriter & writer)
{
    const auto & pixel_format_map = getPixelFormatDispatchMap();
    
    auto pixel_format_it = pixel_format_map.find(settings.png_image.pixel_output_format);
    if (pixel_format_it == pixel_format_map.end())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported PNG pixel output format: {}", settings.png_image.pixel_output_format);
    }

    const auto & coord_format_map = getCoordFormatDispatchMap();
    auto coord_mode_it = coord_format_map.find(settings.png_image.coordinates_format);
    if (coord_mode_it == coord_format_map.end())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported PNG pixel input mode: {}", settings.png_image.coordinates_format);
    }

    auto input_mode_enum = coord_mode_it->second;
    auto pixel_format_enum = pixel_format_it->second;
    size_t coordinate_columns = (input_mode_enum == CoordinatesFormat::EXPLICIT) ? CoordinatesFormatExplicit::CoordinateColumns : CoordinatesFormatImplicit::CoordinateColumns;
    size_t color_channels;
    
    switch (pixel_format_enum)
    {
        case PixelOutputFormat::BINARY:
            color_channels = OuputFormatBinary::Channels;
            break;
        case PixelOutputFormat::GRAYSCALE:
            color_channels = OutputFormatGrayscale::Channels;
            break;
        case PixelOutputFormat::RGB:
            color_channels = OutputFormatRGB::Channels;
            break;
        case PixelOutputFormat::RGBA:
            color_channels = OutputFormatRGBA::Channels;
            break;
    }

    size_t required_columns = coordinate_columns + color_channels;
    if (header.columns() != required_columns)
    {
        throw Exception(
            ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS,
            "Serializer for pixel format ('{}') and coord format ('{}') expects {} columns, but got {}",
            settings.png_image.pixel_output_format,
            settings.png_image.coordinates_format,
            required_columns,
            header.columns());
    }

    if (input_mode_enum == CoordinatesFormat::EXPLICIT)
    {
        switch (pixel_format_enum)
        {
            case PixelOutputFormat::BINARY:
                impl = std::make_unique<SerializerImplTemplated<CoordinatesFormatExplicit, OuputFormatBinary>>(
                    header, settings, writer, OuputFormatBinary::Channels);
                break;
            case PixelOutputFormat::GRAYSCALE:
                impl = std::make_unique<SerializerImplTemplated<CoordinatesFormatExplicit, OutputFormatGrayscale>>(
                    header, settings, writer, OutputFormatGrayscale::Channels);
                break;
            case PixelOutputFormat::RGB:
                impl = std::make_unique<SerializerImplTemplated<CoordinatesFormatExplicit, OutputFormatRGB>>(
                    header, settings, writer, OutputFormatRGB::Channels);
                break;
            case PixelOutputFormat::RGBA:
                impl = std::make_unique<SerializerImplTemplated<CoordinatesFormatExplicit, OutputFormatRGBA>>(
                    header, settings, writer, OutputFormatRGBA::Channels);
                break;
        }
    }
    else /// implicit
    {
        switch (pixel_format_enum)
        {
            case PixelOutputFormat::BINARY:
                impl = std::make_unique<SerializerImplTemplated<CoordinatesFormatImplicit, OuputFormatBinary>>(
                    header, settings, writer, OuputFormatBinary::Channels);
                break;
            case PixelOutputFormat::GRAYSCALE:
                impl = std::make_unique<SerializerImplTemplated<CoordinatesFormatImplicit, OutputFormatGrayscale>>(
                    header, settings, writer, OutputFormatGrayscale::Channels);
                break;
            case PixelOutputFormat::RGB:
                impl = std::make_unique<SerializerImplTemplated<CoordinatesFormatImplicit, OutputFormatRGB>>(
                    header, settings, writer, OutputFormatRGB::Channels);
                break;
            case PixelOutputFormat::RGBA:
                impl = std::make_unique<SerializerImplTemplated<CoordinatesFormatImplicit, OutputFormatRGBA>>(
                    header, settings, writer, OutputFormatRGBA::Channels);
                break;
        }
    }
}

PNGSerializer::~PNGSerializer() = default;


void PNGSerializer::setColumns(const ColumnPtr * columns, size_t num_columns)
{
    impl->commonSetColumns(columns, num_columns);
}

void PNGSerializer::writeRow(size_t row_num)
{
    impl->commonWriteRowImpl(row_num);
}

void PNGSerializer::finalizeWrite()
{
    impl->commonFinalizeWrite();
}

void PNGSerializer::reset()
{
    impl->commonReset();
}

size_t PNGSerializer::getProcessedInputRowCount() const
{
    return impl->processed_input_rows;
}

}
