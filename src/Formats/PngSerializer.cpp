#include "base/types.h"

#include <Columns/ColumnConst.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Formats/FormatSettings.h>
#include <DataTypes/IDataType.h>
#include <Formats/PngSerializer.h>
#include <Formats/PngWriter.h>
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
    auto max_val_u16 = (bit_depth == 16) ? 65535 : 255;
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
            throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Cannot convert to UInt8 for PNG output pixel component. Unsupported type");
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

class PngSerializer::SerializerImpl
{
public:
    SerializerImpl(const FormatSettings & format_settings_, PngWriter & writer_, size_t color_channels_)
        : format_settings(format_settings_)
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
        /// pixels.clear();
        std::fill(pixels.begin(), pixels.end(), std::byte(0));
        processed_input_rows = 0;
        src_columns.clear();
    }

    void commonReset() { clear(); }

protected:
    friend class PngSerializer;

    const FormatSettings & format_settings;
    PngWriter & writer;

    size_t max_width = 1;
    size_t max_height = 1;
    size_t processed_input_rows = 0;

    int bit_depth;
    size_t bytes_per_component;
    size_t channels;

    std::vector<std::byte> pixels;
    std::vector<ColumnPtr> src_columns;
};

template <typename InputModePolicy, typename PixelFormatPolicy>
class SerializerImplTemplated final : public PngSerializer::SerializerImpl
{
public:
    SerializerImplTemplated(const FormatSettings & format_settings_, PngWriter & writer_, size_t color_channels_)
        : PngSerializer::SerializerImpl(format_settings_, writer_, color_channels_)
    {
        if constexpr (PixelFormatPolicy::Format == PngPixelFormat::BINARY)
        {
            if (bit_depth != 8)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Binary pixel format only supports 8-bit depth");
        }
        if constexpr (InputModePolicy::Mode == PngPixelInputMode::EXPLICIT_COORDINATES)
        {
            /// TODO: change for column name "x" and "y" later
            coord_x_col_idx = 0;
            coord_y_col_idx = 1;
        }
    }

    ~SerializerImplTemplated() override = default;


    void commonSetColumns(const ColumnPtr * columns, size_t num_columns) override
    {
        if (num_columns != InputModePolicy::CoordinateColumns + channels)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Expected {} columns, got {}", InputModePolicy::CoordinateColumns + channels, num_columns);

        src_columns.assign(columns, columns + num_columns);

        if constexpr (InputModePolicy::Mode == PngPixelInputMode::EXPLICIT_COORDINATES)
        {
            const auto & x_type = src_columns[coord_x_col_idx]->getDataType();
            const auto & y_type = src_columns[coord_y_col_idx]->getDataType();
            if (!isInteger(x_type) || !isInteger(y_type))
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Expected integer types for explicit coordinates");
        }
    }

    void writePixelDataAt(size_t x, size_t y, const UInt16 * components)
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
        else if (bit_depth == 8)
        {
            for (size_t c = 0; c < channels; ++c)
            {
                ptr[c] = static_cast<std::byte>(static_cast<UInt8>(components[c]));
            }
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported bit depth: {}", bit_depth);
        }
    }

    void commonWriteRowImpl(size_t row_num) override
    {
        /// In row-per-pixel approach we sequentially write the pixels until the buffer is full
        if constexpr (InputModePolicy::Mode == PngPixelInputMode::SCANLINE)
        {
            /// Alternative approach: instead of throwing exception
            /// We can just ignore extra pixels
            if (processed_input_rows >= max_height * max_width)
                throw Exception(ErrorCodes::TOO_MANY_ROWS, "Exceeded maximum image resolution: {}x{}", max_width, max_height);

            size_t current_pixel_index = processed_input_rows;
            size_t x = current_pixel_index % max_width;
            size_t y = current_pixel_index / max_width;

            std::array<UInt16, PixelFormatPolicy::Channels> components;
            size_t data_col_offset = InputModePolicy::CoordinateColumns;
            if constexpr (PixelFormatPolicy::Format == PngPixelFormat::BINARY)
            {
                bool val = extractBool(*src_columns[data_col_offset], row_num);
                components[0] = static_cast<UInt16>(val ? 255 : 0);
            }
            else
            {
                for (size_t c = 0; c < PixelFormatPolicy::Channels; ++c)
                {
                    components[c] = extractPixelComponent(*src_columns[data_col_offset + c], row_num, bit_depth);
                }
            }

            writePixelDataAt(x, y, components.data());
        }
        else if constexpr (InputModePolicy::Mode == PngPixelInputMode::EXPLICIT_COORDINATES)
        {
            auto x_val = src_columns[coord_x_col_idx]->getInt(row_num);
            auto y_val = src_columns[coord_y_col_idx]->getInt(row_num);

            if (x_val < 0 || static_cast<size_t>(x_val) >= max_width || y_val < 0 || static_cast<size_t>(y_val) >= max_height)
            {
                /// Alternative approach: ignore pixels that are out of bounds
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Coordinates ({}, {}) are out of bounds for image size {}x{}",
                    x_val,
                    y_val,
                    max_width,
                    max_height);
            }

            size_t x = static_cast<size_t>(x_val);
            size_t y = static_cast<size_t>(y_val);

            std::array<UInt16, PixelFormatPolicy::Channels> components;
            size_t data_col_offset = InputModePolicy::CoordinateColumns;

            if constexpr (PixelFormatPolicy::Format == PngPixelFormat::BINARY)
            {
                bool val = extractBool(*src_columns[data_col_offset], row_num);
                components[0] = static_cast<UInt16>(val ? 255 : 0);
            }
            else
            {
                for (size_t c = 0; c < PixelFormatPolicy::Channels; ++c)
                {
                    components[c] = extractPixelComponent(*src_columns[data_col_offset + c], row_num, bit_depth);
                }
            }

            writePixelDataAt(x, y, components.data());
        }

        ++processed_input_rows;
    }

private:
    size_t coord_x_col_idx;
    size_t coord_y_col_idx;
};

PngSerializer::PngSerializer(const FormatSettings & settings, PngWriter & writer)
{
    PngPixelFormat pixel_format_enum;
    [[maybe_unused]] int bit_depth = settings.png_image.bit_depth;

    if (settings.png_image.pixel_output_format == "BINARY")
    {
        pixel_format_enum = PngPixelFormat::BINARY;
        bit_depth = 8;
    }
    else if (settings.png_image.pixel_output_format == "GRAYSCALE")
    {
        pixel_format_enum = PngPixelFormat::GRAYSCALE;
    }
    else if (settings.png_image.pixel_output_format == "RGB")
    {
        pixel_format_enum = PngPixelFormat::RGB;
    }
    else if (settings.png_image.pixel_output_format == "RGBA")
    {
        pixel_format_enum = PngPixelFormat::RGBA;
    }
    else
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Unsupported PNG pixel output format: {}", settings.png_image.pixel_output_format);
    }

    PngPixelInputMode input_mode_enum;

    if (settings.png_image.pixel_input_mode == "EXPLICIT_COORDINATES")
    {
        input_mode_enum = PngPixelInputMode::EXPLICIT_COORDINATES;
    }
    else if (settings.png_image.pixel_input_mode == "SCANLINE")
    {
        input_mode_enum = PngPixelInputMode::SCANLINE;
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported PNG pixel input mode: {}", settings.png_image.pixel_input_mode);
    }

    if (input_mode_enum == PngPixelInputMode::EXPLICIT_COORDINATES)
    {
        switch (pixel_format_enum)
        {
            case PngPixelFormat::BINARY:
                impl = std::make_unique<SerializerImplTemplated<InputModeExplicit, PixelFormatBinary>>(
                    settings, writer, PixelFormatBinary::Channels);
                break;
            case PngPixelFormat::GRAYSCALE:
                impl = std::make_unique<SerializerImplTemplated<InputModeExplicit, PixelFormatGrayscale>>(
                    settings, writer, PixelFormatGrayscale::Channels);
                break;
            case PngPixelFormat::RGB:
                impl = std::make_unique<SerializerImplTemplated<InputModeExplicit, PixelFormatRGB>>(
                    settings, writer, PixelFormatRGB::Channels);
                break;
            case PngPixelFormat::RGBA:
                impl = std::make_unique<SerializerImplTemplated<InputModeExplicit, PixelFormatRGBA>>(
                    settings, writer, PixelFormatRGBA::Channels);
                break;
        }
    }
    else /// SCANLINE
    {
        switch (pixel_format_enum)
        {
            case PngPixelFormat::BINARY:
                impl = std::make_unique<SerializerImplTemplated<InputModeScanline, PixelFormatBinary>>(
                    settings, writer, PixelFormatBinary::Channels);
                break;
            case PngPixelFormat::GRAYSCALE:
                impl = std::make_unique<SerializerImplTemplated<InputModeScanline, PixelFormatGrayscale>>(
                    settings, writer, PixelFormatGrayscale::Channels);
                break;
            case PngPixelFormat::RGB:
                impl = std::make_unique<SerializerImplTemplated<InputModeScanline, PixelFormatRGB>>(
                    settings, writer, PixelFormatRGB::Channels);
                break;
            case PngPixelFormat::RGBA:
                impl = std::make_unique<SerializerImplTemplated<InputModeScanline, PixelFormatRGBA>>(
                    settings, writer, PixelFormatRGBA::Channels);
                break;
        }
    }
}

PngSerializer::~PngSerializer() = default;


void PngSerializer::setColumns(const ColumnPtr * columns, size_t num_columns)
{
    impl->commonSetColumns(columns, num_columns);
}

void PngSerializer::writeRow(size_t row_num)
{
    impl->commonWriteRowImpl(row_num);
}

void PngSerializer::finalizeWrite()
{
    impl->commonFinalizeWrite();
}

void PngSerializer::reset()
{
    impl->commonReset();
}

size_t PngSerializer::getProcessedInputRowCount() const
{
    return impl->processed_input_rows;
}

std::unique_ptr<PngSerializer> PngSerializer::create(const Block & header, const FormatSettings & settings, PngWriter & writer)
{
    [[maybe_unused]] PngPixelInputMode input_mode_enum;

    size_t coordinate_columns;

    if (settings.png_image.pixel_input_mode == "EXPLICIT_COORDINATES")
    {
        input_mode_enum = PngPixelInputMode::EXPLICIT_COORDINATES;
        coordinate_columns = InputModeExplicit::CoordinateColumns;
    }
    else if (settings.png_image.pixel_input_mode == "SCANLINE")
    {
        input_mode_enum = PngPixelInputMode::SCANLINE;
        coordinate_columns = InputModeScanline::CoordinateColumns;
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported PNG pixel input mode: {}", settings.png_image.pixel_input_mode);
    }

    PngPixelFormat pixel_format_enum;
    if (settings.png_image.pixel_output_format == "BINARY")
    {
        pixel_format_enum = PngPixelFormat::BINARY;
    }
    else if (settings.png_image.pixel_output_format == "GRAYSCALE")
    {
        pixel_format_enum = PngPixelFormat::GRAYSCALE;
    }
    else if (settings.png_image.pixel_output_format == "RGB")
    {
        pixel_format_enum = PngPixelFormat::RGB;
    }
    else if (settings.png_image.pixel_output_format == "RGBA")
    {
        pixel_format_enum = PngPixelFormat::RGBA;
    }
    else
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Unsupported PNG pixel output format: {}", settings.png_image.pixel_output_format);
    }

    size_t color_channels;
    switch (pixel_format_enum)
    {
        case PngPixelFormat::BINARY:
            color_channels = PixelFormatBinary::Channels;
            break;
        case PngPixelFormat::GRAYSCALE:
            color_channels = PixelFormatGrayscale::Channels;
            break;
        case PngPixelFormat::RGB:
            color_channels = PixelFormatRGB::Channels;
            break;
        case PngPixelFormat::RGBA:
            color_channels = PixelFormatRGBA::Channels;
            break;
    }

    size_t required_columns = coordinate_columns + color_channels;

    if (header.columns() != required_columns)
    {
        throw Exception(
            ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS,
            "Serializer for pixel format '{}' and order '{}' expects {} columns, but got {}",
            settings.png_image.pixel_output_format,
            settings.png_image.pixel_input_mode,
            required_columns,
            header.columns());
    }

    return std::make_unique<PngSerializer>(settings, writer);
}

}
