#include <Formats/PNGSerializer.h>

#include <cstring>
#include <algorithm>
#include <optional>

#include <Columns/ColumnNullable.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeNullable.h>
#include <Formats/PNGWriter.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_NUMBER_OF_COLUMNS;
    extern const int LOGICAL_ERROR;
}

namespace
{
    /// Convert a single value of an integer or float column to an 8-bit pixel component.
    /// Integer values are clamped to [0, 255]. Floating-point values are clamped to [0, 1] and scaled to [0, 255].
    UInt8 extractPixelByte(const IColumn & column, size_t row_num)
    {
        const IColumn * data_column = &column;

        if (const auto * nullable = typeid_cast<const ColumnNullable *>(data_column))
        {
            if (nullable->isNullAt(row_num))
                return 0;
            data_column = &nullable->getNestedColumn();
        }

        switch (data_column->getDataType())
        {
            case TypeIndex::UInt8:
            case TypeIndex::UInt16:
            case TypeIndex::UInt32:
            case TypeIndex::UInt64:
            case TypeIndex::UInt128:
            case TypeIndex::UInt256:
            {
                UInt64 value = data_column->getUInt(row_num);
                return static_cast<UInt8>(std::min<UInt64>(value, 255));
            }
            case TypeIndex::Int8:
            case TypeIndex::Int16:
            case TypeIndex::Int32:
            case TypeIndex::Int64:
            case TypeIndex::Int128:
            case TypeIndex::Int256:
            {
                Int64 value = data_column->getInt(row_num);
                return static_cast<UInt8>(std::clamp<Int64>(value, 0, 255));
            }
            case TypeIndex::Float32:
            case TypeIndex::Float64:
            {
                Float64 value = data_column->getFloat64(row_num);
                if (!std::isfinite(value))
                    return 0;
                value = std::clamp(value, 0.0, 1.0);
                return static_cast<UInt8>(std::lround(value * 255.0));
            }
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Unexpected column type for PNG pixel component");
        }
    }

    UInt8 extractBoolByte(const IColumn & column, size_t row_num)
    {
        const IColumn * data_column = &column;
        if (const auto * nullable = typeid_cast<const ColumnNullable *>(data_column))
        {
            if (nullable->isNullAt(row_num))
                return 0;
            data_column = &nullable->getNestedColumn();
        }
        return data_column->getBool(row_num) ? 255 : 0;
    }

    const IDataType * unwrapNullable(const IDataType * type)
    {
        if (type->isNullable())
            return typeid_cast<const DataTypeNullable &>(*type).getNestedType().get();
        return type;
    }

    bool isAllowedPixelType(const IDataType & type)
    {
        WhichDataType which(*unwrapNullable(&type));
        return which.isNativeInteger() || which.isNativeFloat();
    }

    bool isAllowedBoolType(const IDataType & type)
    {
        return unwrapNullable(&type)->getName() == "Bool";
    }

    bool isAllowedCoordinateType(const IDataType & type)
    {
        return WhichDataType(*unwrapNullable(&type)).isNativeInteger();
    }

    /// Identify a column by its lower-cased name.
    String lowerName(const String & name)
    {
        String result;
        result.reserve(name.size());
        for (char c : name)
            result.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
        return result;
    }
}

class PNGSerializer::Impl
{
public:
    Impl(const Block & header, const FormatSettings & format_settings, PNGWriter & writer);

    void setColumns(const ColumnPtr * columns, size_t num_columns);
    void writeRow(size_t row_num);
    void finalizeWrite();
    void reset();

private:
    enum class Mode : uint8_t
    {
        RGB,
        RGBA,
        Grayscale,
        Binary,
    };

    PNGWriter & writer;
    size_t width = 0;
    size_t height = 0;
    Mode mode = Mode::RGB;
    size_t channels = 0;

    /// Column indices in the input header. nullopt if absent.
    std::optional<size_t> x_idx;
    std::optional<size_t> y_idx;
    std::optional<size_t> r_idx;
    std::optional<size_t> g_idx;
    std::optional<size_t> b_idx;
    std::optional<size_t> a_idx;
    std::optional<size_t> v_idx;

    bool explicit_coords = false;
    size_t implicit_position = 0;

    std::vector<UInt8> pixels;
    std::vector<ColumnPtr> src_columns;

    void writePixel(size_t x, size_t y, const UInt8 * components);
};

PNGSerializer::Impl::Impl(const Block & header, const FormatSettings & format_settings, PNGWriter & writer_)
    : writer(writer_)
    , width(format_settings.image.width)
    , height(format_settings.image.height)
{
    if (width == 0 || height == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Image width and height must be greater than zero (got {}x{})", width, height);

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
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Column '{}' is not recognized by the PNG format. "
                "Expected one of: x, y, r, g, b, a, v (case-insensitive)", col.name);
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
    }

    /// Allocate the image buffer. For RGBA this leaves the image transparent;
    /// for RGB / grayscale / binary this leaves it black.
    pixels.assign(channels * width * height, 0);

    writer.setImage(width, height, channels);
}

void PNGSerializer::Impl::setColumns(const ColumnPtr * columns, size_t num_columns)
{
    src_columns.assign(columns, columns + num_columns);
}

void PNGSerializer::Impl::writePixel(size_t x, size_t y, const UInt8 * components)
{
    UInt8 * ptr = pixels.data() + (y * width + x) * channels;
    std::memcpy(ptr, components, channels);
}

void PNGSerializer::Impl::writeRow(size_t row_num)
{
    UInt8 components[4] = {0, 0, 0, 255};

    switch (mode)
    {
        case Mode::RGB:
            components[0] = extractPixelByte(*src_columns[*r_idx], row_num);
            components[1] = extractPixelByte(*src_columns[*g_idx], row_num);
            components[2] = extractPixelByte(*src_columns[*b_idx], row_num);
            break;
        case Mode::RGBA:
            components[0] = extractPixelByte(*src_columns[*r_idx], row_num);
            components[1] = extractPixelByte(*src_columns[*g_idx], row_num);
            components[2] = extractPixelByte(*src_columns[*b_idx], row_num);
            components[3] = extractPixelByte(*src_columns[*a_idx], row_num);
            break;
        case Mode::Grayscale:
            components[0] = extractPixelByte(*src_columns[*v_idx], row_num);
            break;
        case Mode::Binary:
            components[0] = extractBoolByte(*src_columns[*v_idx], row_num);
            break;
    }

    if (explicit_coords)
    {
        const IColumn * x_col = src_columns[*x_idx].get();
        const IColumn * y_col = src_columns[*y_idx].get();

        if (const auto * nullable = typeid_cast<const ColumnNullable *>(x_col))
        {
            if (nullable->isNullAt(row_num))
                return;
            x_col = &nullable->getNestedColumn();
        }
        if (const auto * nullable = typeid_cast<const ColumnNullable *>(y_col))
        {
            if (nullable->isNullAt(row_num))
                return;
            y_col = &nullable->getNestedColumn();
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
        if (implicit_position >= width * height)
            return;

        size_t x = implicit_position % width;
        size_t y = implicit_position / width;
        ++implicit_position;

        writePixel(x, y, components);
    }
}

void PNGSerializer::Impl::finalizeWrite()
{
    writer.writeImage(reinterpret_cast<const unsigned char *>(pixels.data()));
    writer.finalize();
}

void PNGSerializer::Impl::reset()
{
    std::fill(pixels.begin(), pixels.end(), UInt8(0));
    src_columns.clear();
    implicit_position = 0;
}

PNGSerializer::PNGSerializer(const Block & header, const FormatSettings & settings, PNGWriter & writer)
    : impl(std::make_unique<Impl>(header, settings, writer))
{
}

PNGSerializer::~PNGSerializer() = default;

void PNGSerializer::setColumns(const ColumnPtr * columns, size_t num_columns)
{
    impl->setColumns(columns, num_columns);
}

void PNGSerializer::writeRow(size_t row_num)
{
    impl->writeRow(row_num);
}

void PNGSerializer::finalizeWrite()
{
    impl->finalizeWrite();
}

void PNGSerializer::reset()
{
    impl->reset();
}

}
