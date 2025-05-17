#pragma once

#include <Columns/IColumn_fwd.h>
#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <Formats/FormatSettings.h>
#include <Common/logger_useful.h>

namespace DB
{

using PngPixelFormat = FormatSettings::PngPixelFormat;
using PngPixelInputMode = FormatSettings::PngPixelInputMode;

/// ---------------------------------------------------
/// Policy: Pixel format
/// ---------------------------------------------------

struct PixelFormatBinary
{
    static constexpr auto Channels = 1ULL;
    static constexpr auto Format = PngPixelFormat::BINARY;
};

struct PixelFormatGrayscale
{
    static constexpr auto Channels = 1ULL;
    static constexpr auto Format = PngPixelFormat::GRAYSCALE;
};

struct PixelFormatRGB
{
    static constexpr auto Channels = 3ULL;
    static constexpr auto Format = PngPixelFormat::RGB;
};

struct PixelFormatRGBA
{
    static constexpr auto Channels = 4ULL;
    static constexpr auto Format = PngPixelFormat::RGBA;
};

/// ---------------------------------------------------
/// Policy: Input mode
/// ---------------------------------------------------

struct InputModeScanline
{
    static constexpr PngPixelInputMode Mode = PngPixelInputMode::SCANLINE;
    static constexpr bool ExplicitCoords = false;
    static constexpr auto CoordinateColumns = 0ULL;
};
struct InputModeExplicit
{
    static constexpr PngPixelInputMode Mode = PngPixelInputMode::EXPLICIT_COORDINATES;
    static constexpr bool ExplicitCoords = true;
    static constexpr auto CoordinateColumns = 2;
};

class PngWriter;
class WriteBuffer;

class PngSerializer
{
public:
    PngSerializer(const FormatSettings & settings, PngWriter & writer);

    virtual ~PngSerializer();

    void setColumns(const ColumnPtr * columns, size_t num_columns);

    void writeRow(size_t row_num);

    void finalizeWrite();

    size_t getProcessedInputRowCount() const;

    void reset();

    static std::unique_ptr<PngSerializer> create(const Block & header, const FormatSettings & settings, PngWriter & writer);

protected:
    class SerializerImpl;
    std::unique_ptr<SerializerImpl> impl;

    template <typename InputModePolicy, typename PixelFormatPolicy>
    friend class SerializerImplTemplated;
};

}
