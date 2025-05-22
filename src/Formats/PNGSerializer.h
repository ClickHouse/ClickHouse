#pragma once

#include <Columns/IColumn_fwd.h>
#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <Formats/FormatSettings.h>
#include <Common/logger_useful.h>

namespace DB
{

using PixelOutputFormat = FormatSettings::PNGPixelOutputFormat;
using CoordinatesFormat = FormatSettings::PNGCoordinatesFormat;

/// ---------------------------------------------------
/// Policy: Pixel output format
/// ---------------------------------------------------

struct OuputFormatBinary
{
    static constexpr auto Channels = 1U;
    static constexpr auto Format = PixelOutputFormat::BINARY;
};

struct OutputFormatGrayscale
{
    static constexpr auto Channels = 1U;
    static constexpr auto Format = PixelOutputFormat::GRAYSCALE;
};

struct OutputFormatRGB
{
    static constexpr auto Channels = 3U;
    static constexpr auto Format = PixelOutputFormat::RGB;
};

struct OutputFormatRGBA
{
    static constexpr auto Channels = 4U;
    static constexpr auto Format = PixelOutputFormat::RGBA;
};

/// ---------------------------------------------------
/// Policy: Coordinates format
/// ---------------------------------------------------

struct CoordinatesFormatImplicit
{
    static constexpr auto Format = CoordinatesFormat::IMPLICIT;
    static constexpr auto ExplicitCoords = false;
    static constexpr auto CoordinateColumns = 0U;
};
struct CoordinatesFormatExplicit
{
    static constexpr auto Format = CoordinatesFormat::EXPLICIT;
    static constexpr auto ExplicitCoords = true;
    static constexpr auto CoordinateColumns = 2U;
};

class PNGWriter;

class PNGSerializer
{
public:
    PNGSerializer(const Block & header, const FormatSettings & settings, PNGWriter & writer);

    virtual ~PNGSerializer();

    void setColumns(const ColumnPtr * columns, size_t num_columns);

    void writeRow(size_t row_num);

    void finalizeWrite();

    size_t getProcessedInputRowCount() const;

    void reset();

protected:
    class SerializerImpl;
    std::unique_ptr<SerializerImpl> impl;

    template <typename CoordinatesFormatPolicy, typename PixelsFormatPolicy>
    friend class SerializerImplTemplated;
};

}
