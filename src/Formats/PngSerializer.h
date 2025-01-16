#pragma once

#include "config.h"

#include <Columns/IColumn.h>
#include <Core/NamesAndTypes.h>
#include <Formats/FormatSettings.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_NUMBER_OF_COLUMNS;
    extern const int CANNOT_CONVERT_TYPE;
    extern const int TOO_MANY_ROWS;
}

class PngWriter;
class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;
using DataTypes = std::vector<DataTypePtr>;
class WriteBuffer;

class PngSerializer
{
public:
    virtual ~PngSerializer() = default;

    virtual void setColumns(const ColumnPtr * columns, size_t num_columns) = 0;

    virtual void writeRow(size_t row_num) = 0;

    virtual void finalizeWrite() = 0;

    virtual void reset() = 0;

    static std::unique_ptr<PngSerializer> create(
        const Strings & column_names,
        const DataTypes & data_types,
        size_t width,
        size_t height,
        FormatSettings::PixelMode pixel_mode,
        PngWriter & writer);

    struct RGBA 
    { 
        UInt8 r;
        UInt8 g; 
        UInt8 b; 
        UInt8 a; 
    };

    struct Colors {
        static constexpr auto WHITE = RGBA{255, 255, 255, 255};
        static constexpr auto BLACK = RGBA{0, 0, 0, 255};
        static constexpr auto TRANSPARENT = {0, 0, 0, 0};
    };
};

}
