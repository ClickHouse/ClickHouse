#pragma once

#include <Columns/IColumn_fwd.h>
#include <Core/NamesAndTypes.h>
#include <Formats/FormatSettings.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_NUMBER_OF_COLUMNS;
extern const int CANNOT_CONVERT_TYPE;
extern const int TOO_MANY_ROWS;
}

using PngPixelFormat = FormatSettings::PngPixelFormat;

class PngWriter;
class WriteBuffer;

class PngSerializer
{
public:
    virtual ~PngSerializer();

    virtual void setColumns(const ColumnPtr * columns, size_t num_columns) = 0;

    virtual void writeRow(size_t row_num) = 0;

    void finalizeWrite(size_t width, size_t height);

    size_t getRowCount() const;

    void reset();

    static std::unique_ptr<PngSerializer> create(
        const DataTypes & data_types,
        size_t width,
        size_t height,
        PngPixelFormat pixel_format,
        PngWriter & writer,
        int bit_depth);

protected:
    class SerializerImpl;
    std::unique_ptr<SerializerImpl> impl;
    
    PngSerializer(size_t width_, size_t height_, PngWriter & writer_, int bit_depth_, size_t channels);
};

}
