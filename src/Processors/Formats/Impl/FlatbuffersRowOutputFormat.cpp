#include <Processors/Formats/Impl/FlatbuffersRowOutputFormat.h>
#include <Formats/FormatFactory.h>

#ifdef USE_FLATBUFFERS

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnLowCardinality.h>

#include <IO/WriteHelpers.h>

namespace DB
{

FlatbuffersRowOutputFormat::FlatbuffersRowOutputFormat(const Block & header_, WriteBuffer & out_, const FormatSettings & format_settings_) :
IRowOutputFormat(header_, out_) {}

/*void FlatbuffersRowOutputFormat::write(const Columns & columns, size_t row_num)
{
    size_t columns_size = columns.size();
    for (size_t i = 0; i < columns_size; ++i)
    {
        serializeField(*columns[i], types[i], row_num);
    }
}*/

void registerOutputFormatFlatbuffers(FormatFactory & factory)
{
    factory.registerOutputFormat("Flatbuffers", [](
            WriteBuffer & buf,
            const Block & sample,
            const FormatSettings & settings)
    {
        return std::make_shared<CBORRowOutputFormat>(sample, buf, settings);
    });

    factory.markOutputFormatSupportsParallelFormatting("Flatbuffers");
}
}

#else

namespace DB
{
class FormatFactory;
void registerOutputFormatFlatbuffers(FormatFactory &) {}
}

#endif