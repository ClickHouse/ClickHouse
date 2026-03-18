#include <Processors/Formats/Impl/OneFormat.h>
#include <Formats/FormatFactory.h>
#include <Columns/ColumnsNumber.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

OneInputFormat::OneInputFormat(const Block & header, ReadBuffer & in_) : IInputFormat(header, &in_)
{
    if (header.columns() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "One input format is only suitable for tables with a single column of type UInt8 but the number of columns is {}",
                        header.columns());

    if (!WhichDataType(header.getByPosition(0).type).isUInt8())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "One input format is only suitable for tables with a single column of type String but the column type is {}",
                        header.getByPosition(0).type->getName());
}

Chunk OneInputFormat::read()
{
    if (done)
        return {};

    done = true;
    auto column = ColumnUInt8::create();
    column->insertDefault();
    return Chunk(Columns{std::move(column)}, 1);
}

void registerInputFormatOne(FormatFactory & factory)
{
    factory.registerInputFormat("One", [](
                   ReadBuffer & buf,
                   const Block & sample,
                   const RowInputFormatParams &,
                   const FormatSettings &)
    {
        return std::make_shared<OneInputFormat>(sample, buf);
    });
}

void registerOneSchemaReader(FormatFactory & factory)
{
    factory.registerExternalSchemaReader("One", [](const FormatSettings &)
    {
         return std::make_shared<OneSchemaReader>();
    });
}

}
