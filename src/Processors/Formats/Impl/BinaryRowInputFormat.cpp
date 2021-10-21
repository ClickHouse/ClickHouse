#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <Processors/Formats/Impl/BinaryRowInputFormat.h>
#include <Formats/FormatFactory.h>


namespace DB
{

BinaryRowInputFormat::BinaryRowInputFormat(ReadBuffer & in_, Block header, Params params_, bool with_names_, bool with_types_)
    : IRowInputFormat(std::move(header), in_, params_), with_names(with_names_), with_types(with_types_)
{
}


bool BinaryRowInputFormat::readRow(MutableColumns & columns, RowReadExtension &)
{
    if (in.eof())
        return false;

    size_t num_columns = columns.size();
    for (size_t i = 0; i < num_columns; ++i)
        serializations[i]->deserializeBinary(*columns[i], in);

    return true;
}


void BinaryRowInputFormat::readPrefix()
{
    /// NOTE: The header is completely ignored. This can be easily improved.

    UInt64 columns = 0;
    String tmp;

    if (with_names || with_types)
    {
        readVarUInt(columns, in);
    }

    if (with_names)
    {
        for (size_t i = 0; i < columns; ++i)
        {
            readStringBinary(tmp, in);
        }
    }

    if (with_types)
    {
        for (size_t i = 0; i < columns; ++i)
        {
            readStringBinary(tmp, in);
        }
    }
}


void registerInputFormatProcessorRowBinary(FormatFactory & factory)
{
    factory.registerInputFormatProcessor("RowBinary", [](
        ReadBuffer & buf,
        const Block & sample,
        const IRowInputFormat::Params & params,
        const FormatSettings &)
    {
        return std::make_shared<BinaryRowInputFormat>(buf, sample, params, false, false);
    });

    factory.registerInputFormatProcessor("RowBinaryWithNamesAndTypes", [](
        ReadBuffer & buf,
        const Block & sample,
        const IRowInputFormat::Params & params,
        const FormatSettings &)
    {
        return std::make_shared<BinaryRowInputFormat>(buf, sample, params, true, true);
    });
}

}
