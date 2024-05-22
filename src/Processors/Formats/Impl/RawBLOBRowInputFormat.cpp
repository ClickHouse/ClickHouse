#include <Formats/FormatFactory.h>
#include <Processors/Formats/Impl/RawBLOBRowInputFormat.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <IO/ReadHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

RawBLOBRowInputFormat::RawBLOBRowInputFormat(const Block & header_, ReadBuffer & in_, Params params_)
    : IRowInputFormat(header_, in_, std::move(params_))
{
    if (header_.columns() > 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "This input format is only suitable for tables with a single column of type String but the number of columns is {}",
            header_.columns());

    if (!isString(removeNullable(removeLowCardinality(header_.getByPosition(0).type))))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "This input format is only suitable for tables with a single column of type String but the column type is {}",
            header_.getByPosition(0).type->getName());
}

bool RawBLOBRowInputFormat::readRow(MutableColumns & columns, RowReadExtension &)
{
    if (in->eof())
        return false;

    /// One excessive copy.
    String blob;
    readStringUntilEOF(blob, *in);
    columns.at(0)->insertData(blob.data(), blob.size());
    return false;
}

size_t RawBLOBRowInputFormat::countRows(size_t)
{
    if (done_count_rows)
        return 0;

    done_count_rows = true;
    return 1;
}

void registerInputFormatRawBLOB(FormatFactory & factory)
{
    factory.registerInputFormat("RawBLOB", [](
            ReadBuffer & buf,
            const Block & sample,
            const RowInputFormatParams & params,
            const FormatSettings &)
    {
        return std::make_shared<RawBLOBRowInputFormat>(sample, buf, params);
    });
}

void registerRawBLOBSchemaReader(FormatFactory & factory)
{
    factory.registerExternalSchemaReader("RawBLOB", [](
            const FormatSettings &)
    {
        return std::make_shared<RawBLOBSchemaReader>();
    });
}

}
