#include <Processors/Formats/IRowInputFormat.h>
#include <Formats/FormatSettings.h>
#include <Formats/FormatFactory.h>
#include <Formats/JSONEachRowUtils.h>
#include <Common/assert_cast.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

class JSONAsObjectRowInputFormat : public IRowInputFormat
{
public:
    JSONAsObjectRowInputFormat(ReadBuffer & in_, const Block & header_, Params params_, const FormatSettings & format_settings_);

    bool readRow(MutableColumns & columns, RowReadExtension & ext) override;
    String getName() const override { return "JSONAsObjectRowInputFormat"; }

private:
    const FormatSettings format_settings;
};

JSONAsObjectRowInputFormat::JSONAsObjectRowInputFormat(
    ReadBuffer & in_, const Block & header_, Params params_, const FormatSettings & format_settings_)
    : IRowInputFormat(header_, in_, std::move(params_))
    , format_settings(format_settings_)
{
    if (header_.columns() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Input format JSONAsObject is only suitable for tables with a single column of type Object but the number of columns is {}",
            header_.columns());

    if (!isObject(header_.getByPosition(0).type))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Input format JSONAsObject is only suitable for tables with a single column of type Object but the column type is {}",
            header_.getByPosition(0).type->getName());
}


bool JSONAsObjectRowInputFormat::readRow(MutableColumns & columns, RowReadExtension &)
{
    assert(serializations.size() == 1);
    assert(columns.size() == 1);

    skipWhitespaceIfAny(in);
    if (!in.eof())
        serializations[0]->deserializeTextJSON(*columns[0], in, format_settings);

    skipWhitespaceIfAny(in);
    if (!in.eof() && *in.position() == ',')
        ++in.position();
    skipWhitespaceIfAny(in);

    return !in.eof();
}

}

void registerInputFormatProcessorJSONAsObject(FormatFactory & factory)
{
    factory.registerInputFormatProcessor("JSONAsObject", [](
        ReadBuffer & buf,
        const Block & sample,
        IRowInputFormat::Params params,
        const FormatSettings & settings)
    {
        return std::make_shared<JSONAsObjectRowInputFormat>(buf, sample, std::move(params), settings);
    });
}

void registerFileSegmentationEngineJSONAsObject(FormatFactory & factory)
{
    factory.registerFileSegmentationEngine("JSONAsObject", &fileSegmentationEngineJSONEachRowImpl);
}

}
