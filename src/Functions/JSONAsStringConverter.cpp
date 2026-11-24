#include <Functions/JSONAsStringConverter.h>

#include <Common/SipHash.h>
#include <DataTypes/DataTypeString.h>
#include <Formats/FormatFactory.h>
#include <Functions/FunctionsConversion.h>
#include <Interpreters/Context.h>


namespace DB
{

JSONAsStringConverter::JSONAsStringConverter(const ContextPtr & context)
    : format_settings(std::make_shared<const FormatSettings>(context ? getFormatSettings(context) : FormatSettings{}))
    , format_settings_hash(context ? getFormatSettingsHash(context->getSettingsRef()) : 0)
{
}

ColumnPtr JSONAsStringConverter::convert(const ColumnWithTypeAndName & json) const
{
    ColumnWithTypeAndName full{json.column->convertToFullColumnIfConst(), json.type, json.name};
    return detail::ConvertImplGenericToString<ColumnString>::execute({full}, std::make_shared<DataTypeString>(), full.column->size(), *format_settings);
}

void JSONAsStringConverter::updateHash(SipHash & hash) const
{
    hash.update(format_settings_hash);
}

}
