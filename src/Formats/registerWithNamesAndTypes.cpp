#include <Formats/FormatFactory.h>
#include <Formats/registerWithNamesAndTypes.h>

#include <Core/Block.h>
#include <Common/isValidUTF8.h>

namespace DB
{

bool headerNamesMayProduceRawBytes(const Block & header, bool with_names, bool with_types)
{
    auto is_not_valid_utf8 = [](const std::string & s)
    {
        return !UTF8::isValidUTF8(reinterpret_cast<const UInt8 *>(s.data()), s.size());
    };

    if (with_names)
        for (const auto & name : header.getNames())
            if (is_not_valid_utf8(name))
                return true;

    if (with_types)
        for (const auto & type_name : header.getDataTypeNames())
            if (is_not_valid_utf8(type_name))
                return true;

    return false;
}

void registerWithNamesAndTypes(const std::string & base_format_name, RegisterWithNamesAndTypesFunc register_func)
{
    register_func(base_format_name, false, false);
    register_func(base_format_name + "WithNames", true, false);
    register_func(base_format_name + "WithNamesAndTypes", true, true);
}

void markFormatWithNamesAndTypesSupportsSamplingColumns(const std::string & base_format_name, FormatFactory & factory)
{
    auto setting_checker = [](const FormatSettings & settings){ return settings.with_names_use_header; };
    factory.registerSubsetOfColumnsSupportChecker(base_format_name + "WithNames", setting_checker);
    factory.registerSubsetOfColumnsSupportChecker(base_format_name + "WithNamesAndTypes", setting_checker);
}

}
