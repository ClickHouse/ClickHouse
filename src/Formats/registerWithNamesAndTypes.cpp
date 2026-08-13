#include <Formats/FormatFactory.h>
#include <Formats/registerWithNamesAndTypes.h>

#include <utility>

namespace DB
{

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

void markFormatWithNamesAndTypesSupportsSubsetOfColumnsByPosition(const std::string & base_format_name, FormatFactory & factory)
{
    registerWithNamesAndTypesSubsetOfColumnsByPositionSupportChecker(
        base_format_name,
        factory,
        [](const FormatSettings &){ return true; });
}

void registerWithNamesAndTypesSubsetOfColumnsByPositionSupportChecker(
    const std::string & base_format_name,
    FormatFactory & factory,
    SubsetOfColumnsByPositionSupportChecker subset_of_columns_by_position_support_checker)
{
    factory.registerSubsetOfColumnsByPositionSupportChecker(base_format_name, subset_of_columns_by_position_support_checker);
    factory.registerSubsetOfColumnsByPositionSupportChecker(base_format_name + "WithNames", subset_of_columns_by_position_support_checker);
    factory.registerSubsetOfColumnsByPositionSupportChecker(base_format_name + "WithNamesAndTypes", std::move(subset_of_columns_by_position_support_checker));
}

}
