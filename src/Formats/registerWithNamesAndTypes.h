#pragma once

#include <string>
#include <functional>

namespace DB
{
struct FormatSettings;
class FormatFactory;

using RegisterWithNamesAndTypesFunc = std::function<void(const std::string & format_name, bool with_names, bool with_types)>;
using SubsetOfColumnsByPositionSupportChecker = std::function<bool(const FormatSettings & settings)>;

void registerWithNamesAndTypes(const std::string & base_format_name, RegisterWithNamesAndTypesFunc register_func);

void markFormatWithNamesAndTypesSupportsSamplingColumns(const std::string & base_format_name, FormatFactory & factory);
void markFormatWithNamesAndTypesSupportsSubsetOfColumnsByPosition(const std::string & base_format_name, FormatFactory & factory);
void registerWithNamesAndTypesSubsetOfColumnsByPositionSupportChecker(
    const std::string & base_format_name,
    FormatFactory & factory,
    SubsetOfColumnsByPositionSupportChecker subset_of_columns_by_position_support_checker);

}
