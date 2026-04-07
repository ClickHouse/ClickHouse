#pragma once

#include <string>
#include <functional>

namespace DB
{
class FormatFactory;

using RegisterWithNamesAndTypesFunc = std::function<void(const std::string & format_name, bool with_names, bool with_types)>;
void registerWithNamesAndTypes(const std::string & base_format_name, RegisterWithNamesAndTypesFunc register_func);

void markFormatWithNamesAndTypesSupportsSamplingColumns(const std::string & base_format_name, FormatFactory & factory);

}
