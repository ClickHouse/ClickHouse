#include <Common/ExternalLoaderStatus.h>

#include <base/EnumReflection.h>

namespace DB
{

std::vector<std::pair<String, Int8>> getExternalLoaderStatusEnumAllPossibleValues()
{
    std::vector<std::pair<String, Int8>> out;
    out.reserve(magic_enum::enum_count<ExternalLoaderStatus>());

    for (const auto & [value, str] : magic_enum::enum_entries<ExternalLoaderStatus>())
        out.emplace_back(std::string{str}, static_cast<Int8>(value));

    return out;
}

}
