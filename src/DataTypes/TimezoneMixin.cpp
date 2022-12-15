#include <DataTypes/TimezoneMixin.h>
#include <Common/Exception.h>
#include <filesystem>

const String & TimezoneMixin::checkTimezoneName(const String & timezone_name)
{
    const char * forbidden_beginnings[] = {"/", "./", "~"};
    for (const auto & pattern : forbidden_beginnings)
    {
        if (timezone_name.starts_with(pattern))
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Timezone name cannot start with '{}'", pattern);
    }

    if (timezone_name.find("../") != std::string::npos)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Timezone name cannot contain pattern '../'");

    return timezone_name;
}
