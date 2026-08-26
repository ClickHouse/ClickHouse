#pragma once

#include <IO/ReadSettings.h>
#include <IO/WriteSettings.h>

#include <string>

namespace DB
{

ReadSettings updateIOSchedulingSettings(const ReadSettings & settings, const std::string & read_resource_name, const std::string & write_resource_name);
WriteSettings updateIOSchedulingSettings(const WriteSettings & settings, const std::string & read_resource_name, const std::string & write_resource_name);

}
