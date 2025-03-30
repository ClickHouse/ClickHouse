#pragma once

#include <string>
#include <memory>


namespace DB
{

using String = std::string;

class DriverConfiguration
{
public:
    explicit DriverConfiguration(const String & name);

    DriverConfiguration& setCommand(const String & cmd);

    DriverConfiguration& setContainer(const String & cmd);

    DriverConfiguration& setFile(const String & name);

    DriverConfiguration& setFormat(const String & fmt);

    String driver_name;
    String command;
    String container_command;
    String format;
    String file_name;
    bool is_file = false;
};

using DriverConfigurationPtr = std::shared_ptr<DriverConfiguration>;

}
