#include <Functions/UserDefined/UserDefinedDriverConfiguration.h>


namespace DB
{

DriverConfiguration::DriverConfiguration(const String & name)
    : driver_name(name)
{}

DriverConfiguration& DriverConfiguration::setCommand(const String & cmd)
{
    command = cmd;
    return *this;
}

DriverConfiguration& DriverConfiguration::setFile(const String & name)
{
    is_file = true;
    file_name = name;
    return *this;
}

DriverConfiguration& DriverConfiguration::setContainer(const String & cmd)
{
    container_command = cmd;
    return *this;
}

DriverConfiguration& DriverConfiguration::setFormat(const String & fmt)
{
    format = fmt;
    return *this;
}

}
