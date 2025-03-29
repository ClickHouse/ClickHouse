#include <Functions/UserDefined/UserDefinedDriverConfiguration.h>


namespace DB
{

DriverConfiguration::DriverConfiguration(const String & name)
    : driver_name(name)
{}

DriverConfiguration& DriverConfiguration::setScript(const String & new_script)
{
    script = new_script;
    return *this;
}

DriverConfiguration& DriverConfiguration::setExecution(DriverExecutionType exec_type, const String & extension)
{
    execution.type = exec_type;
    execution.file_extension = extension;
    return *this;
}

DriverConfiguration& DriverConfiguration::setContainer(DriverContainerType container_type, const String & container_script)
{
    container.type = container_type;
    container.script = container_script;
    return *this;
}

DriverConfiguration& DriverConfiguration::setFormat(const String & format)
{
    input_format = format;
    return *this;
}

DriverConfiguration& DriverConfiguration::setPython(DriverExecutionType exec_type)
{
    switch (exec_type)
    {
        case DriverExecutionType::Inline:
            return setScript("python -c").setExecution(exec_type, "");
        case DriverExecutionType::File:
            return setScript("python").setExecution(exec_type, "py");
    }
}

DriverConfiguration& DriverConfiguration::setDocker(const String & image)
{
    return setContainer(DriverContainerType::Docker, "docker run --rm -i " + image + " /bin/bash -c");
}

}
