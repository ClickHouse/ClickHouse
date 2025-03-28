#pragma once

#include <string>
#include <memory>


namespace DB
{

using String = std::string;

enum class DriverExecutionType
{
    Inline,
    File,
};

struct DriverExecution
{
    DriverExecutionType type = DriverExecutionType::Inline;
    String file_extension;
};

enum class DriverContainerType
{
    None,
    Docker
};

struct DriverContainer
{
    DriverContainerType type = DriverContainerType::None;
    String script;
};

class DriverConfiguration
{
public:
    explicit DriverConfiguration(const String & name);

    DriverConfiguration& setScript(const String & new_script);

    DriverConfiguration& setExecution(DriverExecutionType exec_type, const String & extension);

    DriverConfiguration& setContainer(DriverContainerType container_type, const String & script);

    DriverConfiguration& setFormat(const String & format);

    DriverConfiguration& setPython(DriverExecutionType exec_type);

    DriverConfiguration& setDocker(const String & image);

public:
    String driver_name;
    String script;
    DriverExecution execution;
    DriverContainer container;
    String input_format = "TabSeparated";
};

using DriverConfigurationPtr = std::shared_ptr<DriverConfiguration>;

}
