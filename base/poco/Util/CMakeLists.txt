set (SRCS
    src/AbstractConfiguration.cpp
    src/Application.cpp
    src/ConfigurationMapper.cpp
    src/ConfigurationView.cpp
    src/FilesystemConfiguration.cpp
    src/HelpFormatter.cpp
    src/IniFileConfiguration.cpp
    src/IntValidator.cpp
    src/JSONConfiguration.cpp
    src/LayeredConfiguration.cpp
    src/LoggingConfigurator.cpp
    src/LoggingSubsystem.cpp
    src/MapConfiguration.cpp
    src/Option.cpp
    src/OptionCallback.cpp
    src/OptionException.cpp
    src/OptionProcessor.cpp
    src/OptionSet.cpp
    src/PropertyFileConfiguration.cpp
    src/RegExpValidator.cpp
    src/ServerApplication.cpp
    src/Subsystem.cpp
    src/SystemConfiguration.cpp
    src/Timer.cpp
    src/TimerTask.cpp
    src/Validator.cpp
    src/XMLConfiguration.cpp
)

add_library (_poco_util ${SRCS})
add_library (Poco::Util ALIAS _poco_util)

target_include_directories (_poco_util SYSTEM PUBLIC "include")
target_link_libraries (_poco_util PUBLIC Poco::JSON Poco::XML)
