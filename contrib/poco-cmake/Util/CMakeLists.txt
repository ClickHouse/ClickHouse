set (SRCS
    "${LIBRARY_DIR}/Util/src/AbstractConfiguration.cpp"
    "${LIBRARY_DIR}/Util/src/Application.cpp"
    "${LIBRARY_DIR}/Util/src/ConfigurationMapper.cpp"
    "${LIBRARY_DIR}/Util/src/ConfigurationView.cpp"
    "${LIBRARY_DIR}/Util/src/FilesystemConfiguration.cpp"
    "${LIBRARY_DIR}/Util/src/HelpFormatter.cpp"
    "${LIBRARY_DIR}/Util/src/IniFileConfiguration.cpp"
    "${LIBRARY_DIR}/Util/src/IntValidator.cpp"
    "${LIBRARY_DIR}/Util/src/JSONConfiguration.cpp"
    "${LIBRARY_DIR}/Util/src/LayeredConfiguration.cpp"
    "${LIBRARY_DIR}/Util/src/LoggingConfigurator.cpp"
    "${LIBRARY_DIR}/Util/src/LoggingSubsystem.cpp"
    "${LIBRARY_DIR}/Util/src/MapConfiguration.cpp"
    "${LIBRARY_DIR}/Util/src/Option.cpp"
    "${LIBRARY_DIR}/Util/src/OptionCallback.cpp"
    "${LIBRARY_DIR}/Util/src/OptionException.cpp"
    "${LIBRARY_DIR}/Util/src/OptionProcessor.cpp"
    "${LIBRARY_DIR}/Util/src/OptionSet.cpp"
    "${LIBRARY_DIR}/Util/src/PropertyFileConfiguration.cpp"
    "${LIBRARY_DIR}/Util/src/RegExpValidator.cpp"
    "${LIBRARY_DIR}/Util/src/ServerApplication.cpp"
    "${LIBRARY_DIR}/Util/src/Subsystem.cpp"
    "${LIBRARY_DIR}/Util/src/SystemConfiguration.cpp"
    "${LIBRARY_DIR}/Util/src/Timer.cpp"
    "${LIBRARY_DIR}/Util/src/TimerTask.cpp"
    "${LIBRARY_DIR}/Util/src/Validator.cpp"
    "${LIBRARY_DIR}/Util/src/XMLConfiguration.cpp"
)

add_library (_poco_util ${SRCS})
add_library (Poco::Util ALIAS _poco_util)

target_include_directories (_poco_util SYSTEM PUBLIC "${LIBRARY_DIR}/Util/include")
target_link_libraries (_poco_util PUBLIC Poco::JSON Poco::XML)
