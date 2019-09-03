#include <optional>
#include <string>
#include <Poco/AutoPtr.h>
#include <Poco/FileChannel.h>
#include <Poco/Util/Application.h>
#include "OwnFormattingChannel.h"

namespace Poco::Util
{
    class AbstractConfiguration;
}

namespace DB
{
    class SensitiveDataMasker;
}


class Loggers
{
public:
    void buildLoggers(Poco::Util::AbstractConfiguration & config, Poco::Logger & logger, const std::string & cmd_name = "");
    void setLoggerSensitiveDataMasker(Poco::Logger & logger, DB::SensitiveDataMasker * sensitive_data_masker);


    /// Close log files. On next log write files will be reopened.
    void closeLogs(Poco::Logger & logger);

    std::optional<size_t> getLayer() const
    {
        return layer; /// layer setted in inheritor class BaseDaemonApplication.
    }

protected:
    std::optional<size_t> layer;

private:
    Poco::AutoPtr<Poco::FileChannel> log_file;
    Poco::AutoPtr<Poco::FileChannel> error_log_file;
    Poco::AutoPtr<Poco::Channel> syslog_channel;

    /// Previous value of logger element in config. It is used to reinitialize loggers whenever the value changed.
    std::string config_logger;
};
