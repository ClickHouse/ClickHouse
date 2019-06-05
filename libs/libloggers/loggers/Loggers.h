#include <optional>
#include <string>
#include <Poco/AutoPtr.h>
#include <Poco/FileChannel.h>
#include <Poco/Util/Application.h>

namespace Poco::Util
{
class AbstractConfiguration;
}


class Loggers
{
public:
    /// Строит необходимые логгеры
    void buildLoggers(Poco::Util::AbstractConfiguration & config, Poco::Logger & logger, const std::string & cmd_name = "");

    /// Закрыть файлы с логами. При следующей записи, будут созданы новые файлы.
    void closeLogs(Poco::Logger & logger);

    std::optional<size_t> getLayer() const
    {
        return layer; /// layer выставляется в классе-наследнике BaseDaemonApplication.
    }

protected:
    std::optional<size_t> layer;

private:
    /// Файлы с логами.
    Poco::AutoPtr<Poco::FileChannel> log_file;
    Poco::AutoPtr<Poco::FileChannel> error_log_file;
    Poco::AutoPtr<Poco::Channel> syslog_channel;
    /// Previous value of logger element in config. It is used to reinitialize loggers whenever the value changed.
    std::string config_logger;
};
