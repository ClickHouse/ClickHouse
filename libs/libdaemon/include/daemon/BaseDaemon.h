#pragma once

#include <sys/types.h>
#include <port/unistd.h>
#include <iostream>
#include <memory>
#include <functional>
#include <optional>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <chrono>
#include <Poco/Process.h>
#include <Poco/ThreadPool.h>
#include <Poco/TaskNotification.h>
#include <Poco/NumberFormatter.h>
#include <Poco/Util/Application.h>
#include <Poco/Util/ServerApplication.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/FileChannel.h>
#include <Poco/SyslogChannel.h>
#include <Poco/Version.h>
#include <common/Types.h>
#include <common/logger_useful.h>
#include <daemon/GraphiteWriter.h>
#include <Common/Config/ConfigProcessor.h>

namespace Poco { class TaskManager; }


/// \brief Базовый класс для демонов
///
/// \code
/// # Список возможных опций командной строки обрабатываемых демоном:
/// #    --config-file или --config - имя файла конфигурации. По умолчанию - config.xml
/// #    --pid-file - имя PID файла. По умолчанию - pid
/// #    --log-file - имя лог файла
/// #    --error-file - имя лог файла, в который будут помещаться только ошибки
/// #    --daemon - запустить в режиме демона; если не указан - логгирование будет вестись на консоль
/// <daemon_name> --daemon --config-file=localfile.xml --pid-file=pid.pid --log-file=log.log --errorlog-file=error.log
/// \endcode
///
/// Если неперехваченное исключение выкинуто в других потоках (не Task-и), то по-умолчанию
/// используется KillingErrorHandler, который вызывает std::terminate.
///
/// Кроме того, класс позволяет достаточно гибко управлять журналированием. В методе initialize() вызывается метод
/// buildLoggers() который и строит нужные логгеры. Эта функция ожидает увидеть в конфигурации определённые теги
/// заключённые в секции "logger".
/// Если нужно журналирование на консоль, нужно просто не использовать тег "log" или использовать --console.
/// Теги уровней вывода использовать можно в любом случае


class BaseDaemon : public Poco::Util::ServerApplication
{
    friend class SignalListener;

public:
    static constexpr char DEFAULT_GRAPHITE_CONFIG_NAME[] = "graphite";

    BaseDaemon();
    ~BaseDaemon();

    /// Загружает конфигурацию и "строит" логгеры на запись в файлы
    void initialize(Poco::Util::Application &) override;

    /// Читает конфигурацию
    void reloadConfiguration();

    /// Строит необходимые логгеры
    void buildLoggers(Poco::Util::AbstractConfiguration & config);

    /// Определяет параметр командной строки
    void defineOptions(Poco::Util::OptionSet & _options) override;

    /// Заставляет демон завершаться, если хотя бы одна задача завершилась неудачно
    void exitOnTaskError();

    /// Завершение демона ("мягкое")
    void terminate();

    /// Завершение демона ("жёсткое")
    void kill();

    /// Получен ли сигнал на завершение?
    bool isCancelled() const
    {
        return is_cancelled;
    }

    /// Получение ссылки на экземпляр демона
    static BaseDaemon & instance()
    {
        return dynamic_cast<BaseDaemon &>(Poco::Util::Application::instance());
    }

    /// return none if daemon doesn't exist, reference to the daemon otherwise
    static std::optional<std::reference_wrapper<BaseDaemon>> tryGetInstance() { return tryGetInstance<BaseDaemon>(); }

    /// Спит заданное количество секунд или до события wakeup
    void sleep(double seconds);

    /// Разбудить
    void wakeup();

    /// Закрыть файлы с логами. При следующей записи, будут созданы новые файлы.
    void closeLogs();

    /// В Graphite компоненты пути(папки) разделяются точкой.
    /// У нас принят путь формата root_path.hostname_yandex_ru.key
    /// root_path по умолчанию one_min
    /// key - лучше группировать по смыслу. Например "meminfo.cached" или "meminfo.free", "meminfo.total"
    template <class T>
    void writeToGraphite(const std::string & key, const T & value, const std::string & config_name = DEFAULT_GRAPHITE_CONFIG_NAME, time_t timestamp = 0, const std::string & custom_root_path = "")
    {
        auto writer = getGraphiteWriter(config_name);
        if (writer)
            writer->write(key, value, timestamp, custom_root_path);
    }

    template <class T>
    void writeToGraphite(const GraphiteWriter::KeyValueVector<T> & key_vals, const std::string & config_name = DEFAULT_GRAPHITE_CONFIG_NAME, time_t timestamp = 0, const std::string & custom_root_path = "")
    {
        auto writer = getGraphiteWriter(config_name);
        if (writer)
            writer->write(key_vals, timestamp, custom_root_path);
    }

    template <class T>
    void writeToGraphite(const GraphiteWriter::KeyValueVector<T> & key_vals, const std::chrono::system_clock::time_point & current_time, const std::string & custom_root_path)
    {
        auto writer = getGraphiteWriter();
        if (writer)
            writer->write(key_vals, std::chrono::system_clock::to_time_t(current_time), custom_root_path);
    }

    GraphiteWriter * getGraphiteWriter(const std::string & config_name = DEFAULT_GRAPHITE_CONFIG_NAME)
    {
        if (graphite_writers.count(config_name))
            return graphite_writers[config_name].get();
        return nullptr;
    }

    std::optional<size_t> getLayer() const
    {
        return layer;    /// layer выставляется в классе-наследнике BaseDaemonApplication.
    }

protected:
    /// Возвращает TaskManager приложения
    /// все методы task_manager следует вызывать из одного потока
    /// иначе возможен deadlock, т.к. joinAll выполняется под локом, а любой метод тоже берет лок
    Poco::TaskManager & getTaskManager() { return *task_manager; }

    virtual void logRevision() const;

    /// Используется при exitOnTaskError()
    void handleNotification(Poco::TaskFailedNotification *);

    /// thread safe
    virtual void handleSignal(int signal_id);

    /// реализация обработки сигналов завершения через pipe не требует блокировки сигнала с помощью sigprocmask во всех потоках
    void waitForTerminationRequest()
#if POCO_CLICKHOUSE_PATCH || POCO_VERSION >= 0x02000000 // in old upstream poco not vitrual
    override
#endif
    ;
    /// thread safe
    virtual void onInterruptSignals(int signal_id);

    template <class Daemon>
    static std::optional<std::reference_wrapper<Daemon>> tryGetInstance();

    virtual std::string getDefaultCorePath() const;

    std::unique_ptr<Poco::TaskManager> task_manager;

    /// Создание и автоматическое удаление pid файла.
    struct PID
    {
        std::string file;

        /// Создать объект, не создавая PID файл
        PID() {}

        /// Создать объект, создать PID файл
        PID(const std::string & file_) { seed(file_); }

        /// Создать PID файл
        void seed(const std::string & file_);

        /// Удалить PID файл
        void clear();

        ~PID() { clear(); }
    };

    PID pid;

    std::atomic_bool is_cancelled{false};

    /// Флаг устанавливается по сообщению из Task (при аварийном завершении).
    bool task_failed = false;

    bool log_to_console = false;

    /// Событие, чтобы проснуться во время ожидания
    Poco::Event wakeup_event;

    /// Поток, в котором принимается сигнал HUP/USR1 для закрытия логов.
    Poco::Thread signal_listener_thread;
    std::unique_ptr<Poco::Runnable> signal_listener;

    /// Файлы с логами.
    Poco::AutoPtr<Poco::FileChannel> log_file;
    Poco::AutoPtr<Poco::FileChannel> error_log_file;
    Poco::AutoPtr<Poco::Channel> syslog_channel;

    std::map<std::string, std::unique_ptr<GraphiteWriter>> graphite_writers;

    std::optional<size_t> layer;

    std::mutex signal_handler_mutex;
    std::condition_variable signal_event;
    std::atomic_size_t terminate_signals_counter{0};
    std::atomic_size_t sigint_signals_counter{0};

    std::string config_path;
    ConfigProcessor::LoadedConfig loaded_config;
    Poco::Util::AbstractConfiguration * last_configuration = nullptr;

private:

    /// Previous value of logger element in config. It is used to reinitialize loggers whenever the value changed.
    std::string config_logger;
};


template <class Daemon>
std::optional<std::reference_wrapper<Daemon>> BaseDaemon::tryGetInstance()
{
    Daemon * ptr = nullptr;
    try
    {
        ptr = dynamic_cast<Daemon *>(&Poco::Util::Application::instance());
    }
    catch (const Poco::NullPointerException &)
    {
        /// if daemon doesn't exist than instance() throw NullPointerException
    }

    if (ptr)
        return std::optional<std::reference_wrapper<Daemon>>(*ptr);
    else
        return {};
}
