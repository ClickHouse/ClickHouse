#pragma once

#include <string>
#include <time.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Net/SocketStream.h>
#include <Poco/Util/Application.h>
#include <common/logger_useful.h>


/// пишет в Graphite данные в формате
/// path value timestamp\n
/// path может иметь любую вложенность. Директории разделяются с помощью "."
/// у нас принят следующий формат path - root_path.server_name.sub_path.key
class GraphiteWriter
{
public:
    GraphiteWriter(const std::string & config_name, const std::string & sub_path = "");

    template <typename T> using KeyValuePair = std::pair<std::string, T>;
    template <typename T> using KeyValueVector = std::vector<KeyValuePair<T>>;

    template <typename T> void write(const std::string & key, const T & value,
                                     time_t timestamp = 0, const std::string & custom_root_path = "")
    {
        writeImpl(KeyValuePair<T>{ key, value }, timestamp, custom_root_path);
    }

    template <typename T> void write(const KeyValueVector<T> & key_val_vec,
                                     time_t timestamp = 0, const std::string & custom_root_path = "")
    {
        writeImpl(key_val_vec, timestamp, custom_root_path);
    }

    /// возвращает путь root_path.server_name
    static std::string getPerServerPath(const std::string & server_name, const std::string & root_path = "one_min");
private:
    template <typename T>
    void writeImpl(const T & data, time_t timestamp, const std::string & custom_root_path)
    {
        if (!timestamp)
            timestamp = time(nullptr);

        try
        {
            Poco::Net::SocketAddress socket_address(host, port);
            Poco::Net::StreamSocket socket(socket_address);
            socket.setSendTimeout(Poco::Timespan(timeout * 1000000));
            Poco::Net::SocketStream str(socket);

            out(str, data, timestamp, custom_root_path);
        }
        catch (const Poco::Exception & e)
        {
            LOG_WARNING(&Poco::Util::Application::instance().logger(),
                        "Fail to write to Graphite " << host << ":" << port << ". e.what() = " << e.what() << ", e.message() = " << e.message());
        }
    }

    template <typename T>
    void out(std::ostream & os, const KeyValuePair<T> & key_val, time_t timestamp, const std::string & custom_root_path)
    {
        os << (custom_root_path.empty() ? root_path : custom_root_path) <<
            '.' << key_val.first << ' ' << key_val.second << ' ' << timestamp << '\n';
    }

    template <typename T>
    void out(std::ostream & os, const KeyValueVector<T> & key_val_vec, time_t timestamp, const std::string & custom_root_path)
    {
        for (const auto & key_val : key_val_vec)
            out(os, key_val, timestamp, custom_root_path);
    }

    std::string root_path;

    int port;
    std::string host;
    double timeout;
};
