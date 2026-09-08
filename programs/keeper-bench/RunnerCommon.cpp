#include <RunnerCommon.h>

#include <filesystem>
#include <iostream>

#include <Common/Exception.h>
#include <Common/ZooKeeper/ShuffleHost.h>
#include <Common/ZooKeeper/ZooKeeperArgs.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/copyData.h>
#include <Poco/Net/SocketAddress.h>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void ConnectionFactory::initialize(const Strings & hosts_strings, const Poco::Util::AbstractConfiguration * config, bool enable_tracing_)
{
    enable_tracing = enable_tracing_;

    if (!hosts_strings.empty())
    {
        for (const auto & host : hosts_strings)
            connection_infos.push_back({.host = host});
        return;
    }

    if (!config)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "No config file or hosts defined");

    parseHostsFromConfig(*config);

    if (connection_infos.empty())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "No hosts defined in the `connections` config section");
}

void ConnectionFactory::parseHostsFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    const auto fill_connection_details = [&](const std::string & key, auto & connection_info)
    {
        if (config.has(key + ".secure"))
            connection_info.secure = config.getBool(key + ".secure");

        if (config.has(key + ".session_timeout_ms"))
            connection_info.session_timeout_ms = config.getInt(key + ".session_timeout_ms");

        if (config.has(key + ".operation_timeout_ms"))
            connection_info.operation_timeout_ms = config.getInt(key + ".operation_timeout_ms");

        if (config.has(key + ".connection_timeout_ms"))
            connection_info.connection_timeout_ms = config.getInt(key + ".connection_timeout_ms");

        if (config.has(key + ".use_compression"))
            connection_info.use_compression = config.getBool(key + ".use_compression");

        if (config.has(key + ".use_xid_64"))
            connection_info.use_xid_64 = config.getBool(key + ".use_xid_64");
    };

    fill_connection_details("connections", default_connection_info);

    Poco::Util::AbstractConfiguration::Keys connections_keys;
    config.keys("connections", connections_keys);

    for (const auto & key : connections_keys)
    {
        std::string connection_key = "connections." + key;
        auto connection_info = default_connection_info;
        if (key.starts_with("host"))
        {
            connection_info.host = config.getString(connection_key);
            connection_infos.push_back(std::move(connection_info));
        }
        else if (key.starts_with("connection") && key != "connection_timeout_ms")
        {
            connection_info.host = config.getString(connection_key + ".host");
            if (config.has(connection_key + ".sessions"))
                connection_info.sessions = config.getUInt64(connection_key + ".sessions");

            fill_connection_details(connection_key, connection_info);

            connection_infos.push_back(std::move(connection_info));
        }
    }
}

std::shared_ptr<Coordination::ZooKeeper> ConnectionFactory::getConnection(const ConnectionInfo & connection_info, size_t connection_info_idx) const
{
    zkutil::ShuffleHost host;
    host.host = connection_info.host;
    host.secure = connection_info.secure;
    host.original_index = static_cast<UInt8>(connection_info_idx);
    host.address = Poco::Net::SocketAddress{connection_info.host};

    zkutil::ShuffleHosts nodes{host};
    zkutil::ZooKeeperArgs args;
    args.session_timeout_ms = connection_info.session_timeout_ms;
    args.connection_timeout_ms = connection_info.connection_timeout_ms;
    args.operation_timeout_ms = connection_info.operation_timeout_ms;
    args.use_compression = connection_info.use_compression;
    args.use_xid_64 = connection_info.use_xid_64;
    args.pass_opentelemetry_tracing_context = enable_tracing;
    return std::make_shared<Coordination::ZooKeeper>(nodes, args, nullptr, nullptr);
}

void BenchmarkOutput::initializeFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    static const std::string output_key = "output";
    print_to_stdout = config.getBool(output_key + ".stdout", false);
    std::cerr << "Printing output to stdout: " << print_to_stdout << std::endl;

    static const std::string output_file_key = output_key + ".file";
    if (config.has(output_file_key))
    {
        if (config.has(output_file_key + ".path"))
        {
            file_output = config.getString(output_file_key + ".path");
            output_file_with_timestamp = config.getBool(output_file_key + ".with_timestamp");
        }
        else
            file_output = config.getString(output_file_key);

        std::cerr << "Result file path: " << file_output->string() << std::endl;
    }
}

void BenchmarkOutput::write(const std::string & output_string, int64_t start_timestamp_ms) const
{
    if (print_to_stdout)
        std::cout << output_string << std::endl;

    if (file_output)
    {
        auto path = *file_output;

        if (output_file_with_timestamp)
        {
            auto filename = file_output->filename();
            filename = fmt::format("{}_{}{}", filename.stem().generic_string(), start_timestamp_ms, filename.extension().generic_string());
            path = file_output->parent_path() / filename;
        }

        std::cerr << "Storing output to " << std::filesystem::absolute(path) << std::endl;

        DB::WriteBufferFromFile file_output_buffer(path);
        DB::ReadBufferFromString read_buffer(output_string);
        DB::copyData(read_buffer, file_output_buffer);
        file_output_buffer.finalize();
    }
}
