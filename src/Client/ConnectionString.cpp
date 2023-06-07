#include "ConnectionString.h"

#include <Common/Exception.h>
#include <Poco/Exception.h>
#include <Poco/URI.h>

#include <array>
#include <iostream>
#include <string>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

}

namespace
{

using namespace std::string_literals;
using namespace std::literals::string_view_literals;

constexpr auto CONNECTION_URI_SCHEME = "clickhouse:"sv;

void uriDecode(std::string & uri_encoded_string, bool plus_as_space)
{
    std::string temp;
    Poco::URI::decode(uri_encoded_string, temp, plus_as_space);
    std::swap(temp, uri_encoded_string);
}

void getHostAndPort(const Poco::URI & uri, std::vector<std::vector<std::string>> & hosts_and_ports_arguments)
{
    auto host = uri.getHost();
    std::vector<std::string> host_and_port;
    if (!host.empty())
    {
        uriDecode(host, false);
        host_and_port.push_back("--host="s + host);
    }

    // Port can be written without host (":9000"). Empty host name equals to default host.
    auto port = uri.getPort();
    if (port != 0)
        host_and_port.push_back("--port="s + std::to_string(port));

    if (!host_and_port.empty())
        hosts_and_ports_arguments.push_back(std::move(host_and_port));
}

void getHostAndPort(
    Poco::URI & uri,
    std::vector<std::vector<std::string>> & hosts_and_ports_arguments,
    std::string_view host_and_port,
    std::string_view right_part)
{
    // User info does not matter in sub URI
    auto uri_string = std::string(CONNECTION_URI_SCHEME);
    if (!host_and_port.empty())
    {
        uri_string.append("//");
        uri_string.append(host_and_port);
    }

    // Right part from string includes '/database?[params]'
    uri_string.append(right_part);
    try
    {
        uri = Poco::URI(uri_string);
    }
    catch (const Poco::URISyntaxException & invalid_uri_exception)
    {
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS,
                            "Invalid connection string syntax {}: {}", uri_string, invalid_uri_exception.what());
    }

    getHostAndPort(uri, hosts_and_ports_arguments);
}

std::string makeArgument(const std::string & connection_string_parameter_name)
{
    return (connection_string_parameter_name.size() == 1 ? "-"s : "--"s) + connection_string_parameter_name;
}

}

namespace DB
{

bool tryParseConnectionString(
    std::string_view connection_string,
    std::vector<std::string> & common_arguments,
    std::vector<std::vector<std::string>> & hosts_and_ports_arguments)
{
    if (!connection_string.starts_with(CONNECTION_URI_SCHEME))
        return false;

    if (connection_string.size() == CONNECTION_URI_SCHEME.size())
        return true;

    auto offset = CONNECTION_URI_SCHEME.size();
    if ((connection_string.substr(offset).starts_with("//")))
        offset += 2;

    auto hosts_end_pos = std::string_view::npos;
    auto hosts_or_user_info_end_pos = connection_string.find_first_of("?/@", offset);

    auto has_user_info = hosts_or_user_info_end_pos != std::string_view::npos && connection_string[hosts_or_user_info_end_pos] == '@';
    if (has_user_info)
    {
        // Move offset right after user info
        offset = hosts_or_user_info_end_pos + 1;
        hosts_end_pos = connection_string.find_first_of("?/@", offset);
        // Several '@' symbols in connection string is prohibited.
        // If user name contains '@' then it should be percent-encoded.
        // several users: 'usr1@host1,@usr2@host2' is invalid.
        if (hosts_end_pos != std::string_view::npos && connection_string[hosts_end_pos] == '@')
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Symbols '@' in URI in password or user name should be percent-encoded. Individual user names for different hosts also prohibited. {}",
                            connection_string);
        }
    }
    else
        hosts_end_pos = hosts_or_user_info_end_pos;

    auto hosts_end = hosts_end_pos != std::string_view::npos ? connection_string.begin() + hosts_end_pos
                                                             : connection_string.end();

    try
    {
        // Poco::URI doesn't support several hosts in URI.
        // Split string clickhouse:[user_info]host1:port1, ... , hostN:portN[database]?[query_parameters]
        // into multiple string for each host:
        // clickhouse:[user_info]host1:port1[database]?[query_parameters]
        // ...
        // clickhouse:[user_info]hostN:portN[database]?[query_parameters]
        Poco::URI uri;
        auto last_host_begin = connection_string.begin() + offset;
        for (auto it = last_host_begin; it != hosts_end; ++it)
        {
            if (*it == ',')
            {
                getHostAndPort(uri, hosts_and_ports_arguments, {last_host_begin, it}, {hosts_end, connection_string.end()});
                last_host_begin = it + 1;
            }
        }

        if (uri.empty())
        {
            // URI has no host specified
            uri = std::string{connection_string.begin(), connection_string.end()};
            getHostAndPort(uri, hosts_and_ports_arguments);
        }
        else
            getHostAndPort(uri, hosts_and_ports_arguments, {last_host_begin, hosts_end}, {hosts_end, connection_string.end()});

        Poco::URI::QueryParameters params = uri.getQueryParameters();
        for (const auto & param : params)
        {
            if (param.first == "secure" || param.first == "s")
            {
                if (!param.second.empty())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "secure URI argument does not require value");

                common_arguments.push_back(makeArgument(param.first));
            }
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "URI argument {} is unknown", param.first);
        }

        auto user_info = uri.getUserInfo();
        if (!user_info.empty())
        {
            // Poco::URI doesn't decode user name/password by default.
            // But ClickHouse allows to have users with email user name like: 'john@some_mail.com'
            // john@some_mail.com should be percent-encoded: 'john%40some_mail.com'
            uriDecode(user_info, true);
            std::string::size_type pos = user_info.find(':');
            if (pos != std::string::npos)
            {
                common_arguments.push_back("--user");
                common_arguments.push_back(user_info.substr(0, pos));

                ++pos; // Skip ':'
                common_arguments.push_back("--password");
                common_arguments.push_back(user_info.substr(pos));
            }
            else
            {
                common_arguments.push_back("--user");
                common_arguments.push_back(user_info);
            }
        }

        const auto & database_name = uri.getPath();
        size_t start_symbol = database_name.size() > 0u && database_name[0] == '/' ? 1u : 0u;
        if (database_name.size() > start_symbol)
        {
            common_arguments.push_back("--database");
            common_arguments.push_back(start_symbol == 0u ? database_name : database_name.substr(start_symbol));
        }
    }
    catch (const Poco::URISyntaxException & invalid_uri_exception)
    {
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS,
                            "Invalid connection string {}: {}", connection_string, invalid_uri_exception.what());
    }

    return true;
}

}
