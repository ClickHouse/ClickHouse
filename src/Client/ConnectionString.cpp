#include <Client/ConnectionString.h>

#include <Common/Exception.h>
#include <Client/ConnectionParameters.h>
#include <Poco/Exception.h>
#include <Poco/URI.h>

#include <array>
#include <string>
#include <unordered_map>

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

const std::unordered_map<std::string_view, std::string_view> PROHIBITED_CLIENT_OPTIONS = {
    /// Client option, client option long name
    {"-h", "--host"},
    {"--host", "--host"},
    {"--port", "--port"},
    {"--connection", "--connection"},
};

std::string uriDecode(const std::string & uri_encoded_string, bool plus_as_space)
{
    std::string decoded_string;
    Poco::URI::decode(uri_encoded_string, decoded_string, plus_as_space);
    return decoded_string;
}

void getHostAndPort(const Poco::URI & uri, std::vector<std::vector<std::string>> & hosts_and_ports_arguments)
{
    std::vector<std::string> host_and_port;
    const std::string & host = uri.getHost();
    if (!host.empty())
    {
        host_and_port.push_back("--host=" + uriDecode(host, false));
    }

    // Port can be written without host (":9000"). Empty host name equals to default host.
    auto port = uri.getPort();
    if (port != 0)
        host_and_port.push_back("--port=" + std::to_string(port));

    if (!host_and_port.empty())
        hosts_and_ports_arguments.push_back(std::move(host_and_port));
}

void buildConnectionString(
    std::string_view host_and_port,
    std::string_view right_part,
    Poco::URI & uri,
    std::vector<std::vector<std::string>> & hosts_and_ports_arguments)
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
    if (connection_string == CONNECTION_URI_SCHEME)
        return true;

    if (!connection_string.starts_with(CONNECTION_URI_SCHEME))
        return false;

    size_t offset = CONNECTION_URI_SCHEME.size();
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

    const auto hosts_end = hosts_end_pos != std::string_view::npos ? connection_string.begin() + hosts_end_pos
                                                                     : connection_string.end();

    try
    {
        /** Poco::URI doesn't support several hosts in URI.
          * Split string clickhouse:[user[:password]@]host1:port1, ... , hostN:portN[database]?[query_parameters]
          * into multiple string for each host:
          * clickhouse:[user[:password]@]host1:port1[database]?[query_parameters]
          * ...
          * clickhouse:[user[:password]@]hostN:portN[database]?[query_parameters]
          */
        Poco::URI uri;
        auto last_host_begin = connection_string.begin() + offset;
        for (auto it = last_host_begin; it != hosts_end; ++it)
        {
            if (*it == ',')
            {
                buildConnectionString({last_host_begin, it}, {hosts_end, connection_string.end()}, uri, hosts_and_ports_arguments);
                last_host_begin = it + 1;
            }
        }

        if (uri.empty())
        {
            // URI has no host specified
            uri = std::string(connection_string);
            getHostAndPort(uri, hosts_and_ports_arguments);
        }
        else
            buildConnectionString({last_host_begin, hosts_end}, {hosts_end, connection_string.end()}, uri, hosts_and_ports_arguments);

        Poco::URI::QueryParameters params = uri.getQueryParameters();
        for (const auto & param : params)
        {
            if (param.first == "secure" || param.first == "s")
            {
                if (!param.second.empty())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "secure URI query parameter does not allow value");

                common_arguments.push_back(makeArgument(param.first));
            }
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "URI query parameter {} is not supported", param.first);
        }

        auto user_info = uri.getUserInfo();
        if (!user_info.empty())
        {
            // Poco::URI doesn't decode user name/password by default.
            // But ClickHouse allows to have users with email user name like: 'john@some_mail.com'
            // john@some_mail.com should be percent-encoded: 'john%40some_mail.com'
            size_t pos = user_info.find(':');
            if (pos != std::string::npos)
            {
                common_arguments.push_back("--user");
                common_arguments.push_back(uriDecode(user_info.substr(0, pos), true));

                ++pos; // Skip ':'
                common_arguments.push_back("--password");
                if (user_info.size() > pos + 1)
                    common_arguments.push_back(uriDecode(user_info.substr(pos), true));
                else
                {
                    // in case of user_info == 'user:', ':' is specified, but password is empty
                    // then ask user for a password.
                    common_arguments.emplace_back(ConnectionParameters::ASK_PASSWORD);
                }
            }
            else
            {
                common_arguments.push_back("--user");
                common_arguments.push_back(uriDecode(user_info, true));
            }
        }

        const auto & database_name = uri.getPath();
        size_t start_symbol = !database_name.empty() && database_name[0] == '/' ? 1u : 0u;
        if (database_name.size() > start_symbol)
        {
            common_arguments.push_back("--database");
            common_arguments.push_back(start_symbol == 0u ? database_name : database_name.substr(start_symbol));
        }
    }
    catch (const Poco::URISyntaxException & invalid_uri_exception)
    {
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS,
                            "Invalid connection string '{}': {}", connection_string, invalid_uri_exception.what());
    }

    return true;
}

void checkIfCmdLineOptionCanBeUsedWithConnectionString(std::string_view command_line_option)
{
    if (PROHIBITED_CLIENT_OPTIONS.contains(command_line_option))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Mixing a connection string and {} option is prohibited", PROHIBITED_CLIENT_OPTIONS.at(command_line_option));
}

}
