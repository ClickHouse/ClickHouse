#include <Storages/HDFS/HDFSCommon.h>
#include <Poco/URI.h>
#include <boost/algorithm/string/replace.hpp>
#include <re2/re2.h>
#include <filesystem>

#if USE_HDFS
#include <Common/ShellCommand.h>
#include <Common/Exception.h>
#include <IO/Operators.h>
#include <Common/logger_useful.h>

#if USE_KRB5
    #include <Access/KerberosInit.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NETWORK_ERROR;
    extern const int HDFS_ERROR;
    #if USE_KRB5
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int KERBEROS_ERROR;
    #endif // USE_KRB5
}

static constexpr std::string_view CONFIG_PREFIX = "hdfs";
static constexpr std::string_view HDFS_URL_REGEXP = "^hdfs://[^/]*/.*";

namespace detail
{
    void HDFSFsDeleter::operator()(hdfsFS fs_ptr)
    {
        hdfsDisconnect(fs_ptr);
    }
}

HDFSFileInfo::~HDFSFileInfo()
{
    hdfsFreeFileInfo(file_info, length);
}

HDFSBuilderWrapper::HDFSBuilderWrapper() : hdfs_builder(hdfsNewBuilder())
{
}

HDFSBuilderWrapper::~HDFSBuilderWrapper()
{
    hdfsFreeBuilder(hdfs_builder);
}

std::pair<String, String> & HDFSBuilderWrapper::keep(const String & key, const String & value)
{
    return config_storage.emplace_back(key, value);
}

void HDFSBuilderWrapper::loadFromConfig(
    const Poco::Util::AbstractConfiguration & config,
    const String & prefix,
    bool is_user)
{
    Poco::Util::AbstractConfiguration::Keys keys;

    config.keys(prefix, keys);
    for (const auto & key : keys)
    {
        const String key_path = prefix + "." + key;

        String key_name;
        if (key == "hadoop_kerberos_keytab")
        {
            #if USE_KRB5
            need_kinit = true;
            hadoop_kerberos_keytab = config.getString(key_path);
            #else // USE_KRB5
            LOG_WARNING(&Poco::Logger::get("HDFSClient"), "hadoop_kerberos_keytab parameter is ignored because ClickHouse was built without support of krb5 library.");
            #endif // USE_KRB5
            continue;
        }
        else if (key == "hadoop_kerberos_principal")
        {
            #if USE_KRB5
            need_kinit = true;
            hadoop_kerberos_principal = config.getString(key_path);
            hdfsBuilderSetPrincipal(hdfs_builder, hadoop_kerberos_principal.c_str());
            #else // USE_KRB5
            LOG_WARNING(&Poco::Logger::get("HDFSClient"), "hadoop_kerberos_principal parameter is ignored because ClickHouse was built without support of krb5 library.");
            #endif // USE_KRB5
            continue;
        }
        else if (key == "hadoop_security_kerberos_ticket_cache_path")
        {
            #if USE_KRB5
            if (is_user)
            {
                throw Exception("hadoop.security.kerberos.ticket.cache.path cannot be set per user",
                    ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);
            }

            hadoop_security_kerberos_ticket_cache_path = config.getString(key_path);
            // standard param - pass further
            #else // USE_KRB5
            LOG_WARNING(&Poco::Logger::get("HDFSClient"), "hadoop.security.kerberos.ticket.cache.path parameter is ignored because ClickHouse was built without support of krb5 library.");
            #endif // USE_KRB5
        }

        key_name = boost::replace_all_copy(key, "_", ".");

        const auto & [k,v] = keep(key_name, config.getString(key_path));
        hdfsBuilderConfSetStr(hdfs_builder, k.c_str(), v.c_str());
    }
}

#if USE_KRB5
void HDFSBuilderWrapper::runKinit()
{
    LOG_DEBUG(&Poco::Logger::get("HDFSClient"), "Running KerberosInit");
    try
    {
        kerberosInit(hadoop_kerberos_keytab,hadoop_kerberos_principal,hadoop_security_kerberos_ticket_cache_path);
    }
    catch (const DB::Exception & e)
    {
        throw Exception("KerberosInit failure: "+ getExceptionMessage(e, false), ErrorCodes::KERBEROS_ERROR);
    }
    LOG_DEBUG(&Poco::Logger::get("HDFSClient"), "Finished KerberosInit");
}
#endif // USE_KRB5

HDFSBuilderWrapperPtr createHDFSBuilder(const String & uri_str, const Poco::Util::AbstractConfiguration & config)
{
    const Poco::URI uri(uri_str);
    const auto & host = uri.getHost();
    auto port = uri.getPort();

    if (host.empty())
        throw Exception("Illegal HDFS URI: " + uri.toString(), ErrorCodes::BAD_ARGUMENTS);

    auto builder_wrapper = std::make_unique<HDFSBuilderWrapper>();

    auto * builder = builder_wrapper->getBuilder();
    if (builder == nullptr)
    {
        throw Exception(
            ErrorCodes::NETWORK_ERROR,
            "Unable to create builder to connect to HDFS: {}, error: {}",
            uri.toString(), String(hdfsGetLastError()));
    }

    hdfsBuilderConfSetStr(builder, "input.read.timeout", "60000"); // 1 min
    hdfsBuilderConfSetStr(builder, "input.write.timeout", "60000"); // 1 min
    hdfsBuilderConfSetStr(builder, "input.connect.timeout", "60000"); // 1 min

    String user_info = uri.getUserInfo();
    String user;
    if (!user_info.empty() && user_info.front() != ':')
    {
        size_t delim_pos = user_info.find(':');
        if (delim_pos != String::npos)
            user = user_info.substr(0, delim_pos);
        else
            user = user_info;

        hdfsBuilderSetUserName(builder, user.c_str());
    }

    hdfsBuilderSetNameNode(builder, host.c_str());
    if (port != 0)
    {
        hdfsBuilderSetNameNodePort(builder, port);
    }

    if (config.has(std::string(CONFIG_PREFIX)))
    {
        builder_wrapper->loadFromConfig(config, std::string(CONFIG_PREFIX));
    }

    if (!user.empty())
    {
        String user_config_prefix = std::string(CONFIG_PREFIX) + "_" + user;
        if (config.has(user_config_prefix))
        {
            builder_wrapper->loadFromConfig(config, user_config_prefix, true);
        }
    }

#if USE_KRB5
    if (builder_wrapper->need_kinit)
    {
        builder_wrapper->runKinit();
    }
#endif // USE_KRB5

    return builder_wrapper;
}

HDFSFSPtr createHDFSFS(hdfsBuilder * builder)
{
    HDFSFSPtr fs(hdfsBuilderConnect(builder));
    if (fs == nullptr)
        throw Exception("Unable to connect to HDFS: " + String(hdfsGetLastError()),
            ErrorCodes::NETWORK_ERROR);

    return fs;
}

String getNameNodeUrl(const String & hdfs_url)
{
    const size_t pos = hdfs_url.find('/', hdfs_url.find("//") + 2);
    String namenode_url = hdfs_url.substr(0, pos) + "/";
    return namenode_url;
}

String getNameNodeCluster(const String &hdfs_url)
{
    auto pos1 = hdfs_url.find("//") + 2;
    auto pos2 = hdfs_url.find('/', pos1);

    return hdfs_url.substr(pos1, pos2 - pos1);
}

void checkHDFSURL(const String & url)
{
    if (!re2::RE2::FullMatch(url, std::string(HDFS_URL_REGEXP)))
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Bad hdfs url: {}. It should have structure 'hdfs://<host_name>:<port>/<path>'",
            url);
    }
}

HDFSFileInfoPtr getHDFSFileInfo(const HDFSFSPtr & fs, const std::string & path)
{
    auto file_info_holder = std::make_unique<HDFSFileInfo>();

    file_info_holder->file_info = hdfsGetPathInfo(fs.get(), path.data());
    if (!file_info_holder->file_info)
    {
        throw Exception(
            ErrorCodes::HDFS_ERROR,
            "Unable to get info of file `{}`, error: {}",
            path, String(hdfsGetLastError()));
    }

    file_info_holder->length = 1; /// Single file
    return file_info_holder;
}

}

#endif
