#include <Storages/HDFS/HDFSCommon.h>
#include <Poco/URI.h>
#include <boost/algorithm/string/replace.hpp>
#include <re2/re2.h>
#include <filesystem>

#if USE_HDFS
#include <Common/ShellCommand.h>
#include <Common/Exception.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Common/logger_useful.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NETWORK_ERROR;
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int NO_ELEMENTS_IN_CONFIG;
}

const String HDFS_URL_REGEXP = "^hdfs://[^/]*/.*";


void HDFSBuilderWrapper::loadFromConfig(const String & prefix, bool isUser)
{
    Poco::Util::AbstractConfiguration::Keys keys;

    config.keys(prefix, keys);
    for (const auto & key : keys)
    {
        const String key_path = prefix + "." + key;

        String key_name;
        if (key == "hadoop_kerberos_keytab")
        {
            need_kinit = true;
            hadoop_kerberos_keytab = config.getString(key_path);
            continue;
        }
        else if (key == "hadoop_kerberos_principal")
        {
            need_kinit = true;
            hadoop_kerberos_principal = config.getString(key_path);
            hdfsBuilderSetPrincipal(hdfs_builder, hadoop_kerberos_principal.c_str());
            continue;
        }
        else if (key == "hadoop_kerberos_kinit_command")
        {
            need_kinit = true;
            hadoop_kerberos_kinit_command = config.getString(key_path);
            continue;
        }
        else if (key == "hadoop_security_kerberos_ticket_cache_path")
        {
            if (isUser)
            {
                throw Exception("hadoop.security.kerberos.ticket.cache.path cannot be set per user",
                    ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);
            }

            hadoop_security_kerberos_ticket_cache_path = config.getString(key_path);
            // standard param - pass further
        }

        key_name = boost::replace_all_copy(key, "_", ".");

        const auto & [k,v] = keep(key_name, config.getString(key_path));
        hdfsBuilderConfSetStr(hdfs_builder, k.c_str(), v.c_str());
    }
}

String HDFSBuilderWrapper::getKinitCmd()
{

    if (hadoop_kerberos_keytab.empty() || hadoop_kerberos_principal.empty())
    {
        throw Exception("Not enough parameters to run kinit",
            ErrorCodes::NO_ELEMENTS_IN_CONFIG);
    }

    WriteBufferFromOwnString ss;

    String cache_name =  hadoop_security_kerberos_ticket_cache_path.empty() ?
        String() :
        (String(" -c \"") + hadoop_security_kerberos_ticket_cache_path + "\"");

    // command to run looks like
    // kinit -R -t /keytab_dir/clickhouse.keytab -k somebody@TEST.CLICKHOUSE.TECH || ..
    ss << hadoop_kerberos_kinit_command << cache_name <<
        " -R -t \"" << hadoop_kerberos_keytab << "\" -k " << hadoop_kerberos_principal <<
        "|| " << hadoop_kerberos_kinit_command << cache_name << " -t \"" <<
        hadoop_kerberos_keytab << "\" -k " << hadoop_kerberos_principal;
    return ss.str();
}

void HDFSBuilderWrapper::runKinit()
{
    String cmd = getKinitCmd();
    LOG_DEBUG(&Poco::Logger::get("HDFSClient"), "running kinit: {}", cmd);

    std::unique_lock<std::mutex> lck(kinit_mtx);

    auto command = ShellCommand::execute(cmd);
    auto status = command->tryWait();
    if (status)
    {
        throw Exception("kinit failure: " + cmd, ErrorCodes::BAD_ARGUMENTS);
    }
}

void HDFSBuilderWrapper::initialize()
{
    const Poco::URI uri(hdfs_uri);
    const auto & host = uri.getHost();
    auto port = uri.getPort();
    const String path = "//";
    if (host.empty())
        throw Exception("Illegal HDFS URI: " + uri.toString(), ErrorCodes::BAD_ARGUMENTS);

    // Shall set env LIBHDFS3_CONF *before* HDFSBuilderWrapper construction.
    std::call_once(HDFSBuilderWrapper::init_libhdfs3_conf_flag, [this]()
    {
        String libhdfs3_conf = config.getString(HDFSBuilderWrapper::CONFIG_PREFIX + ".libhdfs3_conf", "");
        if (!libhdfs3_conf.empty())
        {
            if (std::filesystem::path{libhdfs3_conf}.is_relative() && !std::filesystem::exists(libhdfs3_conf))
            {
                const String config_path = config.getString("config-file", "config.xml");
                const auto config_dir = std::filesystem::path{config_path}.remove_filename();
                if (std::filesystem::exists(config_dir / libhdfs3_conf))
                    libhdfs3_conf = std::filesystem::absolute(config_dir / libhdfs3_conf);
            }
            setenv("LIBHDFS3_CONF", libhdfs3_conf.c_str(), 1);
        }
    });

    if (get() == nullptr)
        throw Exception("Unable to create builder to connect to HDFS: " +
            uri.toString() + " " + String(hdfsGetLastError()),
            ErrorCodes::NETWORK_ERROR);

    hdfsBuilderConfSetStr(get(), "input.read.timeout", "60000"); // 1 min
    hdfsBuilderConfSetStr(get(), "input.write.timeout", "60000"); // 1 min
    hdfsBuilderConfSetStr(get(), "input.connect.timeout", "60000"); // 1 min

    String user_info = uri.getUserInfo();
    String user;
    if (!user_info.empty() && user_info.front() != ':')
    {
        size_t delim_pos = user_info.find(':');
        if (delim_pos != String::npos)
            user = user_info.substr(0, delim_pos);
        else
            user = user_info;

        hdfsBuilderSetUserName(get(), user.c_str());
    }

    hdfsBuilderSetNameNode(get(), host.c_str());
    if (port != 0)
    {
        hdfsBuilderSetNameNodePort(get(), port);
    }

    if (config.has(HDFSBuilderWrapper::CONFIG_PREFIX))
    {
        loadFromConfig(HDFSBuilderWrapper::CONFIG_PREFIX);
    }

    if (!user.empty())
    {
        String user_config_prefix = HDFSBuilderWrapper::CONFIG_PREFIX + "_" + user;
        if (config.has(user_config_prefix))
        {
            loadFromConfig(user_config_prefix, true);
        }
    }

    if (need_kinit)
    {
        runKinit();
    }
}

HDFSBuilderWrapperFactory & HDFSBuilderWrapperFactory::instance()
{
    static HDFSBuilderWrapperFactory factory;
    return factory;
}

HDFSBuilderWrapperPtr HDFSBuilderWrapperFactory::getOrCreate(const String & hdfs_uri, const Poco::Util::AbstractConfiguration & config)
{
    std::lock_guard lock(mutex);
    auto it = hdfs_builder_wrappers.find(hdfs_uri);
    if (it == hdfs_builder_wrappers.end())
    {
        auto result = std::make_shared<HDFSBuilderWrapper>(hdfs_uri, config);
        hdfs_builder_wrappers.emplace(hdfs_uri, result);
        return result;
    }
    return it->second;
}

// std::mutex HDFSBuilderWrapper::kinit_mtx;

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
    if (!re2::RE2::FullMatch(url, HDFS_URL_REGEXP))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bad hdfs url: {}. It should have structure 'hdfs://<host_name>:<port>/<path>'", url);
}

}

#endif
