#include <IO/HDFSCommon.h>
#include <Poco/URI.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <boost/algorithm/string/replace.hpp>
#include <Interpreters/Context.h>


#if USE_HDFS
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NETWORK_ERROR;
}

const String HDFSBuilderWrapper::CONFIG_PREFIX = "hdfs";

void HDFSBuilderWrapper::loadFromConfig(const Poco::Util::AbstractConfiguration & config, const String & path)
{
    hdfsBuilderConfSetStr(hdfs_builder, "input.read.timeout", "60000"); // 1 min
    hdfsBuilderConfSetStr(hdfs_builder, "input.write.timeout", "60000"); // 1 min
    hdfsBuilderConfSetStr(hdfs_builder, "input.connect.timeout", "60000"); // 1 min

    // hdfsBuilderConfSetStr(rawBuilder, "hadoop.security.authentication", "kerberos");
    // hdfsBuilderConfSetStr(rawBuilder, "dfs.client.log.severity", "TRACE");

    Poco::Util::AbstractConfiguration::Keys keys;

    config.keys(path, keys);
    for (const auto & key : keys)
    {
        const String key_path = path + "." + key;

        String key_name;
        if (key == "hadoop_kerberos_keytab")
        {
            needKinit = true;
            hadoop_kerberos_keytab = config.getString(key_path);
            continue;
        }
        else if (key == "hadoop_kerberos_principal")
        {
            needKinit = true;
            hadoop_kerberos_principal = config.getString(key_path);
            hdfsBuilderSetPrincipal(hdfs_builder, hadoop_kerberos_principal.c_str());

            continue;
        }
        else if (key == "hadoop_kerberos_kinit_command")
        {
            needKinit = true;
            hadoop_kerberos_kinit_command = config.getString(key_path);
            continue;
        }
        else if (key == "hadoop_security_kerberos_ticket_cache_path")
        {
            hadoop_security_kerberos_ticket_cache_path = config.getString(key_path);
            // standard param - pass to libhdfs3
        }

        key_name = boost::replace_all_copy(key, "_", ".");

        const auto & [k,v] = keep(key_name, config.getString(key_path));
        hdfsBuilderConfSetStr(hdfs_builder, k.c_str(), v.c_str());
    }
}

String HDFSBuilderWrapper::getKinitCmd()
{
    std::stringstream ss;
<<<<<<< HEAD

    String cache_name =  hadoop_security_kerberos_ticket_cache_path.empty() ? String() : (String(" -c \"") + hadoop_security_kerberos_ticket_cache_path + "\"");


    ss << hadoop_kerberos_kinit_command << cache_name << " -R -t \"" << hadoop_kerberos_keytab << "\" -k " << hadoop_kerberos_principal <<
        "|| " << hadoop_kerberos_kinit_command << cache_name << " -t \"" << hadoop_kerberos_keytab << "\" -k " << hadoop_kerberos_principal;
    return ss.str();
}

void HDFSBuilderWrapper::runKinit()
{
    String cmd = getKinitCmd();
    LOG_DEBUG(&Poco::Logger::get("HDFSClient"), "running kinit: {}", cmd);

    std::unique_lock<std::mutex> lck(kinit_mtx);

    int ret = system(cmd.c_str());
    if (ret)
    { // check it works !!
        throw Exception("kinit failure: " + std::to_string(ret) + " " + cmd, ErrorCodes::NETWORK_ERROR);
    }
}


=======
    ss << "kinit -R -t \"" << hadoop_kerberos_keytab << "\" -k " << hadoop_kerberos_principal <<
        "|| kinit -t \"" << hadoop_kerberos_keytab << "\" -k " << hadoop_kerberos_principal;
    return ss.str();
}

>>>>>>> kerberized hdfs compiled
HDFSBuilderWrapper createHDFSBuilder(const String & uri_str, const Context & context)
{
    const Poco::URI uri(uri_str);
    const auto & host = uri.getHost();
    auto port = uri.getPort();
    const String path = "//";
    if (host.empty())
        throw Exception("Illegal HDFS URI: " + uri.toString(), ErrorCodes::BAD_ARGUMENTS);

    HDFSBuilderWrapper builder;
    if (builder.get() == nullptr)
        throw Exception("Unable to create builder to connect to HDFS: " + uri.toString() + " " + String(hdfsGetLastError()),
            ErrorCodes::NETWORK_ERROR);
    // hdfsBuilderConfSetStr(builder.get(), "input.read.timeout", "60000"); // 1 min
    // hdfsBuilderConfSetStr(builder.get(), "input.write.timeout", "60000"); // 1 min
    // hdfsBuilderConfSetStr(builder.get(), "input.connect.timeout", "60000"); // 1 min


    // hdfsBuilderConfSetStr(builder.get(), "hadoop.security.authentication", "kerberos");
    // hdfsBuilderConfSetStr(builder.get(), "dfs.client.log.severity", "TRACE");

    const auto & config = context.getConfigRef();
    if (config.has(HDFSBuilderWrapper::CONFIG_PREFIX))
    {
        builder.loadFromConfig(config, HDFSBuilderWrapper::CONFIG_PREFIX);
        if (builder.needKinit)
        {
            String cmd = builder.getKinitCmd();
            int ret = system(cmd.c_str());
            if (ret)
            {
                throw Exception("kinit failure: " + std::to_string(ret) + " " + cmd, ErrorCodes::NETWORK_ERROR);
            }
        }
    }

<<<<<<< HEAD

    // hdfsBuilderConfSetStr(builder.get(), "hadoop.security.authentication", "kerberos");
    // hdfsBuilderConfSetStr(builder.get(), "dfs.client.log.severity", "TRACE");

    const auto & config = context.getConfigRef();

    String user_info = uri.getUserInfo();
    String user;
    if (!user_info.empty() && user_info.front() != ':')
    {
=======
    String user_info = uri.getUserInfo();
    if (!user_info.empty() && user_info.front() != ':')
    {
        String user;
>>>>>>> kerberized hdfs compiled
        size_t delim_pos = user_info.find(':');
        if (delim_pos != String::npos)
            user = user_info.substr(0, delim_pos);
        else
            user = user_info;

        hdfsBuilderSetUserName(builder.get(), user.c_str());
    }
    hdfsBuilderSetNameNode(builder.get(), host.c_str());
    if (port != 0)
    {
        hdfsBuilderSetNameNodePort(builder.get(), port);
    }
    if (config.has(HDFSBuilderWrapper::CONFIG_PREFIX))
    {
        builder.loadFromConfig(config, HDFSBuilderWrapper::CONFIG_PREFIX);
    }
    if (!user.empty())
    {
        String user_config_prefix = HDFSBuilderWrapper::CONFIG_PREFIX + "_" + user;
        if (config.has(user_config_prefix))
            builder.loadFromConfig(config, user_config_prefix);
    }

    if (builder.needKinit)
    {
        builder.runKinit();
    }

    return builder;
}

std::mutex HDFSBuilderWrapper::kinit_mtx;

HDFSFSPtr createHDFSFS(hdfsBuilder * builder)
{
    HDFSFSPtr fs(hdfsBuilderConnect(builder));
    if (fs == nullptr)
        throw Exception("Unable to connect to HDFS: " + String(hdfsGetLastError()),
            ErrorCodes::NETWORK_ERROR);

    return fs;
}

}

#endif
