#include <Storages/HDFS/HDFSCommon.h>

#if USE_HDFS
#include <filesystem>

#include <Poco/URI.h>
#include <boost/algorithm/string/replace.hpp>
#include <re2/re2.h>

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

static const String HDFS_URL_REGEXP = "^hdfs://[^/]*/.*";


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

HDFSFSPool::HDFSFSPool(uint32_t max_items_, uint32_t min_items_, HDFSBuilderWrapperPtr builder_)
    : max_items(max_items_), min_items(std::min(max_items_, min_items_)), builder(std::move(builder_)), current_index(0)
{
    pool.reserve(max_items);

    if (min_items)
    {
        pool.resize(min_items);
        ThreadPool thread_pool(std::min(static_cast<uint32_t>(32), min_items));
        for (size_t i = 0; i < min_items; ++i)
        {
            thread_pool.scheduleOrThrowOnError([this, i]()
            {
                pool[i] = createHDFSFS(builder->get());
            });
        }
        thread_pool.wait();
    }
}

HDFSFSPtr HDFSFSPool::getFS()
{
    std::lock_guard lock{mutex};
    return unsafeGetFS();
}

HDFSFSPtr HDFSFSPool::unsafeGetFS()
{
    if (current_index == pool.size())
    {
        if (pool.size() == max_items)
        {
            current_index = 0;
        }
        else
        {
            pool.emplace_back(createHDFSFS(builder->get()));
        }
    }
    return pool[current_index++];
}

bool HDFSFSPool::executeWithRetries(HDFSFSPtr & fs, std::function<bool(HDFSFSPtr &)> callback)
{
    int retry = 0;
    while (retry++ < 3)
    {
        if (callback(fs))
            return true;

        if (retry < 3)
        {
            std::lock_guard lock{mutex};

            bool need_recreate_fs = false;
            for (auto & i : pool)
            {
                if (i == fs)
                {
                    need_recreate_fs = true;
                    fs = i = createHDFSFS(builder->get());
                    break;
                }
            }

            if (!need_recreate_fs)
            {
                fs = unsafeGetFS();
            }
        }
    }
    return false;
}

HDFSBuilderFSFactory & HDFSBuilderFSFactory::instance()
{
    static HDFSBuilderFSFactory factory;
    return factory;
}

void HDFSBuilderFSFactory::setEnv(const Poco::Util::AbstractConfiguration & config)
{
    String libhdfs3_conf = config.getString("hdfs.libhdfs3_conf", "");
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
}

HDFSBuilderWrapperPtr HDFSBuilderFSFactory::getBuilder(const String & hdfs_uri, const Poco::Util::AbstractConfiguration & config) const
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

HDFSFSPtr HDFSBuilderFSFactory::getFS(const String & hdfs_uri, const Poco::Util::AbstractConfiguration & config) const
{
    auto builder = getBuilder(hdfs_uri, config);
    return getFS(std::move(builder));
}

HDFSFSPtr HDFSBuilderFSFactory::getFS(HDFSBuilderWrapperPtr builder) const
{
    auto pool = getFSPool(std::move(builder));
    return pool->getFS();
}

HDFSFSPoolPtr HDFSBuilderFSFactory::getFSPool(HDFSBuilderWrapperPtr builder) const
{
    HDFSFSPoolPtr pool;
    const auto hdfs_uri = builder->getHDFSUri();

    std::lock_guard lock(mutex);
    auto it = hdfs_fs_pools.find(hdfs_uri);
    if (it == hdfs_fs_pools.end())
    {
        pool = std::make_shared<HDFSFSPool>(max_pool_size, min_pool_size, std::move(builder));
        hdfs_fs_pools.emplace(hdfs_uri, pool);
    }
    else
    {
        pool = it->second;
    }
    return pool;
}

bool HDFSBuilderFSFactory::executeWithRetries(HDFSBuilderWrapperPtr builder, HDFSFSPtr & fs, std::function<bool(HDFSFSPtr &)> callback) const
{
    auto pool = getFSPool(std::move(builder));
    return pool->executeWithRetries(fs, callback);
}


HDFSFSPtr createHDFSFS(hdfsBuilder * builder)
{
    HDFSFSPtr fs(hdfsBuilderConnect(builder), detail::HDFSFsDeleter());
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
