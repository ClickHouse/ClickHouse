#pragma once

#include "config.h"

#if USE_HDFS
#include <memory>
#include <type_traits>
#include <vector>

#include <hdfs/hdfs.h>
#include <base/types.h>

#include <Interpreters/Context.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{

namespace detail
{
    struct HDFSFsDeleter
    {
        void operator()(hdfsFS fs_ptr)
        {
            hdfsDisconnect(fs_ptr);
        }
    };
}


struct HDFSFileInfo
{
    hdfsFileInfo * file_info;
    int length;

    HDFSFileInfo() : file_info(nullptr) , length(0) {}

    HDFSFileInfo(const HDFSFileInfo & other) = delete;
    HDFSFileInfo(HDFSFileInfo && other) = default;
    HDFSFileInfo & operator=(const HDFSFileInfo & other) = delete;
    HDFSFileInfo & operator=(HDFSFileInfo && other) = default;
    ~HDFSFileInfo();
};


class HDFSBuilderWrapper
{

friend HDFSBuilderWrapper createHDFSBuilder(const String & uri_str, const Poco::Util::AbstractConfiguration &);

static const String CONFIG_PREFIX;

public:
    HDFSBuilderWrapper() : hdfs_builder(hdfsNewBuilder()) {}

    ~HDFSBuilderWrapper() { hdfsFreeBuilder(hdfs_builder); }

    HDFSBuilderWrapper(const HDFSBuilderWrapper &) = delete;
    HDFSBuilderWrapper & operator=(const HDFSBuilderWrapper &) = delete;

    HDFSBuilderWrapper(HDFSBuilderWrapper && other) noexcept
    {
        *this = std::move(other);
    }

    HDFSBuilderWrapper & operator=(HDFSBuilderWrapper && other) noexcept
    {
        std::swap(hdfs_builder, other.hdfs_builder);
        config_stor = std::move(other.config_stor);
        hadoop_kerberos_keytab = std::move(other.hadoop_kerberos_keytab);
        hadoop_kerberos_principal = std::move(other.hadoop_kerberos_principal);
        hadoop_security_kerberos_ticket_cache_path = std::move(other.hadoop_security_kerberos_ticket_cache_path);
        need_kinit = std::move(other.need_kinit);
        return *this;
    }

    hdfsBuilder * get() { return hdfs_builder; }

private:
    void loadFromConfig(const Poco::Util::AbstractConfiguration & config, const String & prefix, bool isUser = false);

    // hdfs builder relies on an external config data storage
    std::pair<String, String>& keep(const String & k, const String & v)
    {
        return config_stor.emplace_back(std::make_pair(k, v));
    }

    hdfsBuilder * hdfs_builder = nullptr;
    std::vector<std::pair<String, String>> config_stor;

    #if USE_KRB5
    void runKinit();
    String hadoop_kerberos_keytab;
    String hadoop_kerberos_principal;
    String hadoop_security_kerberos_ticket_cache_path;
    bool need_kinit{false};
    #endif // USE_KRB5
};

using HDFSFSPtr = std::unique_ptr<std::remove_pointer_t<hdfsFS>, detail::HDFSFsDeleter>;


// set read/connect timeout, default value in libhdfs3 is about 1 hour, and too large
/// TODO Allow to tune from query Settings.
HDFSBuilderWrapper createHDFSBuilder(const String & uri_str, const Poco::Util::AbstractConfiguration &);
HDFSFSPtr createHDFSFS(hdfsBuilder * builder);


String getNameNodeUrl(const String & hdfs_url);
String getNameNodeCluster(const String & hdfs_url);

/// Check that url satisfy structure 'hdfs://<host_name>:<port>/<path>'
/// and throw exception if it doesn't;
void checkHDFSURL(const String & url);

}
#endif
