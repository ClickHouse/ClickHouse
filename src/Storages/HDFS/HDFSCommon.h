#pragma once

#include <Common/config.h>

#if USE_HDFS
#include <memory>
#include <type_traits>
#include <vector>

#include <hdfs/hdfs.h>
#include <base/types.h>
#include <mutex>

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

    ~HDFSFileInfo()
    {
        hdfsFreeFileInfo(file_info, length);
    }
};


class HDFSBuilderWrapper
{
public:
    HDFSBuilderWrapper(const String & hdfs_uri_, const Poco::Util::AbstractConfiguration & config_)
        : hdfs_builder(hdfsNewBuilder()), hdfs_uri(hdfs_uri_), config(config_)
    {
        initialize();
    }

    ~HDFSBuilderWrapper() { hdfsFreeBuilder(hdfs_builder); }

    HDFSBuilderWrapper(const HDFSBuilderWrapper &) = delete;
    HDFSBuilderWrapper(HDFSBuilderWrapper &&) = default;

    HDFSBuilderWrapper & operator=(const HDFSBuilderWrapper &) = delete;

    hdfsBuilder * get() { return hdfs_builder; }

private:
    void initialize();

    void loadFromConfig(const String & prefix, bool isUser = false);

    String getKinitCmd();

    void runKinit();

    // hdfs builder relies on an external config data storage
    std::pair<String, String>& keep(const String & k, const String & v)
    {
        return config_stor.emplace_back(std::make_pair(k, v));
    }

    inline static const String CONFIG_PREFIX = "hdfs";
    inline static std::mutex kinit_mtx;
    inline static std::once_flag init_libhdfs3_conf_flag;

    hdfsBuilder * hdfs_builder;
    const String hdfs_uri;
    const Poco::Util::AbstractConfiguration & config;
    String hadoop_kerberos_keytab;
    String hadoop_kerberos_principal;
    String hadoop_kerberos_kinit_command = "kinit";
    String hadoop_security_kerberos_ticket_cache_path;

    std::vector<std::pair<String, String>> config_stor;
    bool need_kinit{false};
};

using HDFSBuilderWrapperPtr = std::shared_ptr<HDFSBuilderWrapper>;

class HDFSBuilderWrapperFactory final: public boost::noncopyable
{
public:
    static HDFSBuilderWrapperFactory & instance();

    HDFSBuilderWrapperPtr getOrCreate(const String & hdfs_uri, const Poco::Util::AbstractConfiguration & config);

private:
    std::mutex mutex;
    std::map<String, HDFSBuilderWrapperPtr> hdfs_builder_wrappers;

};

using HDFSFSPtr = std::unique_ptr<std::remove_pointer_t<hdfsFS>, detail::HDFSFsDeleter>;


HDFSFSPtr createHDFSFS(hdfsBuilder * builder);
String getNameNodeUrl(const String & hdfs_url);
String getNameNodeCluster(const String & hdfs_url);

/// Check that url satisfy structure 'hdfs://<host_name>:<port>/<path>'
/// and throw exception if it doesn't;
void checkHDFSURL(const String & url);

}
#endif
