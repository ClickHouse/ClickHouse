#pragma once

#include <Common/config.h>

#if USE_HDFS
#include <memory>
#include <type_traits>
#include <vector>

#include <hdfs/hdfs.h>
#include <common/types.h>
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

    HDFSFileInfo()
        : file_info(nullptr)
        , length(0)
    {
    }
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
    hdfsBuilder * hdfs_builder;
    String hadoop_kerberos_keytab;
    String hadoop_kerberos_principal;
    String hadoop_kerberos_kinit_command = "kinit";
    String hadoop_security_kerberos_ticket_cache_path;

    static std::mutex kinit_mtx;

    std::vector<std::pair<String, String>> config_stor;

    // hdfs builder relies on an external config data storage
    std::pair<String, String>& keep(const String & k, const String & v)
    {
        return config_stor.emplace_back(std::make_pair(k, v));
    }

    bool need_kinit{false};

    static const String CONFIG_PREFIX;

private:

    void loadFromConfig(const Poco::Util::AbstractConfiguration & config, const String & config_path, bool isUser = false);

    String getKinitCmd();

    void runKinit();

public:

    hdfsBuilder *
    get()
    {
        return hdfs_builder;
    }

    HDFSBuilderWrapper()
        : hdfs_builder(hdfsNewBuilder())
    {
    }

    ~HDFSBuilderWrapper()
    {
        hdfsFreeBuilder(hdfs_builder);

    }

    HDFSBuilderWrapper(const HDFSBuilderWrapper &) = delete;
    HDFSBuilderWrapper(HDFSBuilderWrapper &&) = default;

    friend HDFSBuilderWrapper createHDFSBuilder(const String & uri_str, const Poco::Util::AbstractConfiguration &);
};

using HDFSFSPtr = std::unique_ptr<std::remove_pointer_t<hdfsFS>, detail::HDFSFsDeleter>;

// set read/connect timeout, default value in libhdfs3 is about 1 hour, and too large
/// TODO Allow to tune from query Settings.
HDFSBuilderWrapper createHDFSBuilder(const String & uri_str, const Poco::Util::AbstractConfiguration &);
HDFSFSPtr createHDFSFS(hdfsBuilder * builder);
}
#endif
