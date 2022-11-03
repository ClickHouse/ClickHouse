#pragma once

#include "config.h"

#if USE_HDFS
#include <memory>
#include <type_traits>
#include <vector>

#include <base/types.h>

#include <Interpreters/Context.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <boost/noncopyable.hpp>
#include <hdfs/hdfs.h>


namespace DB
{

namespace detail
{
    struct HDFSFsDeleter
    {
        void operator()(hdfsFS fs_ptr);
    };
}

using HDFSFSPtr = std::unique_ptr<std::remove_pointer_t<hdfsFS>, detail::HDFSFsDeleter>;

struct HDFSFileInfo : boost::noncopyable
{
    hdfsFileInfo * file_info = nullptr;
    int length = 0;

    ~HDFSFileInfo();
};

using HDFSFileInfoPtr = std::unique_ptr<HDFSFileInfo>;

class HDFSBuilderWrapper;
using HDFSBuilderWrapperPtr = std::unique_ptr<HDFSBuilderWrapper>;

class HDFSBuilderWrapper : boost::noncopyable
{
    static const String CONFIG_PREFIX;
    friend HDFSBuilderWrapperPtr createHDFSBuilder(const String & uri_str, const Poco::Util::AbstractConfiguration &);

public:
    HDFSBuilderWrapper();

    ~HDFSBuilderWrapper();

    hdfsBuilder * getBuilder() { return hdfs_builder; }

private:
    using KeyValue = std::pair<String, String>;
    using KeyValueConfig = std::vector<KeyValue>;

    void loadFromConfig(const Poco::Util::AbstractConfiguration & config, const String & prefix, bool is_user = false);

    /// hdfs_builder relies on an external config data storage
    KeyValue & keep(const String & key, const String & value);

    hdfsBuilder * hdfs_builder;

    KeyValueConfig config_storage;

#if USE_KRB5
    void runKinit();

    String hadoop_kerberos_keytab;
    String hadoop_kerberos_principal;
    String hadoop_security_kerberos_ticket_cache_path;

    bool need_kinit{false};
#endif // USE_KRB5
};

// set read/connect timeout, default value in libhdfs3 is about 1 hour, and too large
/// TODO Allow to tune from query Settings.
HDFSBuilderWrapperPtr createHDFSBuilder(const String & uri_str, const Poco::Util::AbstractConfiguration &);
HDFSFSPtr createHDFSFS(hdfsBuilder * builder);


String getNameNodeUrl(const String & hdfs_url);
String getNameNodeCluster(const String & hdfs_url);

/// Check that url satisfy structure 'hdfs://<host_name>:<port>/<path>'
/// and throw exception if it doesn't;
void checkHDFSURL(const String & url);

HDFSFileInfoPtr getHDFSFileInfo(const HDFSFSPtr & fs, const std::string & path);

}
#endif
