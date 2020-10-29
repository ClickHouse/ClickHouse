#pragma once
#include <Common/config.h>
#include <memory>
#include <type_traits>
#include <vector>
#include <string>

#if USE_HDFS
#include <hdfs/hdfs.h>
#include <Storages/IStorage.h>

namespace DB
{
namespace detail
{
/* struct HDFSBuilderDeleter */
/* { */
/*     void operator()(hdfsBuilder * builder_ptr) */
/*     { */
/*         hdfsFreeBuilder(builder_ptr); */
/*     } */
/* }; */
struct HDFSFsDeleter
{
    void operator()(hdfsFS fs_ptr)
    {
        hdfsDisconnect(fs_ptr);
    }
};



#if 0

class KinitTaskHolder
{
    using Container = std::map<std::string, BackgroundSchedulePool::TaskHolder>;
    Container container;


    String make_key(const HDFSBuilderWrapper & hbw)
    {
        return hbw.hadoop_kerberos_keytab + "^"
            + hbw.hadoop_kerberos_principal + "^"
            + std::to_string(time_relogin);
    }

public:
    using Descriptor = Container::iterator;

    Descriptor addTask(const HDFSBuilderWrapper & hdfs_builder_wrapper)
    {
        auto key = make_key(hdfs_builder_wrapper);

        auto it = container.find(key);
        if ( it != std::end(container))
        {
            it = container.insert({key, task}).first;
        }

        return it.second->getptr();

    }
    void delTask(Descriptor it)
    {
        container.erase(it);
    }
};

#endif

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

    std::pair<String, String>& keep(const String & k, const String & v)
    {
        return config_stor.emplace_back(std::make_pair(k, v));
    }

    void loadFromConfig(const Poco::Util::AbstractConfiguration & config,
				const String & config_path, bool isUser = false);

    String getKinitCmd();

    bool needKinit{false};

    void runKinit();

		void makeCachePath(const String & cachePath, String user = "");

    static const String CONFIG_PREFIX;

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

    friend HDFSBuilderWrapper createHDFSBuilder(const String & uri_str, const Context & context);
};




/* using HDFSBuilderPtr = std::unique_ptr<hdfsBuilder, detail::HDFSBuilderDeleter>; */
using HDFSFSPtr = std::unique_ptr<std::remove_pointer_t<hdfsFS>, detail::HDFSFsDeleter>;

// set read/connect timeout, default value in libhdfs3 is about 1 hour, and too large
/// TODO Allow to tune from query Settings.
HDFSBuilderWrapper createHDFSBuilder(const String & uri_str, const Context & context);
HDFSFSPtr createHDFSFS(hdfsBuilder * builder);
}
#endif
