#include <Common/config.h>
#include <memory>
#include <type_traits>
#include <Poco/URI.h>

#if USE_HDFS
#include <hdfs/hdfs.h> // Y_IGNORE

namespace DB
{
namespace detail
{
struct HDFSBuilderDeleter
{
    void operator()(hdfsBuilder * builder_ptr)
    {
        hdfsFreeBuilder(builder_ptr);
    }
};
struct HDFSFsDeleter
{
    void operator()(hdfsFS fs_ptr)
    {
        hdfsDisconnect(fs_ptr);
    }
};

}

using HDFSBuilderPtr = std::unique_ptr<hdfsBuilder, detail::HDFSBuilderDeleter>;
using HDFSFSPtr = std::unique_ptr<std::remove_pointer_t<hdfsFS>, detail::HDFSFsDeleter>;

// set read/connect timeout, default value in libhdfs3 is about 1 hour, and too large
/// TODO Allow to tune from query Settings.
HDFSBuilderPtr createHDFSBuilder(const Poco::URI & hdfs_uri);
HDFSFSPtr createHDFSFS(hdfsBuilder * builder);
}
#endif
