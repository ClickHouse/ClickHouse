#pragma once
#include <Common/config.h>
#include <memory>
#include <type_traits>

#if USE_HDFS
#include <hdfs/hdfs.h>

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
using HDFSBuilderPtr = std::unique_ptr<hdfsBuilder, detail::HDFSBuilderDeleter>;
using HDFSFSPtr = std::unique_ptr<std::remove_pointer_t<hdfsFS>, detail::HDFSFsDeleter>;

// set read/connect timeout, default value in libhdfs3 is about 1 hour, and too large
/// TODO Allow to tune from query Settings.
HDFSBuilderPtr createHDFSBuilder(const std::string & uri_str);
HDFSFSPtr createHDFSFS(hdfsBuilder * builder);
}
#endif
