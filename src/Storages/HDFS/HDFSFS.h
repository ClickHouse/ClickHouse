#pragma once
#include <hdfs/hdfs.h>
#include <memory>

namespace DB
{

namespace detail
{
    struct HDFSFsDeleter;
}

using HDFSFSImpl = std::unique_ptr<std::remove_pointer_t<hdfsFS>, detail::HDFSFsDeleter>;
// typedef std::unique_ptr<std::remove_pointer_t<hdfsFS>, detail::HDFSFsDeleter> HDFSFSImpl;
struct HDFSFS : public HDFSFSImpl {};

}
