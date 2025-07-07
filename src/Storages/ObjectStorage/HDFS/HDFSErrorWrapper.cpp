#include <Storages/ObjectStorage/HDFS/HDFSErrorWrapper.h>

#if USE_HDFS

namespace DB
{

HDFSErrorWrapper::HDFSErrorWrapper(
        const std::string & hdfs_uri_,
        const Poco::Util::AbstractConfiguration & config_
): builder(createHDFSBuilder(hdfs_uri_, config_))
{
}

}

#endif
