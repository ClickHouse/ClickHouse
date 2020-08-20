#include <IO/HDFSCommon.h>
#include <Poco/URI.h>

#if USE_HDFS
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NETWORK_ERROR;
}

HDFSBuilderPtr createHDFSBuilder(const std::string & uri_str)
{
    const Poco::URI uri(uri_str);
    const auto & host = uri.getHost();
    auto port = uri.getPort();
    const std::string path = "//";
    if (host.empty())
        throw Exception("Illegal HDFS URI: " + uri.toString(), ErrorCodes::BAD_ARGUMENTS);

    HDFSBuilderPtr builder(hdfsNewBuilder());
    if (builder == nullptr)
        throw Exception("Unable to create builder to connect to HDFS: " + uri.toString() + " " + std::string(hdfsGetLastError()),
            ErrorCodes::NETWORK_ERROR);
    hdfsBuilderConfSetStr(builder.get(), "input.read.timeout", "60000"); // 1 min
    hdfsBuilderConfSetStr(builder.get(), "input.write.timeout", "60000"); // 1 min
    hdfsBuilderConfSetStr(builder.get(), "input.connect.timeout", "60000"); // 1 min

    std::string user_info = uri.getUserInfo();
    if (!user_info.empty() && user_info.front() != ':')
    {
        std::string user;
        size_t delim_pos = user_info.find(':');
        if (delim_pos != std::string::npos)
            user = user_info.substr(0, delim_pos);
        else
            user = user_info;

        hdfsBuilderSetUserName(builder.get(), user.c_str());
    }
    hdfsBuilderSetNameNode(builder.get(), host.c_str());
    if (port != 0)
    {
        hdfsBuilderSetNameNodePort(builder.get(), port);
    }
    return builder;
}

HDFSFSPtr createHDFSFS(hdfsBuilder * builder)
{
    HDFSFSPtr fs(hdfsBuilderConnect(builder));
    if (fs == nullptr)
        throw Exception("Unable to connect to HDFS: " + std::string(hdfsGetLastError()),
            ErrorCodes::NETWORK_ERROR);

    return fs;
}
}
#endif
