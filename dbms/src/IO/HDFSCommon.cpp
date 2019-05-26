#include <IO/HDFSCommon.h>

#if USE_HDFS
#include <Common/Exception.h>
namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NETWORK_ERROR;
}
HDFSBuilderPtr createHDFSBuilder(const Poco::URI & uri)
{
    auto & host = uri.getHost();
    auto port = uri.getPort();
    auto & path = uri.getPath();
    if (host.empty() || port == 0 || path.empty())
        throw Exception("Illegal HDFS URI: " + uri.toString(), ErrorCodes::BAD_ARGUMENTS);

    HDFSBuilderPtr builder(hdfsNewBuilder());
    if (builder == nullptr)
        throw Exception("Unable to create builder to connect to HDFS: " + uri.toString() + " " + std::string(hdfsGetLastError()),
            ErrorCodes::NETWORK_ERROR);
    hdfsBuilderConfSetStr(builder.get(), "input.read.timeout", "60000"); // 1 min
    hdfsBuilderConfSetStr(builder.get(), "input.write.timeout", "60000"); // 1 min
    hdfsBuilderConfSetStr(builder.get(), "input.connect.timeout", "60000"); // 1 min

    hdfsBuilderSetNameNode(builder.get(), host.c_str());
    hdfsBuilderSetNameNodePort(builder.get(), port);
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
