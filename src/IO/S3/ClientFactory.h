#pragma once

#include <Common/config.h>


#if USE_AWS_S3

#include <atomic>
#include <memory>

#include <base/types.h>
#include <IO/S3/Client.h>

#include <aws/core/Aws.h>


namespace DB
{
    class RemoteHostFilter;
    struct HttpHeader;
    using HeaderCollection = std::vector<HttpHeader>;
}

namespace DB::S3
{

class ClientFactory
{
public:
    ~ClientFactory();

    static ClientFactory & instance();

    std::unique_ptr<Client> create(const ClientConfiguration & client_configuration, const URI & uri);

    ClientConfiguration createClientConfiguration(const RemoteHostFilter & remote_host_filter);

private:
    ClientFactory();

    Aws::SDKOptions aws_options;
    std::atomic<bool> s3_requests_logging_enabled;
};

}

#endif
