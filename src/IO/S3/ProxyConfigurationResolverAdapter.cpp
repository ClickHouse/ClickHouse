#include <IO/S3/ProxyConfigurationResolverAdapter.h>

#if USE_AWS_S3

namespace DB::S3
{

namespace
{
    auto protocolToAWSScheme(DB::ProxyConfiguration::Protocol protocol)
    {
        switch (protocol)
        {
            case DB::ProxyConfiguration::Protocol::HTTP:
                return Aws::Http::Scheme::HTTP;
            case DB::ProxyConfiguration::Protocol::HTTPS:
                return Aws::Http::Scheme::HTTPS;
            case DB::ProxyConfiguration::Protocol::ANY:
                return Aws::Http::Scheme::HTTPS;
        }
    }

    auto AWSSchemeToProtocol(Aws::Http::Scheme scheme)
    {
        switch (scheme)
        {
            case Aws::Http::Scheme::HTTP:
                return DB::ProxyConfiguration::Protocol::HTTP;
            case Aws::Http::Scheme::HTTPS:
                return DB::ProxyConfiguration::Protocol::HTTPS;
        }
    }
}

ClientConfigurationPerRequest ProxyConfigurationResolverAdapter::getConfiguration(const Aws::Http::HttpRequest & request)
{
    bool is_https = request.GetUri().GetScheme() == Aws::Http::Scheme::HTTPS;
    auto method = is_https ? ProxyConfigurationResolver::Protocol::HTTPS : ProxyConfigurationResolver::Protocol::HTTP;

    auto proxy_configuration = resolver->resolve(method);

    return ClientConfigurationPerRequest {
        protocolToAWSScheme(proxy_configuration.protocol),
        proxy_configuration.host,
        proxy_configuration.port
    };
}

void ProxyConfigurationResolverAdapter::errorReport(const ClientConfigurationPerRequest & config)
{
    return resolver->errorReport(DB::ProxyConfiguration {
        config.proxy_host,
        AWSSchemeToProtocol(config.proxy_scheme),
        static_cast<uint16_t>(config.proxy_port)
    });
}

}

#endif
