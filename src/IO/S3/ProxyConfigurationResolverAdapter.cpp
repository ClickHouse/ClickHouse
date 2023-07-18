#include <IO/S3/ProxyConfigurationResolverAdapter.h>

#if USE_AWS_S3

namespace DB::S3
{

ClientConfigurationPerRequest ProxyConfigurationResolverAdapter::getConfiguration(const Aws::Http::HttpRequest & request)
{
    bool is_https = request.GetUri().GetScheme() == Aws::Http::Scheme::HTTPS;
    auto method = is_https ? ProxyConfigurationResolver::Method::HTTPS : ProxyConfigurationResolver::Method::HTTP;

    auto proxy_configuration = resolver->resolve(method);

    return ClientConfigurationPerRequest {
        Aws::Http::SchemeMapper::FromString(proxy_configuration.scheme.c_str()),
        proxy_configuration.host,
        proxy_configuration.port
    };
}

void ProxyConfigurationResolverAdapter::errorReport(const ClientConfigurationPerRequest & config)
{
    return resolver->errorReport(DB::ProxyConfiguration {
        config.proxy_host,
        Aws::Http::SchemeMapper::ToString(config.proxy_scheme),
        static_cast<uint16_t>(config.proxy_port)
    });
}

}

#endif
