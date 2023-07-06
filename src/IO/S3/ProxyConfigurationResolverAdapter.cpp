#include <IO/S3/ProxyConfigurationResolverAdapter.h>

namespace DB::S3
{

ClientConfigurationPerRequest ProxyConfigurationResolverAdapter::getConfiguration(const Aws::Http::HttpRequest & request)
{
    auto proxy_configuration = resolver->resolve(request.GetUri().GetScheme() == Aws::Http::Scheme::HTTPS);

    return ClientConfigurationPerRequest {
        Aws::Http::SchemeMapper::FromString(proxy_configuration.scheme.c_str()),
        proxy_configuration.host,
        proxy_configuration.port
    };
}

void ProxyConfigurationResolverAdapter::errorReport(const ClientConfigurationPerRequest & config)
{
    return resolver->errorReport(DB::ProxyConfiguration{
        config.proxy_host,
        Aws::Http::SchemeMapper::ToString(config.proxy_scheme),
        static_cast<uint16_t>(config.proxy_port)
    });
}

}
