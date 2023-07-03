#include <IO/S3/ProxyConfigurationResolverAdapter.h>

namespace DB::S3
{

ClientConfigurationPerRequest ProxyConfigurationResolverAdapter::getConfiguration(const Aws::Http::HttpRequest & request)
{
    if (auto proxy_configuration_opt = resolver->resolve(request.GetUri().GetScheme() == Aws::Http::Scheme::HTTPS))
    {
        auto proxy_configuration = proxy_configuration_opt.value();
        return ClientConfigurationPerRequest {
            Aws::Http::SchemeMapper::FromString(proxy_configuration.scheme.c_str()),
            proxy_configuration.host,
            proxy_configuration.port
        };
    }

    return {};
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
