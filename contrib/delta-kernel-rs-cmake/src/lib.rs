pub use delta_kernel_ffi;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use delta_kernel::engine::default::storage::insert_url_handler;
use delta_kernel::object_store::aws::AmazonS3Builder;
use delta_kernel::object_store::azure::MicrosoftAzureBuilder;
use delta_kernel::object_store::client::{HttpClient, HttpConnector};
use delta_kernel::object_store::gcp::GoogleCloudStorageBuilder;
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::{ClientConfigKey, ClientOptions, Error, ObjectStore, Result};
use url::Url;

#[derive(Debug, Default)]
struct NoRedirectConnector;

fn config_error(key: &ClientConfigKey, value: &str, kind: &str) -> Error
{
    Error::Generic {
        store: "Config",
        source: format!("failed to parse \"{value}\" of `{}` as {kind}", key.as_ref()).into(),
    }
}

fn client_error(source: reqwest::Error) -> Error
{
    Error::Generic { store: "HTTP client", source: Box::new(source) }
}

fn get_bool(options: &ClientOptions, key: ClientConfigKey) -> Result<bool>
{
    let Some(value) = options.get_config_value(&key) else { return Ok(false) };
    match value.to_ascii_lowercase().as_str()
    {
        "1" | "true" | "on" | "yes" | "y" => Ok(true),
        "0" | "false" | "off" | "no" | "n" => Ok(false),
        _ => Err(config_error(&key, &value, "boolean")),
    }
}

fn get_duration(options: &ClientOptions, key: ClientConfigKey) -> Result<Option<Duration>>
{
    options
        .get_config_value(&key)
        .map(|value| humantime::parse_duration(&value).map_err(|_| config_error(&key, &value, "Duration")))
        .transpose()
}

fn get_number<T: std::str::FromStr>(options: &ClientOptions, key: ClientConfigKey) -> Result<Option<T>>
{
    options
        .get_config_value(&key)
        .map(|value| value.parse().map_err(|_| config_error(&key, &value, "number")))
        .transpose()
}

fn build_client(options: &ClientOptions) -> Result<reqwest::Client>
{
    let mut builder = reqwest::ClientBuilder::new().redirect(reqwest::redirect::Policy::none());

    let user_agent = options.get_config_value(&ClientConfigKey::UserAgent);
    builder = builder.user_agent(user_agent.as_deref().unwrap_or("object_store/0.13.2"));

    if let Some(proxy_url) = options.get_config_value(&ClientConfigKey::ProxyUrl)
    {
        let mut proxy = reqwest::Proxy::all(proxy_url).map_err(client_error)?;
        if let Some(certificate) = options.get_config_value(&ClientConfigKey::ProxyCaCertificate)
        {
            let certificate = reqwest::tls::Certificate::from_pem(certificate.as_bytes()).map_err(client_error)?;
            builder = builder.add_root_certificate(certificate);
        }
        if let Some(proxy_excludes) = options.get_config_value(&ClientConfigKey::ProxyExcludes)
        {
            proxy = proxy.no_proxy(reqwest::NoProxy::from_string(&proxy_excludes));
        }
        builder = builder.proxy(proxy);
    }

    if let Some(timeout) = get_duration(options, ClientConfigKey::Timeout)?
    {
        builder = builder.timeout(timeout);
    }
    if let Some(timeout) = get_duration(options, ClientConfigKey::ConnectTimeout)?
    {
        builder = builder.connect_timeout(timeout);
    }
    if let Some(timeout) = get_duration(options, ClientConfigKey::PoolIdleTimeout)?
    {
        builder = builder.pool_idle_timeout(timeout);
    }
    if let Some(max) = get_number::<usize>(options, ClientConfigKey::PoolMaxIdlePerHost)?
    {
        builder = builder.pool_max_idle_per_host(max);
    }
    if let Some(interval) = get_duration(options, ClientConfigKey::Http2KeepAliveInterval)?
    {
        builder = builder.http2_keep_alive_interval(interval);
    }
    if let Some(timeout) = get_duration(options, ClientConfigKey::Http2KeepAliveTimeout)?
    {
        builder = builder.http2_keep_alive_timeout(timeout);
    }
    if get_bool(options, ClientConfigKey::Http2KeepAliveWhileIdle)?
    {
        builder = builder.http2_keep_alive_while_idle(true);
    }
    if let Some(size) = get_number::<u32>(options, ClientConfigKey::Http2MaxFrameSize)?
    {
        builder = builder.http2_max_frame_size(Some(size));
    }
    if get_bool(options, ClientConfigKey::Http1Only)?
    {
        builder = builder.http1_only();
    }
    if get_bool(options, ClientConfigKey::Http2Only)?
    {
        builder = builder.http2_prior_knowledge();
    }
    if get_bool(options, ClientConfigKey::AllowInvalidCertificates)?
    {
        builder = builder.danger_accept_invalid_certs(true);
    }

    builder
        .no_gzip()
        .no_brotli()
        .no_zstd()
        .no_deflate()
        .https_only(!get_bool(options, ClientConfigKey::AllowHttp)?)
        .build()
        .map_err(client_error)
}

impl HttpConnector for NoRedirectConnector
{
    fn connect(&self, options: &ClientOptions) -> Result<HttpClient>
    {
        Ok(HttpClient::new(build_client(options)?))
    }
}

macro_rules! no_redirect_store {
    ($builder:ty, $url:expr, $options:expr) => {{
        let builder = $options.into_iter().fold(
            <$builder>::new().with_url($url.to_string()),
            |builder, (key, value)| match key.to_ascii_lowercase().parse() {
                Ok(k) => builder.with_config(k, value),
                Err(_) => builder,
            },
        );
        Box::new(builder.with_http_connector(NoRedirectConnector).build()?) as Box<dyn ObjectStore>
    }};
}

fn s3_store(url: &Url, options: HashMap<String, String>) -> Result<(Box<dyn ObjectStore>, Path)>
{
    Ok((no_redirect_store!(AmazonS3Builder, url, options), Path::from_url_path(url.path())?))
}

fn azure_store(url: &Url, options: HashMap<String, String>) -> Result<(Box<dyn ObjectStore>, Path)>
{
    Ok((no_redirect_store!(MicrosoftAzureBuilder, url, options), Path::from_url_path(url.path())?))
}

fn gcs_store(url: &Url, options: HashMap<String, String>) -> Result<(Box<dyn ObjectStore>, Path)>
{
    Ok((no_redirect_store!(GoogleCloudStorageBuilder, url, options), Path::from_url_path(url.path())?))
}

#[no_mangle]
pub extern "C" fn ch_delta_kernel_disable_redirects() -> bool
{
    let handlers: [(&str, fn(&Url, HashMap<String, String>) -> Result<(Box<dyn ObjectStore>, Path)>); 8] = [
        ("s3", s3_store),
        ("s3a", s3_store),
        ("az", azure_store),
        ("adl", azure_store),
        ("azure", azure_store),
        ("abfs", azure_store),
        ("abfss", azure_store),
        ("gs", gcs_store),
    ];
    handlers
        .into_iter()
        .all(|(scheme, handler)| insert_url_handler(scheme, Arc::new(handler)).is_ok())
}
