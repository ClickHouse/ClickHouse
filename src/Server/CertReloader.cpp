#include "CertReloader.h"

namespace DB
{
#if USE_SSL
int cert_reloader_dispatch_set_cert(SSL * ssl, [[maybe_unused]] void * arg)
{
    return CertReloader::instance().SetCert(ssl);
}

int CertReloader::SetCert(SSL * ssl)
{
    std::shared_lock lock(lck);
    SSL_use_certificate(ssl, const_cast<X509 *>(cert->certificate()));
    SSL_use_RSAPrivateKey(ssl, key->impl()->getRSA());

    int err = SSL_check_private_key(ssl);
    if (err != 1)
    {
        std::string msg = Poco::Net::Utility::getLastError();
        LOG_ERROR(log, "Unusable keypair {}", msg);
        return -1;
    }
    return 1;
}

void CertReloader::init(const Poco::Util::AbstractConfiguration & config)
{
    LOG_DEBUG(log, "Initializing config reloader.");

    auto & ssl_manager = Poco::Net::SSLManager::instance();
    const auto ssl_ctx_ptr = ssl_manager.defaultServerContext();
    auto ctx = ssl_ctx_ptr->sslContext();
    SSL_CTX_set_cert_cb(ctx, cert_reloader_dispatch_set_cert, nullptr);

    reload(config);
}

void CertReloader::reload(const Poco::Util::AbstractConfiguration & config)
{
    LOG_DEBUG(log, "Handling cert reload.");
    const auto cert_file_ = config.getString("openSSL.server.certificateFile", "");
    const auto key_file_ = config.getString("openSSL.server.privateKeyFile", "");

    if (setKeyFile(key_file_) || setCertFile(cert_file_))
    {
        LOG_INFO(log, "Reloading cert({}), key({})", cert_file, key_file);
        {
            std::unique_lock lock(lck);
            key.reset(new Poco::Crypto::RSAKey("", key_file));
            cert.reset(new Poco::Crypto::X509Certificate(cert_file));
        }
        LOG_INFO(log, "Reloaded cert {}", cert_file);
    }
}

bool CertReloader::setKeyFile(const std::string key_file_)
{
    if (key_file_.empty())
        return false;

    stat_t st;
    int res = stat(key_file_.c_str(), &st);
    if (res == -1)
        return false;

    if (st.st_mtime != key_file_st.st_mtime || key_file != key_file_)
    {
        key_file = key_file_;
        key_file_st = st;
        return true;
    }

    return false;
}

bool CertReloader::setCertFile(const std::string cert_file_)
{
    if (cert_file_.empty())
        return false;

    stat_t st;
    int res = stat(cert_file_.c_str(), &st);
    if (res == -1)
        return false;

    if (st.st_mtime != cert_file_st.st_mtime || cert_file != cert_file_)
    {
        cert_file = cert_file_;
        cert_file_st = st;
        return true;
    }

    return false;
}

#endif

}
