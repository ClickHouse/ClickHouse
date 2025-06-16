#include "config.h"

#if USE_SSL
#include <Common/Base64.h>
#include <Common/Exception.h>
#include <Common/HTTPConnectionPool.h>
#include <Common/OpenSSLHelpers.h>
#include <Common/SipHash.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/logger_useful.h>
#include <Core/BackgroundSchedulePool.h>
#include <Disks/IO/ReadBufferFromWebServer.h>
#include <IO/HTTPCommon.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Poco/Base64Encoder.h>
#include <Poco/DateTimeFormat.h>
#include <Poco/DateTimeFormatter.h>
#include <Poco/DateTimeParser.h>
#include <Poco/DigestEngine.h>
#include <Poco/File.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/SHA1Engine.h>
#include <Poco/StreamCopier.h>
#include <Poco/String.h>
#include <Poco/Timespan.h>
#include <Poco/URI.h>
#include <Server/CertificateReloader.h>
#include <Server/ACME/Client.h>

#include <fmt/core.h>
#include <fmt/ranges.h>

#include <filesystem>
#include <memory>
#include <mutex>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace ACME
{

namespace fs = std::filesystem;
using DirectoryPtr = std::shared_ptr<Directory>;

Client & Client::instance()
{
    static Client instance;
    return instance;
}

void Client::shutdown()
{
    if (refresh_certificates_task)
        refresh_certificates_task->deactivate();

    if (authentication_task)
        authentication_task->deactivate();

    if (refresh_key_task)
        refresh_key_task->deactivate();
}

std::optional<VersionedCertificate> Client::requestCertificate() const
{
    auto context = Context::getGlobalContextInstance();
    auto zk = context->getZooKeeper();

    std::string pkey;
    std::string certificate;

    /// All domains have the same certificate
    std::string domain = domains.front();

    zk->tryGet(fs::path(ZOOKEEPER_BASE_PATH) / acme_hostname / "domains" / domain / "private_key", pkey);
    zk->tryGet(fs::path(ZOOKEEPER_BASE_PATH) / acme_hostname / "domains" / domain / "certificate", certificate);

    if (pkey.empty() || certificate.empty())
    {
        LOG_DEBUG(log, "No certificate found for domain {}", domain);
        return std::nullopt;
    }

    return VersionedCertificate
    {
        .private_key = KeyPair::fromBuffer(pkey),
        .certificate = X509Certificate::fromBuffer(certificate),
        .version = sipHash128String(pkey + certificate)
    };
}

void Client::initialize(const Poco::Util::AbstractConfiguration & config)
{
    if (initialized)
        return;

    std::lock_guard key_lock(private_acme_key_mutex);
    if (private_acme_key && api && api->isReady())
        return;

    if (!config.has("acme"))
        return;

    auto http_port = config.getInt("http_port");
    if (http_port != 80)
        LOG_WARNING(log, "For ACME HTTP challenge HTTP port must be 80, but is {}", http_port);

    directory_url = config.getString("acme.directory_url", LetsEncrypt::PRODUCTION_DIRECTORY_URL);
    contact_email = config.getString("acme.email", "");
    terms_of_service_agreed = config.getBool("acme.terms_of_service_agreed");

    Poco::Util::AbstractConfiguration::Keys domains_keys;
    config.keys("acme.domains", domains_keys);

    std::vector<std::string> served_domains;
    for (const auto & key : domains_keys)
    {
        served_domains.push_back(config.getString("acme.domains." + key));
        LOG_DEBUG(log, "Serving domain: {}", served_domains.back());
    }
    chassert(!served_domains.empty());

    domains = served_domains;
    refresh_certificates_task_interval = config.getInt("acme.refresh_certificates_task_interval", /* one hour */ 1000 * 60 * 60);
    refresh_certificates_before = config.getInt("acme.refresh_certificates_before", /* one month */ 60 * 60 * 24 * 30);

    acme_hostname = Poco::URI(directory_url).getHost();
    LOG_DEBUG(log, "ACME server hostname: {}", acme_hostname);

    auto zk = Context::getGlobalContextInstance()->getZooKeeper();

    zk->createIfNotExists(fs::path(ZOOKEEPER_BASE_PATH), "");
    zk->createIfNotExists(fs::path(ZOOKEEPER_BASE_PATH) / acme_hostname, "");
    zk->createIfNotExists(fs::path(ZOOKEEPER_BASE_PATH) / acme_hostname / "challenges", "");
    zk->createIfNotExists(fs::path(ZOOKEEPER_BASE_PATH) / acme_hostname / "domains", "");

    for (const auto & domain : domains)
        zk->createIfNotExists(fs::path(ZOOKEEPER_BASE_PATH) / acme_hostname / "domains" / domain, "");

    BackgroundSchedulePool & bgpool = Context::getGlobalContextInstance()->getSchedulePool();

    refresh_certificates_task = bgpool.createTask("ACME::refresh_certificates_fn", [this, &config] {refresh_certificates_fn(config);});
    authentication_task = bgpool.createTask("ACME::authentication_fn", [this] { authentication_fn(); });
    refresh_key_task = bgpool.createTask("ACME::refresh_key_fn", [this] { refresh_key_fn(); });

    if (!private_acme_key)
        refresh_key_task->activateAndSchedule();
}

void Client::refresh_certificates_fn(const Poco::Util::AbstractConfiguration & config)
{
    chassert(keys_initialized);
    chassert(api && api->isReady());

    try
    {
        auto context = Context::getGlobalContextInstance();
        auto zk = context->getZooKeeper();

        bool need_refresh = false;
        for (const auto & domain : domains)
        {
            std::string private_key;
            std::string pem_certificate;

            zk->tryGet(fs::path(ZOOKEEPER_BASE_PATH) / acme_hostname / "domains" / domain / "private_key", private_key);
            zk->tryGet(fs::path(ZOOKEEPER_BASE_PATH) / acme_hostname / "domains" / domain / "certificate", pem_certificate);

            if (private_key.empty() || pem_certificate.empty())
            {
                need_refresh = true;
                break;
            }

            auto x509_certificate_list = X509Certificate::fromBuffer(pem_certificate);
            auto & x509_certificate = x509_certificate_list.front();

            LOG_TRACE(log, "Certificate for domain {} expires on {}", domain, x509_certificate.expiresOn());

            int tzd;
            auto expiration_date = Poco::DateTimeParser::parse("%y%m%d%H%M%S", x509_certificate.expiresOn(), tzd);
            auto best_before = Poco::Timestamp() + Poco::Timespan(refresh_certificates_before * Poco::Timespan::SECONDS);

            if (expiration_date < best_before)
            {
                LOG_INFO(log, "Certificate for domain {} expires soon, initiating refresh", domain);
                need_refresh = true;
            }
        }

        if (!need_refresh)
        {
            LOG_DEBUG(log, "All certificates are up to date");

            CertificateReloader::instance().tryLoad(config);

            refresh_certificates_task->scheduleAfter(refresh_certificates_task_interval);
            return;
        }

        auto active_order_path = fs::path(ZOOKEEPER_BASE_PATH) / acme_hostname / "active_order";
        if ((!lock || !lock->isLocked()) && zk->exists(active_order_path))
        {
            LOG_DEBUG(log, "Certificate order is in progress by another replica, skipping");
            refresh_certificates_task->scheduleAfter(refresh_certificates_task_interval);
            return;
        }

        /// Start the order
        if (!active_order.has_value())
        {
            lock = std::make_shared<zkutil::ZooKeeperLock>(zk, fs::path(ZOOKEEPER_BASE_PATH) / acme_hostname, "active_order", "ACME::Client");
            lock->tryLock();

            auto order_callback = [&](std::string token)
            {
                auto path = fs::path(ZOOKEEPER_BASE_PATH) / acme_hostname / "challenges" / token;
                zk->createIfNotExists(path, token);
            };

            active_order = api->order(domains, order_callback);

            refresh_certificates_task->scheduleAfter(REFRESH_TASK_IN_A_SECOND);
            return;
        }

        lock->tryLock();

        auto order_url = active_order.value();
        auto order_data = api->describeOrder(Poco::URI(order_url));
        LOG_DEBUG(log, "Order {} status: {}", order_url, order_data.status);

        if (order_data.status == "invalid")
        {
            active_order.reset();
            lock.reset();

            LOG_WARNING(log, "Order {} is invalid, retrying", order_url);

            refresh_certificates_task->scheduleAfter(REFRESH_TASK_AFTER_ERROR_MS);
            return;
        }

        if (order_data.status == "ready")
        {
            auto key = KeyPair::generateRSA(4096, RSA_F4);
            auto pkey = key.privateKey();

            for (const auto & domain : domains)
            {
                auto path = fs::path(ZOOKEEPER_BASE_PATH) / acme_hostname / "domains" / domain / "private_key";
                zk->createOrUpdate(path, pkey, zkutil::CreateMode::Persistent);
                LOG_DEBUG(log, "Updated private key for domain {}", domain);
            }

            LOG_DEBUG(log, "Finalizing order {}", order_url);
            api->finalizeOrder(order_data.finalize_url, domains, key);

            refresh_certificates_task->scheduleAfter(REFRESH_TASK_HAPPY_PATH_MS);
            return;
        }

        if (order_data.status != "valid")
        {
            LOG_DEBUG(log, "Order {} is not ready yet (status: {}), retrying", order_url, order_data.status);

            refresh_certificates_task->scheduleAfter(refresh_certificates_task_interval);
            return;
        }

        if (order_data.status == "valid")
        {
            auto certificate = api->pullCertificate(order_data.certificate_url);

            for (const auto & domain : domains)
            {
                auto path = fs::path(ZOOKEEPER_BASE_PATH) / acme_hostname / "domains" / domain / "certificate";
                zk->createOrUpdate(path, certificate, zkutil::CreateMode::Persistent);
                LOG_DEBUG(log, "Updated certificate for domain {}", domain);
            }

            active_order.reset();

            lock->unlock();
            lock.reset();

            LOG_DEBUG(log, "Certificates successfully updated for domains {}", fmt::join(domains, ", "));
        }

        CertificateReloader::instance().tryLoad(config);
        LOG_INFO(log, "Successfully issued a new certificate");
    }
    catch (...)
    {
        refresh_certificates_task->scheduleAfter(REFRESH_TASK_AFTER_ERROR_MS);
        tryLogCurrentException("ACME::Client");
    }

    refresh_certificates_task->scheduleAfter(refresh_certificates_task_interval);
}

void Client::authentication_fn()
{
    LOG_DEBUG(log, "Running ACME::Client authentication task");
    if (api && api->isReady())
        return;

    chassert(keys_initialized);

    {
        std::lock_guard key_lock(private_acme_key_mutex);

        if (!private_acme_key)
        {
            LOG_FATAL(log, "Private key is not initialized, retrying in 1 second. This is a bug.");
            refresh_key_task->scheduleAfter(REFRESH_TASK_IN_A_SECOND);
            return;
        }
    }

    try
    {
        std::lock_guard key_lock(private_acme_key_mutex);

        if (!api)
            api = std::make_shared<ACME::API>(ACME::API::Configuration{
                .directory_url = directory_url,
                .contact_email = contact_email,
                .private_key = private_acme_key,
            });

        chassert(api->isReady());

        refresh_certificates_task->activateAndSchedule();
        return;
    }
    catch (...)
    {
        authentication_task->scheduleAfter(REFRESH_TASK_IN_A_SECOND);
        tryLogCurrentException("ACME::Client");
    }

    authentication_task->scheduleAfter(REFRESH_TASK_IN_A_SECOND);
}

void Client::refresh_key_fn()
{
    LOG_DEBUG(log, "Running ACME::Client key refresh task");

    {
        std::lock_guard key_lock(private_acme_key_mutex);

        if (private_acme_key)
        {
            keys_initialized = true;
            return;
        }
    }

    std::string private_key;

    try
    {
        auto context = Context::getGlobalContextInstance();
        auto zk = context->getZooKeeper();

        zk->tryGet(fs::path(ZOOKEEPER_BASE_PATH) / acme_hostname / "account_private_key", private_key);
        if (private_key.empty())
        {
            LOG_INFO(log, "Generating new RSA private key for ACME account");

            auto rsa_key = KeyPair::generateRSA(4096, RSA_F4);
            private_key = rsa_key.privateKey();

            zk->createIfNotExists(fs::path(ZOOKEEPER_BASE_PATH) / acme_hostname / "account_private_key", private_key);

            refresh_key_task->schedule();
            return;
        }
    }
    catch (...)
    {
        refresh_key_task->scheduleAfter(REFRESH_TASK_IN_A_SECOND);
        tryLogCurrentException("ACME::Client");
        return;
    }

    chassert(!private_key.empty());

    {
        std::lock_guard key_lock(private_acme_key_mutex);

        chassert(!private_acme_key);
        private_acme_key = std::make_shared<KeyPair>(KeyPair::fromBuffer(private_key));
    }

    LOG_TRACE(log, "ACME private key successfully loaded");

    keys_initialized = true;
    authentication_task->activateAndSchedule();
}


std::string Client::requestChallenge(const std::string & uri)
{
    LOG_TRACE(log, "Challenge requested for uri: {}", uri);

    if (!keys_initialized)
    {
        LOG_WARNING(log, "ACME keys are not initialized, skipping challenge request");
        return "";
    }

    if (!uri.starts_with(CHALLENGE_HTTP_PATH))
        return "";

    std::vector<std::string> uri_segments;
    Poco::URI parsed_url(uri);
    parsed_url.getPathSegments(uri_segments);

    /// We've already validated prefix, segments[2] should be our token.
    if (uri_segments.size() < 3)
        return "";
    std::string token_from_uri = uri_segments[2];

    auto context = Context::getGlobalContextInstance();
    auto zk = context->getZooKeeper();

    auto active_challenges = zk->getChildren(fs::path(ZOOKEEPER_BASE_PATH) / acme_hostname / "challenges");
    if (active_challenges.empty())
    {
        LOG_DEBUG(log, "No challenge found for uri: {}", uri);
        return "";
    }

    if (std::find(active_challenges.begin(), active_challenges.end(), token_from_uri) == active_challenges.end())
    {
        LOG_DEBUG(log, "Challenge for uri {} is not found", uri);
        return "";
    }

    /// Generate a JWK thumbprint
    std::string jwk;
    {
        std::lock_guard key_lock(private_acme_key_mutex);
        if (!private_acme_key)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Private ACME key is not initialized");
        jwk = JSONWebKey::fromRSAKey(*private_acme_key).toString();
    }
    auto jwk_thumbprint = encodeSHA256(jwk);
    jwk_thumbprint = base64Encode(jwk_thumbprint, /*url_encoding*/ true, /*no_padding*/ true);

    /// Certificate might be ready any second now, start checking for it
    refresh_certificates_task->scheduleAfter(REFRESH_TASK_HAPPY_PATH_MS);
    return token_from_uri + "." + jwk_thumbprint;
}

}
}
#endif
