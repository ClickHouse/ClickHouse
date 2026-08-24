#include "config.h"

#ifdef USE_LICENSE_PUBLIC_KEY

#    include <string>
#    include <utility>
#    include <vector>
#    include <Core/BackgroundSchedulePool.h>
#    include <Core/ServerSettings.h>
#    include <Interpreters/Context.h>
#    include <base/scope_guard.h>
#    include <openssl/bio.h>
#    include <openssl/err.h>
#    include <openssl/evp.h>
#    include <openssl/pem.h>
#    include <Poco/DigestEngine.h>
#    include <Poco/SHA1Engine.h>
#    include <Poco/Base64Decoder.h>
#    include <Poco/MemoryStream.h>
#    include <Poco/Net/DNS.h>
#    include <Poco/Net/HostEntry.h>
#    include <Poco/StreamCopier.h>
#    include <Common/Crypto/KeyPair.h>
#    include <Common/LicenseChecker.h>
#    include <Common/logger_useful.h>

namespace CurrentMetrics
{
extern const Metric LicenseRemainingSeconds;
}

namespace DB
{

namespace ServerSetting
{
extern const ServerSettingsString license_key;
}

static auto logger = getLogger("LicenseChecker");

static KeyPair loadPublicKey()
{
    try
    {
        String formatted_pem = "-----BEGIN PUBLIC KEY-----\n" + String(USE_LICENSE_PUBLIC_KEY) + "\n-----END PUBLIC KEY-----\n";

        LOG_INFO(logger, "Loading public key: {}", formatted_pem);

        return KeyPair::fromFile(formatted_pem);
    }
    catch (const Poco::Exception & e)
    {
        throw std::runtime_error("Failed to load public key: " + String(e.what()));
    }
}

static String base64Decode(const String & encoded)
{
    String decoded;
    Poco::MemoryInputStream istr(encoded.data(), encoded.size());
    Poco::Base64Decoder decoder(istr);
    Poco::StreamCopier::copyToString(decoder, decoded);
    return decoded;
}

static uint64_t getCurrentTimestampInSeconds()
{
    return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

static bool isLicenseExpired(const String & license)
{
    size_t first_dot = license.find('.');
    size_t last_dot = license.rfind('.');

    if (first_dot == String::npos || last_dot == String::npos || first_dot == last_dot)
    {
        throw std::runtime_error("Invalid license format");
    }

    String expire_str = license.substr(first_dot + 1, last_dot - first_dot - 1);

    uint64_t expire_time;
    try
    {
        expire_time = static_cast<uint64_t>(std::stoull(expire_str));
    }
    catch (const std::exception &)
    {
        throw std::runtime_error("Invalid expire time format");
    }

    auto current_time = getCurrentTimestampInSeconds();
    LOG_INFO(logger, "License expire time: {}, currentTime: {} unit: Second", expire_time, current_time);
    CurrentMetrics::set(CurrentMetrics::LicenseRemainingSeconds, expire_time - current_time);
    return current_time > expire_time;
}

static String makeHashDomain(const String & license)
{
    // Create SHA1 hash of license
    Poco::SHA1Engine engine1;
    engine1.update(license);
    auto hash = Poco::DigestEngine::digestToHex(engine1.digest());

    // Construct domain name
    return hash + ".license.clickhouse.com";
}

// send dns query to clickhouse.com host to let us know who are using clickhouse-private
static bool sendDnsQuery(const String & license)
{
    LOG_DEBUG(logger, "DNS query sent for license: {}", license);
    try
    {
        String domain = makeHashDomain(license);
        Poco::Net::DNS::hostByName(domain);
        return true;
    }
    catch (const Poco::Exception &)
    {
        return false;
    }
}

// Verify license string
// Format: {org_id}.{expire_timestamp}.{base64_signature}
static bool verifyLicense(const String & license, const KeyPair & pubKey)
{
    LOG_INFO(logger, "Verifying license: {}, public key {}", license, String(USE_LICENSE_PUBLIC_KEY));

    sendDnsQuery(license);
    try
    {
        // Split the license string
        size_t pos1 = license.find('.');
        size_t pos2 = license.find('.', pos1 + 1);
        if (pos1 == String::npos || pos2 == String::npos)
        {
            LOG_ERROR(logger, "Failed to parse license: {}", license);
            return false;
        }

        String message = license.substr(0, pos2);
        String signature_b64 = license.substr(pos2 + 1);

        LOG_DEBUG(logger, "Parsed message: {}", message);
        LOG_DEBUG(logger, "Base64 signature length: {}", signature_b64.length());

        String decoded_str = base64Decode(signature_b64);
        std::vector<unsigned char> signature(decoded_str.begin(), decoded_str.end());
        LOG_DEBUG(logger, "Decoded signature length: {}", signature.size());

        EVP_MD_CTX * ctx = EVP_MD_CTX_new();
        if (!ctx)
        {
            LOG_ERROR(logger, "Failed to create EVP_MD_CTX");
            return false;
        }
        SCOPE_EXIT(EVP_MD_CTX_free(ctx));

        EVP_PKEY * pkey = static_cast<EVP_PKEY*>(pubKey);
        bool verified = EVP_VerifyInit_ex(ctx, EVP_sha256(), nullptr) && EVP_VerifyUpdate(ctx, message.data(), message.size())
            && EVP_VerifyFinal(ctx, reinterpret_cast<const unsigned char *>(signature.data()), uint(signature.size()), pkey);

        if (!verified)
        {
            LOG_ERROR(logger, "Failed to verify license: {}", license);
            return false;
        }

        if (isLicenseExpired(license))
        {
            LOG_ERROR(logger, "License {} is expired", license);
            return false;
        }

        LOG_INFO(logger, "License {} verified successfully", license);
        return true;
    }
    catch (const std::exception & e)
    {
        LOG_ERROR(logger, "Exception during license verification: {}, license: {}", e.what(), license);
        return false;
    }
}

LicenseChecker::LicenseChecker()
{
    licenseValid.store(true);
    auto check_task_holder
        = Context::getGlobalContextInstance()->getSchedulePool().createTask("LicenseChecker", [this] { checkLicenseRoutine(); });
    check_task = std::make_unique<BackgroundSchedulePoolTaskHolder>(std::move(check_task_holder));
    (*check_task)->activateAndSchedule();
}

void LicenseChecker::checkAndSetLicenseValidity(String license)
{
    auto key = loadPublicKey();

    if (!verifyLicense(license, key))
    {
        LOG_INFO(logger, "License {} is invalid", license);
        licenseValid.store(false);
        return;
    }
    LOG_INFO(logger, "License {} is valid", license);
    licenseValid.store(true);
}

void LicenseChecker::checkLicenseRoutine()
{
    String license_key = Context::getGlobalContextInstance()->getServerSettings()[ServerSetting::license_key];
    checkAndSetLicenseValidity(license_key);
    (*check_task)->scheduleAfter(6 * 60 * 60 * 1000); // 6 hours
}

LicenseChecker & LicenseChecker::getInstance()
{
    static LicenseChecker instance;
    return instance;
}

}

#endif
