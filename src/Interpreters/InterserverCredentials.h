#pragma once

#include <Common/SecretString.h>
#include <Common/Crypto/X509Certificate.h>
#include <Poco/Util/AbstractConfiguration.h>
#include "config.h"

#if USE_SSL
#    include <Poco/Net/Context.h>
#endif

#include <unordered_set>
#include <utility>
#include <vector>

namespace DB
{

/// `InterserverCredentials` stores Basic credentials and interserver certificate settings.
/// For example:
///    <interserver_http_credentials>
///        <user>admin</user>
///        <password>222</password>
///        <!-- To support mix of un/authenticated clients -->
///        <!-- <allow_empty>true</allow_empty> -->
///        <old>
///            <!-- Allow authentication using previous passwords during rotation -->
///            <user>admin</user>
///            <password>qqq</password>
///        </old>
///        <openSSL>
///            <client>
///                <certificateFile>/path/to/client-cert.pem</certificateFile>
///                <privateKeyFile>/path/to/client-key.pem</privateKeyFile>
///            </client>
///        </openSSL>
///        <ssl_certificates>
///            <subject_alt_name>URI:spiffe://example.org/clickhouse/replica</subject_alt_name>
///        </ssl_certificates>
///        <old>
///            <!-- Allow authentication using previous users during rotation -->
///            <user>johny</user>
///            <password>333</password>
///        </old>
///    </interserver_http_credentials>
class InterserverCredentials
{
public:
    using UserWithPassword = std::pair<std::string, SecretString>;
    using CheckResult = std::pair<std::string, bool>;
    using CurrentCredentials = std::vector<UserWithPassword>;

    InterserverCredentials(const InterserverCredentials &) = delete;

    static std::unique_ptr<InterserverCredentials> make(const Poco::Util::AbstractConfiguration & config, const std::string & root_tag);

    InterserverCredentials(
        std::string current_user_,
        SecretString current_password_,
        CurrentCredentials all_users_store_
#if USE_SSL
        , std::string client_certificate_file_,
        std::string client_private_key_file_,
        X509Certificate::Subjects allowed_certificate_subjects_
#endif
    )
        : current_user(std::move(current_user_))
        , current_password(std::move(current_password_))
        , all_users_store(std::move(all_users_store_))
#if USE_SSL
        , client_certificate_file(std::move(client_certificate_file_))
        , client_private_key_file(std::move(client_private_key_file_))
        , allowed_certificate_subjects(std::move(allowed_certificate_subjects_))
#endif
    {}

    [[nodiscard]] CheckResult isValidUser(const UserWithPassword & credentials) const;
    [[nodiscard]] CheckResult isValidUser(std::string_view user, std::string_view password) const;

#if USE_SSL
    [[nodiscard]] bool usesClientCertificateAuthentication() const { return !client_certificate_file.empty(); }
    void initializeClientTLSContext(const Poco::Util::AbstractConfiguration & config, const std::string & root_tag);
    [[nodiscard]] Poco::Net::Context::Ptr getClientTLSContext() const { return client_tls_context; }
    [[nodiscard]] const X509Certificate::Subjects & getAllowedCertificateSubjects() const { return allowed_certificate_subjects; }
#endif

    [[nodiscard]] std::string getUser() const { return current_user; }

    [[nodiscard]] const SecretString & getPassword() const { return current_password; }


private:
    std::string current_user;
    SecretString current_password;

    /// In common situation this store contains one record
    CurrentCredentials all_users_store;

#if USE_SSL
    std::string client_certificate_file;
    std::string client_private_key_file;
    Poco::Net::Context::Ptr client_tls_context;
    X509Certificate::Subjects allowed_certificate_subjects;
#endif

    static CurrentCredentials parseCredentialsFromConfig(
        const std::string & current_user_,
        const SecretString & current_password_,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & root_tag,
        bool has_allowed_subjects);
};

using InterserverCredentialsPtr = std::shared_ptr<const InterserverCredentials>;

}
