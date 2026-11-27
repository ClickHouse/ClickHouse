/**
 * Poco-based HTTP(S) transport for rdhttp_poco.c, the ClickHouse replacement
 * for the libcurl-based rdhttp.c of librdkafka.
 */

#include "rdhttp_poco_transport.h"

#include <Poco/Net/Context.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPSClientSession.h>
#include <Poco/Timespan.h>
#include <Poco/URI.h>

#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <istream>
#include <memory>
#include <string>

namespace
{

constexpr int default_timeout_s = 30;

void addCertificatesFromPEM(Poco::Net::Context & context, const char * ca_pem)
{
    BIO * bio = BIO_new_mem_buf(ca_pem, -1);
    if (!bio)
        throw Poco::Exception("Failed to allocate memory BIO for CA certificates");

    X509_STORE * store = SSL_CTX_get_cert_store(context.sslContext());
    size_t added = 0;
    while (X509 * cert = PEM_read_bio_X509(bio, nullptr, nullptr, nullptr))
    {
        int ok = X509_STORE_add_cert(store, cert);
        X509_free(cert);
        if (ok != 1)
            break;
        ++added;
    }
    BIO_free(bio);

    if (added == 0)
        throw Poco::Exception("No CA certificates could be loaded from `https.ca.pem`");
}

std::unique_ptr<Poco::Net::HTTPClientSession> makeSession(
    const Poco::URI & uri, const char * ca_location, const char * ca_pem)
{
    if (uri.getScheme() == "http")
        return std::make_unique<Poco::Net::HTTPClientSession>(uri.getHost(), uri.getPort());

    /// "probe" asks the libcurl-based implementation to probe standard CA
    /// locations, which is what the default verification paths provide here.
    std::string ca(ca_location ? ca_location : "");
    if (ca == "probe")
        ca.clear();

    /// When an explicit CA file (`https.ca.location`) or an inline PEM bundle
    /// (`https.ca.pem`) is configured, trust only those roots and do not also
    /// load the system trust store. This matches the libcurl-based transport,
    /// where these options replace the default trust roots rather than
    /// extending them, preserving custom-CA/pinning semantics.
    const bool have_ca_pem = ca_pem && *ca_pem;

    Poco::Net::Context::Ptr context(new Poco::Net::Context(
        Poco::Net::Context::TLSV1_2_CLIENT_USE, /*privateKeyFile=*/"",
        /*certificateFile=*/"", ca, Poco::Net::Context::VERIFY_STRICT,
        /*verificationDepth=*/9, /*loadDefaultCAs=*/ca.empty() && !have_ca_pem));

    if (ca.empty() && have_ca_pem)
        addCertificatesFromPEM(*context, ca_pem);

    return std::make_unique<Poco::Net::HTTPSClientSession>(uri.getHost(), uri.getPort(), context);
}

}

extern "C" int rd_http_poco_perform(const char *url,
                                    const char **headers,
                                    size_t headers_cnt,
                                    const char *post_fields,
                                    size_t post_fields_size,
                                    int timeout_s,
                                    const char *ca_location,
                                    const char *ca_pem,
                                    rd_http_poco_write_cb_t write_cb,
                                    void *write_opaque,
                                    long *response_code,
                                    char **content_type,
                                    char *errstr,
                                    size_t errstr_size)
{
    try
    {
        Poco::URI uri(url);
        if (uri.getScheme() != "http" && uri.getScheme() != "https")
        {
            snprintf(errstr, errstr_size, "Unsupported URL scheme: %s", url);
            return -1;
        }

        auto session = makeSession(uri, ca_location, ca_pem);
        auto timeout = Poco::Timespan(timeout_s > 0 ? timeout_s : default_timeout_s, 0);
        session->setTimeout(timeout, timeout, timeout);

        auto path = uri.getPathAndQuery();
        if (path.empty())
            path = "/";
        Poco::Net::HTTPRequest request(
            post_fields ? Poco::Net::HTTPRequest::HTTP_POST : Poco::Net::HTTPRequest::HTTP_GET,
            path, Poco::Net::HTTPMessage::HTTP_1_1);

        for (size_t i = 0; i < headers_cnt; ++i)
        {
            const char * sep = strchr(headers[i], ':');
            if (!sep)
                continue;
            std::string name(headers[i], sep - headers[i]);
            const char * value = sep + 1;
            while (*value == ' ')
                ++value;
            /// As in libcurl, a header without a value is not sent at all.
            if (*value)
                request.add(name, value);
        }

        if (post_fields)
            request.setContentLength(static_cast<std::streamsize>(post_fields_size));

        auto & body_stream = session->sendRequest(request);
        if (post_fields && post_fields_size > 0)
            body_stream.write(post_fields, static_cast<std::streamsize>(post_fields_size));

        Poco::Net::HTTPResponse response;
        auto & response_stream = session->receiveResponse(response);

        *response_code = static_cast<long>(response.getStatus());
        if (content_type)
        {
            const std::string & ct = response.get(Poco::Net::HTTPMessage::CONTENT_TYPE, "");
            *content_type = ct.empty() ? nullptr : strdup(ct.c_str());
        }

        char buf[16384];
        while (response_stream.good())
        {
            response_stream.read(buf, sizeof(buf));
            auto count = static_cast<size_t>(response_stream.gcount());
            if (count == 0)
                break;
            if (write_cb(buf, 1, count, write_opaque) != count)
            {
                snprintf(errstr, errstr_size, "Response is too large");
                return -1;
            }
        }
        if (response_stream.bad())
        {
            snprintf(errstr, errstr_size, "Error reading response body from %s", url);
            return -1;
        }

        return 0;
    }
    catch (const Poco::Exception & e)
    {
        snprintf(errstr, errstr_size, "%s", e.displayText().c_str());
        return -1;
    }
    catch (const std::exception & e)
    {
        snprintf(errstr, errstr_size, "%s", e.what());
        return -1;
    }
}
