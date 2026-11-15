#include "config.h"

#if USE_SSL

#include <gtest/gtest.h>

#include <Common/Crypto/X509Certificate.h>
#include <Common/Exception.h>
#include <Common/tests/gtest_ephemeral_certificate.h>

#include <openssl/err.h>

#include <fstream>
#include <sstream>


using namespace DB;

namespace
{

size_t countQueuedErrors()
{
    size_t count = 0;
    while (ERR_get_error() != 0)
        ++count;
    return count;
}

std::string readFile(const std::string & path)
{
    std::ifstream in(path);
    std::stringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

}

/// Reading to the end of a certificate file ends on an expected `no start line` error,
/// which must not stay queued for the next, unrelated, error message.
TEST(X509Certificate, SuccessfulReadLeavesNoErrorsQueued)
{
    EphemeralCert cert;
    const auto pem = readFile(cert.cert_path);

    ERR_clear_error();
    ASSERT_EQ(X509Certificate::fromBuffer(pem).size(), 1u);
    EXPECT_EQ(countQueuedErrors(), 0u);

    ASSERT_EQ(X509Certificate::fromBuffer(pem + pem).size(), 2u);
    EXPECT_EQ(countQueuedErrors(), 0u);
}

/// Errors queued before the read are not ours to discard.
TEST(X509Certificate, SuccessfulReadKeepsUnrelatedQueuedErrors)
{
    EphemeralCert cert;
    const auto pem = readFile(cert.cert_path);

    ERR_clear_error();
    ERR_raise(ERR_LIB_USER, 1);
    ASSERT_EQ(X509Certificate::fromBuffer(pem).size(), 1u);
    EXPECT_EQ(countQueuedErrors(), 1u);
}

TEST(X509Certificate, FailedReadReportsOwnErrors)
{
    ERR_clear_error();
    try
    {
        X509Certificate::fromBuffer("-----BEGIN CERTIFICATE-----\nnot base64!\n-----END CERTIFICATE-----\n");
        FAIL() << "Expected an exception";
    }
    catch (const Exception & e)
    {
        EXPECT_NE(std::string(e.what()).find("PEM_read_bio_X509 failed"), std::string::npos);
    }
    EXPECT_EQ(countQueuedErrors(), 0u);
}

#endif
