#include "config.h"

#if USE_AVRO && USE_SSL && USE_AWS_S3

#include <gtest/gtest.h>

#include <Databases/DataLake/S3TablesCredentialRefresh.h>
#include <Databases/DataLake/StorageCredentials.h>

#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProvider.h>

#include <mutex>

namespace
{

class RotatingAWSCredentialsProvider : public Aws::Auth::AWSCredentialsProvider
{
public:
    explicit RotatingAWSCredentialsProvider(std::vector<Aws::Auth::AWSCredentials> credentials_sets_)
        : credentials_sets(std::move(credentials_sets_))
    {
    }

    Aws::Auth::AWSCredentials GetAWSCredentials() override
    {
        std::lock_guard lock(mutex);
        const size_t index = call_count++;
        if (index >= credentials_sets.size())
            return credentials_sets.back();
        return credentials_sets[index];
    }

private:
    std::vector<Aws::Auth::AWSCredentials> credentials_sets;
    std::mutex mutex;
    size_t call_count = 0;
};

}

TEST(S3TablesCredentialRefresh, FallsBackToCatalogIAMWhenVendedCredentialsMissing)
{
    RotatingAWSCredentialsProvider provider({
        Aws::Auth::AWSCredentials("access_key_1", "secret_key_1", "session_token_1"),
        Aws::Auth::AWSCredentials("access_key_2", "secret_key_2", "session_token_2"),
    });

    DataLake::ICatalog::CredentialsRefreshCallback base_callback = []() -> std::shared_ptr<DataLake::IStorageCredentials>
    {
        return nullptr;
    };

    auto first = DataLake::resolveS3TablesRefreshCredentials(base_callback, provider);
    ASSERT_NE(first, nullptr);
    auto first_s3 = std::dynamic_pointer_cast<DataLake::S3Credentials>(first);
    ASSERT_NE(first_s3, nullptr);
    EXPECT_EQ(first_s3->getAccessKeyId(), "access_key_1");
    EXPECT_EQ(first_s3->getSecretAccessKey(), "secret_key_1");
    EXPECT_EQ(first_s3->getSessionToken(), "session_token_1");

    auto second = DataLake::resolveS3TablesRefreshCredentials(base_callback, provider);
    ASSERT_NE(second, nullptr);
    auto second_s3 = std::dynamic_pointer_cast<DataLake::S3Credentials>(second);
    ASSERT_NE(second_s3, nullptr);
    EXPECT_EQ(second_s3->getAccessKeyId(), "access_key_2");
    EXPECT_EQ(second_s3->getSecretAccessKey(), "secret_key_2");
    EXPECT_EQ(second_s3->getSessionToken(), "session_token_2");
}

TEST(S3TablesCredentialRefresh, PrefersVendedCredentialsWhenPresent)
{
    RotatingAWSCredentialsProvider provider({
        Aws::Auth::AWSCredentials("catalog_access", "catalog_secret", "catalog_token"),
    });

    DataLake::ICatalog::CredentialsRefreshCallback base_callback = []() -> std::shared_ptr<DataLake::IStorageCredentials>
    {
        return std::make_shared<DataLake::S3Credentials>("vended_access", "vended_secret", "vended_token");
    };

    auto creds = DataLake::resolveS3TablesRefreshCredentials(base_callback, provider);
    ASSERT_NE(creds, nullptr);
    auto s3_creds = std::dynamic_pointer_cast<DataLake::S3Credentials>(creds);
    ASSERT_NE(s3_creds, nullptr);
    EXPECT_EQ(s3_creds->getAccessKeyId(), "vended_access");
    EXPECT_EQ(s3_creds->getSecretAccessKey(), "vended_secret");
    EXPECT_EQ(s3_creds->getSessionToken(), "vended_token");
}

TEST(S3TablesCredentialRefresh, FallsBackWhenVendedCredentialsEmpty)
{
    RotatingAWSCredentialsProvider provider({
        Aws::Auth::AWSCredentials("catalog_access", "catalog_secret", "catalog_token"),
    });

    DataLake::ICatalog::CredentialsRefreshCallback base_callback = []() -> std::shared_ptr<DataLake::IStorageCredentials>
    {
        return std::make_shared<DataLake::S3Credentials>("", "", "");
    };

    auto creds = DataLake::resolveS3TablesRefreshCredentials(base_callback, provider);
    ASSERT_NE(creds, nullptr);
    auto s3_creds = std::dynamic_pointer_cast<DataLake::S3Credentials>(creds);
    ASSERT_NE(s3_creds, nullptr);
    EXPECT_EQ(s3_creds->getAccessKeyId(), "catalog_access");
    EXPECT_EQ(s3_creds->getSecretAccessKey(), "catalog_secret");
    EXPECT_EQ(s3_creds->getSessionToken(), "catalog_token");
}

#endif
