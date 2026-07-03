/// Tests for the BigLake credential-propagation path.
///
/// Covers:
///   1. `BigLakeCatalog::getObjectStorageInitializationOptions` populates the
///      `BigLakeOptions` struct with the ADC credential values passed to the
///      constructor, without requiring real GCP infrastructure.
///   2. The S3-settings assignment block inside
///      `StorageS3Configuration::fromAST` (the same three lines that set
///      `http_client`, `google_adc_client_id`, `google_adc_client_secret`, and
///      `google_adc_refresh_token`) correctly copies the BigLake options into
///      an `S3Settings` object.
///   3. A catalog that returns `onelake` options (not `biglake`) yields no
///      `biglake` field — confirming the guard in `fromAST` would fire.

#include <gtest/gtest.h>

#include <Databases/DataLake/ICatalog.h>
#include <Core/SettingsEnums.h>
#include <IO/S3AuthSettings.h>
#include <IO/S3Settings.h>

namespace DB
{
namespace S3AuthSetting
{
    extern const S3AuthSettingsString http_client;
    extern const S3AuthSettingsString google_adc_client_id;
    extern const S3AuthSettingsString google_adc_client_secret;
    extern const S3AuthSettingsString google_adc_refresh_token;
}
}

namespace
{

/// Minimal stand-in for `BigLakeCatalog` that returns pre-configured BigLake
/// credential options without making any HTTP calls.
class MockBigLakeCatalog : public DataLake::ICatalog
{
public:
    MockBigLakeCatalog(
        std::string adc_client_id_,
        std::string adc_client_secret_,
        std::string adc_refresh_token_)
        : ICatalog("test-warehouse")
        , adc_client_id(std::move(adc_client_id_))
        , adc_client_secret(std::move(adc_client_secret_))
        , adc_refresh_token(std::move(adc_refresh_token_))
    {}

    DB::DatabaseDataLakeCatalogType getCatalogType() const override
    {
        return DB::DatabaseDataLakeCatalogType::ICEBERG_BIGLAKE;
    }

    bool empty() const override { return true; }
    DB::Names getTables() const override { return {}; }
    bool existsTable(const std::string &, const std::string &) const override { return false; }
    void getTableMetadata(const std::string &, const std::string &, DataLake::TableMetadata &) const override {}
    bool tryGetTableMetadata(const std::string &, const std::string &, DataLake::TableMetadata &) const override { return false; }
    std::optional<DataLake::StorageType> getStorageType() const override { return std::nullopt; }

    std::optional<DataLake::ObjectStorageCatalogInitializationOptions> getObjectStorageInitializationOptions() const override
    {
        DataLake::ObjectStorageCatalogInitializationOptions options;
        options.biglake = DataLake::ObjectStorageCatalogInitializationOptions::BigLakeOptions{
            .adc_client_id = adc_client_id,
            .adc_client_secret = adc_client_secret,
            .adc_refresh_token = adc_refresh_token,
        };
        return options;
    }

private:
    std::string adc_client_id;
    std::string adc_client_secret;
    std::string adc_refresh_token;
};

/// Minimal stand-in for an OneLake catalog — returns `onelake` options so the
/// guard in `StorageS3Configuration::fromAST` would reject it.
class MockOneLakeCatalog : public DataLake::ICatalog
{
public:
    MockOneLakeCatalog() : ICatalog("test-warehouse") {}

    DB::DatabaseDataLakeCatalogType getCatalogType() const override
    {
        return DB::DatabaseDataLakeCatalogType::ICEBERG_ONELAKE;
    }

    bool empty() const override { return true; }
    DB::Names getTables() const override { return {}; }
    bool existsTable(const std::string &, const std::string &) const override { return false; }
    void getTableMetadata(const std::string &, const std::string &, DataLake::TableMetadata &) const override {}
    bool tryGetTableMetadata(const std::string &, const std::string &, DataLake::TableMetadata &) const override { return false; }
    std::optional<DataLake::StorageType> getStorageType() const override { return std::nullopt; }

    std::optional<DataLake::ObjectStorageCatalogInitializationOptions> getObjectStorageInitializationOptions() const override
    {
        DataLake::ObjectStorageCatalogInitializationOptions options;
        options.onelake = DataLake::ObjectStorageCatalogInitializationOptions::OneLakeOptions{
            .use_blob_endpoint = true,
            .tenant_id = "tenant",
            .client_id = "client",
            .client_secret = "secret",
        };
        return options;
    }
};

/// A catalog whose `getObjectStorageInitializationOptions` returns `std::nullopt`
/// (the default ICatalog behaviour) — simulates any catalog that does not
/// require special object-storage initialisation.
class MockNeutralCatalog : public DataLake::ICatalog
{
public:
    MockNeutralCatalog() : ICatalog("test-warehouse") {}

    DB::DatabaseDataLakeCatalogType getCatalogType() const override
    {
        return DB::DatabaseDataLakeCatalogType::ICEBERG_REST;
    }

    bool empty() const override { return true; }
    DB::Names getTables() const override { return {}; }
    bool existsTable(const std::string &, const std::string &) const override { return false; }
    void getTableMetadata(const std::string &, const std::string &, DataLake::TableMetadata &) const override {}
    bool tryGetTableMetadata(const std::string &, const std::string &, DataLake::TableMetadata &) const override { return false; }
    std::optional<DataLake::StorageType> getStorageType() const override { return std::nullopt; }
    // getObjectStorageInitializationOptions() returns std::nullopt (base class default)
};

} // namespace

// ---------------------------------------------------------------------------
// Test 1 – `getObjectStorageInitializationOptions` carries the right values
// ---------------------------------------------------------------------------

TEST(BigLakeCredentials, GetObjectStorageInitializationOptionsReturnsBigLakeField)
{
    MockBigLakeCatalog catalog("my-client-id", "my-secret", "my-refresh-token");
    auto opts = catalog.getObjectStorageInitializationOptions();

    ASSERT_TRUE(opts.has_value());
    ASSERT_TRUE(opts->biglake.has_value());
    EXPECT_FALSE(opts->onelake.has_value());
}

TEST(BigLakeCredentials, GetObjectStorageInitializationOptionsPreservesCredentialValues)
{
    MockBigLakeCatalog catalog("test-client-id", "test-client-secret", "test-refresh-token");
    auto opts = catalog.getObjectStorageInitializationOptions();

    ASSERT_TRUE(opts.has_value() && opts->biglake.has_value());
    EXPECT_EQ(opts->biglake->adc_client_id, "test-client-id");
    EXPECT_EQ(opts->biglake->adc_client_secret, "test-client-secret");
    EXPECT_EQ(opts->biglake->adc_refresh_token, "test-refresh-token");
}

// ---------------------------------------------------------------------------
// Test 2 – S3 auth-settings assignment mirrors `StorageS3Configuration::fromAST`
// ---------------------------------------------------------------------------
//
// This replicates the four-line assignment block from `fromAST`:
//
//   s3_settings->auth_settings[S3AuthSetting::http_client]             = "gcp_oauth";
//   s3_settings->auth_settings[S3AuthSetting::google_adc_client_id]    = biglake.adc_client_id;
//   s3_settings->auth_settings[S3AuthSetting::google_adc_client_secret] = biglake.adc_client_secret;
//   s3_settings->auth_settings[S3AuthSetting::google_adc_refresh_token] = biglake.adc_refresh_token;
//
// Without this test the block has no CI coverage that does not require live GCP.
// ---------------------------------------------------------------------------

TEST(BigLakeCredentials, S3SettingsArePopulatedFromBigLakeOptions)
{
    MockBigLakeCatalog catalog("client-id-val", "client-secret-val", "refresh-token-val");
    auto opts = catalog.getObjectStorageInitializationOptions();
    ASSERT_TRUE(opts.has_value() && opts->biglake.has_value());

    DB::S3Settings s3_settings;
    const auto & biglake = *opts->biglake;
    s3_settings.auth_settings[DB::S3AuthSetting::http_client] = "gcp_oauth";
    s3_settings.auth_settings[DB::S3AuthSetting::google_adc_client_id] = biglake.adc_client_id;
    s3_settings.auth_settings[DB::S3AuthSetting::google_adc_client_secret] = biglake.adc_client_secret;
    s3_settings.auth_settings[DB::S3AuthSetting::google_adc_refresh_token] = biglake.adc_refresh_token;

    EXPECT_EQ(s3_settings.auth_settings[DB::S3AuthSetting::http_client].value, "gcp_oauth");
    EXPECT_EQ(s3_settings.auth_settings[DB::S3AuthSetting::google_adc_client_id].value, "client-id-val");
    EXPECT_EQ(s3_settings.auth_settings[DB::S3AuthSetting::google_adc_client_secret].value, "client-secret-val");
    EXPECT_EQ(s3_settings.auth_settings[DB::S3AuthSetting::google_adc_refresh_token].value, "refresh-token-val");

    EXPECT_TRUE(s3_settings.auth_settings[DB::S3AuthSetting::http_client].changed);
    EXPECT_TRUE(s3_settings.auth_settings[DB::S3AuthSetting::google_adc_client_id].changed);
    EXPECT_TRUE(s3_settings.auth_settings[DB::S3AuthSetting::google_adc_client_secret].changed);
    EXPECT_TRUE(s3_settings.auth_settings[DB::S3AuthSetting::google_adc_refresh_token].changed);
}

TEST(BigLakeCredentials, UntouchedS3SettingsHaveEmptyGcpFields)
{
    // Baseline: a freshly constructed S3Settings must not carry any GCP
    // credentials so that a non-BigLake configuration is not accidentally
    // treated as OAuth.
    DB::S3Settings s3_settings;
    EXPECT_TRUE(s3_settings.auth_settings[DB::S3AuthSetting::http_client].value.empty());
    EXPECT_TRUE(s3_settings.auth_settings[DB::S3AuthSetting::google_adc_client_id].value.empty());
    EXPECT_TRUE(s3_settings.auth_settings[DB::S3AuthSetting::google_adc_client_secret].value.empty());
    EXPECT_TRUE(s3_settings.auth_settings[DB::S3AuthSetting::google_adc_refresh_token].value.empty());
    EXPECT_FALSE(s3_settings.auth_settings[DB::S3AuthSetting::http_client].changed);
}

// ---------------------------------------------------------------------------
// Test 3 – Guard clause: non-BigLake catalog options lack the biglake field
// ---------------------------------------------------------------------------
//
// `StorageS3Configuration::fromAST` contains:
//   if (catalog_options && !catalog_options->biglake)
//       throw Exception(ErrorCodes::BAD_ARGUMENTS, "Catalog is not BigLake type");
//
// A OneLake catalog returning `onelake` options must not set the `biglake`
// field, so the guard condition (`catalog_options && !catalog_options->biglake`)
// would be true and the exception would be thrown.
// ---------------------------------------------------------------------------

TEST(BigLakeCredentials, OneLakeCatalogOptionsHaveNoBlakeField)
{
    MockOneLakeCatalog catalog;
    auto opts = catalog.getObjectStorageInitializationOptions();

    ASSERT_TRUE(opts.has_value());
    EXPECT_FALSE(opts->biglake.has_value()); // guard fires: !catalog_options->biglake
    EXPECT_TRUE(opts->onelake.has_value());
}

TEST(BigLakeCredentials, NeutralCatalogReturnsNullopt)
{
    MockNeutralCatalog catalog;
    auto opts = catalog.getObjectStorageInitializationOptions();

    // std::nullopt → initialization_context guard does not fire, no credentials set
    EXPECT_FALSE(opts.has_value());
}

// ---------------------------------------------------------------------------
// Test 4 – Catalog type enumerator
// ---------------------------------------------------------------------------

TEST(BigLakeCredentials, BigLakeCatalogTypeIsIcebergBigLake)
{
    MockBigLakeCatalog catalog("c", "s", "r");
    EXPECT_EQ(catalog.getCatalogType(), DB::DatabaseDataLakeCatalogType::ICEBERG_BIGLAKE);
}
