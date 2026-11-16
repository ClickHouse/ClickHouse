#include <Interpreters/MecabDictionaryManager.h>

#if USE_MECAB

#include <Interpreters/Context.h>
#include <Common/Exception.h>
#include <Common/RemoteHostFilter.h>
#include <Common/logger_useful.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/copyData.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/Archives/ArchiveUtils.h>
#include <IO/Archives/createArchiveReader.h>
#include <IO/Archives/IArchiveReader.h>

#if USE_AWS_S3
#include <IO/ReadBufferFromS3.h>
#include <IO/S3/Client.h>
#include <IO/S3/URI.h>
#include <IO/S3AuthSettings.h>
#include <IO/S3RequestSettings.h>
#include <aws/core/auth/AWSCredentials.h>
#endif

#include <Poco/String.h>
#include <Poco/URI.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Net/HTTPBasicCredentials.h>

#include <mecab.h>

#include <openssl/evp.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int CHECKSUM_DOESNT_MATCH;
    extern const int CANNOT_LOAD_CONFIG;
    extern const int SUPPORT_IS_DISABLED;
    extern const int BAD_ARGUMENTS;
}

#if USE_AWS_S3
namespace S3AuthSetting
{
    extern const S3AuthSettingsString access_key_id;
    extern const S3AuthSettingsString secret_access_key;
    extern const S3AuthSettingsString session_token;
    extern const S3AuthSettingsString region;
    extern const S3AuthSettingsString server_side_encryption_customer_key_base64;
    extern const S3AuthSettingsString role_arn;
    extern const S3AuthSettingsString role_session_name;
    extern const S3AuthSettingsString external_id;
    extern const S3AuthSettingsBool use_environment_credentials;
    extern const S3AuthSettingsBool use_insecure_imds_request;
    extern const S3AuthSettingsBool no_sign_request;
    extern const S3AuthSettingsUInt64 expiration_window_seconds;
}
#endif

MecabDictionary::MecabDictionary(std::unique_ptr<MeCab::Model> model_, String dictionary_path_)
    : model(std::move(model_)), dictionary_path(std::move(dictionary_path_))
{
}

namespace
{

constexpr auto CONFIG_PREFIX = "tokenizer.japanese";
constexpr auto SYSTEM_DICTIONARY_FILE = "sys.dic";

std::string configKey(std::string_view name)
{
    return std::string(CONFIG_PREFIX) + '.' + std::string(name);
}

String toHexLowercase(std::string_view bytes)
{
    static constexpr char digits[] = "0123456789abcdef";
    String out;
    out.resize(bytes.size() * 2);
    for (size_t i = 0; i < bytes.size(); ++i)
    {
        const auto c = static_cast<unsigned char>(bytes[i]);
        out[2 * i] = digits[c >> 4];
        out[2 * i + 1] = digits[c & 0x0F];
    }
    return out;
}

/// Copies `source` to `out` and returns the lowercase-hex SHA-256 of the bytes, in a single streaming
/// pass so peak memory does not grow with the archive size.
String copyToFileAndHashSHA256(ReadBuffer & source, WriteBuffer & out)
{
    std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> ctx(EVP_MD_CTX_new(), EVP_MD_CTX_free);
    if (!ctx || !EVP_DigestInit_ex(ctx.get(), EVP_sha256(), nullptr))
        throw Exception(ErrorCodes::CANNOT_LOAD_CONFIG, "Failed to initialize SHA-256 for the Japanese dictionary");

    while (!source.eof())
    {
        const size_t chunk = source.available();
        EVP_DigestUpdate(ctx.get(), source.position(), chunk);
        out.write(source.position(), chunk);
        source.position() += chunk;
    }

    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned int digest_size = 0;
    EVP_DigestFinal_ex(ctx.get(), digest, &digest_size);
    return toHexLowercase({reinterpret_cast<const char *>(digest), digest_size});
}

/// True if S3 auth is configured under <japanese>; then an http(s) location uses the S3 client.
bool hasS3Configuration(const ContextPtr & context)
{
    const auto & config = context->getConfigRef();
    return config.has(configKey("access_key_id"))
        || config.has(configKey("no_sign_request"))
        || config.has(configKey("use_environment_credentials"))
        || config.has(configKey("region"));
}

#if USE_AWS_S3
/// Reads an S3 object. The endpoint comes from the URL, so any S3-compatible service works (not only
/// AWS); credentials/region are read from the <japanese> config.
std::unique_ptr<ReadBuffer> openS3Source(const String & location, const ContextPtr & context)
{
    const auto auth_settings = S3::S3AuthSettings(context->getConfigRef(), context->getSettingsRef(), CONFIG_PREFIX);
    const S3::URI uri(location);

    static constexpr unsigned s3_max_redirects = 10;
    static constexpr unsigned s3_retry_attempts = 10;

    S3::PocoHTTPClientConfiguration client_configuration = S3::ClientFactory::instance().createClientConfiguration(
        auth_settings[S3AuthSetting::region],
        context->getRemoteHostFilter(),
        s3_max_redirects,
        S3::PocoHTTPClientConfiguration::RetryStrategy{.max_retries = s3_retry_attempts},
        /*s3_slow_all_threads_after_network_error=*/true,
        /*s3_slow_all_threads_after_retryable_error=*/false,
        /*enable_s3_requests_logging=*/false,
        /*for_disk_s3=*/false,
        /*opt_disk_name=*/{},
        /*request_throttler=*/{},
        uri.uri.getScheme());
    client_configuration.endpointOverride = uri.endpoint;

    S3::ClientSettings client_settings{
        .use_virtual_addressing = uri.is_virtual_hosted_style,
        .disable_checksum = false,
        .gcs_issue_compose_request = false,
        .is_s3express_bucket = S3::isS3ExpressEndpoint(uri.endpoint),
    };

    auto client = S3::ClientFactory::instance().create(
        client_configuration,
        client_settings,
        auth_settings[S3AuthSetting::access_key_id],
        auth_settings[S3AuthSetting::secret_access_key],
        auth_settings[S3AuthSetting::server_side_encryption_customer_key_base64],
        auth_settings.server_side_encryption_kms_config,
        auth_settings.headers,
        S3::CredentialsConfiguration{
            .use_environment_credentials = auth_settings[S3AuthSetting::use_environment_credentials],
            .use_insecure_imds_request = auth_settings[S3AuthSetting::use_insecure_imds_request],
            .expiration_window_seconds = auth_settings[S3AuthSetting::expiration_window_seconds],
            .no_sign_request = auth_settings[S3AuthSetting::no_sign_request],
            .role_arn = auth_settings[S3AuthSetting::role_arn],
            .role_session_name = auth_settings[S3AuthSetting::role_session_name],
            .external_id = auth_settings[S3AuthSetting::external_id],
        },
        auth_settings[S3AuthSetting::session_token]);

    return std::make_unique<ReadBufferFromS3>(
        std::move(client), uri.bucket, uri.key, uri.version_id, S3::S3RequestSettings{}, context->getReadSettings());
}
#endif

/// Picks the transport from the location scheme: s3://|gs://|oss:// (or http(s) with S3 auth) -> S3
/// client; other http(s):// -> plain download; file:// -> local file. Anything else is rejected.
std::unique_ptr<ReadBuffer> openArchiveSource(const String & location, const ContextPtr & context)
{
    const bool is_http = location.starts_with("http://") || location.starts_with("https://");
    const bool is_s3_scheme = location.starts_with("s3://") || location.starts_with("gs://") || location.starts_with("oss://");

    if (is_s3_scheme || (is_http && hasS3Configuration(context)))
    {
#if USE_AWS_S3
        return openS3Source(location, context);
#else
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "Cannot load the MeCab dictionary from '{}': this ClickHouse build has no S3 support", location);
#endif
    }

    if (is_http)
    {
        Poco::URI uri(location);
        auto timeouts = ConnectionTimeouts::getHTTPTimeouts(context->getSettingsRef(), context->getServerSettings());

        return BuilderRWBufferFromHTTP(uri)
            .withConnectionGroup(HTTPConnectionGroupType::HTTP)
            .withSettings(context->getReadSettings())
            .withTimeouts(timeouts)
            .withHostFilter(&context->getRemoteHostFilter())
            .create({});
    }

    static constexpr std::string_view file_scheme = "file://";
    if (location.starts_with(file_scheme))
        return std::make_unique<ReadBufferFromFile>(location.substr(file_scheme.size()));

    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Unsupported <dictionary_location> '{}': expected an s3://, gs://, oss://, http(s):// or file:// URL", location);
}

/// Extracts every file from the archive into `destination`, preserving relative paths.
void extractArchive(const fs::path & archive_path, const fs::path & destination)
{
    auto reader = createArchiveReader(archive_path.string());
    auto enumerator = reader->firstFile();
    if (!enumerator)
        return;

    do
    {
        const String & name = enumerator->getFileName();
        if (name.ends_with("/")) /// directory entry
            continue;

        /// Guard against path traversal (Zip Slip): reject absolute paths and entries that would
        /// escape the destination directory. We only ever write regular files at the normalized path.
        const fs::path relative = fs::path(name).lexically_normal();
        if (relative.is_absolute() || (!relative.empty() && *relative.begin() == ".."))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Refusing to extract Japanese dictionary entry '{}': it escapes the destination directory", name);

        const fs::path out_path = destination / relative;
        fs::create_directories(out_path.parent_path());

        auto file_in = reader->readFile(name, /*throw_on_not_found=*/ true);
        WriteBufferFromFile file_out(out_path.string());
        copyData(*file_in, file_out);
        file_out.finalize();
    } while (enumerator->nextFile());
}

/// Finds the directory containing the compiled system dictionary (`sys.dic`).
fs::path findDictionaryDir(const fs::path & root)
{
    for (const auto & entry : fs::recursive_directory_iterator(root))
        if (entry.is_regular_file() && entry.path().filename() == SYSTEM_DICTIONARY_FILE)
            return entry.path().parent_path();

    throw Exception(
        ErrorCodes::CANNOT_LOAD_CONFIG,
        "The Japanese dictionary archive does not contain a compiled MeCab system dictionary ('{}')",
        SYSTEM_DICTIONARY_FILE);
}

MecabDictionaryPtr createModel(const fs::path & dictionary_dir)
{
    LoggerPtr log = getLogger("MecabDictionaryManager");

    /// MeCab requires a resource file (`mecabrc`) to exist (dictionary params come from
    /// `<dictionary_dir>/dicrc` via `-d`). Provide an empty one if the archive lacks it, so loading
    /// does not fall back to a system-wide `/etc/mecabrc`.
    const fs::path rc_file = dictionary_dir / "mecabrc";
    if (!fs::exists(rc_file))
        WriteBufferFromFile(rc_file.string()).finalize();

    const String dictionary_dir_str = dictionary_dir.string();
    const String rc_file_str = rc_file.string();

    std::vector<char *> argv{
        const_cast<char *>("mecab"),
        const_cast<char *>("-d"), const_cast<char *>(dictionary_dir_str.c_str()),
        const_cast<char *>("-r"), const_cast<char *>(rc_file_str.c_str()),
    };

    std::unique_ptr<MeCab::Model> model(MeCab::createModel(static_cast<int>(argv.size()), argv.data()));
    if (!model)
        throw Exception(
            ErrorCodes::CANNOT_LOAD_CONFIG,
            "Failed to load the MeCab model from '{}': {}",
            dictionary_dir_str, MeCab::getLastError());

    LOG_INFO(log, "Loaded MeCab model from {}", dictionary_dir_str);
    return std::make_shared<const MecabDictionary>(std::move(model), dictionary_dir_str);
}

}

MecabDictionaryManager & MecabDictionaryManager::instance()
{
    static MecabDictionaryManager manager;
    return manager;
}

MecabDictionaryPtr MecabDictionaryManager::getJapaneseDictionary()
{
    std::lock_guard lock(mutex);
    /// Loaded lazily on first use and cached for the process lifetime; changing the configured
    /// dictionary requires a server restart. A failed load is not cached, so the next call retries.
    if (!cached_dictionary)
        cached_dictionary = loadJapaneseDictionary();
    return cached_dictionary;
}

MecabDictionaryPtr MecabDictionaryManager::loadJapaneseDictionary()
{
    auto context = Context::getGlobalContextInstance();
    if (!context)
        throw Exception(ErrorCodes::CANNOT_LOAD_CONFIG, "Cannot load the MeCab dictionary: the global context is not available");

    const auto & config = context->getConfigRef();
    if (!config.has(CONFIG_PREFIX))
        throw Exception(
            ErrorCodes::NO_ELEMENTS_IN_CONFIG,
            "The Japanese tokenizer requires a dictionary configured under <tokenizer><japanese> in the server configuration");

    LoggerPtr log = getLogger("MecabDictionaryManager");

    const String location = config.getString(configKey("dictionary_location"), "");
    const String expected_sha = Poco::toLower(config.getString(configKey("dictionary_sha"), ""));

    if (location.empty())
        throw Exception(
            ErrorCodes::NO_ELEMENTS_IN_CONFIG,
            "Missing <tokenizer><japanese><dictionary_location> in the server configuration");
    if (expected_sha.empty())
        throw Exception(
            ErrorCodes::NO_ELEMENTS_IN_CONFIG,
            "Missing <tokenizer><japanese><dictionary_sha> in the server configuration. "
            "The SHA-256 of the dictionary archive is required to verify its integrity before loading");

    /// Validate the SHA before using it as a path component (it keys the cache directory below).
    bool valid_sha = expected_sha.size() == 64;
    for (char c : expected_sha)
        valid_sha &= (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
    if (!valid_sha)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Invalid <tokenizer><japanese><dictionary_sha> '{}': expected a 64-character hex SHA-256", expected_sha);

    /// Cache under the server data path, keyed by the verified SHA (download+verify happen once).
    const fs::path cache_dir = fs::path(context->getPath()) / "mecab_dictionaries" / expected_sha;
    /// Written only after a fully successful download+verify+extract; its presence means the cached
    /// dictionary is complete and can be reused without downloading again.
    const fs::path ready_marker = cache_dir / ".ready";

    /// The archive type is detected from the file extension (`createArchiveReader`), so the location
    /// must name an archive with a supported extension. Keep that name for the cached file.
    String archive_name = location.substr(location.find_last_of('/') + 1);
    if (const auto query_pos = archive_name.find('?'); query_pos != String::npos)
        archive_name = archive_name.substr(0, query_pos);
    if (!hasSupportedArchiveExtension(archive_name))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Cannot determine the archive type of the Japanese dictionary from <dictionary_location> '{}'. "
            "The location must name an archive with a supported extension (for example .tar.gz, .tar.zst or .zip)",
            location);
    const fs::path archive_path = cache_dir / archive_name;

    if (!fs::exists(ready_marker))
    {
        LOG_INFO(log, "Downloading Japanese dictionary from {}", location);
        fs::create_directories(cache_dir);

        /// 1. Download the archive to a local file, hashing it in the same streaming pass so peak
        ///    memory does not grow with the archive size.
        String actual_sha;
        {
            auto source = openArchiveSource(location, context);
            WriteBufferFromFile out(archive_path.string());
            actual_sha = copyToFileAndHashSHA256(*source, out);
            out.finalize();
        }

        /// 2. Verify the SHA-256 BEFORE using the archive (guards against corruption or tampering).
        if (actual_sha != expected_sha)
        {
            fs::remove_all(cache_dir);
            throw Exception(
                ErrorCodes::CHECKSUM_DOESNT_MATCH,
                "SHA-256 mismatch for the Japanese dictionary downloaded from '{}'. "
                "Expected {} but the downloaded file has {}. The dictionary was NOT loaded. "
                "This indicates the file is corrupted or has been tampered with; verify "
                "<tokenizer><japanese><dictionary_sha> and the source at <dictionary_location>.",
                location, expected_sha, actual_sha);
        }

        /// 3. Extract, then mark the cache entry complete only after a fully successful verify+extract.
        extractArchive(archive_path, cache_dir);
        fs::remove(archive_path);
        WriteBufferFromFile(ready_marker.string()).finalize();

        LOG_INFO(log, "Japanese dictionary verified (sha256={}) and extracted to {}", expected_sha, cache_dir.string());
    }

    /// 4. Locate the compiled system dictionary and load the model.
    return createModel(findDictionaryDir(cache_dir));
}

}

#endif
