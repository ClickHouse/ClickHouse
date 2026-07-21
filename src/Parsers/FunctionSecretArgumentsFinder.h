#pragma once

#include <map>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>
#include <sys/types.h>

#include <base/types.h>

namespace DB
{

struct QualifiedTableName;

class AbstractFunction
{
    friend class FunctionSecretArgumentsFinder;
public:
    class Argument
    {
    public:
        virtual ~Argument() = default;
        virtual std::unique_ptr<AbstractFunction> getFunction() const = 0;
        virtual bool isIdentifier() const = 0;
        virtual bool tryGetString(String * res, bool allow_identifier) const = 0;
    };
    class Arguments
    {
    public:
        virtual ~Arguments() = default;
        virtual size_t size() const = 0;
        virtual std::unique_ptr<Argument> at(size_t n) const = 0;
    };

    virtual ~AbstractFunction() = default;
    virtual String name() const = 0;
    bool hasArguments() const { return !!arguments; }

protected:
    std::unique_ptr<Arguments> arguments;
};

class FunctionSecretArgumentsFinder
{
public:
    struct Result
    {
        /// Result constructed by default means no arguments will be hidden.
        size_t start = static_cast<size_t>(-1);
        size_t count = 0; /// Mostly it's either 0 or 1. There are only a few cases where `count` can be greater than 1 (e.g. see `encrypt`).
                            /// In all known cases secret arguments are consecutive
        bool are_named = false; /// Arguments like `password = 'password'` are considered as named arguments.
        /// E.g. "headers" in `url('..', headers('foo' = '[HIDDEN]'))`
        std::vector<std::string> nested_maps;
        /// Full replacement of an argument. Only supported when count is 1, otherwise all arguments will be replaced with this string.
        /// It's needed in cases when we don't want to hide the entire parameter, but some part of it, e.g. "connection_string" in
        /// `azureBlobStorage('DefaultEndpointsProtocol=https;AccountKey=secretkey;...', ...)` should be replaced with
        /// `azureBlobStorage('DefaultEndpointsProtocol=https;AccountKey=[HIDDEN];...', ...)`.
        std::string replacement;
        /// Whether to wrap a result using full argument replacement in quotes.
        bool quote_replacement = true;
        /// Per-argument replacements by raw argument index; the text is emitted verbatim (it must carry
        /// its own quoting). Used when only a part of an argument is secret, e.g. a presigned S3 URL
        /// keeps its host and path while the credential query parameters are hidden. Unlike
        /// `replacement`, this composes with the other masking (span, nested maps).
        std::map<size_t, std::string> replaced_arguments;
        /// Individually masked arguments by raw argument index; the value tells whether the argument
        /// is a named `key = value` (the key stays visible and only the value is hidden). Valid S3
        /// syntax can interleave secrets with non-secret arguments (e.g. a named `session_token`
        /// after `format`), which a single contiguous span cannot represent without hiding the
        /// non-secret arguments in between.
        std::map<size_t, bool> masked_arguments;

        bool hasSecrets() const
        {
            return count != 0 || !nested_maps.empty() || !replaced_arguments.empty() || !masked_arguments.empty();
        }
    };

    explicit FunctionSecretArgumentsFinder(std::unique_ptr<AbstractFunction> && function_) : function(std::move(function_)) {}

    FunctionSecretArgumentsFinder::Result getResult() const { return result; }

    /// Whether a key of the `extra_credentials(..)` nested map carries a non-secret identifier whose
    /// value stays visible when the map is masked (`role_arn` and `role_session_name`; the map's
    /// secret is `external_id`). Any other key - unknown, malformed or an expression - fails closed.
    static bool isNonSecretExtraCredentialsKey(std::string_view key)
    {
        return key == "role_arn" || key == "role_session_name";
    }

protected:
    const std::unique_ptr<AbstractFunction> function;
    Result result;

    /// Named arguments carrying S3 secrets, shared by every S3 form (explicit-url and named-collection).
    /// `external_id` is the shared secret of the assume-role triple; the other two (`role_arn`,
    /// `role_session_name`) are non-secret identifiers passed inside `extra_credentials` and stay
    /// visible (see isNonSecretExtraCredentialsKey).
    static constexpr std::string_view s3_secret_keys[]
        = {"secret_access_key", "session_token", "google_adc_client_secret", "google_adc_refresh_token", "external_id"};

    void markSecretArgument(size_t index, bool argument_is_named = false);

    /// `headers(..)` and `extra_credentials(..)` are nested maps whose values are secret auth material
    /// (`extra_credentials` carries the assume-role secret `external_id`; its non-secret identifiers
    /// stay visible, see isNonSecretExtraCredentialsKey). The parsers accept them at any position, not
    /// just at the tail. Record them so their values are hidden with the keys kept.
    /// Idempotent: each map is recorded at most once.
    void maskNestedSecretMaps();

    /// Single source of truth for reading an S3-style argument list the way the S3 parsers do:
    /// `headers(..)` / `extra_credentials(..)` can appear at any position and are stripped before
    /// positional slots are assigned; `key = value` arguments are named, and those with a key from
    /// `s3_secret_keys` are masked (every occurrence: a duplicated key is logged before validation
    /// rejects it); everything else (literals and constant expressions) occupies positional slots
    /// in order. Returns the raw AST indices of the positional arguments; all slot arithmetic must
    /// use them instead of raw indices.
    /// Most parsers reject a positional after the first `key = value` argument, so such positionals
    /// are masked (the query is logged before validation and the intended slot is unknowable). The
    /// backup locator (`BackupInfo::fromAST`) instead collects positionals independently of named
    /// overrides; it passes `positionals_allowed_after_named` to collect them in order.
    std::vector<size_t> classifyS3Arguments(size_t start = 0, bool positionals_allowed_after_named = false);

    /// Masks the positional secrets of the explicit-url S3 form: `secret_access_key` at slot
    /// `url_slot + 2`, and a positional `session_token` at slot `url_slot + 3` unless that slot is
    /// actually the format, mirroring the parser's disambiguation. NOSIGN and `s3('url', 'format', ...)`
    /// forms carry no positional secrets at slot 3+.
    void maskS3PositionalSecrets(const std::vector<size_t> & positional, size_t url_slot);

    /// For S3 locators that accept nothing positional beyond `secret_access_key` (the S3 database
    /// engine and the backup S3 destination): mask every positional from `first_slot` on, failing
    /// closed on invalid extra positionals, which are logged before validation rejects them.
    void maskS3PositionalsFrom(const std::vector<size_t> & positional, size_t first_slot);

    /// The S3 URL itself can carry credentials: a userinfo part and presigned-URL query parameters.
    /// If the url positional does, replace it with a partially masked copy that keeps the host and
    /// path visible. The field set mirrors `BackupInfo::removeCredentialsFromS3URL`.
    void maskS3UrlArgument(const std::vector<size_t> & positional, size_t url_slot);

    void findOrdinaryFunctionSecretArguments();
    void findMySQLFunctionSecretArguments();
    void findMongoDBSecretArguments();
    void findRedisTableEngineSecretArguments();
    void findArrowFlightSecretArguments();
    void findXDBCSecretArguments();

    /// Similar to `findSecretNamedArgument`, but if the value is a URI with credentials,
    /// masks only the password part instead of hiding the entire value.
    void maskXDBCSecretNamedArgument(std::string_view key, size_t start);

    void findS3FunctionSecretArguments(bool is_cluster_function);
    void findAzureBlobStorageFunctionSecretArguments(bool is_cluster_function);
    bool maskAzureConnectionString(ssize_t url_arg_idx, bool argument_is_named = false, size_t start = 0);
    void findURLSecretArguments();

    bool tryGetStringFromArgument(size_t arg_idx, String * res, bool allow_identifier = true) const;
    static bool tryGetStringFromArgument(const AbstractFunction::Argument & argument, String * res, bool allow_identifier = true);

    void findRemoteFunctionSecretArguments();

    /// Tries to get either a database name or a qualified table name from an argument.
    /// Empty string is also allowed (it means the default database).
    /// The function is used by findRemoteFunctionSecretArguments() to determine how many arguments to skip before a password.
    bool tryGetDatabaseNameOrQualifiedTableName(
        size_t arg_idx,
        std::optional<String> & res_database,
        std::optional<QualifiedTableName> & res_qualified_table_name) const;

    void findEncryptionFunctionSecretArguments();
    void findHMACSecretArguments();
    void findTableEngineSecretArguments();
    void findExternalDistributedTableEngineSecretArguments();
    void findS3TableEngineSecretArguments();
    void findAzureBlobStorageTableEngineSecretArguments();
    void findRedisFunctionSecretArguments();
    void findYTsaurusStorageTableEngineSecretArguments();
    void findDatabaseEngineSecretArguments();
    void findMySQLDatabaseSecretArguments();
    void findS3DatabaseSecretArguments();
    void findDataLakeCatalogSecretArguments();
    void findBackupDatabaseSecretArguments();
    void findBackupNameSecretArguments();

    /// Whether a specified argument can be the name of a named collection?
    bool isNamedCollectionName(size_t arg_idx) const;

    /// Looks for an argument with a specified name. This function looks for arguments in format `key=value` where the key is specified.
    /// Returns -1 if no argument was found.
    ssize_t findNamedArgument(String * res, std::string_view key, size_t start = 0);

    /// Looks for secret arguments with a specified name in format `key=value` and marks them secret.
    /// Marks *every* occurrence, not just the first: a malformed query is formatted for logging before
    /// duplicate-key validation runs, so `session_token = 'a', session_token = 'b'` must hide both.
    bool findSecretNamedArgument(std::string_view key, size_t start = 0);

    /// Masks the secrets of an S3 named-collection form: the secret named overrides (every occurrence,
    /// in any order; the span covering them may hide a non-secret argument in between, which is safe)
    /// and the `headers(...)` / `extra_credentials(...)` map overrides.
    void findS3NamedCollectionSecretArguments(size_t start = 0);
};

}
