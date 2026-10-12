#include <Storages/ObjectStorage/S3/S3SecretArguments.h>

#include <algorithm>

#include <Common/HiddenSecret.h>
#include <Common/KnownObjectNames.h>
#include <Common/StringUtils.h>
#include <Common/maskURIPassword.h>
#include <Common/quoteString.h>
#include <Interpreters/FunctionSecretArgumentsFinder.h>

namespace DB
{

namespace
{

/// Named arguments carrying S3 secrets, shared by every S3 form (explicit-url and named-collection).
/// `external_id` and `role_session_name` are the secrets of the assume-role triple; the third key
/// (`role_arn`) is a non-secret identifier passed inside `extra_credentials` and stays visible
/// (see isNonSecretExtraCredentialsKey).
/// The keys of the `extra_credentials(..)` nested map whose value stays visible when the map is masked.
/// Only `role_arn` qualifies: it names the role to assume, like `access_key_id` names a key. The other two
/// keys of the assume-role triple are secrets: `external_id` is its shared secret, and `role_session_name`
/// can be one too, because a trust policy can require a specific value through the `sts:RoleSessionName`
/// condition (the ClickHouse Cloud guide documents exactly this use). Any other key fails closed.
/// The `.backup` metadata is a different matter: its `<base_backup>` locator keeps `role_session_name`
/// on purpose, so that a role-authenticated backup chain stays restorable (see `BackupInfo.cpp`).
constexpr std::string_view extra_credentials_visible_keys[] = {"role_arn"};

constexpr std::string_view s3_secret_keys[]
    = {"secret_access_key", "session_token", "google_adc_client_secret", "google_adc_refresh_token", "external_id",
       "role_session_name"};

bool isBinaryEquals(const AbstractFunction & argument)
{
    return argument.name() == "equals" && argument.hasArguments() && argument.arguments->size() == 2;
}

bool backupS3MapHasUnsafeVisibleArgument(const AbstractFunction & map)
{
    if (!map.hasArguments())
        return true;

    for (size_t i = 0; i < map.arguments->size(); ++i)
    {
        const auto child = map.arguments->at(i)->getFunction();
        if (!child || !isBinaryEquals(*child))
            continue;

        String key;
        if (!FunctionSecretArgumentsFinder::tryGetStringFromArgument(*child->arguments->at(0), &key))
            return true;
        if (map.name() == "extra_credentials" && isNonSecretExtraCredentialsKey(key)
            && !FunctionSecretArgumentsFinder::tryGetStringFromArgument(*child->arguments->at(1), nullptr))
            return true;
    }
    return false;
}

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
std::vector<size_t> classifyS3Arguments(FunctionSecretArgumentsFinder & finder, size_t start = 0, bool positionals_allowed_after_named = false)
{
    /// The parsers strip the nested maps before positional slots are assigned.
    maskHeadersAndExtraCredentials(finder);

    const auto & function = finder.function;
    std::vector<size_t> positional;
    bool seen_named = false;
    for (size_t i = start; i < function->arguments->size(); ++i)
    {
        if (const auto f = function->arguments->at(i)->getFunction())
        {
            const auto name = f->name();
            if (name == "headers" || name == "extra_credentials")
                continue;
            if (isBinaryEquals(*f))
            {
                seen_named = true;
                String key;
                if (f->arguments->at(0)->tryGetString(&key, /* allow_identifier= */ true))
                {
                    if (isS3SecretKey(key))
                    {
                        finder.markSecretArgument(i, /* argument_is_named= */ true);
                    }
                    else if (key == "url")
                    {
                        /// A `url` override can itself carry credentials (userinfo, presign parameters).
                        String url;
                        if (f->arguments->at(1)->tryGetString(&url, /* allow_identifier= */ false))
                        {
                            if (maskS3URICredentials(url))
                                finder.result.replaced_arguments[i] = "url = " + quoteString(url);
                        }
                        else
                        {
                            /// A url built from an expression can embed credentials in its pieces;
                            /// we cannot evaluate it here, so fail closed and hide the value.
                            finder.markSecretArgument(i, /* argument_is_named= */ true);
                        }
                    }
                    else if (!f->arguments->at(1)->tryGetString(nullptr, /* allow_identifier= */ true)
                             && !f->arguments->at(1)->tryGetLiteralText(nullptr))
                    {
                        /// A visible non-secret override (`format`, `structure`, `role_arn`, ...) whose
                        /// value is not a plain literal or identifier can be a nested secret carrier,
                        /// e.g. `format = headers('Authorization' = '...')`, formatted verbatim before
                        /// the parser rejects the non-literal value. Fail closed and hide the value.
                        finder.markSecretArgument(i, /* argument_is_named= */ true);
                    }
                }
                else
                {
                    /// The parsers evaluate the key as a constant expression, so it can name any secret
                    /// key. We cannot evaluate it here, so fail closed and hide the value (the key
                    /// expression itself stays visible; keys are not secrets).
                    finder.markSecretArgument(i, /* argument_is_named= */ true);
                }
                continue;
            }
        }
        if (seen_named && !positionals_allowed_after_named)
        {
            /// The parsers reject positional arguments after the first `key = value` argument, but the
            /// query is logged before validation and the intended slot is unknowable; fail closed.
            finder.markSecretArgument(i);
            continue;
        }
        positional.push_back(i);
    }
    return positional;
}

/// Masks the positional secrets (`secret_access_key`, `session_token`) of the explicit-url S3
/// form, selecting the signature by argument count and `with_structure` exactly like the parser
/// (`S3StorageParsedArguments::fromAST`). `positional` is the positional-only argument list, `url`
/// at `url_slot` (1 for `s3Cluster`, 0 otherwise). Value-based disambiguations (NOSIGN, format)
/// fail closed on an unevaluable expression: the potential credential slot is masked.
void maskS3PositionalSecrets(
    FunctionSecretArgumentsFinder & finder, const std::vector<size_t> & positional, size_t url_slot, bool with_structure)
{
    /// The parser (`S3StorageParsedArguments::fromAST`) selects the signature from the positional
    /// `count` (the number of arguments from `url` on) and `with_structure`, disambiguating only
    /// NOSIGN and format-vs-secret by looking at an argument's value. Across every signature the only
    /// credential positionals are `secret_access_key` at slot 2 and `session_token` at slot 3, so we
    /// reproduce the parser's per-count decision for just those two slots.
    ///
    /// Value tests fail closed: an unevaluable expression is not recognized as NOSIGN or a format, so
    /// the slot that would then be a credential is masked. A query built from a computed format thus
    /// loses that non-secret argument in the AST dump, which is safe. The query-tree path resolves such
    /// expressions to constants first, so it classifies them exactly.
    if (url_slot >= positional.size())
        return;
    const size_t count = positional.size() - url_slot;

    auto value_is = [&](size_t slot, auto && predicate) -> bool
    {
        String value;
        return url_slot + slot < positional.size()
            && finder.tryGetStringFromArgument(positional[url_slot + slot], &value) && predicate(value);
    };
    auto is_nosign = [&](size_t slot) { return value_is(slot, [](const String & v) { return equalsCaseInsensitive(v, "NOSIGN"); }); };
    auto is_format = [&](size_t slot) { return value_is(slot, [](const String & v) { return v == "auto" || KnownFormatNames::instance().exists(v); }); };

    bool secret_access_key = false; /// slot 2
    bool session_token = false;     /// slot 3
    switch (count)
    {
        case 0: case 1: case 2: /// url only, or url + format/NOSIGN
            break;
        case 3:
            secret_access_key = !is_nosign(1) && !is_format(1);
            break;
        case 4:
            secret_access_key = !is_nosign(1) && !(with_structure && is_format(1));
            session_token = secret_access_key && !is_format(3);
            break;
        case 5:
            secret_access_key = !with_structure || !is_nosign(1);
            session_token = secret_access_key && !is_format(3);
            break;
        case 6:
            secret_access_key = true;
            session_token = !with_structure || !is_format(3);
            break;
        default: /// count >= 7: access-key form only, both credential slots always present
            secret_access_key = true;
            session_token = true;
            break;
    }

    if (secret_access_key)
        finder.markSecretArgument(positional[url_slot + 2]);
    if (session_token)
        finder.markSecretArgument(positional[url_slot + 3]);
}

/// For S3 locators that accept nothing positional beyond `secret_access_key` (the S3 database
/// engine and the backup S3 destination): mask every positional from `first_slot` on, failing
/// closed on invalid extra positionals, which are logged before validation rejects them.
void maskS3PositionalsFrom(FunctionSecretArgumentsFinder & finder, const std::vector<size_t> & positional, size_t first_slot)
{
    for (size_t slot = first_slot; slot < positional.size(); ++slot)
        finder.markSecretArgument(positional[slot]);
}

/// The S3 URL itself can carry credentials: a userinfo part and presigned-URL query parameters.
/// If the url positional does, replace it with a partially masked copy that keeps the host and
/// path visible. The field set mirrors `BackupInfo::removeCredentialsFromS3URL`.
void maskS3UrlArgument(FunctionSecretArgumentsFinder & finder, const std::vector<size_t> & positional, size_t url_slot)
{
    if (url_slot >= positional.size())
        return;
    String url;
    if (!finder.tryGetStringFromArgument(positional[url_slot], &url, /* allow_identifier= */ false))
    {
        /// The parsers evaluate a constant-expression url before signature parsing, so a url built
        /// from an expression can embed credentials in its pieces; we cannot evaluate it here, so
        /// fail closed and hide it whole.
        finder.markSecretArgument(positional[url_slot]);
        return;
    }
    if (maskS3URICredentials(url))
        finder.result.replaced_arguments[positional[url_slot]] = quoteString(url);
}

/// Masks the secrets of an S3 named-collection form: the secret named overrides (every occurrence,
/// in any order; the span covering them may hide a non-secret argument in between, which is safe)
/// and the `headers(...)` / `extra_credentials(...)` map overrides.
void findS3NamedCollectionSecretArguments(FunctionSecretArgumentsFinder & finder, size_t start)
{
    /// After the collection name every argument must be a named `option = value` override or a nested
    /// map; a positional argument is invalid but logged before validation rejects it, so fail closed
    /// and hide every positional the classification returns.
    maskS3PositionalsFrom(finder, classifyS3Arguments(finder, start), 0);
}

void findS3FunctionSecretArguments(FunctionSecretArgumentsFinder & finder, bool is_cluster_function)
{
    /// s3Cluster('cluster_name', 'url', ...) has 'url' as its second argument.
    size_t url_slot = is_cluster_function ? 1 : 0;

    if (finder.isNamedCollectionName(url_slot))
    {
        /// s3(named_collection, ..., secret_access_key = 'secret_access_key', ...)
        /// s3Cluster('cluster_name', named_collection, ..., secret_access_key = 'secret_access_key', ...)
        findS3NamedCollectionSecretArguments(finder, url_slot + 1);
        return;
    }

    const auto positional = classifyS3Arguments(finder);
    maskS3UrlArgument(finder, positional, url_slot);

    /// The table function accepts a positional `structure`, unless a `structure = ...` named override
    /// is given (the parser then turns `with_structure` off). The parser evaluates key expressions, so
    /// an unevaluable key might resolve to `structure`; treat any unreadable key as disabling it too.
    /// This fails closed: `with_structure = false` only ever masks the same slots or more.
    const auto & function = finder.function;
    bool with_structure = true;
    for (size_t i = 0; i < function->arguments->size(); ++i)
    {
        const auto equals_func = function->arguments->at(i)->getFunction();
        if (!equals_func || equals_func->name() != "equals" || !equals_func->hasArguments() || equals_func->arguments->size() != 2)
            continue;
        String key;
        if (!equals_func->arguments->at(0)->tryGetString(&key, /* allow_identifier= */ true) || key == "structure")
        {
            with_structure = false;
            break;
        }
    }
    maskS3PositionalSecrets(finder, positional, url_slot, with_structure);
}

void findS3TableEngineSecretArguments(FunctionSecretArgumentsFinder & finder)
{
    if (finder.isNamedCollectionName(0))
    {
        /// S3(named_collection, ..., secret_access_key = 'secret_access_key')
        findS3NamedCollectionSecretArguments(finder, 1);
        return;
    }

    const auto positional = classifyS3Arguments(finder);
    maskS3UrlArgument(finder, positional, 0);

    /// The table engine takes its structure from the column list, never as an argument.
    maskS3PositionalSecrets(finder, positional, 0, /* with_structure= */ false);
}

void findS3DatabaseSecretArguments(FunctionSecretArgumentsFinder & finder)
{
    if (finder.isNamedCollectionName(0))
    {
        /// S3(named_collection, ..., secret_access_key = 'password', ...)
        findS3NamedCollectionSecretArguments(finder, 1);
    }
    else
    {
        /// S3('url', 'access_key_id', 'secret_access_key' [, session_token = ..., google_adc_* = ...]):
        /// the engine accepts no positional argument beyond secret_access_key, so fail closed from
        /// slot 2 on. Non-secret named overrides (e.g. `use_environment_credentials = 1`) stay visible.
        const auto positional = classifyS3Arguments(finder);
        maskS3UrlArgument(finder, positional, 0);
        maskS3PositionalsFrom(finder, positional, 2);
    }
}

void maskBackupS3VisibleArguments(FunctionSecretArgumentsFinder & finder, const std::vector<size_t> & positional)
{
    const auto & function = finder.function;
    auto & result = finder.result;

    for (const size_t index : positional)
    {
        if (result.masked_arguments.contains(index) || result.replaced_arguments.contains(index))
            continue;
        String value;
        if (!function->arguments->at(index)->tryGetString(&value, /* allow_identifier= */ false))
        {
            finder.markSecretArgument(index);
            continue;
        }
        if (maskS3URICredentials(value))
            result.replaced_arguments[index] = quoteString(value);
    }

    for (size_t i = 0; i < function->arguments->size(); ++i)
    {
        const auto named = function->arguments->at(i)->getFunction();
        if (!named)
            continue;

        if ((named->name() == "headers" || named->name() == "extra_credentials")
            && backupS3MapHasUnsafeVisibleArgument(*named))
        {
            result.replaced_arguments[i] = HIDDEN_SECRET_LITERAL;
            continue;
        }

        if (!isBinaryEquals(*named))
            continue;
        if (!FunctionSecretArgumentsFinder::tryGetStringFromArgument(*named->arguments->at(0), nullptr))
        {
            finder.markSecretArgument(i);
            continue;
        }
        if (result.masked_arguments.contains(i) || result.replaced_arguments.contains(i))
            continue;
        String value;
        if (named->arguments->at(1)->tryGetString(&value, /* allow_identifier= */ true) && maskS3URICredentials(value))
            finder.markSecretArgument(i, /* argument_is_named= */ true);
    }
}

void findS3BackupSecretArguments(FunctionSecretArgumentsFinder & finder)
{
    const auto & function = finder.function;
    const bool is_named_collection = finder.isNamedCollectionName(0);
    const auto positional = classifyS3Arguments(finder, is_named_collection ? 1 : 0, /* positionals_allowed_after_named= */ true);
    /// `BackupInfo::fromAST` reads only literals as positionals (a last function is the credential clause)
    /// and rejects anything else after the statement is logged, so such a locator has no valid triple.
    bool only_literals = true;
    for (const size_t index : positional)
    {
        if (!function->arguments->at(index)->tryGetLiteralText(nullptr))
        {
            finder.markSecretArgument(index);
            only_literals = false;
        }
    }
    if (is_named_collection)
    {
        /// BACKUP ... TO S3(named_collection[, 'filename'], ..., secret_access_key = '...', ...):
        /// unlike the other named-collection S3 forms, the backup locator accepts one positional
        /// (the non-secret filename), in any position relative to the named overrides; anything
        /// positional beyond it is invalid, so fail closed there.
        maskS3PositionalsFrom(finder, positional, 1);
    }
    else
    {
        /// `BackupInfo::fromAST` also rejects a function other than `key = value` before the last argument, such as a
        /// map skipped above, so such a locator has no valid triple either.
        bool has_function_before_last = false;
        for (size_t i = 0; i + 1 < function->arguments->size() && !has_function_before_last; ++i)
        {
            const auto f = function->arguments->at(i)->getFunction();
            has_function_before_last = f && f->name() != "equals";
        }
        /// BACKUP ... TO S3(url [, aws_access_key_id, aws_secret_access_key] [, session_token = ..., ...]):
        /// the locator accepts exactly one or three positionals; the valid triple keeps the url and
        /// access_key_id visible and hides the secret at slot 2. Any other positional count is invalid
        /// but logged before validation, and the intended slots are unknowable, so fail closed on
        /// everything after the url.
        maskS3UrlArgument(finder, positional, 0);
        maskS3PositionalsFrom(finder, positional, positional.size() == 3 && only_literals && !has_function_before_last ? 2 : 1);
    }
    maskBackupS3VisibleArguments(finder, positional);
}

}

void maskHeadersAndExtraCredentials(FunctionSecretArgumentsFinder & finder)
{
    finder.maskNestedSecretMap("headers");
    finder.maskNestedSecretMap("extra_credentials", {std::begin(extra_credentials_visible_keys), std::end(extra_credentials_visible_keys)});
}

bool isNonSecretExtraCredentialsKey(std::string_view key)
{
    return std::ranges::contains(extra_credentials_visible_keys, key);
}

bool isS3SecretKey(std::string_view key)
{
    return std::find(std::begin(s3_secret_keys), std::end(s3_secret_keys), key) != std::end(s3_secret_keys);
}

bool maskS3URICredentials(String & url)
{
    /// The parameter set mirrors `BackupInfo::removeCredentialsFromS3URL` (which strips the same fields from
    /// persisted backup metadata). Both scans live in `Common/maskURIPassword.h` and are checked against the
    /// regular expressions they replaced in `src/Common/tests/gtest_mask_uri_password.cpp`.
    bool changed = maskURIUserinfo(url);
    changed |= maskPresignedURLParameters(url);
    return changed;
}

/// `classifyS3Arguments` reads the interleaved named arguments and the nested maps itself, before any generic
/// positional pass could hide a map that follows a named argument whole.

SecretArgumentsSpec s3TableFunctionSecretArguments(bool is_cluster_function)
{
    /// s3('url', 'aws_access_key_id', 'aws_secret_access_key', ...)
    /// s3Cluster('cluster_name', 'url', 'aws_access_key_id', 'aws_secret_access_key', ...)
    return {.custom = [is_cluster_function](FunctionSecretArgumentsFinder & finder)
            { findS3FunctionSecretArguments(finder, is_cluster_function); },
            .custom_reads_named_arguments = true};
}

SecretArgumentsSpec s3TableEngineSecretArguments()
{
    /// S3('url', ['aws_access_key_id', 'aws_secret_access_key',] ...)
    return {.custom = findS3TableEngineSecretArguments, .custom_reads_named_arguments = true};
}

SecretArgumentsSpec s3DatabaseSecretArguments()
{
    /// S3('url', 'access_key_id', 'secret_access_key')
    return {.custom = findS3DatabaseSecretArguments, .custom_reads_named_arguments = true};
}

SecretArgumentsSpec s3BackupSecretArguments()
{
    return {.custom = findS3BackupSecretArguments};
}

}
