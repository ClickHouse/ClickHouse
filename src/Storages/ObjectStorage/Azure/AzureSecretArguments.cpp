#include <Storages/ObjectStorage/Azure/AzureSecretArguments.h>

#include <algorithm>
#include <optional>

#include <Common/KnownObjectNames.h>
#include <Common/StringUtils.h>
#include <Common/maskURIPassword.h>
#include <Interpreters/FunctionSecretArgumentsFinder.h>
#include <base/defines.h>

namespace DB
{

namespace
{

/// How an Azure destination reads a connection value, and whether the masking here can show it.
enum class AzureConnectionValue
{
    /// An http(s) scheme and a host, with no userinfo, query or fragment: each of those carries a
    /// credential of its own (`http://user:key@host`, a SAS `?sig=`).
    PlainStorageAccountURL,
    /// A connection string, whose secret keys `maskAzureConnectionString` masks in place.
    ConnectionString,
    /// A value that can carry a credential no rule here masks.
    Unmaskable,
};

AzureConnectionValue classifyAzureConnectionValue(const String & value)
{
    static constexpr std::string_view SEPARATOR = "://";
    const size_t separator = value.find(SEPARATOR);
    const std::string_view scheme = std::string_view(value).substr(0, std::min(separator, value.length()));
    /// The scheme grammar `maskURIUserinfo` reads. A connection string does not match it, even when
    /// one of its values embeds an endpoint URL.
    const bool is_url = separator != String::npos && !scheme.empty() && isAlphaASCII(scheme.front())
        && std::all_of(
               scheme.begin(), scheme.end(), [](char c) { return isAlphaNumericASCII(c) || c == '+' || c == '.' || c == '-'; });
    /// `maskAzureConnectionString` masks nothing in a value starting with `http`, so one that is no
    /// URL either would be left as written.
    if (!is_url)
        return value.starts_with("http") ? AzureConnectionValue::Unmaskable : AzureConnectionValue::ConnectionString;

    if ((!equalsCaseInsensitive(scheme, "http") && !equalsCaseInsensitive(scheme, "https"))
        || value.find_first_of("?#") != String::npos)
        return AzureConnectionValue::Unmaskable;

    const size_t authority_begin = separator + SEPARATOR.length();
    const size_t authority_end = std::min(value.find('/', authority_begin), value.length());
    if (authority_end == authority_begin || value.find('@', authority_begin) < authority_end)
        return AzureConnectionValue::Unmaskable;
    return AzureConnectionValue::PlainStorageAccountURL;
}

bool maskAzureConnectionString(FunctionSecretArgumentsFinder & finder, ssize_t url_arg_idx, bool argument_is_named = false, size_t start = 0)
{
    auto & result = finder.result;
    String url_arg;
    if (argument_is_named)
    {
        url_arg_idx = finder.findNamedArgument(&url_arg, "connection_string", start);
        if (url_arg_idx == -1 || url_arg.empty())
            url_arg_idx = finder.findNamedArgument(&url_arg, "storage_account_url", start);
        if (url_arg_idx == -1 || url_arg.empty())
            return false;
    }
    else
    {
        if (!finder.tryGetStringFromArgument(url_arg_idx, &url_arg))
            return false;
    }

    if (!url_arg.starts_with("http"))
    {
        if (maskConnectionStringKey(url_arg, "AccountKey="))
        {
            chassert(result.count == 0); /// We shouldn't use replacement with masking other arguments
            result.start = url_arg_idx;
            result.are_named = argument_is_named;
            result.count = 1;
            result.replacement = url_arg;
            return true;
        }

        if (maskConnectionStringKey(url_arg, "SharedAccessSignature="))
        {
            chassert(result.count == 0); /// We shouldn't use replacement with masking other arguments
            result.start = url_arg_idx;
            result.are_named = argument_is_named;
            result.count = 1;
            result.replacement = url_arg;
            return true;
        }
    }

    return false;
}

/// Whether the arguments an `AzureBlobStorage(named_collection, ...)` destination or table takes
/// from `start` can be shown: only an argument written here can carry a credential, and each has
/// to be readable enough to tell that it does not. `positional_limit` bounds the plain literals
/// read beside the overrides: one filename for a backup locator, none for a table engine.
bool azureCollectionArgumentsAreShowable(FunctionSecretArgumentsFinder & finder, size_t start, size_t positional_limit)
{
    const auto & function = finder.function;
    size_t positionals = 0;
    for (size_t i = start, size = function->arguments->size(); i < size; ++i)
    {
        const auto argument_function = function->arguments->at(i)->getFunction();
        if (argument_function && argument_function->name() == "equals")
        {
            /// A key this rule cannot read hides which credential the override carries; a value that is
            /// no plain literal or identifier can nest one (`headers('Authorization' = '...')`).
            if (argument_function->arguments && argument_function->arguments->size() == 2
                && FunctionSecretArgumentsFinder::tryGetStringFromArgument(*argument_function->arguments->at(0), nullptr)
                && (FunctionSecretArgumentsFinder::tryGetStringFromArgument(*argument_function->arguments->at(1), nullptr)
                    || argument_function->arguments->at(1)->tryGetLiteralText(nullptr)))
                continue;
            return false;
        }
        if (++positionals > positional_limit || !function->arguments->at(i)->tryGetLiteralText(nullptr))
            return false;
    }

    /// A destination reads at most one of the two mutually exclusive connection keys, and rejects a
    /// second one only after the statement has been formatted, so a surplus one stays as written.
    size_t connection_overrides = 0;
    for (const auto & key : {"connection_string", "storage_account_url"})
        for (ssize_t i = finder.findNamedArgument(nullptr, key, start); i >= 0;
             i = finder.findNamedArgument(nullptr, key, static_cast<size_t>(i) + 1))
            ++connection_overrides;

    if (connection_overrides > 1)
        return false;

    for (const auto & key : {"connection_string", "storage_account_url"})
    {
        String value;
        if (finder.findNamedArgument(&value, key, start) < 0)
            continue;
        /// Hiding a connection string replaces its whole argument, which cannot be combined with
        /// hiding `account_key`.
        const auto shape = classifyAzureConnectionValue(value);
        if (value.empty() || shape == AzureConnectionValue::Unmaskable
            || (shape == AzureConnectionValue::ConnectionString
                && finder.findNamedArgument(nullptr, "account_key", start) >= 0))
            return false;
    }
    return true;
}

/// Hides a connection value that no rule here can show: a URL carrying a credential of its own
/// (`user:key@`, a SAS `?sig=...`), or a value that is not a plain string literal.
void maskUnshowableAzureConnectionValue(FunctionSecretArgumentsFinder & finder, size_t index, const AbstractFunction::Argument & value, bool argument_is_named)
{
    String text;
    if (!value.tryGetString(&text, /* allow_identifier= */ false) || classifyAzureConnectionValue(text) == AzureConnectionValue::Unmaskable)
        finder.markSecretArgument(index, argument_is_named);
}

/// The same, for the `connection_string` and `storage_account_url` overrides of a named collection from `start` on.
void maskUnshowableAzureConnectionOverrides(FunctionSecretArgumentsFinder & finder, size_t start)
{
    for (const auto * key : {"connection_string", "storage_account_url"})
    {
        for (ssize_t i = finder.findNamedArgument(nullptr, key, start); i >= 0; i = finder.findNamedArgument(nullptr, key, static_cast<size_t>(i) + 1))
        {
            const auto index = static_cast<size_t>(i);
            maskUnshowableAzureConnectionValue(finder, index, *finder.function->arguments->at(index)->getFunction()->arguments->at(1), true);
        }
    }
}

void findAzureBlobStorageFunctionSecretArguments(FunctionSecretArgumentsFinder & finder, bool is_cluster_function)
{
    /// azureBlobStorageCluster('cluster_name', 'conn_string/storage_account_url', ...) has 'conn_string/storage_account_url' as its second argument.
    size_t url_arg_idx = is_cluster_function ? 1 : 0;

    if (!is_cluster_function && finder.isNamedCollectionName(0))
    {
        /// azureBlobStorage(named_collection, ..., account_key = 'account_key', ...)
        if (maskAzureConnectionString(finder, -1, true, 1))
            return;
        maskUnshowableAzureConnectionOverrides(finder, 1);
        finder.findSecretNamedArgument("account_key", 1);
        return;
    }
    if (is_cluster_function && finder.isNamedCollectionName(1))
    {
        /// azureBlobStorageCluster(cluster, named_collection, ..., account_key = 'account_key', ...)
        if (maskAzureConnectionString(finder, -1, true, 2))
            return;
        maskUnshowableAzureConnectionOverrides(finder, 2);
        finder.findSecretNamedArgument("account_key", 2);
        return;
    }

    if (maskAzureConnectionString(finder, url_arg_idx))
        return;

    if (url_arg_idx < finder.function->arguments->size())
        maskUnshowableAzureConnectionValue(finder, url_arg_idx, *finder.function->arguments->at(url_arg_idx), false);

    /// We should check other arguments first because we don't need to do any replacement in case of
    /// azureBlobStorage(connection_string|storage_account_url, container_name, blobpath, format) -- in this case there is no account_key argument
    /// azureBlobStorageCluster(cluster, connection_string|storage_account_url, container_name, blobpath, format) -- in this case there is no account_key argument
    size_t count = finder.function->arguments->size();
    if ((url_arg_idx + 4 <= count) && (count <= url_arg_idx + 7))
    {
        String fourth_arg;
        if (finder.tryGetStringFromArgument(url_arg_idx + 3, &fourth_arg))
        {
            if (fourth_arg == "auto" || KnownFormatNames::instance().exists(fourth_arg))
                return;
        }
    }

    /// We're going to replace 'account_key' with '[HIDDEN]' if account_key is used in the signature
    if (url_arg_idx + 4 < count)
        finder.markSecretArgument(url_arg_idx + 4);
}

void findAzureBlobStorageTableEngineSecretArguments(FunctionSecretArgumentsFinder & finder)
{
    /// AzureBlobStorage(connection_string|storage_account_url, container_name, blobpath, format, [account_name, account_key, ...])
    size_t url_arg_idx = 0;

    if (finder.isNamedCollectionName(url_arg_idx))
    {
        /// AzureBlobStorage(named_collection, ..., account_key = 'account_key', ...)
        if (!azureCollectionArgumentsAreShowable(finder, url_arg_idx + 1, /* positional_limit= */ 0))
        {
            finder.maskEveryArgument();
            return;
        }
        if (maskAzureConnectionString(finder, -1, true, 1))
            return;
        finder.findSecretNamedArgument("account_key", 1);
        return;
    }

    /// We should check other arguments first because we don't need to do any replacement in case of
    /// AzureBlobStorage(connection_string|storage_account_url, container_name, blobpath, format) -- in this case there is no account_key argument
    size_t count = finder.function->arguments->size();
    bool fourth_argument_is_format = false;
    if ((url_arg_idx + 4 <= count) && (count <= url_arg_idx + 7))
    {
        String fourth_arg;
        if (finder.tryGetStringFromArgument(url_arg_idx + 3, &fourth_arg))
            fourth_argument_is_format = fourth_arg == "auto" || KnownFormatNames::instance().exists(fourth_arg);
    }
    /// Which argument holds a credential: the two-argument shape takes a shared access signature beside
    /// the url (`endpoint.sas_auth`), the longer ones an `account_key` - unless the fourth names a format.
    std::optional<size_t> credential_arg_idx;
    if (count == url_arg_idx + 2)
        credential_arg_idx = url_arg_idx + 1;
    else if (!fourth_argument_is_format && (url_arg_idx + 4 < count))
        credential_arg_idx = url_arg_idx + 4;

    /// The engine reads this argument as a connection string or as a plain account url; a value of
    /// another shape is read by neither rule below, and a hidden connection string replaces it whole.
    String connection_value;
    const auto shape = finder.tryGetStringFromArgument(url_arg_idx, &connection_value)
        ? classifyAzureConnectionValue(connection_value)
        : AzureConnectionValue::Unmaskable;
    if (shape == AzureConnectionValue::Unmaskable || (shape == AzureConnectionValue::ConnectionString && credential_arg_idx))
    {
        finder.maskEveryArgument();
        return;
    }

    if (maskAzureConnectionString(finder, url_arg_idx))
        return;

    if (credential_arg_idx)
        finder.markSecretArgument(*credential_arg_idx);
}

/// A backup destination reads a different signature than the table engine of the same name, so the
/// table-engine rule leaves an argument it does not model visible.
void findAzureBlobStorageBackupSecretArguments(FunctionSecretArgumentsFinder & finder)
{
    /// The destination reads AzureBlobStorage(named_collection [, 'filename'] [, key = value, ...]),
    /// ('connection_string|storage_account_url', 'container', 'path'), or those three followed by
    /// ('account_name', 'account_key'). An argument no shape reads holds whatever was written in it.
    const size_t count = finder.function->arguments->size();

    if (finder.isNamedCollectionName(0))
    {
        if (!azureCollectionArgumentsAreShowable(finder, 1, /* positional_limit= */ 1))
        {
            finder.maskEveryArgument();
            return;
        }
        if (maskAzureConnectionString(finder, -1, /* argument_is_named= */ true, 1))
            return;
        finder.findSecretNamedArgument("account_key", 1);
        return;
    }

    if ((count != 3 && count != 5) || !FunctionSecretArgumentsFinder::hasOnlyLiteralArguments(*finder.function))
    {
        finder.maskEveryArgument();
        return;
    }

    if (count == 3)
    {
        /// Only this shape accepts a connection string, which can embed `AccountKey`. A value that is no
        /// string is read by neither the classification below nor the destination.
        String connection_value;
        if (!finder.tryGetStringFromArgument(0, &connection_value)
            || classifyAzureConnectionValue(connection_value) == AzureConnectionValue::Unmaskable)
        {
            finder.maskEveryArgument();
            return;
        }
        maskAzureConnectionString(finder, 0);
        return;
    }

    String storage_account_url;
    if (!finder.tryGetStringFromArgument(0, &storage_account_url)
        || classifyAzureConnectionValue(storage_account_url) != AzureConnectionValue::PlainStorageAccountURL)
    {
        /// This shape requires a plain account URL. A connection string here can only be hidden whole,
        /// which cannot be combined with hiding `account_key`.
        finder.maskEveryArgument();
        return;
    }
    finder.markSecretArgument(4);
}

}

SecretArgumentsSpec azureTableFunctionSecretArguments(bool is_cluster_function)
{
    /// azureBlobStorage(connection_string|storage_account_url, container_name, blobpath, account_name, account_key, format, compression, structure)
    /// azureBlobStorageCluster(cluster, connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression, structure])
    return {.custom = [is_cluster_function](FunctionSecretArgumentsFinder & finder)
            { findAzureBlobStorageFunctionSecretArguments(finder, is_cluster_function); }};
}

SecretArgumentsSpec azureTableEngineSecretArguments()
{
    return {.custom = findAzureBlobStorageTableEngineSecretArguments};
}

SecretArgumentsSpec azureBackupSecretArguments()
{
    return {.custom = findAzureBlobStorageBackupSecretArguments};
}

}
