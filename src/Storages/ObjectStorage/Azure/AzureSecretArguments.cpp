#include <Storages/ObjectStorage/Azure/AzureSecretArguments.h>

#include <algorithm>
#include <optional>
#include <unordered_set>

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

    if (!url_arg.starts_with("http") && maskAzureConnectionStringSecrets(url_arg))
    {
        chassert(result.count == 0); /// We shouldn't use replacement with masking other arguments
        result.start = url_arg_idx;
        result.are_named = argument_is_named;
        result.count = 1;
        result.replacement = url_arg;
        return true;
    }

    return false;
}

/// Hides every `extra_credentials(...)` whole, its keys included: a key can be written as anything, a credential too.
void maskAzureExtraCredentials(FunctionSecretArgumentsFinder & finder)
{
    for (size_t i = 0, size = finder.function->arguments->size(); i < size; ++i)
    {
        if (const auto f = finder.function->arguments->at(i)->getFunction(); f && f->name() == "extra_credentials")
            finder.markSecretArgument(i);
    }
}

/// The keys an Azure named collection accepts (`StorageAzureConfiguration`). An override with any
/// other key, which can name any credential, is rejected only after the statement is logged.
bool isAzureCollectionKey(std::string_view key)
{
    static const std::unordered_set<std::string_view> keys = {"blob_path", "container", "format", "compression", "structure",
        "compression_method", "account_name", "account_key", "connection_string", "storage_account_url", "partition_strategy",
        "partition_columns_in_data_file", "client_id", "tenant_id"};
    return keys.contains(key);
}

/// The key of a `key = value` argument if it is one of those keys. Any other left operand can be the
/// credential itself (`'sp=r&sig=...' = 'x'`), so its argument is hidden whole.
std::optional<String> tryGetAzureKey(const AbstractFunction & equals)
{
    String key;
    if (equals.arguments && equals.arguments->size() == 2
        && FunctionSecretArgumentsFinder::tryGetStringFromArgument(*equals.arguments->at(0), &key) && isAzureCollectionKey(key))
        return key;
    return {};
}

/// Hides every override from `start` on that this rule cannot read: one whose key an Azure named collection does not
/// accept or whose value is no plain literal (it can nest a credential), and every connection override when there are
/// several: only one is read, and only one can be partly shown.
void maskUnreadAzureCollectionOverrides(FunctionSecretArgumentsFinder & finder, size_t start)
{
    std::vector<size_t> connection_overrides;
    for (size_t i = start, size = finder.function->arguments->size(); i < size; ++i)
    {
        const auto f = finder.function->arguments->at(i)->getFunction();
        if (!f || f->name() != "equals")
            continue;
        const auto key = tryGetAzureKey(*f);
        if (!key)
            finder.markSecretArgument(i);
        else if (!(FunctionSecretArgumentsFinder::tryGetStringFromArgument(*f->arguments->at(1), nullptr)
                   || f->arguments->at(1)->tryGetLiteralText(nullptr)))
            finder.markSecretArgument(i, /* argument_is_named= */ true);
        if (key == "connection_string" || key == "storage_account_url")
            connection_overrides.push_back(i);
    }

    if (connection_overrides.size() > 1)
        for (const size_t i : connection_overrides)
            finder.markSecretArgument(i, /* argument_is_named= */ true);
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
            String key;
            if (argument_function->arguments && argument_function->arguments->size() == 2
                && FunctionSecretArgumentsFinder::tryGetStringFromArgument(*argument_function->arguments->at(0), &key)
                && isAzureCollectionKey(key)
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

/// The raw indexes of the arguments the Azure parser assigns slots to, the ones before `start` (the cluster name) as
/// written: it strips the `SETTINGS` clause and the first `extra_credentials(...)`, and a `key = value` takes no slot.
/// Also hides the value of a key the explicit form does not read (all of an argument whose key is no Azure key), and
/// every argument when a second `extra_credentials(...)` shifts the slots.
std::vector<size_t> azureSlotArguments(FunctionSecretArgumentsFinder & finder, size_t start)
{
    std::vector<size_t> result;
    size_t extra_credentials = 0;
    for (size_t i = 0, size = finder.function->arguments->size(); i < size; ++i)
    {
        const auto argument = finder.function->arguments->at(i);
        if (argument->isSettings())
            continue;
        const auto f = i < start ? nullptr : argument->getFunction();
        if (f && f->name() == "extra_credentials")
        {
            ++extra_credentials;
            continue;
        }
        if (f && f->name() == "equals" && f->arguments && f->arguments->size() == 2)
        {
            const auto key = tryGetAzureKey(*f);
            if (!key)
                finder.markSecretArgument(i);
            else if ((key != "partition_strategy" && key != "partition_columns_in_data_file") || !f->arguments->at(1)->tryGetLiteralText(nullptr))
                finder.markSecretArgument(i, /* argument_is_named= */ true);
            continue;
        }
        result.push_back(i);
    }

    if (extra_credentials > 1)
    {
        finder.maskEveryArgument();
        return {};
    }
    return result;
}

/// The raw index of the argument holding a credential, if any: the two-argument shape takes a shared access signature
/// beside the url (`endpoint.sas_auth`), the longer ones an `account_key` - unless the fourth names a format.
std::optional<size_t> azureCredentialArgument(const FunctionSecretArgumentsFinder & finder, const std::vector<size_t> & slot_arguments, size_t url_arg_idx)
{
    const size_t count = slot_arguments.size();
    if (count == url_arg_idx + 2)
        return slot_arguments[url_arg_idx + 1];
    if (count <= url_arg_idx + 4)
        return {};
    String fourth_arg;
    if (count <= url_arg_idx + 7 && finder.tryGetStringFromArgument(slot_arguments[url_arg_idx + 3], &fourth_arg)
        && (fourth_arg == "auto" || KnownFormatNames::instance().exists(fourth_arg)))
        return {};
    return slot_arguments[url_arg_idx + 4];
}

/// After the collection name every argument must be an override or `extra_credentials(...)`; a positional one is
/// invalid but logged before validation rejects it, and can be a credential.
void maskPositionalsAfterAzureCollection(FunctionSecretArgumentsFinder & finder, size_t start)
{
    for (size_t i = start, size = finder.function->arguments->size(); i < size; ++i)
    {
        const auto f = finder.function->arguments->at(i)->getFunction();
        if (!f || (f->name() != "equals" && f->name() != "extra_credentials"))
            finder.markSecretArgument(i);
    }
}

void findAzureBlobStorageFunctionSecretArguments(FunctionSecretArgumentsFinder & finder, bool is_cluster_function)
{
    /// azureBlobStorageCluster('cluster_name', 'conn_string/storage_account_url', ...) has 'conn_string/storage_account_url' as its second argument.
    size_t url_arg_idx = is_cluster_function ? 1 : 0;

    maskAzureExtraCredentials(finder);

    /// The other values are hidden before a connection string is: hiding that replaces its whole argument.
    if (!is_cluster_function && finder.isNamedCollectionName(0))
    {
        /// azureBlobStorage(named_collection, ..., account_key = 'account_key', ...)
        maskPositionalsAfterAzureCollection(finder, 1);
        maskUnreadAzureCollectionOverrides(finder, 1);
        finder.findSecretNamedArgument("account_key", 1);
        if (maskAzureConnectionString(finder, -1, true, 1))
            return;
        maskUnshowableAzureConnectionOverrides(finder, 1);
        return;
    }
    if (is_cluster_function && finder.isNamedCollectionName(1))
    {
        /// azureBlobStorageCluster(cluster, named_collection, ..., account_key = 'account_key', ...)
        maskPositionalsAfterAzureCollection(finder, 2);
        maskUnreadAzureCollectionOverrides(finder, 2);
        finder.findSecretNamedArgument("account_key", 2);
        if (maskAzureConnectionString(finder, -1, true, 2))
            return;
        maskUnshowableAzureConnectionOverrides(finder, 2);
        return;
    }

    /// Slot `n` is the raw argument `arg(n)`; past the end it is an index no argument has.
    const auto slot_arguments = azureSlotArguments(finder, url_arg_idx);
    auto arg = [&](size_t slot) { return slot < slot_arguments.size() ? slot_arguments[slot] : finder.function->arguments->size(); };

    if (const auto credential_arg_idx = azureCredentialArgument(finder, slot_arguments, url_arg_idx))
        finder.markSecretArgument(*credential_arg_idx);

    if (maskAzureConnectionString(finder, arg(url_arg_idx)))
        return;

    if (arg(url_arg_idx) < finder.function->arguments->size())
        maskUnshowableAzureConnectionValue(finder, arg(url_arg_idx), *finder.function->arguments->at(arg(url_arg_idx)), false);
}

void findAzureBlobStorageTableEngineSecretArguments(FunctionSecretArgumentsFinder & finder)
{
    /// AzureBlobStorage(connection_string|storage_account_url, container_name, blobpath, format, [account_name, account_key, ...])
    size_t url_arg_idx = 0;

    maskAzureExtraCredentials(finder);

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

    /// Slot `n` is the raw argument `arg(n)`; past the end it is an index no argument has.
    const auto slot_arguments = azureSlotArguments(finder, url_arg_idx);
    auto arg = [&](size_t slot) { return slot < slot_arguments.size() ? slot_arguments[slot] : finder.function->arguments->size(); };
    const auto credential_arg_idx = azureCredentialArgument(finder, slot_arguments, url_arg_idx);

    /// The engine reads this argument as a connection string or as a plain account url; a value of
    /// another shape is read by neither rule below, and a hidden connection string replaces it whole.
    String connection_value;
    const auto shape = finder.tryGetStringFromArgument(arg(url_arg_idx), &connection_value)
        ? classifyAzureConnectionValue(connection_value)
        : AzureConnectionValue::Unmaskable;
    if (shape == AzureConnectionValue::Unmaskable || (shape == AzureConnectionValue::ConnectionString && credential_arg_idx))
    {
        finder.maskEveryArgument();
        return;
    }

    if (maskAzureConnectionString(finder, arg(url_arg_idx)))
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
