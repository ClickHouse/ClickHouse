#include <Interpreters/FunctionSecretArgumentsFinder.h>

#include <algorithm>

#include <base/defines.h>

namespace DB
{

void FunctionSecretArgumentsFinder::apply(const SecretArgumentsSpec & spec, bool generic_rules)
{
    if (!function->hasArguments() || !spec.hasArgumentSecrets())
        return;

    if (generic_rules && !spec.custom_reads_named_arguments)
    {
        /// The query is formatted for logging before validation rejects a positional argument after a
        /// `key = value` one, and the slot it was meant for is unknowable: hide every such positional.
        classifyPositionalArguments();
        markNamedArgumentsWithUnreadableKeys(0);
    }

    findPositionalAndNamedSecretArguments(spec);

    if (spec.custom)
        spec.custom(*this);
}

void FunctionSecretArgumentsFinder::findPositionalAndNamedSecretArguments(const SecretArgumentsSpec & spec)
{
    /// Not gated on the named-collection form: an identifier can also be a positional endpoint
    /// (`redis(localhost, ...)`), and a collection rejects positional overrides anyway.
    for (const size_t slot : spec.positional_secret_slots)
    {
        const auto positional = classifyPositionalArguments();
        if (slot < positional.size())
            markSecretArgument(positional[slot]);

        /// The explicit form constant-folds its positionals, so an `equals` at the secret slot can be the
        /// folded secret with the secret as its left operand (`Redis('host:port', 0, 'password' = 'x')`):
        /// hide it whole. An identifier first argument does not prove a named collection (`redis(localhost, ...)`),
        /// so there only an identifier left operand (`port = 6379`), which does not fold, is kept as an override.
        if (slot < function->arguments->size())
        {
            const auto equals_func = function->arguments->at(slot)->getFunction();
            if (equals_func && equals_func->name() == "equals"
                && (!function->arguments->at(0)->isIdentifier() || !equals_func->arguments || equals_func->arguments->size() != 2
                    || !equals_func->arguments->at(0)->isIdentifier()))
                markSecretArgument(slot);
        }
    }

    for (const auto & key : spec.secret_keys)
        findSecretNamedArgument(key);
}

void FunctionSecretArgumentsFinder::maskEveryArgument()
{
    for (size_t i = 0, size = function->arguments->size(); i < size; ++i)
        markSecretArgument(i);
}

bool FunctionSecretArgumentsFinder::hasOnlyLiteralArguments(const AbstractFunction & function)
{
    if (!function.hasArguments())
        return true;
    for (size_t i = 0, size = function.arguments->size(); i < size; ++i)
        if (!function.arguments->at(i)->tryGetLiteralText(nullptr))
            return false;
    return true;
}

void FunctionSecretArgumentsFinder::markSecretArgument(size_t index, bool argument_is_named)
{
    if (index >= function->arguments->size())
        return;
    chassert(result.replacement.empty()); /// We shouldn't use replacement with masking other arguments
    /// Each argument is masked individually: valid S3 syntax can interleave secrets with non-secret
    /// arguments, which a contiguous span cannot represent without hiding the arguments in between.
    /// A malformed query can mark the same index as both named and positional; the positional form
    /// wins, hiding the argument whole (fail closed).
    auto [it, inserted] = result.masked_arguments.emplace(index, argument_is_named);
    if (!inserted)
        it->second &= argument_is_named;
}

void FunctionSecretArgumentsFinder::maskNestedSecretMaps()
{
    for (size_t i = 0, size = function->arguments->size(); i < size; ++i)
    {
        const auto f = function->arguments->at(i)->getFunction();
        if (!f)
            continue;
        const auto name = f->name();
        if ((name == "headers" || name == "extra_credentials")
            && std::find(result.nested_maps.begin(), result.nested_maps.end(), name) == result.nested_maps.end())
            result.nested_maps.push_back(name);
    }
}

std::vector<size_t> FunctionSecretArgumentsFinder::classifyPositionalArguments(size_t start)
{
    std::vector<size_t> positional;
    bool seen_named = false;
    for (size_t i = start; i < function->arguments->size(); ++i)
    {
        const auto argument = function->arguments->at(i);
        if (argument->isSettings())
            continue;

        const auto equals_func = argument->getFunction();
        if (equals_func && equals_func->name() == "equals" && equals_func->hasArguments()
            && equals_func->arguments->size() == 2)
        {
            seen_named = true;
            continue;
        }

        if (seen_named)
        {
            markSecretArgument(i);
            continue;
        }

        positional.push_back(i);
    }
    return positional;
}

void FunctionSecretArgumentsFinder::markNamedArgumentsWithUnreadableKeys(size_t start)
{
    /// The named-collection parser does not require the key of a `key = value` argument to be a plain
    /// literal or identifier: `getKeyValueFromASTImpl` evaluates it as a constant expression, so
    /// `mysql(creds, concat('ssl_ca', '_pem') = 'SECRET', table = 't')` passes a TLS credential too.
    /// This finder works on the AST alone and cannot evaluate an expression, so it fails closed: the
    /// value of every argument whose key it cannot read is hidden. Hiding the value of a non-secret
    /// argument written that way is harmless, while leaving a credential visible is not.
    for (size_t i = start; i < function->arguments->size(); ++i)
    {
        const auto equals_func = function->arguments->at(i)->getFunction();
        if (!equals_func || (equals_func->name() != "equals"))
            continue;

        if (!equals_func->arguments || equals_func->arguments->size() != 2)
            continue;

        if (tryGetStringFromArgument(*equals_func->arguments->at(0), nullptr))
            continue;

        markSecretArgument(i, /* argument_is_named= */ true);
    }
}

void FunctionSecretArgumentsFinder::findBrokerSecretArguments(
    std::span<const std::string_view> secret_keys, std::string_view address_key)
{
    /// NATS(named_collection [, nats_password = 'password'] [, nats_token = 'token']
    ///      [, nats_credential_file = '/path'] [, nats_credentials = 'user JWT and seed']
    ///      [, nats_url = 'nats://user:password@host:4222']
    ///      [, nats_server_list = 'nats://user:password@host:4222,...'], ...)
    /// RabbitMQ(named_collection [, rabbitmq_password = '...'] [, rabbitmq_address = 'amqp://user:pass@host'], ...)
    /// The only positional argument these engines accept is the name of a named collection, so the
    /// credentials can only appear as named overrides. The `SETTINGS` clause form is masked
    /// separately by the engine's own `SETTINGS_TO_HIDE`, which this function must stay in sync with.
    /// A destination key (`nats_server_list`) is hidden whole: each list entry can carry userinfo.
    /// Fail closed on a key we cannot read as a plain literal: it can name a secret setting.
    for (size_t i = 0; i < function->arguments->size(); ++i)
    {
        const auto equals_func = function->arguments->at(i)->getFunction();
        if (!equals_func || equals_func->name() != "equals" || !equals_func->hasArguments()
            || equals_func->arguments->size() != 2)
        {
            /// The engine accepts no positional arguments except the collection name in the first
            /// position, but it rejects them only after the query has been formatted for logging.
            /// A malformed positional argument can carry a secret (a credential file path, a url
            /// with a password), so hide it whole rather than leak it (fail closed).
            if (i > 0 || !function->arguments->at(i)->isIdentifier())
                markSecretArgument(i, /* argument_is_named= */ false);
            continue;
        }

        String key;
        if (!equals_func->arguments->at(0)->tryGetString(&key, /* allow_identifier= */ true))
        {
            markSecretArgument(i, /* argument_is_named= */ true);
        }
        else if (key == address_key)
        {
            String url;
            if (equals_func->arguments->at(1)->tryGetString(&url, /* allow_identifier= */ false))
            {
                /// An '@' is the only reliable sign of a credential here; see the engine's `_fwd.h`.
                if (url.contains('@'))
                    markSecretArgument(i, /* argument_is_named= */ true);
            }
            else
            {
                /// A url built from a constant expression can embed credentials in its pieces, which
                /// we cannot evaluate here; hide it whole rather than leak.
                markSecretArgument(i, /* argument_is_named= */ true);
            }
        }
        else if (std::find(secret_keys.begin(), secret_keys.end(), key) != secret_keys.end())
        {
            markSecretArgument(i, /* argument_is_named= */ true);
        }
    }
}

bool FunctionSecretArgumentsFinder::tryGetStringFromArgument(size_t arg_idx, String * res, bool allow_identifier) const
{
    if (arg_idx >= function->arguments->size())
        return false;

    return tryGetStringFromArgument(*function->arguments->at(arg_idx), res, allow_identifier);
}

bool FunctionSecretArgumentsFinder::tryGetStringFromArgument(const AbstractFunction::Argument & argument, String * res, bool allow_identifier)
{
    return argument.tryGetString(res, allow_identifier);
}

bool FunctionSecretArgumentsFinder::isNamedCollectionName(size_t arg_idx) const
{
    if (function->arguments->size() <= arg_idx)
        return false;

    return function->arguments->at(arg_idx)->isIdentifier();
}

ssize_t FunctionSecretArgumentsFinder::findNamedArgument(String * res, std::string_view key, size_t start)
{
    for (size_t i = start; i < function->arguments->size(); ++i)
    {
        const auto & argument = function->arguments->at(i);
        const auto equals_func = argument->getFunction();
        if (!equals_func || (equals_func->name() != "equals"))
            continue;

        if (!equals_func->arguments || equals_func->arguments->size() != 2)
            continue;

        String found_key;
        if (!tryGetStringFromArgument(*equals_func->arguments->at(0), &found_key))
            continue;

        if (found_key == key)
        {
            tryGetStringFromArgument(*equals_func->arguments->at(1), res);
            return i;
        }
    }

    return -1;
}

bool FunctionSecretArgumentsFinder::findSecretNamedArgument(std::string_view key, size_t start)
{
    bool found = false;
    for (ssize_t arg_idx = findNamedArgument(nullptr, key, start); arg_idx >= 0;
         arg_idx = findNamedArgument(nullptr, key, static_cast<size_t>(arg_idx) + 1))
    {
        markSecretArgument(arg_idx, /* argument_is_named= */ true);
        found = true;
    }
    return found;
}

}
