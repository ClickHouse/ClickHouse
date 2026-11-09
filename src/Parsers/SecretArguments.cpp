#include <Parsers/SecretArguments.h>

#include <Common/Exception.h>

#include <atomic>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static std::atomic<const ISecretArgumentsFinder *> secret_arguments_finder = nullptr;

const NoSecretArgumentsFinder & NoSecretArgumentsFinder::instance()
{
    static const NoSecretArgumentsFinder finder;
    return finder;
}

void setSecretArgumentsFinder(const ISecretArgumentsFinder * finder)
{
    secret_arguments_finder = finder;
}

const ISecretArgumentsFinder & getSecretArgumentsFinder()
{
    const auto * finder = secret_arguments_finder.load();
    if (!finder)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The secret arguments finder is not installed");
    return *finder;
}

}
