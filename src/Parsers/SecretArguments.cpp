#include <Parsers/SecretArguments.h>

#include <atomic>

namespace DB
{

static std::atomic<const ISecretArgumentsFinder *> secret_arguments_finder = nullptr;

void setSecretArgumentsFinder(const ISecretArgumentsFinder * finder)
{
    secret_arguments_finder = finder;
}

const ISecretArgumentsFinder * getSecretArgumentsFinder()
{
    return secret_arguments_finder;
}

}
