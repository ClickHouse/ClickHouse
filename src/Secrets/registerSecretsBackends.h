#pragma once

namespace DB
{
    class SecretsManager;

    void registerSecretsBackends(SecretsManager & manager);
};
