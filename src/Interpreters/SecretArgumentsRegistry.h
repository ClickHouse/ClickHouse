#pragma once

#include <Parsers/SecretArguments.h>

namespace DB
{

/// Finds secrets with the `SecretArgumentsSpec` that each factory registration declares: table functions, then
/// functions for `ORDINARY_FUNCTION`; table, database and backup engines; dictionary sources and engine settings.
class SecretArgumentsRegistry final : public ISecretArgumentsFinder
{
public:
    static const SecretArgumentsRegistry & instance();

    SecretArgumentsResult find(ASTFunction::Kind kind, const AbstractFunction & function) const override;
    std::optional<String> renderSecretSetting(const String & name, const Field & value) const override;
    bool maskDictionarySourceValue(const String & key, String & value) const override;
};

}
