#include <Storages/MergeTree/WhatIfSettings.h>

#include <Core/Field.h>
#include <Parsers/ASTSetQuery.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_SETTING_VALUE;
    extern const int UNKNOWN_SETTING;
}

WhatIfSettings WhatIfSettings::fromAST(const ASTPtr & settings_ast)
{
    WhatIfSettings result;
    if (!settings_ast)
        return result;

    const auto * set_query = settings_ast->as<ASTSetQuery>();
    if (!set_query)
        return result;

    for (const auto & change : set_query->changes)
    {
        if (change.name == "empirical")
        {
            if (change.value.getType() != Field::Types::UInt64)
                throw Exception(
                    ErrorCodes::INVALID_SETTING_VALUE,
                    "Invalid type {} for setting '{}' in EXPLAIN WHATIF, expected an integer 0 or 1",
                    change.value.getTypeName(),
                    change.name);

            auto value = change.value.safeGet<UInt64>();
            if (value > 1)
                throw Exception(
                    ErrorCodes::INVALID_SETTING_VALUE,
                    "Invalid value {} for setting '{}' in EXPLAIN WHATIF, expected 0 or 1",
                    value,
                    change.name);

            result.empirical = value != 0;
        }
        else
        {
            throw Exception(
                ErrorCodes::UNKNOWN_SETTING,
                "Unknown setting \"{}\" for EXPLAIN WHATIF query. Supported settings: empirical",
                change.name);
        }
    }
    return result;
}

}
