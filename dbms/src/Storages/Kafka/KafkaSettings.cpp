#include <Common/config.h>
#if USE_RDKAFKA

#include <Storages/Kafka/KafkaSettings.h>
#include <Parsers/ASTCreateQuery.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void KafkaSettings::loadFromQuery(ASTStorage & storage_def)
{
    if (storage_def.settings)
    {
        for (const ASTSetQuery::Change & setting : storage_def.settings->changes)
        {
#define SET(TYPE, NAME, DEFAULT, DESCRIPTION) \
            else if (setting.name == #NAME) NAME.set(setting.value);

            if (false) {}
            APPLY_FOR_KAFKA_SETTINGS(SET)
            else
                throw Exception(
                    "Unknown setting " + setting.name + " for storage " + storage_def.engine->name,
                    ErrorCodes::BAD_ARGUMENTS);
#undef SET
        }
    }
    else
    {
        auto settings_ast = std::make_shared<ASTSetQuery>();
        settings_ast->is_standalone = false;
        storage_def.set(storage_def.settings, settings_ast);
    }
}

}
#endif
