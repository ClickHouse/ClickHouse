#include <Storages/Kafka/KafkaSettings.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTFunction.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
    extern const int BAD_ARGUMENTS;
}

IMPLEMENT_SETTINGS_TRAITS(KafkaSettingsTraits, LIST_OF_KAFKA_SETTINGS)

void KafkaSettings::loadFromQuery(ASTStorage & storage_def)
{
    if (storage_def.settings)
    {
        try
        {
            applyChanges(storage_def.settings->changes);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::UNKNOWN_SETTING)
                e.addMessage("for storage " + storage_def.engine->name);
            throw;
        }
    }
    else
    {
        auto settings_ast = std::make_shared<ASTSetQuery>();
        settings_ast->is_standalone = false;
        storage_def.set(storage_def.settings, settings_ast);
    }
}

void KafkaSettings::sanityCheck() const
{
    if (kafka_consumers_pool_ttl_ms < KAFKA_RESCHEDULE_MS)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The value of 'kafka_consumers_pool_ttl_ms' ({}) cannot be less then rescheduled interval ({})",
            kafka_consumers_pool_ttl_ms, KAFKA_RESCHEDULE_MS);

    if (kafka_consumers_pool_ttl_ms > KAFKA_CONSUMERS_POOL_TTL_MS_MAX)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The value of 'kafka_consumers_pool_ttl_ms' ({}) cannot be too big (greater then {}), since this may cause live memory leaks",
            kafka_consumers_pool_ttl_ms, KAFKA_CONSUMERS_POOL_TTL_MS_MAX);
}

}
