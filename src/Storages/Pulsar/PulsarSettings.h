#pragma once

#include <Core/BaseSettings.h>
#include <Core/Settings.h>


namespace DB
{
class ASTStorage;

// const auto KAFKA_RESCHEDULE_MS = 500;
// const auto KAFKA_CLEANUP_TIMEOUT_MS = 3000;
// once per minute leave do reschedule (we can't lock threads in pool forever)
// const auto KAFKA_MAX_THREAD_WORK_DURATION_MS = 60000;
// 10min
// const auto KAFKA_CONSUMERS_POOL_TTL_MS_MAX = 600'000;

#define PULSAR_RELATED_SETTINGS(M, ALIAS) \
    M(String, pulsar_host_port, "", "A host of broker for Pulsar engine.", 0) \
    M(String, pulsar_topic_list, "", "A list of Pulsar topics.", 0) \
    M(UInt64, pulsar_num_consumers, 1, "The number of consumers per table for Pulsar engine.", 0) \

#define OBSOLETE_PULSAR_SETTINGS(M, ALIAS) \
    MAKE_OBSOLETE(M, Char, pulsar_row_delimiter, '\0') \

#define LIST_OF_PULSAR_SETTINGS(M, ALIAS)  \
    PULSAR_RELATED_SETTINGS(M, ALIAS)      \
    OBSOLETE_PULSAR_SETTINGS(M, ALIAS)     \
    LIST_OF_ALL_FORMAT_SETTINGS(M, ALIAS) \

DECLARE_SETTINGS_TRAITS(PulsarSettingsTraits, LIST_OF_PULSAR_SETTINGS)


/** Settings for the Pulsar engine.
  * Could be loaded from a CREATE TABLE query (SETTINGS clause).
  */
struct PulsarSettings : public BaseSettings<PulsarSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);

    // void sanityCheck() const;
};

}
