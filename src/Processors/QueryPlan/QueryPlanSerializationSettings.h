#pragma once

#include <Core/BaseSettings.h>
#include <Core/SettingsEnums.h>
#include <Core/Defines.h>


namespace DB
{

#define PLAN_SERIALIZATION_SETTINGS(M, ALIAS) \
    M(UInt64, max_rows_in_distinct, 0, "Maximum number of elements during execution of DISTINCT.", 0) \
    M(UInt64, max_bytes_in_distinct, 0, "Maximum total size of state (in uncompressed bytes) in memory for the execution of DISTINCT.", 0) \
    M(OverflowMode, distinct_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.", 0) \

DECLARE_SETTINGS_TRAITS(QueryPlanSerializationSettingsTraits, PLAN_SERIALIZATION_SETTINGS)

struct QueryPlanSerializationSettings : public BaseSettings<QueryPlanSerializationSettingsTraits>
{
    QueryPlanSerializationSettings() = default;
};

}
