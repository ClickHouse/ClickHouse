#include <Interpreters/JoinInfo.h>

namespace DB
{

namespace Setting
{
#define DECLARE_JOIN_SETTINGS_EXTERN(type, name) \
    extern const Settings##type name;

    APPLY_FOR_JOIN_SETTINGS(DECLARE_JOIN_SETTINGS_EXTERN)
#undef DECLARE_JOIN_SETTINGS_EXTERN
}

JoinSettings JoinSettings::create(const Settings & query_settings)
{
    JoinSettings join_settings;

#define COPY_JOIN_SETTINGS_FROM_QUERY(type, name) \
    join_settings.name = query_settings[Setting::name];

    APPLY_FOR_JOIN_SETTINGS(COPY_JOIN_SETTINGS_FROM_QUERY)
#undef COPY_JOIN_SETTINGS_FROM_QUERY

    return join_settings;
}

}
