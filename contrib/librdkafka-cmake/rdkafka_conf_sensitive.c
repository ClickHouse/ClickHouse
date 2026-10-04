/// Wrapper around librdkafka's rdkafka_conf.c: compiles the file (this is the only place it is
/// compiled, see the SRCS list in contrib/librdkafka-cmake/CMakeLists.txt) and adds an accessor
/// for the static rd_kafka_properties table, which is the source of truth for the _RK_SENSITIVE
/// flag. ClickHouse uses the flag to hide the values of sensitive properties (e.g. sasl.password)
/// in its logs. No public librdkafka API exposes the flag: the redacting dump
/// (rd_kafka_anyconf_dump with redact_sensitive) is static, and the public rd_kafka_conf_dump
/// does not redact.

#include "rdkafka_conf.c"

#include <pthread.h>

/* The table ends with a terminator entry, so the count is an upper bound and the
 * array is always NULL-terminated. The names point into the static table. */
static const char * chrd_sensitive_names[sizeof(rd_kafka_properties) / sizeof(*rd_kafka_properties)];
static pthread_once_t chrd_sensitive_names_once = PTHREAD_ONCE_INIT;

static void chrd_fill_sensitive_names(void)
{
    const struct rd_kafka_property * prop = NULL;
    size_t n = 0;
    for (prop = rd_kafka_properties; prop->name; prop++)
        if (prop->scope & _RK_SENSITIVE)
            chrd_sensitive_names[n++] = prop->name;
}

const char * const * chrd_kafka_conf_sensitive_properties(void)
{
    pthread_once(&chrd_sensitive_names_once, chrd_fill_sensitive_names);
    return chrd_sensitive_names;
}
