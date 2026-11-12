/// See contrib/librdkafka-cmake/rdkafka_conf_sensitive.c
#pragma once

#ifdef __cplusplus
extern "C" {
#endif

/// The names of the configuration properties that librdkafka marks with the _RK_SENSITIVE flag or stores
/// as pointers (memory addresses), i.e. whose values must not appear in logs, as a NULL-terminated array of static strings.
const char * const * chrd_kafka_conf_sensitive_properties(void);

#ifdef __cplusplus
}
#endif
