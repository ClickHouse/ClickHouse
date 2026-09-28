/// Compile a private copy of librdkafka's rdkafka_conf.c to gain access to the static
/// rd_kafka_properties table, which is the source of truth for the _RK_SENSITIVE flag.
/// ClickHouse uses the flag to hide the values of sensitive properties (e.g. sasl.password)
/// in its logs. No public librdkafka API exposes the flag: the redacting dump
/// (rd_kafka_anyconf_dump with redact_sensitive) is static, and the public rd_kafka_conf_dump
/// does not redact.
///
/// Every extern symbol that rdkafka_conf.c defines is renamed with a chrd_ prefix, so this
/// copy does not clash with the real library at link time. If a future librdkafka version
/// adds a new extern symbol to rdkafka_conf.c, the build fails with a duplicate-symbol
/// link error - add the rename below.

#define rd_kafka_anyconf_destroy                            chrd_kafka_anyconf_destroy
#define rd_kafka_anyconf_dump_dbg                           chrd_kafka_anyconf_dump_dbg
#define rd_kafka_conf                                       chrd_kafka_conf
#define rd_kafka_conf_desensitize                           chrd_kafka_conf_desensitize
#define rd_kafka_conf_destroy                               chrd_kafka_conf_destroy
#define rd_kafka_conf_dump                                  chrd_kafka_conf_dump
#define rd_kafka_conf_dump_free                             chrd_kafka_conf_dump_free
#define rd_kafka_conf_dup                                   chrd_kafka_conf_dup
#define rd_kafka_conf_dup_filter                            chrd_kafka_conf_dup_filter
#define rd_kafka_conf_enable_sasl_queue                     chrd_kafka_conf_enable_sasl_queue
#define rd_kafka_conf_finalize                              chrd_kafka_conf_finalize
#define rd_kafka_conf_finalize_oauthbearer_oidc             chrd_kafka_conf_finalize_oauthbearer_oidc
#define rd_kafka_conf_finalize_oauthbearer_oidc_grant_type  chrd_kafka_conf_finalize_oauthbearer_oidc_grant_type
#define rd_kafka_conf_get                                   chrd_kafka_conf_get
#define rd_kafka_conf_get_default_topic_conf                chrd_kafka_conf_get_default_topic_conf
#define rd_kafka_conf_is_modified                           chrd_kafka_conf_is_modified
#define rd_kafka_conf_kv_get                                chrd_kafka_conf_kv_get
#define rd_kafka_conf_kv_split                              chrd_kafka_conf_kv_split
#define rd_kafka_conf_new                                   chrd_kafka_conf_new
#define rd_kafka_conf_prop_find                             chrd_kafka_conf_prop_find
#define rd_kafka_conf_properties_show                       chrd_kafka_conf_properties_show
#define rd_kafka_conf_set                                   chrd_kafka_conf_set
#define rd_kafka_conf_set_background_event_cb               chrd_kafka_conf_set_background_event_cb
#define rd_kafka_conf_set_closesocket_cb                    chrd_kafka_conf_set_closesocket_cb
#define rd_kafka_conf_set_connect_cb                        chrd_kafka_conf_set_connect_cb
#define rd_kafka_conf_set_consume_cb                        chrd_kafka_conf_set_consume_cb
#define rd_kafka_conf_set_default_topic_conf                chrd_kafka_conf_set_default_topic_conf
#define rd_kafka_conf_set_dr_cb                             chrd_kafka_conf_set_dr_cb
#define rd_kafka_conf_set_dr_msg_cb                         chrd_kafka_conf_set_dr_msg_cb
#define rd_kafka_conf_set_engine_callback_data              chrd_kafka_conf_set_engine_callback_data
#define rd_kafka_conf_set_error_cb                          chrd_kafka_conf_set_error_cb
#define rd_kafka_conf_set_events                            chrd_kafka_conf_set_events
#define rd_kafka_conf_set_log_cb                            chrd_kafka_conf_set_log_cb
#define rd_kafka_conf_set_oauthbearer_token_refresh_cb      chrd_kafka_conf_set_oauthbearer_token_refresh_cb
#define rd_kafka_conf_set_offset_commit_cb                  chrd_kafka_conf_set_offset_commit_cb
#define rd_kafka_conf_set_opaque                            chrd_kafka_conf_set_opaque
#define rd_kafka_conf_set_open_cb                           chrd_kafka_conf_set_open_cb
#define rd_kafka_conf_set_rebalance_cb                      chrd_kafka_conf_set_rebalance_cb
#define rd_kafka_conf_set_resolve_cb                        chrd_kafka_conf_set_resolve_cb
#define rd_kafka_conf_set_socket_cb                         chrd_kafka_conf_set_socket_cb
#define rd_kafka_conf_set_ssl_cert_verify_cb                chrd_kafka_conf_set_ssl_cert_verify_cb
#define rd_kafka_conf_set_stats_cb                          chrd_kafka_conf_set_stats_cb
#define rd_kafka_conf_set_throttle_cb                       chrd_kafka_conf_set_throttle_cb
#define rd_kafka_conf_warn                                  chrd_kafka_conf_warn
#define rd_kafka_confval_disable                            chrd_kafka_confval_disable
#define rd_kafka_confval_get_int                            chrd_kafka_confval_get_int
#define rd_kafka_confval_get_ptr                            chrd_kafka_confval_get_ptr
#define rd_kafka_confval_get_str                            chrd_kafka_confval_get_str
#define rd_kafka_confval_init_int                           chrd_kafka_confval_init_int
#define rd_kafka_confval_init_ptr                           chrd_kafka_confval_init_ptr
#define rd_kafka_confval_set_type                           chrd_kafka_confval_set_type
#define rd_kafka_default_topic_conf_dup                     chrd_kafka_default_topic_conf_dup
#define rd_kafka_desensitize_str                            chrd_kafka_desensitize_str
#define rd_kafka_topic_conf_desensitize                     chrd_kafka_topic_conf_desensitize
#define rd_kafka_topic_conf_destroy                         chrd_kafka_topic_conf_destroy
#define rd_kafka_topic_conf_dump                            chrd_kafka_topic_conf_dump
#define rd_kafka_topic_conf_dup                             chrd_kafka_topic_conf_dup
#define rd_kafka_topic_conf_finalize                        chrd_kafka_topic_conf_finalize
#define rd_kafka_topic_conf_get                             chrd_kafka_topic_conf_get
#define rd_kafka_topic_conf_new                             chrd_kafka_topic_conf_new
#define rd_kafka_topic_conf_set                             chrd_kafka_topic_conf_set
#define rd_kafka_topic_conf_set_msg_order_cmp               chrd_kafka_topic_conf_set_msg_order_cmp
#define rd_kafka_topic_conf_set_opaque                      chrd_kafka_topic_conf_set_opaque
#define rd_kafka_topic_conf_set_partitioner_cb              chrd_kafka_topic_conf_set_partitioner_cb
#define unittest_conf                                       chrd_unittest_conf

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
