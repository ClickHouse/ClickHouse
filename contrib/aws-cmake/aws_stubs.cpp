// aws_stubs.cpp
#include <cstdio>
#include <cstdlib>

extern "C" {

#define AWS_NOOP(sym) \
    __attribute__((weak)) void sym() { \
        /* No operation */ \
    }

AWS_NOOP(aws_tls_client_ctx_new)
AWS_NOOP(aws_tls_server_ctx_new)
AWS_NOOP(aws_tls_init_static_state)
AWS_NOOP(aws_tls_clean_up_static_state)
AWS_NOOP(aws_mqtt_library_init)
AWS_NOOP(aws_mqtt_library_clean_up)

} // extern "C"
