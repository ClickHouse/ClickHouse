// aws_stubs.cpp
#include <cstdio>
#include <cstdlib>

extern "C" {

#define AWS_TRAP(sym) \
    __attribute__((weak)) void sym() { \
        std::fprintf(stderr, "TRAPPED: " #sym " called\n"); \
        std::abort(); \
    }

AWS_TRAP(aws_tls_client_ctx_new)
AWS_TRAP(aws_tls_server_ctx_new)
AWS_TRAP(aws_tls_init_static_state)
AWS_TRAP(aws_tls_clean_up_static_state)
AWS_TRAP(aws_mqtt_library_init)
AWS_TRAP(aws_mqtt_library_clean_up)

} // extern "C"
