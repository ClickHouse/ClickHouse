extern "C" {

/// Symbols for aws-c-mqtt and aws-s2n-tls
/// which are not used anywhere except those stubs.

// TLS context creation (returns null)
__attribute__((weak)) void *aws_tls_client_ctx_new() { return nullptr; }
__attribute__((weak)) void *aws_tls_server_ctx_new() { return nullptr; }

// TLS static init/cleanup (do nothing)
__attribute__((weak)) void aws_tls_init_static_state() {}
__attribute__((weak)) void aws_tls_clean_up_static_state() {}

// MQTT init/cleanup (do nothing)
__attribute__((weak)) void aws_mqtt_library_init() {}
__attribute__((weak)) void aws_mqtt_library_clean_up() {}

// Darwin-specific TLS handlers (return null or 0)
__attribute__((weak)) void *aws_tls_client_handler_new() { return nullptr; }
__attribute__((weak)) void *aws_tls_server_handler_new() { return nullptr; }
__attribute__((weak)) int aws_tls_client_handler_start_negotiation() { return 0; }
__attribute__((weak)) void *aws_tls_handler_protocol() { return nullptr; }
__attribute__((weak)) int aws_tls_is_alpn_available() { return 0; }

} // extern "C"
