extern "C" __attribute__((weak)) int dwyu_link_function();
int dwyu_weak_caller() { return dwyu_link_function ? dwyu_link_function() : 0; }
