/// We provide dummy implementation as a replacement of this function from glibc,
///  because we ensure that this function is not used.
/// We provide our own implementation of __cxa_thread_atexit that doesn't use __cxa_thread_atexit_impl.
/// But it is still needed for linking.
int __cxa_thread_atexit_impl(void (*dtor)(void*), void * obj, void * dso_symbol) { __builtin_trap(); }
