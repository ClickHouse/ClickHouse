#include <thread>

_LIBCPP_BEGIN_NAMESPACE_STD

int __libcpp_thread_create(__libcpp_thread_t * t, void * (*func)(void *), void * arg)
{
    return pthread_create(t, nullptr, func, arg);
}

__libcpp_thread_id __libcpp_thread_get_id(const __libcpp_thread_t * t)
{
    return *t;
}

int __libcpp_thread_join(__libcpp_thread_t * t)
{
    return pthread_join(*t, nullptr);
}

int __libcpp_thread_detach(__libcpp_thread_t * t)
{
    return pthread_detach(*t);
}

int __libcpp_tls_create(__libcpp_tls_key * key, void (*at_exit)(void *))
{
    return pthread_key_create(key, at_exit);
}

void * __libcpp_tls_get(__libcpp_tls_key key)
{
    return pthread_getspecific(key);
}

int __libcpp_tls_set(__libcpp_tls_key key, void * p)
{
    return pthread_setspecific(key, p);
}

_LIBCPP_END_NAMESPACE_STD
