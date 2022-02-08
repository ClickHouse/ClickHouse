/// In glibc 2.32 new version of some symbols had been added [1]:
///
///     $ nm -D clickhouse | fgrep -e @GLIBC_2.32
///                      U pthread_getattr_np@GLIBC_2.32
///                      U pthread_sigmask@GLIBC_2.32
///
///   [1]: https://www.spinics.net/lists/fedora-devel/msg273044.html
///
/// Right now ubuntu 20.04 is used as official image for building
/// ClickHouse, however once it will be switched someone may not be happy
/// with that fact that he/she cannot use official binaries anymore because
/// they have glibc < 2.32.
///
/// To avoid this dependency, let's force previous version of those
/// symbols from glibc.
///
/// Also note, that the following approach had been tested:
/// a) -Wl,--wrap -- but it goes into endless recursion whey you try to do
///    something like this:
///
///     int __pthread_getattr_np_compact(pthread_t thread, pthread_attr_t *attr);
///     GLIBC_COMPAT_SYMBOL(__pthread_getattr_np_compact, pthread_getattr_np)
///     int __pthread_getattr_np_compact(pthread_t thread, pthread_attr_t *attr);
///     int __wrap_pthread_getattr_np(pthread_t thread, pthread_attr_t *attr)
///     {
///         return __pthread_getattr_np_compact(thread, attr);
///     }
///
///     int __pthread_sigmask_compact(int how, const sigset_t *set, sigset_t *oldset);
///     GLIBC_COMPAT_SYMBOL(__pthread_sigmask_compact, pthread_sigmask)
///     int __pthread_sigmask_compact(int how, const sigset_t *set, sigset_t *oldset);
///     int __wrap_pthread_sigmask(int how, const sigset_t *set, sigset_t *oldset)
///     {
///         return __pthread_sigmask_compact(how, set, oldset);
///     }
///
/// b) -Wl,--defsym -- same problems (and you cannot use version of symbol with
///    version in the expression)
/// c) this approach -- simply add this file with -include directive.

#if defined(__amd64__)
#define GLIBC_COMPAT_SYMBOL(func) __asm__(".symver " #func "," #func "@GLIBC_2.2.5");
#elif defined(__aarch64__)
#define GLIBC_COMPAT_SYMBOL(func) __asm__(".symver " #func "," #func "@GLIBC_2.17");
#else
#error Your platform is not supported.
#endif

GLIBC_COMPAT_SYMBOL(pthread_sigmask)
GLIBC_COMPAT_SYMBOL(pthread_getattr_np)
