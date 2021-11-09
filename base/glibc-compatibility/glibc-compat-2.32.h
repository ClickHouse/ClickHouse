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
#define GLIBC_COMPAT_SYMBOL2(func, func_impl) __asm__(".symver " #func "," #func_impl "@GLIBC_2.2.5");
#elif defined(__aarch64__)
#define GLIBC_COMPAT_SYMBOL(func) __asm__(".symver " #func "," #func "@GLIBC_2.17");
#define GLIBC_COMPAT_SYMBOL2(func, func_impl) __asm__(".symver " #func "," #func_impl "@GLIBC_2.17");
#else
#error Your platform is not supported.
#endif

#define GLIBC_COMPATIBILITY_ON

GLIBC_COMPAT_SYMBOL(pthread_sigmask)
GLIBC_COMPAT_SYMBOL(pthread_getattr_np)

GLIBC_COMPAT_SYMBOL2(fstat, __fxstat)
GLIBC_COMPAT_SYMBOL2(fstat64, __fxstat64)
GLIBC_COMPAT_SYMBOL2(fstatat, __fxstatat)
GLIBC_COMPAT_SYMBOL2(lstat, __lxstat)
GLIBC_COMPAT_SYMBOL2(lstat64, __lxstat64)
GLIBC_COMPAT_SYMBOL2(mknod, __xmknod)
GLIBC_COMPAT_SYMBOL2(mknodat, __xmknodat)
GLIBC_COMPAT_SYMBOL2(stat, __xstat)
GLIBC_COMPAT_SYMBOL2(stat64, __xstat64)
