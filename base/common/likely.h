#if defined(_MSC_VER)
#   if !defined(likely)
#      define likely(x)   (x)
#   endif
#   if !defined(unlikely)
#      define unlikely(x) (x)
#   endif
#else
#   if !defined(likely)
#       define likely(x)   (__builtin_expect(!!(x), 1))
#   endif
#   if !defined(unlikely)
#       define unlikely(x) (__builtin_expect(!!(x), 0))
#   endif
#endif
