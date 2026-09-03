
#ifndef NVCOMP_EXPORT_H
#define NVCOMP_EXPORT_H

#ifdef NVCOMP_STATIC_DEFINE
#  define NVCOMP_EXPORT
#  define NVCOMP_NO_EXPORT
#else
#  ifndef NVCOMP_EXPORT
#    ifdef nvcomp_EXPORTS
        /* We are building this library */
#      define NVCOMP_EXPORT __attribute__((visibility("default")))
#    else
        /* We are using this library */
#      define NVCOMP_EXPORT __attribute__((visibility("default")))
#    endif
#  endif

#  ifndef NVCOMP_NO_EXPORT
#    define NVCOMP_NO_EXPORT __attribute__((visibility("hidden")))
#  endif
#endif

#ifndef NVCOMP_DEPRECATED
#  define NVCOMP_DEPRECATED 
#endif

#ifndef NVCOMP_DEPRECATED_EXPORT
#  define NVCOMP_DEPRECATED_EXPORT NVCOMP_EXPORT NVCOMP_DEPRECATED
#endif

#ifndef NVCOMP_DEPRECATED_NO_EXPORT
#  define NVCOMP_DEPRECATED_NO_EXPORT NVCOMP_NO_EXPORT NVCOMP_DEPRECATED
#endif

/* NOLINTNEXTLINE(readability-avoid-unconditional-preprocessor-if) */
#if 0 /* DEFINE_NO_DEPRECATED */
#  ifndef NVCOMP_NO_DEPRECATED
#    define NVCOMP_NO_DEPRECATED
#  endif
#endif

#endif /* NVCOMP_EXPORT_H */
