set(MUSL_SOURCE_DIR ${ClickHouse_SOURCE_DIR}/contrib/musl)
set(MUSL_BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR})

set(MUSL_C_SOURCES
#    ${MUSL_SOURCE_DIR}/src/aio/aio.c
#    ${MUSL_SOURCE_DIR}/src/aio/aio_suspend.c
#    ${MUSL_SOURCE_DIR}/src/aio/lio_listio.c
#    ${MUSL_SOURCE_DIR}/src/complex/__cexp.c
#    ${MUSL_SOURCE_DIR}/src/complex/__cexpf.c
#    ${MUSL_SOURCE_DIR}/src/complex/cabs.c
#    ${MUSL_SOURCE_DIR}/src/complex/cabsf.c
#    ${MUSL_SOURCE_DIR}/src/complex/cabsl.c
#    ${MUSL_SOURCE_DIR}/src/complex/cacos.c
#    ${MUSL_SOURCE_DIR}/src/complex/cacosf.c
#    ${MUSL_SOURCE_DIR}/src/complex/cacosh.c
#    ${MUSL_SOURCE_DIR}/src/complex/cacoshf.c
#    ${MUSL_SOURCE_DIR}/src/complex/cacoshl.c
#    ${MUSL_SOURCE_DIR}/src/complex/cacosl.c
#    ${MUSL_SOURCE_DIR}/src/complex/carg.c
#    ${MUSL_SOURCE_DIR}/src/complex/cargf.c
#    ${MUSL_SOURCE_DIR}/src/complex/cargl.c
#    ${MUSL_SOURCE_DIR}/src/complex/casin.c
#    ${MUSL_SOURCE_DIR}/src/complex/casinf.c
#    ${MUSL_SOURCE_DIR}/src/complex/casinh.c
#    ${MUSL_SOURCE_DIR}/src/complex/casinhf.c
#    ${MUSL_SOURCE_DIR}/src/complex/casinhl.c
#    ${MUSL_SOURCE_DIR}/src/complex/casinl.c
#    ${MUSL_SOURCE_DIR}/src/complex/catan.c
#    ${MUSL_SOURCE_DIR}/src/complex/catanf.c
#    ${MUSL_SOURCE_DIR}/src/complex/catanh.c
#    ${MUSL_SOURCE_DIR}/src/complex/catanhf.c
#    ${MUSL_SOURCE_DIR}/src/complex/catanhl.c
#    ${MUSL_SOURCE_DIR}/src/complex/catanl.c
#    ${MUSL_SOURCE_DIR}/src/complex/ccos.c
#    ${MUSL_SOURCE_DIR}/src/complex/ccosf.c
#    ${MUSL_SOURCE_DIR}/src/complex/ccosh.c
#    ${MUSL_SOURCE_DIR}/src/complex/ccoshf.c
#    ${MUSL_SOURCE_DIR}/src/complex/ccoshl.c
#    ${MUSL_SOURCE_DIR}/src/complex/ccosl.c
#    ${MUSL_SOURCE_DIR}/src/complex/cexp.c
#    ${MUSL_SOURCE_DIR}/src/complex/cexpf.c
#    ${MUSL_SOURCE_DIR}/src/complex/cexpl.c
#    ${MUSL_SOURCE_DIR}/src/complex/cimag.c
#    ${MUSL_SOURCE_DIR}/src/complex/cimagf.c
#    ${MUSL_SOURCE_DIR}/src/complex/cimagl.c
#    ${MUSL_SOURCE_DIR}/src/complex/clog.c
#    ${MUSL_SOURCE_DIR}/src/complex/clogf.c
#    ${MUSL_SOURCE_DIR}/src/complex/clogl.c
#    ${MUSL_SOURCE_DIR}/src/complex/conj.c
#    ${MUSL_SOURCE_DIR}/src/complex/conjf.c
#    ${MUSL_SOURCE_DIR}/src/complex/conjl.c
#    ${MUSL_SOURCE_DIR}/src/complex/cpow.c
#    ${MUSL_SOURCE_DIR}/src/complex/cpowf.c
#    ${MUSL_SOURCE_DIR}/src/complex/cpowl.c
#    ${MUSL_SOURCE_DIR}/src/complex/cproj.c
#    ${MUSL_SOURCE_DIR}/src/complex/cprojf.c
#    ${MUSL_SOURCE_DIR}/src/complex/cprojl.c
#    ${MUSL_SOURCE_DIR}/src/complex/creal.c
#    ${MUSL_SOURCE_DIR}/src/complex/crealf.c
#    ${MUSL_SOURCE_DIR}/src/complex/creall.c
#    ${MUSL_SOURCE_DIR}/src/complex/csin.c
#    ${MUSL_SOURCE_DIR}/src/complex/csinf.c
#    ${MUSL_SOURCE_DIR}/src/complex/csinh.c
#    ${MUSL_SOURCE_DIR}/src/complex/csinhf.c
#    ${MUSL_SOURCE_DIR}/src/complex/csinhl.c
#    ${MUSL_SOURCE_DIR}/src/complex/csinl.c
#    ${MUSL_SOURCE_DIR}/src/complex/csqrt.c
#    ${MUSL_SOURCE_DIR}/src/complex/csqrtf.c
#    ${MUSL_SOURCE_DIR}/src/complex/csqrtl.c
#    ${MUSL_SOURCE_DIR}/src/complex/ctan.c
#    ${MUSL_SOURCE_DIR}/src/complex/ctanf.c
#    ${MUSL_SOURCE_DIR}/src/complex/ctanh.c
#    ${MUSL_SOURCE_DIR}/src/complex/ctanhf.c
#    ${MUSL_SOURCE_DIR}/src/complex/ctanhl.c
#    ${MUSL_SOURCE_DIR}/src/complex/ctanl.c
#    ${MUSL_SOURCE_DIR}/src/conf/confstr.c
#    ${MUSL_SOURCE_DIR}/src/conf/fpathconf.c
#    ${MUSL_SOURCE_DIR}/src/conf/legacy.c
#    ${MUSL_SOURCE_DIR}/src/conf/pathconf.c
#    ${MUSL_SOURCE_DIR}/src/conf/sysconf.c
#    ${MUSL_SOURCE_DIR}/src/crypt/crypt.c
#    ${MUSL_SOURCE_DIR}/src/crypt/crypt_blowfish.c
#    ${MUSL_SOURCE_DIR}/src/crypt/crypt_des.c
#    ${MUSL_SOURCE_DIR}/src/crypt/crypt_md5.c
#    ${MUSL_SOURCE_DIR}/src/crypt/crypt_r.c
#    ${MUSL_SOURCE_DIR}/src/crypt/crypt_sha256.c
#    ${MUSL_SOURCE_DIR}/src/crypt/crypt_sha512.c
#    ${MUSL_SOURCE_DIR}/src/crypt/encrypt.c
#    ${MUSL_SOURCE_DIR}/src/ctype/__ctype_b_loc.c
#    ${MUSL_SOURCE_DIR}/src/ctype/__ctype_get_mb_cur_max.c
#    ${MUSL_SOURCE_DIR}/src/ctype/__ctype_tolower_loc.c
#    ${MUSL_SOURCE_DIR}/src/ctype/__ctype_toupper_loc.c
#    ${MUSL_SOURCE_DIR}/src/ctype/isalnum.c
#    ${MUSL_SOURCE_DIR}/src/ctype/isalpha.c
#    ${MUSL_SOURCE_DIR}/src/ctype/isascii.c
#    ${MUSL_SOURCE_DIR}/src/ctype/isblank.c
#    ${MUSL_SOURCE_DIR}/src/ctype/iscntrl.c
#    ${MUSL_SOURCE_DIR}/src/ctype/isdigit.c
#    ${MUSL_SOURCE_DIR}/src/ctype/isgraph.c
#    ${MUSL_SOURCE_DIR}/src/ctype/islower.c
#    ${MUSL_SOURCE_DIR}/src/ctype/isprint.c
#    ${MUSL_SOURCE_DIR}/src/ctype/ispunct.c
#    ${MUSL_SOURCE_DIR}/src/ctype/isspace.c
#    ${MUSL_SOURCE_DIR}/src/ctype/isupper.c
#    ${MUSL_SOURCE_DIR}/src/ctype/iswalnum.c
#    ${MUSL_SOURCE_DIR}/src/ctype/iswalpha.c
#    ${MUSL_SOURCE_DIR}/src/ctype/iswblank.c
#    ${MUSL_SOURCE_DIR}/src/ctype/iswcntrl.c
#    ${MUSL_SOURCE_DIR}/src/ctype/iswctype.c
#    ${MUSL_SOURCE_DIR}/src/ctype/iswdigit.c
#    ${MUSL_SOURCE_DIR}/src/ctype/iswgraph.c
#    ${MUSL_SOURCE_DIR}/src/ctype/iswlower.c
#    ${MUSL_SOURCE_DIR}/src/ctype/iswprint.c
#    ${MUSL_SOURCE_DIR}/src/ctype/iswpunct.c
#    ${MUSL_SOURCE_DIR}/src/ctype/iswspace.c
#    ${MUSL_SOURCE_DIR}/src/ctype/iswupper.c
#    ${MUSL_SOURCE_DIR}/src/ctype/iswxdigit.c
#    ${MUSL_SOURCE_DIR}/src/ctype/isxdigit.c
#    ${MUSL_SOURCE_DIR}/src/ctype/toascii.c
#    ${MUSL_SOURCE_DIR}/src/ctype/tolower.c
#    ${MUSL_SOURCE_DIR}/src/ctype/toupper.c
#    ${MUSL_SOURCE_DIR}/src/ctype/towctrans.c
#    ${MUSL_SOURCE_DIR}/src/ctype/wcswidth.c
#    ${MUSL_SOURCE_DIR}/src/ctype/wctrans.c
#    ${MUSL_SOURCE_DIR}/src/ctype/wcwidth.c
#    ${MUSL_SOURCE_DIR}/src/dirent/alphasort.c
#    ${MUSL_SOURCE_DIR}/src/dirent/closedir.c
#    ${MUSL_SOURCE_DIR}/src/dirent/dirfd.c
#    ${MUSL_SOURCE_DIR}/src/dirent/fdopendir.c
#    ${MUSL_SOURCE_DIR}/src/dirent/opendir.c
#    ${MUSL_SOURCE_DIR}/src/dirent/readdir.c
#    ${MUSL_SOURCE_DIR}/src/dirent/readdir_r.c
#    ${MUSL_SOURCE_DIR}/src/dirent/rewinddir.c
#    ${MUSL_SOURCE_DIR}/src/dirent/scandir.c
#    ${MUSL_SOURCE_DIR}/src/dirent/seekdir.c
#    ${MUSL_SOURCE_DIR}/src/dirent/telldir.c
#    ${MUSL_SOURCE_DIR}/src/dirent/versionsort.c
#    ${MUSL_SOURCE_DIR}/src/env/__environ.c
#    ${MUSL_SOURCE_DIR}/src/env/__init_tls.c
#    ${MUSL_SOURCE_DIR}/src/env/__libc_start_main.c
#    ${MUSL_SOURCE_DIR}/src/env/__reset_tls.c
#    ${MUSL_SOURCE_DIR}/src/env/__stack_chk_fail.c
#    ${MUSL_SOURCE_DIR}/src/env/clearenv.c
#    ${MUSL_SOURCE_DIR}/src/env/getenv.c
#    ${MUSL_SOURCE_DIR}/src/env/putenv.c
#    ${MUSL_SOURCE_DIR}/src/env/secure_getenv.c
#    ${MUSL_SOURCE_DIR}/src/env/setenv.c
#    ${MUSL_SOURCE_DIR}/src/env/unsetenv.c
    ${MUSL_SOURCE_DIR}/src/errno/__errno_location.c
#    ${MUSL_SOURCE_DIR}/src/errno/strerror.c
#    ${MUSL_SOURCE_DIR}/src/exit/_Exit.c
#    ${MUSL_SOURCE_DIR}/src/exit/abort.c
#    ${MUSL_SOURCE_DIR}/src/exit/assert.c
#    ${MUSL_SOURCE_DIR}/src/exit/at_quick_exit.c
#    ${MUSL_SOURCE_DIR}/src/exit/atexit.c
#    ${MUSL_SOURCE_DIR}/src/exit/exit.c
#    ${MUSL_SOURCE_DIR}/src/exit/quick_exit.c
#    ${MUSL_SOURCE_DIR}/src/fcntl/creat.c
#    ${MUSL_SOURCE_DIR}/src/fcntl/fcntl.c
#    ${MUSL_SOURCE_DIR}/src/fcntl/open.c
#    ${MUSL_SOURCE_DIR}/src/fcntl/openat.c
#    ${MUSL_SOURCE_DIR}/src/fcntl/posix_fadvise.c
#    ${MUSL_SOURCE_DIR}/src/fcntl/posix_fallocate.c
#    ${MUSL_SOURCE_DIR}/src/fenv/__flt_rounds.c
#    ${MUSL_SOURCE_DIR}/src/fenv/fegetexceptflag.c
#    ${MUSL_SOURCE_DIR}/src/fenv/feholdexcept.c
#    ${MUSL_SOURCE_DIR}/src/fenv/fesetexceptflag.c
#    ${MUSL_SOURCE_DIR}/src/fenv/fesetround.c
#    ${MUSL_SOURCE_DIR}/src/fenv/feupdateenv.c
#    ${MUSL_SOURCE_DIR}/src/internal/defsysinfo.c
#    ${MUSL_SOURCE_DIR}/src/internal/floatscan.c
#    ${MUSL_SOURCE_DIR}/src/internal/intscan.c
#    ${MUSL_SOURCE_DIR}/src/internal/libc.c
#    ${MUSL_SOURCE_DIR}/src/internal/procfdname.c
#    ${MUSL_SOURCE_DIR}/src/internal/shgetc.c
    ${MUSL_SOURCE_DIR}/src/internal/syscall_ret.c
#    ${MUSL_SOURCE_DIR}/src/internal/vdso.c
#    ${MUSL_SOURCE_DIR}/src/internal/version.c
#    ${MUSL_SOURCE_DIR}/src/ipc/ftok.c
#    ${MUSL_SOURCE_DIR}/src/ipc/msgctl.c
#    ${MUSL_SOURCE_DIR}/src/ipc/msgget.c
#    ${MUSL_SOURCE_DIR}/src/ipc/msgrcv.c
#    ${MUSL_SOURCE_DIR}/src/ipc/msgsnd.c
#    ${MUSL_SOURCE_DIR}/src/ipc/semctl.c
#    ${MUSL_SOURCE_DIR}/src/ipc/semget.c
#    ${MUSL_SOURCE_DIR}/src/ipc/semop.c
#    ${MUSL_SOURCE_DIR}/src/ipc/semtimedop.c
#    ${MUSL_SOURCE_DIR}/src/ipc/shmat.c
#    ${MUSL_SOURCE_DIR}/src/ipc/shmctl.c
#    ${MUSL_SOURCE_DIR}/src/ipc/shmdt.c
#    ${MUSL_SOURCE_DIR}/src/ipc/shmget.c
#    ${MUSL_SOURCE_DIR}/src/ldso/__dlsym.c
#    ${MUSL_SOURCE_DIR}/src/ldso/dl_iterate_phdr.c
#    ${MUSL_SOURCE_DIR}/src/ldso/dladdr.c
#    ${MUSL_SOURCE_DIR}/src/ldso/dlclose.c
#    ${MUSL_SOURCE_DIR}/src/ldso/dlerror.c
#    ${MUSL_SOURCE_DIR}/src/ldso/dlinfo.c
#    ${MUSL_SOURCE_DIR}/src/ldso/dlopen.c
#    ${MUSL_SOURCE_DIR}/src/legacy/cuserid.c
#    ${MUSL_SOURCE_DIR}/src/legacy/daemon.c
#    ${MUSL_SOURCE_DIR}/src/legacy/err.c
#    ${MUSL_SOURCE_DIR}/src/legacy/euidaccess.c
#    ${MUSL_SOURCE_DIR}/src/legacy/ftw.c
#    ${MUSL_SOURCE_DIR}/src/legacy/futimes.c
#    ${MUSL_SOURCE_DIR}/src/legacy/getdtablesize.c
#    ${MUSL_SOURCE_DIR}/src/legacy/getloadavg.c
#    ${MUSL_SOURCE_DIR}/src/legacy/getpagesize.c
#    ${MUSL_SOURCE_DIR}/src/legacy/getpass.c
#    ${MUSL_SOURCE_DIR}/src/legacy/getusershell.c
#    ${MUSL_SOURCE_DIR}/src/legacy/isastream.c
#    ${MUSL_SOURCE_DIR}/src/legacy/lutimes.c
#    ${MUSL_SOURCE_DIR}/src/legacy/ulimit.c
#    ${MUSL_SOURCE_DIR}/src/legacy/utmpx.c
#    ${MUSL_SOURCE_DIR}/src/legacy/valloc.c
#    ${MUSL_SOURCE_DIR}/src/linux/adjtime.c
#    ${MUSL_SOURCE_DIR}/src/linux/adjtimex.c
#    ${MUSL_SOURCE_DIR}/src/linux/arch_prctl.c
#    ${MUSL_SOURCE_DIR}/src/linux/brk.c
#    ${MUSL_SOURCE_DIR}/src/linux/cache.c
#    ${MUSL_SOURCE_DIR}/src/linux/cap.c
#    ${MUSL_SOURCE_DIR}/src/linux/chroot.c
#    ${MUSL_SOURCE_DIR}/src/linux/clock_adjtime.c
#    ${MUSL_SOURCE_DIR}/src/linux/clone.c
#    ${MUSL_SOURCE_DIR}/src/linux/copy_file_range.c
#    ${MUSL_SOURCE_DIR}/src/linux/epoll.c
#    ${MUSL_SOURCE_DIR}/src/linux/eventfd.c
#    ${MUSL_SOURCE_DIR}/src/linux/fallocate.c
#    ${MUSL_SOURCE_DIR}/src/linux/fanotify.c
#    ${MUSL_SOURCE_DIR}/src/linux/flock.c
#    ${MUSL_SOURCE_DIR}/src/linux/getdents.c
#    ${MUSL_SOURCE_DIR}/src/linux/getrandom.c
#    ${MUSL_SOURCE_DIR}/src/linux/gettid.c
#    ${MUSL_SOURCE_DIR}/src/linux/inotify.c
#    ${MUSL_SOURCE_DIR}/src/linux/ioperm.c
#    ${MUSL_SOURCE_DIR}/src/linux/iopl.c
#    ${MUSL_SOURCE_DIR}/src/linux/klogctl.c
#    ${MUSL_SOURCE_DIR}/src/linux/membarrier.c
#    ${MUSL_SOURCE_DIR}/src/linux/memfd_create.c
#    ${MUSL_SOURCE_DIR}/src/linux/mlock2.c
#    ${MUSL_SOURCE_DIR}/src/linux/module.c
#    ${MUSL_SOURCE_DIR}/src/linux/mount.c
#    ${MUSL_SOURCE_DIR}/src/linux/name_to_handle_at.c
#    ${MUSL_SOURCE_DIR}/src/linux/open_by_handle_at.c
#    ${MUSL_SOURCE_DIR}/src/linux/personality.c
#    ${MUSL_SOURCE_DIR}/src/linux/pivot_root.c
#    ${MUSL_SOURCE_DIR}/src/linux/ppoll.c
#    ${MUSL_SOURCE_DIR}/src/linux/prctl.c
#    ${MUSL_SOURCE_DIR}/src/linux/prlimit.c
#    ${MUSL_SOURCE_DIR}/src/linux/process_vm.c
#    ${MUSL_SOURCE_DIR}/src/linux/ptrace.c
#    ${MUSL_SOURCE_DIR}/src/linux/quotactl.c
#    ${MUSL_SOURCE_DIR}/src/linux/readahead.c
#    ${MUSL_SOURCE_DIR}/src/linux/reboot.c
#    ${MUSL_SOURCE_DIR}/src/linux/remap_file_pages.c
#    ${MUSL_SOURCE_DIR}/src/linux/sbrk.c
#    ${MUSL_SOURCE_DIR}/src/linux/sendfile.c
#    ${MUSL_SOURCE_DIR}/src/linux/setfsgid.c
#    ${MUSL_SOURCE_DIR}/src/linux/setfsuid.c
#    ${MUSL_SOURCE_DIR}/src/linux/setgroups.c
#    ${MUSL_SOURCE_DIR}/src/linux/sethostname.c
#    ${MUSL_SOURCE_DIR}/src/linux/setns.c
#    ${MUSL_SOURCE_DIR}/src/linux/settimeofday.c
#    ${MUSL_SOURCE_DIR}/src/linux/signalfd.c
#    ${MUSL_SOURCE_DIR}/src/linux/splice.c
#    ${MUSL_SOURCE_DIR}/src/linux/stime.c
#    ${MUSL_SOURCE_DIR}/src/linux/swap.c
#    ${MUSL_SOURCE_DIR}/src/linux/sync_file_range.c
#    ${MUSL_SOURCE_DIR}/src/linux/syncfs.c
#    ${MUSL_SOURCE_DIR}/src/linux/sysinfo.c
#    ${MUSL_SOURCE_DIR}/src/linux/tee.c
#    ${MUSL_SOURCE_DIR}/src/linux/timerfd.c
#    ${MUSL_SOURCE_DIR}/src/linux/unshare.c
#    ${MUSL_SOURCE_DIR}/src/linux/utimes.c
#    ${MUSL_SOURCE_DIR}/src/linux/vhangup.c
#    ${MUSL_SOURCE_DIR}/src/linux/vmsplice.c
#    ${MUSL_SOURCE_DIR}/src/linux/wait3.c
#    ${MUSL_SOURCE_DIR}/src/linux/wait4.c
#    ${MUSL_SOURCE_DIR}/src/linux/xattr.c
    ${MUSL_SOURCE_DIR}/src/locale/__lctrans.c
#    ${MUSL_SOURCE_DIR}/src/locale/__mo_lookup.c
#    ${MUSL_SOURCE_DIR}/src/locale/bind_textdomain_codeset.c
#    ${MUSL_SOURCE_DIR}/src/locale/c_locale.c
#    ${MUSL_SOURCE_DIR}/src/locale/catclose.c
#    ${MUSL_SOURCE_DIR}/src/locale/catgets.c
#    ${MUSL_SOURCE_DIR}/src/locale/catopen.c
#    ${MUSL_SOURCE_DIR}/src/locale/dcngettext.c
#    ${MUSL_SOURCE_DIR}/src/locale/duplocale.c
#    ${MUSL_SOURCE_DIR}/src/locale/freelocale.c
#    ${MUSL_SOURCE_DIR}/src/locale/iconv.c
#    ${MUSL_SOURCE_DIR}/src/locale/iconv_close.c
#    ${MUSL_SOURCE_DIR}/src/locale/langinfo.c
#    ${MUSL_SOURCE_DIR}/src/locale/locale_map.c
#    ${MUSL_SOURCE_DIR}/src/locale/localeconv.c
#    ${MUSL_SOURCE_DIR}/src/locale/newlocale.c
#    ${MUSL_SOURCE_DIR}/src/locale/pleval.c
#    ${MUSL_SOURCE_DIR}/src/locale/setlocale.c
#    ${MUSL_SOURCE_DIR}/src/locale/strcoll.c
#    ${MUSL_SOURCE_DIR}/src/locale/strfmon.c
#    ${MUSL_SOURCE_DIR}/src/locale/strxfrm.c
#    ${MUSL_SOURCE_DIR}/src/locale/textdomain.c
#    ${MUSL_SOURCE_DIR}/src/locale/uselocale.c
#    ${MUSL_SOURCE_DIR}/src/locale/wcscoll.c
#    ${MUSL_SOURCE_DIR}/src/locale/wcsxfrm.c
#    ${MUSL_SOURCE_DIR}/src/malloc/calloc.c
#    ${MUSL_SOURCE_DIR}/src/malloc/lite_malloc.c
#    ${MUSL_SOURCE_DIR}/src/malloc/mallocng/aligned_alloc.c
#    ${MUSL_SOURCE_DIR}/src/malloc/mallocng/donate.c
#    ${MUSL_SOURCE_DIR}/src/malloc/mallocng/free.c
#    ${MUSL_SOURCE_DIR}/src/malloc/mallocng/malloc.c
#    ${MUSL_SOURCE_DIR}/src/malloc/mallocng/malloc_usable_size.c
#    ${MUSL_SOURCE_DIR}/src/malloc/mallocng/realloc.c
#    ${MUSL_SOURCE_DIR}/src/malloc/memalign.c
#    ${MUSL_SOURCE_DIR}/src/malloc/posix_memalign.c
#    ${MUSL_SOURCE_DIR}/src/malloc/replaced.c
#    ${MUSL_SOURCE_DIR}/src/math/__cos.c
#    ${MUSL_SOURCE_DIR}/src/math/__cosdf.c
#    ${MUSL_SOURCE_DIR}/src/math/__cosl.c
#    ${MUSL_SOURCE_DIR}/src/math/__expo2.c
#    ${MUSL_SOURCE_DIR}/src/math/__expo2f.c
#    ${MUSL_SOURCE_DIR}/src/math/__fpclassify.c
#    ${MUSL_SOURCE_DIR}/src/math/__fpclassifyf.c
#    ${MUSL_SOURCE_DIR}/src/math/__fpclassifyl.c
#    ${MUSL_SOURCE_DIR}/src/math/__math_divzero.c
#    ${MUSL_SOURCE_DIR}/src/math/__math_divzerof.c
#    ${MUSL_SOURCE_DIR}/src/math/__math_invalid.c
#    ${MUSL_SOURCE_DIR}/src/math/__math_invalidf.c
#    ${MUSL_SOURCE_DIR}/src/math/__math_invalidl.c
#    ${MUSL_SOURCE_DIR}/src/math/__math_oflow.c
#    ${MUSL_SOURCE_DIR}/src/math/__math_oflowf.c
#    ${MUSL_SOURCE_DIR}/src/math/__math_uflow.c
#    ${MUSL_SOURCE_DIR}/src/math/__math_uflowf.c
#    ${MUSL_SOURCE_DIR}/src/math/__math_xflow.c
#    ${MUSL_SOURCE_DIR}/src/math/__math_xflowf.c
#    ${MUSL_SOURCE_DIR}/src/math/__polevll.c
#    ${MUSL_SOURCE_DIR}/src/math/__rem_pio2.c
#    ${MUSL_SOURCE_DIR}/src/math/__rem_pio2_large.c
#    ${MUSL_SOURCE_DIR}/src/math/__rem_pio2f.c
#    ${MUSL_SOURCE_DIR}/src/math/__rem_pio2l.c
#    ${MUSL_SOURCE_DIR}/src/math/__signbit.c
#    ${MUSL_SOURCE_DIR}/src/math/__signbitf.c
#    ${MUSL_SOURCE_DIR}/src/math/__signbitl.c
#    ${MUSL_SOURCE_DIR}/src/math/__sin.c
#    ${MUSL_SOURCE_DIR}/src/math/__sindf.c
#    ${MUSL_SOURCE_DIR}/src/math/__sinl.c
#    ${MUSL_SOURCE_DIR}/src/math/__tan.c
#    ${MUSL_SOURCE_DIR}/src/math/__tandf.c
#    ${MUSL_SOURCE_DIR}/src/math/__tanl.c
#    ${MUSL_SOURCE_DIR}/src/math/acos.c
#    ${MUSL_SOURCE_DIR}/src/math/acosf.c
#    ${MUSL_SOURCE_DIR}/src/math/acosh.c
#    ${MUSL_SOURCE_DIR}/src/math/acoshf.c
#    ${MUSL_SOURCE_DIR}/src/math/acoshl.c
#    ${MUSL_SOURCE_DIR}/src/math/asin.c
#    ${MUSL_SOURCE_DIR}/src/math/asinf.c
#    ${MUSL_SOURCE_DIR}/src/math/asinh.c
#    ${MUSL_SOURCE_DIR}/src/math/asinhf.c
#    ${MUSL_SOURCE_DIR}/src/math/asinhl.c
#    ${MUSL_SOURCE_DIR}/src/math/atan.c
#    ${MUSL_SOURCE_DIR}/src/math/atan2.c
#    ${MUSL_SOURCE_DIR}/src/math/atan2f.c
#    ${MUSL_SOURCE_DIR}/src/math/atanf.c
#    ${MUSL_SOURCE_DIR}/src/math/atanh.c
#    ${MUSL_SOURCE_DIR}/src/math/atanhf.c
#    ${MUSL_SOURCE_DIR}/src/math/atanhl.c
#    ${MUSL_SOURCE_DIR}/src/math/cbrt.c
#    ${MUSL_SOURCE_DIR}/src/math/cbrtf.c
#    ${MUSL_SOURCE_DIR}/src/math/cbrtl.c
#    ${MUSL_SOURCE_DIR}/src/math/ceil.c
#    ${MUSL_SOURCE_DIR}/src/math/ceilf.c
#    ${MUSL_SOURCE_DIR}/src/math/copysign.c
#    ${MUSL_SOURCE_DIR}/src/math/copysignf.c
#    ${MUSL_SOURCE_DIR}/src/math/copysignl.c
#    ${MUSL_SOURCE_DIR}/src/math/cos.c
#    ${MUSL_SOURCE_DIR}/src/math/cosf.c
#    ${MUSL_SOURCE_DIR}/src/math/cosh.c
#    ${MUSL_SOURCE_DIR}/src/math/coshf.c
#    ${MUSL_SOURCE_DIR}/src/math/coshl.c
#    ${MUSL_SOURCE_DIR}/src/math/cosl.c
#    ${MUSL_SOURCE_DIR}/src/math/erf.c
#    ${MUSL_SOURCE_DIR}/src/math/erff.c
#    ${MUSL_SOURCE_DIR}/src/math/erfl.c
#    ${MUSL_SOURCE_DIR}/src/math/exp.c
#    ${MUSL_SOURCE_DIR}/src/math/exp10.c
#    ${MUSL_SOURCE_DIR}/src/math/exp10f.c
#    ${MUSL_SOURCE_DIR}/src/math/exp10l.c
#    ${MUSL_SOURCE_DIR}/src/math/exp2.c
#    ${MUSL_SOURCE_DIR}/src/math/exp2f.c
#    ${MUSL_SOURCE_DIR}/src/math/exp2f_data.c
#    ${MUSL_SOURCE_DIR}/src/math/exp_data.c
#    ${MUSL_SOURCE_DIR}/src/math/expf.c
#    ${MUSL_SOURCE_DIR}/src/math/expm1.c
#    ${MUSL_SOURCE_DIR}/src/math/expm1f.c
#    ${MUSL_SOURCE_DIR}/src/math/fdim.c
#    ${MUSL_SOURCE_DIR}/src/math/fdimf.c
#    ${MUSL_SOURCE_DIR}/src/math/fdiml.c
#    ${MUSL_SOURCE_DIR}/src/math/finite.c
#    ${MUSL_SOURCE_DIR}/src/math/finitef.c
#    ${MUSL_SOURCE_DIR}/src/math/floor.c
#    ${MUSL_SOURCE_DIR}/src/math/floorf.c
#    ${MUSL_SOURCE_DIR}/src/math/fmal.c
#    ${MUSL_SOURCE_DIR}/src/math/fmax.c
#    ${MUSL_SOURCE_DIR}/src/math/fmaxf.c
#    ${MUSL_SOURCE_DIR}/src/math/fmaxl.c
#    ${MUSL_SOURCE_DIR}/src/math/fmin.c
#    ${MUSL_SOURCE_DIR}/src/math/fminf.c
#    ${MUSL_SOURCE_DIR}/src/math/fminl.c
#    ${MUSL_SOURCE_DIR}/src/math/fmod.c
#    ${MUSL_SOURCE_DIR}/src/math/fmodf.c
#    ${MUSL_SOURCE_DIR}/src/math/frexp.c
#    ${MUSL_SOURCE_DIR}/src/math/frexpf.c
#    ${MUSL_SOURCE_DIR}/src/math/frexpl.c
#    ${MUSL_SOURCE_DIR}/src/math/hypot.c
#    ${MUSL_SOURCE_DIR}/src/math/hypotf.c
#    ${MUSL_SOURCE_DIR}/src/math/hypotl.c
#    ${MUSL_SOURCE_DIR}/src/math/ilogb.c
#    ${MUSL_SOURCE_DIR}/src/math/ilogbf.c
#    ${MUSL_SOURCE_DIR}/src/math/ilogbl.c
#    ${MUSL_SOURCE_DIR}/src/math/j0.c
#    ${MUSL_SOURCE_DIR}/src/math/j0f.c
#    ${MUSL_SOURCE_DIR}/src/math/j1.c
#    ${MUSL_SOURCE_DIR}/src/math/j1f.c
#    ${MUSL_SOURCE_DIR}/src/math/jn.c
#    ${MUSL_SOURCE_DIR}/src/math/jnf.c
#    ${MUSL_SOURCE_DIR}/src/math/ldexp.c
#    ${MUSL_SOURCE_DIR}/src/math/ldexpf.c
#    ${MUSL_SOURCE_DIR}/src/math/ldexpl.c
#    ${MUSL_SOURCE_DIR}/src/math/lgamma.c
#    ${MUSL_SOURCE_DIR}/src/math/lgamma_r.c
#    ${MUSL_SOURCE_DIR}/src/math/lgammaf.c
#    ${MUSL_SOURCE_DIR}/src/math/lgammaf_r.c
#    ${MUSL_SOURCE_DIR}/src/math/lgammal.c
#    ${MUSL_SOURCE_DIR}/src/math/llround.c
#    ${MUSL_SOURCE_DIR}/src/math/llroundf.c
#    ${MUSL_SOURCE_DIR}/src/math/llroundl.c
#    ${MUSL_SOURCE_DIR}/src/math/log.c
#    ${MUSL_SOURCE_DIR}/src/math/log10.c
#    ${MUSL_SOURCE_DIR}/src/math/log10f.c
#    ${MUSL_SOURCE_DIR}/src/math/log1p.c
#    ${MUSL_SOURCE_DIR}/src/math/log1pf.c
#    ${MUSL_SOURCE_DIR}/src/math/log2.c
#    ${MUSL_SOURCE_DIR}/src/math/log2_data.c
#    ${MUSL_SOURCE_DIR}/src/math/log2f.c
#    ${MUSL_SOURCE_DIR}/src/math/log2f_data.c
#    ${MUSL_SOURCE_DIR}/src/math/log_data.c
#    ${MUSL_SOURCE_DIR}/src/math/logb.c
#    ${MUSL_SOURCE_DIR}/src/math/logbf.c
#    ${MUSL_SOURCE_DIR}/src/math/logbl.c
#    ${MUSL_SOURCE_DIR}/src/math/logf.c
#    ${MUSL_SOURCE_DIR}/src/math/logf_data.c
#    ${MUSL_SOURCE_DIR}/src/math/lround.c
#    ${MUSL_SOURCE_DIR}/src/math/lroundf.c
#    ${MUSL_SOURCE_DIR}/src/math/lroundl.c
#    ${MUSL_SOURCE_DIR}/src/math/modf.c
#    ${MUSL_SOURCE_DIR}/src/math/modff.c
#    ${MUSL_SOURCE_DIR}/src/math/modfl.c
#    ${MUSL_SOURCE_DIR}/src/math/nan.c
#    ${MUSL_SOURCE_DIR}/src/math/nanf.c
#    ${MUSL_SOURCE_DIR}/src/math/nanl.c
#    ${MUSL_SOURCE_DIR}/src/math/nearbyint.c
#    ${MUSL_SOURCE_DIR}/src/math/nearbyintf.c
#    ${MUSL_SOURCE_DIR}/src/math/nearbyintl.c
#    ${MUSL_SOURCE_DIR}/src/math/nextafter.c
#    ${MUSL_SOURCE_DIR}/src/math/nextafterf.c
#    ${MUSL_SOURCE_DIR}/src/math/nextafterl.c
#    ${MUSL_SOURCE_DIR}/src/math/nexttoward.c
#    ${MUSL_SOURCE_DIR}/src/math/nexttowardf.c
#    ${MUSL_SOURCE_DIR}/src/math/nexttowardl.c
#    ${MUSL_SOURCE_DIR}/src/math/pow.c
#    ${MUSL_SOURCE_DIR}/src/math/pow_data.c
#    ${MUSL_SOURCE_DIR}/src/math/powf.c
#    ${MUSL_SOURCE_DIR}/src/math/powf_data.c
#    ${MUSL_SOURCE_DIR}/src/math/powl.c
#    ${MUSL_SOURCE_DIR}/src/math/remainder.c
#    ${MUSL_SOURCE_DIR}/src/math/remainderf.c
#    ${MUSL_SOURCE_DIR}/src/math/remquo.c
#    ${MUSL_SOURCE_DIR}/src/math/remquof.c
#    ${MUSL_SOURCE_DIR}/src/math/rint.c
#    ${MUSL_SOURCE_DIR}/src/math/rintf.c
#    ${MUSL_SOURCE_DIR}/src/math/round.c
#    ${MUSL_SOURCE_DIR}/src/math/roundf.c
#    ${MUSL_SOURCE_DIR}/src/math/roundl.c
#    ${MUSL_SOURCE_DIR}/src/math/scalb.c
#    ${MUSL_SOURCE_DIR}/src/math/scalbf.c
#    ${MUSL_SOURCE_DIR}/src/math/scalbln.c
#    ${MUSL_SOURCE_DIR}/src/math/scalblnf.c
#    ${MUSL_SOURCE_DIR}/src/math/scalblnl.c
#    ${MUSL_SOURCE_DIR}/src/math/scalbn.c
#    ${MUSL_SOURCE_DIR}/src/math/scalbnf.c
#    ${MUSL_SOURCE_DIR}/src/math/scalbnl.c
#    ${MUSL_SOURCE_DIR}/src/math/signgam.c
#    ${MUSL_SOURCE_DIR}/src/math/significand.c
#    ${MUSL_SOURCE_DIR}/src/math/significandf.c
#    ${MUSL_SOURCE_DIR}/src/math/sin.c
#    ${MUSL_SOURCE_DIR}/src/math/sincos.c
#    ${MUSL_SOURCE_DIR}/src/math/sincosf.c
#    ${MUSL_SOURCE_DIR}/src/math/sincosl.c
#    ${MUSL_SOURCE_DIR}/src/math/sinf.c
#    ${MUSL_SOURCE_DIR}/src/math/sinh.c
#    ${MUSL_SOURCE_DIR}/src/math/sinhf.c
#    ${MUSL_SOURCE_DIR}/src/math/sinhl.c
#    ${MUSL_SOURCE_DIR}/src/math/sinl.c
#    ${MUSL_SOURCE_DIR}/src/math/sqrt_data.c
#    ${MUSL_SOURCE_DIR}/src/math/tan.c
#    ${MUSL_SOURCE_DIR}/src/math/tanf.c
#    ${MUSL_SOURCE_DIR}/src/math/tanh.c
#    ${MUSL_SOURCE_DIR}/src/math/tanhf.c
#    ${MUSL_SOURCE_DIR}/src/math/tanhl.c
#    ${MUSL_SOURCE_DIR}/src/math/tanl.c
#    ${MUSL_SOURCE_DIR}/src/math/tgamma.c
#    ${MUSL_SOURCE_DIR}/src/math/tgammaf.c
#    ${MUSL_SOURCE_DIR}/src/math/tgammal.c
#    ${MUSL_SOURCE_DIR}/src/math/trunc.c
#    ${MUSL_SOURCE_DIR}/src/math/truncf.c
#    ${MUSL_SOURCE_DIR}/src/math/x86_64/fabs.c
#    ${MUSL_SOURCE_DIR}/src/math/x86_64/fabsf.c
#    ${MUSL_SOURCE_DIR}/src/math/x86_64/fabsl.c
#    ${MUSL_SOURCE_DIR}/src/math/x86_64/fma.c
#    ${MUSL_SOURCE_DIR}/src/math/x86_64/fmaf.c
#    ${MUSL_SOURCE_DIR}/src/math/x86_64/fmodl.c
#    ${MUSL_SOURCE_DIR}/src/math/x86_64/llrint.c
#    ${MUSL_SOURCE_DIR}/src/math/x86_64/llrintf.c
#    ${MUSL_SOURCE_DIR}/src/math/x86_64/llrintl.c
#    ${MUSL_SOURCE_DIR}/src/math/x86_64/lrint.c
#    ${MUSL_SOURCE_DIR}/src/math/x86_64/lrintf.c
#    ${MUSL_SOURCE_DIR}/src/math/x86_64/lrintl.c
#    ${MUSL_SOURCE_DIR}/src/math/x86_64/remainderl.c
#    ${MUSL_SOURCE_DIR}/src/math/x86_64/remquol.c
#    ${MUSL_SOURCE_DIR}/src/math/x86_64/rintl.c
#    ${MUSL_SOURCE_DIR}/src/math/x86_64/sqrt.c
#    ${MUSL_SOURCE_DIR}/src/math/x86_64/sqrtf.c
#    ${MUSL_SOURCE_DIR}/src/math/x86_64/sqrtl.c
#    ${MUSL_SOURCE_DIR}/src/misc/a64l.c
#    ${MUSL_SOURCE_DIR}/src/misc/basename.c
#    ${MUSL_SOURCE_DIR}/src/misc/dirname.c
#    ${MUSL_SOURCE_DIR}/src/misc/ffs.c
#    ${MUSL_SOURCE_DIR}/src/misc/ffsl.c
#    ${MUSL_SOURCE_DIR}/src/misc/ffsll.c
#    ${MUSL_SOURCE_DIR}/src/misc/fmtmsg.c
#    ${MUSL_SOURCE_DIR}/src/misc/forkpty.c
#    ${MUSL_SOURCE_DIR}/src/misc/get_current_dir_name.c
#    ${MUSL_SOURCE_DIR}/src/misc/getauxval.c
#    ${MUSL_SOURCE_DIR}/src/misc/getdomainname.c
#    ${MUSL_SOURCE_DIR}/src/misc/getentropy.c
#    ${MUSL_SOURCE_DIR}/src/misc/gethostid.c
#    ${MUSL_SOURCE_DIR}/src/misc/getopt.c
#    ${MUSL_SOURCE_DIR}/src/misc/getopt_long.c
#    ${MUSL_SOURCE_DIR}/src/misc/getpriority.c
#    ${MUSL_SOURCE_DIR}/src/misc/getresgid.c
#    ${MUSL_SOURCE_DIR}/src/misc/getresuid.c
#    ${MUSL_SOURCE_DIR}/src/misc/getrlimit.c
#    ${MUSL_SOURCE_DIR}/src/misc/getrusage.c
#    ${MUSL_SOURCE_DIR}/src/misc/getsubopt.c
#    ${MUSL_SOURCE_DIR}/src/misc/initgroups.c
#    ${MUSL_SOURCE_DIR}/src/misc/ioctl.c
#    ${MUSL_SOURCE_DIR}/src/misc/issetugid.c
#    ${MUSL_SOURCE_DIR}/src/misc/lockf.c
#    ${MUSL_SOURCE_DIR}/src/misc/login_tty.c
#    ${MUSL_SOURCE_DIR}/src/misc/mntent.c
#    ${MUSL_SOURCE_DIR}/src/misc/nftw.c
#    ${MUSL_SOURCE_DIR}/src/misc/openpty.c
#    ${MUSL_SOURCE_DIR}/src/misc/ptsname.c
#    ${MUSL_SOURCE_DIR}/src/misc/pty.c
#    ${MUSL_SOURCE_DIR}/src/misc/realpath.c
#    ${MUSL_SOURCE_DIR}/src/misc/setdomainname.c
#    ${MUSL_SOURCE_DIR}/src/misc/setpriority.c
#    ${MUSL_SOURCE_DIR}/src/misc/setrlimit.c
#    ${MUSL_SOURCE_DIR}/src/misc/syscall.c
#    ${MUSL_SOURCE_DIR}/src/misc/syslog.c
#    ${MUSL_SOURCE_DIR}/src/misc/uname.c
#    ${MUSL_SOURCE_DIR}/src/misc/wordexp.c
#    ${MUSL_SOURCE_DIR}/src/mman/madvise.c
#    ${MUSL_SOURCE_DIR}/src/mman/mincore.c
#    ${MUSL_SOURCE_DIR}/src/mman/mlock.c
#    ${MUSL_SOURCE_DIR}/src/mman/mlockall.c
#    ${MUSL_SOURCE_DIR}/src/mman/mmap.c
#    ${MUSL_SOURCE_DIR}/src/mman/mprotect.c
#    ${MUSL_SOURCE_DIR}/src/mman/mremap.c
#    ${MUSL_SOURCE_DIR}/src/mman/msync.c
#    ${MUSL_SOURCE_DIR}/src/mman/munlock.c
#    ${MUSL_SOURCE_DIR}/src/mman/munlockall.c
#    ${MUSL_SOURCE_DIR}/src/mman/munmap.c
#    ${MUSL_SOURCE_DIR}/src/mman/posix_madvise.c
#    ${MUSL_SOURCE_DIR}/src/mman/shm_open.c
#    ${MUSL_SOURCE_DIR}/src/mq/mq_close.c
#    ${MUSL_SOURCE_DIR}/src/mq/mq_getattr.c
#    ${MUSL_SOURCE_DIR}/src/mq/mq_notify.c
#    ${MUSL_SOURCE_DIR}/src/mq/mq_open.c
#    ${MUSL_SOURCE_DIR}/src/mq/mq_receive.c
#    ${MUSL_SOURCE_DIR}/src/mq/mq_send.c
#    ${MUSL_SOURCE_DIR}/src/mq/mq_setattr.c
#    ${MUSL_SOURCE_DIR}/src/mq/mq_timedreceive.c
#    ${MUSL_SOURCE_DIR}/src/mq/mq_timedsend.c
#    ${MUSL_SOURCE_DIR}/src/mq/mq_unlink.c
#    ${MUSL_SOURCE_DIR}/src/multibyte/btowc.c
#    ${MUSL_SOURCE_DIR}/src/multibyte/c16rtomb.c
#    ${MUSL_SOURCE_DIR}/src/multibyte/c32rtomb.c
#    ${MUSL_SOURCE_DIR}/src/multibyte/internal.c
#    ${MUSL_SOURCE_DIR}/src/multibyte/mblen.c
#    ${MUSL_SOURCE_DIR}/src/multibyte/mbrlen.c
#    ${MUSL_SOURCE_DIR}/src/multibyte/mbrtoc16.c
#    ${MUSL_SOURCE_DIR}/src/multibyte/mbrtoc32.c
#    ${MUSL_SOURCE_DIR}/src/multibyte/mbrtowc.c
#    ${MUSL_SOURCE_DIR}/src/multibyte/mbsinit.c
#    ${MUSL_SOURCE_DIR}/src/multibyte/mbsnrtowcs.c
#    ${MUSL_SOURCE_DIR}/src/multibyte/mbsrtowcs.c
#    ${MUSL_SOURCE_DIR}/src/multibyte/mbstowcs.c
#    ${MUSL_SOURCE_DIR}/src/multibyte/mbtowc.c
#    ${MUSL_SOURCE_DIR}/src/multibyte/wcrtomb.c
#    ${MUSL_SOURCE_DIR}/src/multibyte/wcsnrtombs.c
#    ${MUSL_SOURCE_DIR}/src/multibyte/wcsrtombs.c
#    ${MUSL_SOURCE_DIR}/src/multibyte/wcstombs.c
#    ${MUSL_SOURCE_DIR}/src/multibyte/wctob.c
#    ${MUSL_SOURCE_DIR}/src/multibyte/wctomb.c
    ${MUSL_SOURCE_DIR}/src/network/accept.c
    ${MUSL_SOURCE_DIR}/src/network/accept4.c
    ${MUSL_SOURCE_DIR}/src/network/bind.c
    ${MUSL_SOURCE_DIR}/src/network/connect.c
    ${MUSL_SOURCE_DIR}/src/network/dn_comp.c
    ${MUSL_SOURCE_DIR}/src/network/dn_expand.c
    ${MUSL_SOURCE_DIR}/src/network/dn_skipname.c
    ${MUSL_SOURCE_DIR}/src/network/dns_parse.c
    ${MUSL_SOURCE_DIR}/src/network/ent.c
    ${MUSL_SOURCE_DIR}/src/network/ether.c
    ${MUSL_SOURCE_DIR}/src/network/freeaddrinfo.c
    ${MUSL_SOURCE_DIR}/src/network/gai_strerror.c
    ${MUSL_SOURCE_DIR}/src/network/getaddrinfo.c
    ${MUSL_SOURCE_DIR}/src/network/gethostbyaddr.c
    ${MUSL_SOURCE_DIR}/src/network/gethostbyaddr_r.c
    ${MUSL_SOURCE_DIR}/src/network/gethostbyname.c
    ${MUSL_SOURCE_DIR}/src/network/gethostbyname2.c
    ${MUSL_SOURCE_DIR}/src/network/gethostbyname2_r.c
    ${MUSL_SOURCE_DIR}/src/network/gethostbyname_r.c
    ${MUSL_SOURCE_DIR}/src/network/getifaddrs.c
    ${MUSL_SOURCE_DIR}/src/network/getnameinfo.c
    ${MUSL_SOURCE_DIR}/src/network/getpeername.c
    ${MUSL_SOURCE_DIR}/src/network/getservbyname.c
    ${MUSL_SOURCE_DIR}/src/network/getservbyname_r.c
    ${MUSL_SOURCE_DIR}/src/network/getservbyport.c
    ${MUSL_SOURCE_DIR}/src/network/getservbyport_r.c
    ${MUSL_SOURCE_DIR}/src/network/getsockname.c
    ${MUSL_SOURCE_DIR}/src/network/getsockopt.c
    ${MUSL_SOURCE_DIR}/src/network/h_errno.c
    ${MUSL_SOURCE_DIR}/src/network/herror.c
    ${MUSL_SOURCE_DIR}/src/network/hstrerror.c
    ${MUSL_SOURCE_DIR}/src/network/htonl.c
    ${MUSL_SOURCE_DIR}/src/network/htons.c
    ${MUSL_SOURCE_DIR}/src/network/if_freenameindex.c
    ${MUSL_SOURCE_DIR}/src/network/if_indextoname.c
    ${MUSL_SOURCE_DIR}/src/network/if_nameindex.c
    ${MUSL_SOURCE_DIR}/src/network/if_nametoindex.c
    ${MUSL_SOURCE_DIR}/src/network/in6addr_any.c
    ${MUSL_SOURCE_DIR}/src/network/in6addr_loopback.c
    ${MUSL_SOURCE_DIR}/src/network/inet_addr.c
    ${MUSL_SOURCE_DIR}/src/network/inet_aton.c
    ${MUSL_SOURCE_DIR}/src/network/inet_legacy.c
    ${MUSL_SOURCE_DIR}/src/network/inet_ntoa.c
    ${MUSL_SOURCE_DIR}/src/network/inet_ntop.c
    ${MUSL_SOURCE_DIR}/src/network/inet_pton.c
    ${MUSL_SOURCE_DIR}/src/network/listen.c
    ${MUSL_SOURCE_DIR}/src/network/lookup_ipliteral.c
    ${MUSL_SOURCE_DIR}/src/network/lookup_name.c
    ${MUSL_SOURCE_DIR}/src/network/lookup_serv.c
    ${MUSL_SOURCE_DIR}/src/network/netlink.c
    ${MUSL_SOURCE_DIR}/src/network/netname.c
    ${MUSL_SOURCE_DIR}/src/network/ns_parse.c
    ${MUSL_SOURCE_DIR}/src/network/ntohl.c
    ${MUSL_SOURCE_DIR}/src/network/ntohs.c
    ${MUSL_SOURCE_DIR}/src/network/proto.c
    ${MUSL_SOURCE_DIR}/src/network/recv.c
    ${MUSL_SOURCE_DIR}/src/network/recvfrom.c
    ${MUSL_SOURCE_DIR}/src/network/recvmmsg.c
    ${MUSL_SOURCE_DIR}/src/network/recvmsg.c
    ${MUSL_SOURCE_DIR}/src/network/res_init.c
    ${MUSL_SOURCE_DIR}/src/network/res_mkquery.c
    ${MUSL_SOURCE_DIR}/src/network/res_msend.c
    ${MUSL_SOURCE_DIR}/src/network/res_query.c
    ${MUSL_SOURCE_DIR}/src/network/res_querydomain.c
    ${MUSL_SOURCE_DIR}/src/network/res_send.c
    ${MUSL_SOURCE_DIR}/src/network/res_state.c
    ${MUSL_SOURCE_DIR}/src/network/resolvconf.c
    ${MUSL_SOURCE_DIR}/src/network/send.c
    ${MUSL_SOURCE_DIR}/src/network/sendmmsg.c
    ${MUSL_SOURCE_DIR}/src/network/sendmsg.c
    ${MUSL_SOURCE_DIR}/src/network/sendto.c
    ${MUSL_SOURCE_DIR}/src/network/serv.c
    ${MUSL_SOURCE_DIR}/src/network/setsockopt.c
    ${MUSL_SOURCE_DIR}/src/network/shutdown.c
    ${MUSL_SOURCE_DIR}/src/network/sockatmark.c
    ${MUSL_SOURCE_DIR}/src/network/socket.c
    ${MUSL_SOURCE_DIR}/src/network/socketpair.c
#    ${MUSL_SOURCE_DIR}/src/passwd/fgetgrent.c
#    ${MUSL_SOURCE_DIR}/src/passwd/fgetpwent.c
#    ${MUSL_SOURCE_DIR}/src/passwd/fgetspent.c
#    ${MUSL_SOURCE_DIR}/src/passwd/getgr_a.c
#    ${MUSL_SOURCE_DIR}/src/passwd/getgr_r.c
#    ${MUSL_SOURCE_DIR}/src/passwd/getgrent.c
#    ${MUSL_SOURCE_DIR}/src/passwd/getgrent_a.c
#    ${MUSL_SOURCE_DIR}/src/passwd/getgrouplist.c
#    ${MUSL_SOURCE_DIR}/src/passwd/getpw_a.c
#    ${MUSL_SOURCE_DIR}/src/passwd/getpw_r.c
#    ${MUSL_SOURCE_DIR}/src/passwd/getpwent.c
#    ${MUSL_SOURCE_DIR}/src/passwd/getpwent_a.c
#    ${MUSL_SOURCE_DIR}/src/passwd/getspent.c
#    ${MUSL_SOURCE_DIR}/src/passwd/getspnam.c
#    ${MUSL_SOURCE_DIR}/src/passwd/getspnam_r.c
#    ${MUSL_SOURCE_DIR}/src/passwd/lckpwdf.c
#    ${MUSL_SOURCE_DIR}/src/passwd/nscd_query.c
#    ${MUSL_SOURCE_DIR}/src/passwd/putgrent.c
#    ${MUSL_SOURCE_DIR}/src/passwd/putpwent.c
#    ${MUSL_SOURCE_DIR}/src/passwd/putspent.c
#    ${MUSL_SOURCE_DIR}/src/prng/__rand48_step.c
#    ${MUSL_SOURCE_DIR}/src/prng/__seed48.c
#    ${MUSL_SOURCE_DIR}/src/prng/drand48.c
#    ${MUSL_SOURCE_DIR}/src/prng/lcong48.c
#    ${MUSL_SOURCE_DIR}/src/prng/lrand48.c
#    ${MUSL_SOURCE_DIR}/src/prng/mrand48.c
#    ${MUSL_SOURCE_DIR}/src/prng/rand.c
#    ${MUSL_SOURCE_DIR}/src/prng/rand_r.c
#    ${MUSL_SOURCE_DIR}/src/prng/random.c
#    ${MUSL_SOURCE_DIR}/src/prng/seed48.c
#    ${MUSL_SOURCE_DIR}/src/prng/srand48.c
#    ${MUSL_SOURCE_DIR}/src/process/execl.c
#    ${MUSL_SOURCE_DIR}/src/process/execle.c
#    ${MUSL_SOURCE_DIR}/src/process/execlp.c
#    ${MUSL_SOURCE_DIR}/src/process/execv.c
#    ${MUSL_SOURCE_DIR}/src/process/execve.c
#    ${MUSL_SOURCE_DIR}/src/process/execvp.c
#    ${MUSL_SOURCE_DIR}/src/process/fexecve.c
#    ${MUSL_SOURCE_DIR}/src/process/fork.c
#    ${MUSL_SOURCE_DIR}/src/process/posix_spawn.c
#    ${MUSL_SOURCE_DIR}/src/process/posix_spawn_file_actions_addchdir.c
#    ${MUSL_SOURCE_DIR}/src/process/posix_spawn_file_actions_addclose.c
#    ${MUSL_SOURCE_DIR}/src/process/posix_spawn_file_actions_adddup2.c
#    ${MUSL_SOURCE_DIR}/src/process/posix_spawn_file_actions_addfchdir.c
#    ${MUSL_SOURCE_DIR}/src/process/posix_spawn_file_actions_addopen.c
#    ${MUSL_SOURCE_DIR}/src/process/posix_spawn_file_actions_destroy.c
#    ${MUSL_SOURCE_DIR}/src/process/posix_spawn_file_actions_init.c
#    ${MUSL_SOURCE_DIR}/src/process/posix_spawnattr_destroy.c
#    ${MUSL_SOURCE_DIR}/src/process/posix_spawnattr_getflags.c
#    ${MUSL_SOURCE_DIR}/src/process/posix_spawnattr_getpgroup.c
#    ${MUSL_SOURCE_DIR}/src/process/posix_spawnattr_getsigdefault.c
#    ${MUSL_SOURCE_DIR}/src/process/posix_spawnattr_getsigmask.c
#    ${MUSL_SOURCE_DIR}/src/process/posix_spawnattr_init.c
#    ${MUSL_SOURCE_DIR}/src/process/posix_spawnattr_sched.c
#    ${MUSL_SOURCE_DIR}/src/process/posix_spawnattr_setflags.c
#    ${MUSL_SOURCE_DIR}/src/process/posix_spawnattr_setpgroup.c
#    ${MUSL_SOURCE_DIR}/src/process/posix_spawnattr_setsigdefault.c
#    ${MUSL_SOURCE_DIR}/src/process/posix_spawnattr_setsigmask.c
#    ${MUSL_SOURCE_DIR}/src/process/posix_spawnp.c
#    ${MUSL_SOURCE_DIR}/src/process/system.c
#    ${MUSL_SOURCE_DIR}/src/process/wait.c
#    ${MUSL_SOURCE_DIR}/src/process/waitid.c
#    ${MUSL_SOURCE_DIR}/src/process/waitpid.c
#    ${MUSL_SOURCE_DIR}/src/regex/fnmatch.c
#    ${MUSL_SOURCE_DIR}/src/regex/glob.c
#    ${MUSL_SOURCE_DIR}/src/regex/regcomp.c
#    ${MUSL_SOURCE_DIR}/src/regex/regerror.c
#    ${MUSL_SOURCE_DIR}/src/regex/regexec.c
#    ${MUSL_SOURCE_DIR}/src/sched/affinity.c
#    ${MUSL_SOURCE_DIR}/src/sched/sched_cpucount.c
#    ${MUSL_SOURCE_DIR}/src/sched/sched_get_priority_max.c
#    ${MUSL_SOURCE_DIR}/src/sched/sched_getcpu.c
#    ${MUSL_SOURCE_DIR}/src/sched/sched_getparam.c
#    ${MUSL_SOURCE_DIR}/src/sched/sched_getscheduler.c
#    ${MUSL_SOURCE_DIR}/src/sched/sched_rr_get_interval.c
#    ${MUSL_SOURCE_DIR}/src/sched/sched_setparam.c
#    ${MUSL_SOURCE_DIR}/src/sched/sched_setscheduler.c
#    ${MUSL_SOURCE_DIR}/src/sched/sched_yield.c
#    ${MUSL_SOURCE_DIR}/src/search/hsearch.c
#    ${MUSL_SOURCE_DIR}/src/search/insque.c
#    ${MUSL_SOURCE_DIR}/src/search/lsearch.c
#    ${MUSL_SOURCE_DIR}/src/search/tdelete.c
#    ${MUSL_SOURCE_DIR}/src/search/tdestroy.c
#    ${MUSL_SOURCE_DIR}/src/search/tfind.c
#    ${MUSL_SOURCE_DIR}/src/search/tsearch.c
#    ${MUSL_SOURCE_DIR}/src/search/twalk.c
#    ${MUSL_SOURCE_DIR}/src/select/poll.c
#    ${MUSL_SOURCE_DIR}/src/select/pselect.c
#    ${MUSL_SOURCE_DIR}/src/select/select.c
#    ${MUSL_SOURCE_DIR}/src/signal/block.c
#    ${MUSL_SOURCE_DIR}/src/signal/getitimer.c
#    ${MUSL_SOURCE_DIR}/src/signal/kill.c
#    ${MUSL_SOURCE_DIR}/src/signal/killpg.c
#    ${MUSL_SOURCE_DIR}/src/signal/psiginfo.c
#    ${MUSL_SOURCE_DIR}/src/signal/psignal.c
#    ${MUSL_SOURCE_DIR}/src/signal/raise.c
#    ${MUSL_SOURCE_DIR}/src/signal/setitimer.c
#    ${MUSL_SOURCE_DIR}/src/signal/sigaction.c
#    ${MUSL_SOURCE_DIR}/src/signal/sigaddset.c
#    ${MUSL_SOURCE_DIR}/src/signal/sigaltstack.c
#    ${MUSL_SOURCE_DIR}/src/signal/sigandset.c
#    ${MUSL_SOURCE_DIR}/src/signal/sigdelset.c
#    ${MUSL_SOURCE_DIR}/src/signal/sigemptyset.c
#    ${MUSL_SOURCE_DIR}/src/signal/sigfillset.c
#    ${MUSL_SOURCE_DIR}/src/signal/sighold.c
#    ${MUSL_SOURCE_DIR}/src/signal/sigignore.c
#    ${MUSL_SOURCE_DIR}/src/signal/siginterrupt.c
#    ${MUSL_SOURCE_DIR}/src/signal/sigisemptyset.c
#    ${MUSL_SOURCE_DIR}/src/signal/sigismember.c
#    ${MUSL_SOURCE_DIR}/src/signal/siglongjmp.c
#    ${MUSL_SOURCE_DIR}/src/signal/signal.c
#    ${MUSL_SOURCE_DIR}/src/signal/sigorset.c
#    ${MUSL_SOURCE_DIR}/src/signal/sigpause.c
#    ${MUSL_SOURCE_DIR}/src/signal/sigpending.c
#    ${MUSL_SOURCE_DIR}/src/signal/sigprocmask.c
#    ${MUSL_SOURCE_DIR}/src/signal/sigqueue.c
#    ${MUSL_SOURCE_DIR}/src/signal/sigrelse.c
#    ${MUSL_SOURCE_DIR}/src/signal/sigrtmax.c
#    ${MUSL_SOURCE_DIR}/src/signal/sigrtmin.c
#    ${MUSL_SOURCE_DIR}/src/signal/sigset.c
#    ${MUSL_SOURCE_DIR}/src/signal/sigsetjmp_tail.c
#    ${MUSL_SOURCE_DIR}/src/signal/sigsuspend.c
#    ${MUSL_SOURCE_DIR}/src/signal/sigtimedwait.c
#    ${MUSL_SOURCE_DIR}/src/signal/sigwait.c
#    ${MUSL_SOURCE_DIR}/src/signal/sigwaitinfo.c
    ${MUSL_SOURCE_DIR}/src/stat/__xstat.c
#    ${MUSL_SOURCE_DIR}/src/stat/chmod.c
#    ${MUSL_SOURCE_DIR}/src/stat/fchmod.c
#    ${MUSL_SOURCE_DIR}/src/stat/fchmodat.c
#    ${MUSL_SOURCE_DIR}/src/stat/fstat.c
#    ${MUSL_SOURCE_DIR}/src/stat/fstatat.c
#    ${MUSL_SOURCE_DIR}/src/stat/futimens.c
#    ${MUSL_SOURCE_DIR}/src/stat/futimesat.c
#    ${MUSL_SOURCE_DIR}/src/stat/lchmod.c
#    ${MUSL_SOURCE_DIR}/src/stat/lstat.c
#    ${MUSL_SOURCE_DIR}/src/stat/mkdir.c
#    ${MUSL_SOURCE_DIR}/src/stat/mkdirat.c
#    ${MUSL_SOURCE_DIR}/src/stat/mkfifo.c
#    ${MUSL_SOURCE_DIR}/src/stat/mkfifoat.c
#    ${MUSL_SOURCE_DIR}/src/stat/mknod.c
#    ${MUSL_SOURCE_DIR}/src/stat/mknodat.c
#    ${MUSL_SOURCE_DIR}/src/stat/stat.c
#    ${MUSL_SOURCE_DIR}/src/stat/statvfs.c
#    ${MUSL_SOURCE_DIR}/src/stat/umask.c
#    ${MUSL_SOURCE_DIR}/src/stat/utimensat.c
    ${MUSL_SOURCE_DIR}/src/stdio/__fclose_ca.c
    ${MUSL_SOURCE_DIR}/src/stdio/__fdopen.c
    ${MUSL_SOURCE_DIR}/src/stdio/__fmodeflags.c
    ${MUSL_SOURCE_DIR}/src/stdio/__fopen_rb_ca.c
    ${MUSL_SOURCE_DIR}/src/stdio/__lockfile.c
    ${MUSL_SOURCE_DIR}/src/stdio/__overflow.c
    ${MUSL_SOURCE_DIR}/src/stdio/__stdio_close.c
    ${MUSL_SOURCE_DIR}/src/stdio/__stdio_exit.c
    ${MUSL_SOURCE_DIR}/src/stdio/__stdio_read.c
    ${MUSL_SOURCE_DIR}/src/stdio/__stdio_seek.c
    ${MUSL_SOURCE_DIR}/src/stdio/__stdio_write.c
    ${MUSL_SOURCE_DIR}/src/stdio/__stdout_write.c
    ${MUSL_SOURCE_DIR}/src/stdio/__toread.c
    ${MUSL_SOURCE_DIR}/src/stdio/__towrite.c
    ${MUSL_SOURCE_DIR}/src/stdio/__uflow.c
#    ${MUSL_SOURCE_DIR}/src/stdio/asprintf.c
#    ${MUSL_SOURCE_DIR}/src/stdio/clearerr.c
#    ${MUSL_SOURCE_DIR}/src/stdio/dprintf.c
#    ${MUSL_SOURCE_DIR}/src/stdio/ext.c
#    ${MUSL_SOURCE_DIR}/src/stdio/ext2.c
#    ${MUSL_SOURCE_DIR}/src/stdio/fclose.c
#    ${MUSL_SOURCE_DIR}/src/stdio/feof.c
#    ${MUSL_SOURCE_DIR}/src/stdio/ferror.c
#    ${MUSL_SOURCE_DIR}/src/stdio/fflush.c
#    ${MUSL_SOURCE_DIR}/src/stdio/fgetc.c
#    ${MUSL_SOURCE_DIR}/src/stdio/fgetln.c
#    ${MUSL_SOURCE_DIR}/src/stdio/fgetpos.c
#    ${MUSL_SOURCE_DIR}/src/stdio/fgets.c
#    ${MUSL_SOURCE_DIR}/src/stdio/fgetwc.c
#    ${MUSL_SOURCE_DIR}/src/stdio/fgetws.c
#    ${MUSL_SOURCE_DIR}/src/stdio/fileno.c
#    ${MUSL_SOURCE_DIR}/src/stdio/flockfile.c
#    ${MUSL_SOURCE_DIR}/src/stdio/fmemopen.c
#    ${MUSL_SOURCE_DIR}/src/stdio/fopen.c
#    ${MUSL_SOURCE_DIR}/src/stdio/fopencookie.c
#    ${MUSL_SOURCE_DIR}/src/stdio/fprintf.c
#    ${MUSL_SOURCE_DIR}/src/stdio/fputc.c
#    ${MUSL_SOURCE_DIR}/src/stdio/fputs.c
#    ${MUSL_SOURCE_DIR}/src/stdio/fputwc.c
#    ${MUSL_SOURCE_DIR}/src/stdio/fputws.c
#    ${MUSL_SOURCE_DIR}/src/stdio/fread.c
#    ${MUSL_SOURCE_DIR}/src/stdio/freopen.c
#    ${MUSL_SOURCE_DIR}/src/stdio/fscanf.c
#    ${MUSL_SOURCE_DIR}/src/stdio/fseek.c
#    ${MUSL_SOURCE_DIR}/src/stdio/fsetpos.c
#    ${MUSL_SOURCE_DIR}/src/stdio/ftell.c
#    ${MUSL_SOURCE_DIR}/src/stdio/ftrylockfile.c
#    ${MUSL_SOURCE_DIR}/src/stdio/funlockfile.c
#    ${MUSL_SOURCE_DIR}/src/stdio/fwide.c
#    ${MUSL_SOURCE_DIR}/src/stdio/fwprintf.c
#    ${MUSL_SOURCE_DIR}/src/stdio/fwrite.c
#    ${MUSL_SOURCE_DIR}/src/stdio/fwscanf.c
#    ${MUSL_SOURCE_DIR}/src/stdio/getc.c
#    ${MUSL_SOURCE_DIR}/src/stdio/getc_unlocked.c
#    ${MUSL_SOURCE_DIR}/src/stdio/getchar.c
#    ${MUSL_SOURCE_DIR}/src/stdio/getchar_unlocked.c
#    ${MUSL_SOURCE_DIR}/src/stdio/getdelim.c
#    ${MUSL_SOURCE_DIR}/src/stdio/getline.c
#    ${MUSL_SOURCE_DIR}/src/stdio/gets.c
#    ${MUSL_SOURCE_DIR}/src/stdio/getw.c
#    ${MUSL_SOURCE_DIR}/src/stdio/getwc.c
#    ${MUSL_SOURCE_DIR}/src/stdio/getwchar.c
#    ${MUSL_SOURCE_DIR}/src/stdio/ofl.c
#    ${MUSL_SOURCE_DIR}/src/stdio/ofl_add.c
#    ${MUSL_SOURCE_DIR}/src/stdio/open_memstream.c
#    ${MUSL_SOURCE_DIR}/src/stdio/open_wmemstream.c
#    ${MUSL_SOURCE_DIR}/src/stdio/pclose.c
#    ${MUSL_SOURCE_DIR}/src/stdio/perror.c
#    ${MUSL_SOURCE_DIR}/src/stdio/popen.c
#    ${MUSL_SOURCE_DIR}/src/stdio/printf.c
#    ${MUSL_SOURCE_DIR}/src/stdio/putc.c
#    ${MUSL_SOURCE_DIR}/src/stdio/putc_unlocked.c
#    ${MUSL_SOURCE_DIR}/src/stdio/putchar.c
#    ${MUSL_SOURCE_DIR}/src/stdio/putchar_unlocked.c
#    ${MUSL_SOURCE_DIR}/src/stdio/puts.c
#    ${MUSL_SOURCE_DIR}/src/stdio/putw.c
#    ${MUSL_SOURCE_DIR}/src/stdio/putwc.c
#    ${MUSL_SOURCE_DIR}/src/stdio/putwchar.c
#    ${MUSL_SOURCE_DIR}/src/stdio/remove.c
#    ${MUSL_SOURCE_DIR}/src/stdio/rename.c
#    ${MUSL_SOURCE_DIR}/src/stdio/rewind.c
#    ${MUSL_SOURCE_DIR}/src/stdio/scanf.c
#    ${MUSL_SOURCE_DIR}/src/stdio/setbuf.c
#    ${MUSL_SOURCE_DIR}/src/stdio/setbuffer.c
#    ${MUSL_SOURCE_DIR}/src/stdio/setlinebuf.c
#    ${MUSL_SOURCE_DIR}/src/stdio/setvbuf.c
#    ${MUSL_SOURCE_DIR}/src/stdio/snprintf.c
#    ${MUSL_SOURCE_DIR}/src/stdio/sprintf.c
#    ${MUSL_SOURCE_DIR}/src/stdio/sscanf.c
#    ${MUSL_SOURCE_DIR}/src/stdio/stderr.c
#    ${MUSL_SOURCE_DIR}/src/stdio/stdin.c
#    ${MUSL_SOURCE_DIR}/src/stdio/stdout.c
#    ${MUSL_SOURCE_DIR}/src/stdio/swprintf.c
#    ${MUSL_SOURCE_DIR}/src/stdio/swscanf.c
#    ${MUSL_SOURCE_DIR}/src/stdio/tempnam.c
#    ${MUSL_SOURCE_DIR}/src/stdio/tmpfile.c
#    ${MUSL_SOURCE_DIR}/src/stdio/tmpnam.c
#    ${MUSL_SOURCE_DIR}/src/stdio/ungetc.c
#    ${MUSL_SOURCE_DIR}/src/stdio/ungetwc.c
#    ${MUSL_SOURCE_DIR}/src/stdio/vasprintf.c
#    ${MUSL_SOURCE_DIR}/src/stdio/vdprintf.c
#    ${MUSL_SOURCE_DIR}/src/stdio/vfprintf.c
#    ${MUSL_SOURCE_DIR}/src/stdio/vfscanf.c
#    ${MUSL_SOURCE_DIR}/src/stdio/vfwprintf.c
#    ${MUSL_SOURCE_DIR}/src/stdio/vfwscanf.c
#    ${MUSL_SOURCE_DIR}/src/stdio/vprintf.c
#    ${MUSL_SOURCE_DIR}/src/stdio/vscanf.c
#    ${MUSL_SOURCE_DIR}/src/stdio/vsnprintf.c
#    ${MUSL_SOURCE_DIR}/src/stdio/vsprintf.c
#    ${MUSL_SOURCE_DIR}/src/stdio/vsscanf.c
#    ${MUSL_SOURCE_DIR}/src/stdio/vswprintf.c
#    ${MUSL_SOURCE_DIR}/src/stdio/vswscanf.c
#    ${MUSL_SOURCE_DIR}/src/stdio/vwprintf.c
#    ${MUSL_SOURCE_DIR}/src/stdio/vwscanf.c
#    ${MUSL_SOURCE_DIR}/src/stdio/wprintf.c
#    ${MUSL_SOURCE_DIR}/src/stdio/wscanf.c
#    ${MUSL_SOURCE_DIR}/src/stdlib/abs.c
#    ${MUSL_SOURCE_DIR}/src/stdlib/atof.c
#    ${MUSL_SOURCE_DIR}/src/stdlib/atoi.c
#    ${MUSL_SOURCE_DIR}/src/stdlib/atol.c
#    ${MUSL_SOURCE_DIR}/src/stdlib/atoll.c
#    ${MUSL_SOURCE_DIR}/src/stdlib/bsearch.c
#    ${MUSL_SOURCE_DIR}/src/stdlib/div.c
#    ${MUSL_SOURCE_DIR}/src/stdlib/ecvt.c
#    ${MUSL_SOURCE_DIR}/src/stdlib/fcvt.c
#    ${MUSL_SOURCE_DIR}/src/stdlib/gcvt.c
#    ${MUSL_SOURCE_DIR}/src/stdlib/imaxabs.c
#    ${MUSL_SOURCE_DIR}/src/stdlib/imaxdiv.c
#    ${MUSL_SOURCE_DIR}/src/stdlib/labs.c
#    ${MUSL_SOURCE_DIR}/src/stdlib/ldiv.c
#    ${MUSL_SOURCE_DIR}/src/stdlib/llabs.c
#    ${MUSL_SOURCE_DIR}/src/stdlib/lldiv.c
#    ${MUSL_SOURCE_DIR}/src/stdlib/qsort.c
#    ${MUSL_SOURCE_DIR}/src/stdlib/strtod.c
#    ${MUSL_SOURCE_DIR}/src/stdlib/strtol.c
#    ${MUSL_SOURCE_DIR}/src/stdlib/wcstod.c
#    ${MUSL_SOURCE_DIR}/src/stdlib/wcstol.c
#    ${MUSL_SOURCE_DIR}/src/string/bcmp.c
#    ${MUSL_SOURCE_DIR}/src/string/bcopy.c
#    ${MUSL_SOURCE_DIR}/src/string/bzero.c
#    ${MUSL_SOURCE_DIR}/src/string/explicit_bzero.c
#    ${MUSL_SOURCE_DIR}/src/string/index.c
#    ${MUSL_SOURCE_DIR}/src/string/memccpy.c
#    ${MUSL_SOURCE_DIR}/src/string/memchr.c
#    ${MUSL_SOURCE_DIR}/src/string/memcmp.c
#    ${MUSL_SOURCE_DIR}/src/string/memmem.c
#    ${MUSL_SOURCE_DIR}/src/string/mempcpy.c
#    ${MUSL_SOURCE_DIR}/src/string/memrchr.c
#    ${MUSL_SOURCE_DIR}/src/string/rindex.c
#    ${MUSL_SOURCE_DIR}/src/string/stpcpy.c
#    ${MUSL_SOURCE_DIR}/src/string/stpncpy.c
#    ${MUSL_SOURCE_DIR}/src/string/strcasecmp.c
#    ${MUSL_SOURCE_DIR}/src/string/strcasestr.c
#    ${MUSL_SOURCE_DIR}/src/string/strcat.c
#    ${MUSL_SOURCE_DIR}/src/string/strchr.c
#    ${MUSL_SOURCE_DIR}/src/string/strchrnul.c
#    ${MUSL_SOURCE_DIR}/src/string/strcmp.c
#    ${MUSL_SOURCE_DIR}/src/string/strcpy.c
#    ${MUSL_SOURCE_DIR}/src/string/strcspn.c
#    ${MUSL_SOURCE_DIR}/src/string/strdup.c
#    ${MUSL_SOURCE_DIR}/src/string/strerror_r.c
#    ${MUSL_SOURCE_DIR}/src/string/strlcat.c
#    ${MUSL_SOURCE_DIR}/src/string/strlcpy.c
#    ${MUSL_SOURCE_DIR}/src/string/strlen.c
#    ${MUSL_SOURCE_DIR}/src/string/strncasecmp.c
#    ${MUSL_SOURCE_DIR}/src/string/strncat.c
#    ${MUSL_SOURCE_DIR}/src/string/strncmp.c
#    ${MUSL_SOURCE_DIR}/src/string/strncpy.c
#    ${MUSL_SOURCE_DIR}/src/string/strndup.c
#    ${MUSL_SOURCE_DIR}/src/string/strnlen.c
#    ${MUSL_SOURCE_DIR}/src/string/strpbrk.c
#    ${MUSL_SOURCE_DIR}/src/string/strrchr.c
#    ${MUSL_SOURCE_DIR}/src/string/strsep.c
#    ${MUSL_SOURCE_DIR}/src/string/strsignal.c
#    ${MUSL_SOURCE_DIR}/src/string/strspn.c
#    ${MUSL_SOURCE_DIR}/src/string/strstr.c
#    ${MUSL_SOURCE_DIR}/src/string/strtok.c
#    ${MUSL_SOURCE_DIR}/src/string/strtok_r.c
#    ${MUSL_SOURCE_DIR}/src/string/strverscmp.c
#    ${MUSL_SOURCE_DIR}/src/string/swab.c
#    ${MUSL_SOURCE_DIR}/src/string/wcpcpy.c
#    ${MUSL_SOURCE_DIR}/src/string/wcpncpy.c
#    ${MUSL_SOURCE_DIR}/src/string/wcscasecmp.c
#    ${MUSL_SOURCE_DIR}/src/string/wcscasecmp_l.c
#    ${MUSL_SOURCE_DIR}/src/string/wcscat.c
#    ${MUSL_SOURCE_DIR}/src/string/wcschr.c
#    ${MUSL_SOURCE_DIR}/src/string/wcscmp.c
#    ${MUSL_SOURCE_DIR}/src/string/wcscpy.c
#    ${MUSL_SOURCE_DIR}/src/string/wcscspn.c
#    ${MUSL_SOURCE_DIR}/src/string/wcsdup.c
#    ${MUSL_SOURCE_DIR}/src/string/wcslen.c
#    ${MUSL_SOURCE_DIR}/src/string/wcsncasecmp.c
#    ${MUSL_SOURCE_DIR}/src/string/wcsncasecmp_l.c
#    ${MUSL_SOURCE_DIR}/src/string/wcsncat.c
#    ${MUSL_SOURCE_DIR}/src/string/wcsncmp.c
#    ${MUSL_SOURCE_DIR}/src/string/wcsncpy.c
#    ${MUSL_SOURCE_DIR}/src/string/wcsnlen.c
#    ${MUSL_SOURCE_DIR}/src/string/wcspbrk.c
#    ${MUSL_SOURCE_DIR}/src/string/wcsrchr.c
#    ${MUSL_SOURCE_DIR}/src/string/wcsspn.c
#    ${MUSL_SOURCE_DIR}/src/string/wcsstr.c
#    ${MUSL_SOURCE_DIR}/src/string/wcstok.c
#    ${MUSL_SOURCE_DIR}/src/string/wcswcs.c
#    ${MUSL_SOURCE_DIR}/src/string/wmemchr.c
#    ${MUSL_SOURCE_DIR}/src/string/wmemcmp.c
#    ${MUSL_SOURCE_DIR}/src/string/wmemcpy.c
#    ${MUSL_SOURCE_DIR}/src/string/wmemmove.c
#    ${MUSL_SOURCE_DIR}/src/string/wmemset.c
#    ${MUSL_SOURCE_DIR}/src/temp/__randname.c
#    ${MUSL_SOURCE_DIR}/src/temp/mkdtemp.c
#    ${MUSL_SOURCE_DIR}/src/temp/mkostemp.c
#    ${MUSL_SOURCE_DIR}/src/temp/mkostemps.c
#    ${MUSL_SOURCE_DIR}/src/temp/mkstemp.c
#    ${MUSL_SOURCE_DIR}/src/temp/mkstemps.c
#    ${MUSL_SOURCE_DIR}/src/temp/mktemp.c
#    ${MUSL_SOURCE_DIR}/src/termios/cfgetospeed.c
#    ${MUSL_SOURCE_DIR}/src/termios/cfmakeraw.c
#    ${MUSL_SOURCE_DIR}/src/termios/cfsetospeed.c
#    ${MUSL_SOURCE_DIR}/src/termios/tcdrain.c
#    ${MUSL_SOURCE_DIR}/src/termios/tcflow.c
#    ${MUSL_SOURCE_DIR}/src/termios/tcflush.c
#    ${MUSL_SOURCE_DIR}/src/termios/tcgetattr.c
#    ${MUSL_SOURCE_DIR}/src/termios/tcgetsid.c
#    ${MUSL_SOURCE_DIR}/src/termios/tcgetwinsize.c
#    ${MUSL_SOURCE_DIR}/src/termios/tcsendbreak.c
#    ${MUSL_SOURCE_DIR}/src/termios/tcsetattr.c
#    ${MUSL_SOURCE_DIR}/src/termios/tcsetwinsize.c
    ${MUSL_SOURCE_DIR}/src/thread/__lock.c
    ${MUSL_SOURCE_DIR}/src/thread/__syscall_cp.c
#    ${MUSL_SOURCE_DIR}/src/thread/__timedwait.c
#    ${MUSL_SOURCE_DIR}/src/thread/__tls_get_addr.c
#    ${MUSL_SOURCE_DIR}/src/thread/__wait.c
#    ${MUSL_SOURCE_DIR}/src/thread/call_once.c
#    ${MUSL_SOURCE_DIR}/src/thread/cnd_broadcast.c
#    ${MUSL_SOURCE_DIR}/src/thread/cnd_destroy.c
#    ${MUSL_SOURCE_DIR}/src/thread/cnd_init.c
#    ${MUSL_SOURCE_DIR}/src/thread/cnd_signal.c
#    ${MUSL_SOURCE_DIR}/src/thread/cnd_timedwait.c
#    ${MUSL_SOURCE_DIR}/src/thread/cnd_wait.c
#    ${MUSL_SOURCE_DIR}/src/thread/default_attr.c
#    ${MUSL_SOURCE_DIR}/src/thread/lock_ptc.c
#    ${MUSL_SOURCE_DIR}/src/thread/mtx_destroy.c
#    ${MUSL_SOURCE_DIR}/src/thread/mtx_init.c
#    ${MUSL_SOURCE_DIR}/src/thread/mtx_lock.c
#    ${MUSL_SOURCE_DIR}/src/thread/mtx_timedlock.c
#    ${MUSL_SOURCE_DIR}/src/thread/mtx_trylock.c
#    ${MUSL_SOURCE_DIR}/src/thread/mtx_unlock.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_atfork.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_attr_destroy.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_attr_get.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_attr_init.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_attr_setdetachstate.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_attr_setguardsize.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_attr_setinheritsched.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_attr_setschedparam.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_attr_setschedpolicy.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_attr_setscope.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_attr_setstack.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_attr_setstacksize.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_barrier_destroy.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_barrier_init.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_barrier_wait.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_barrierattr_destroy.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_barrierattr_init.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_barrierattr_setpshared.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_cancel.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_cleanup_push.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_cond_broadcast.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_cond_destroy.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_cond_init.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_cond_signal.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_cond_timedwait.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_cond_wait.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_condattr_destroy.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_condattr_init.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_condattr_setclock.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_condattr_setpshared.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_create.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_detach.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_equal.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_getattr_np.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_getconcurrency.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_getcpuclockid.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_getschedparam.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_getspecific.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_join.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_key_create.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_kill.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_mutex_consistent.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_mutex_destroy.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_mutex_getprioceiling.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_mutex_init.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_mutex_lock.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_mutex_setprioceiling.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_mutex_timedlock.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_mutex_trylock.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_mutex_unlock.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_mutexattr_destroy.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_mutexattr_init.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_mutexattr_setprotocol.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_mutexattr_setpshared.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_mutexattr_setrobust.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_mutexattr_settype.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_once.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_rwlock_destroy.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_rwlock_init.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_rwlock_rdlock.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_rwlock_timedrdlock.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_rwlock_timedwrlock.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_rwlock_tryrdlock.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_rwlock_trywrlock.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_rwlock_unlock.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_rwlock_wrlock.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_rwlockattr_destroy.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_rwlockattr_init.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_rwlockattr_setpshared.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_self.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_setattr_default_np.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_setcancelstate.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_setcanceltype.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_setconcurrency.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_setname_np.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_setschedparam.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_setschedprio.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_setspecific.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_sigmask.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_spin_destroy.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_spin_init.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_spin_lock.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_spin_trylock.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_spin_unlock.c
#    ${MUSL_SOURCE_DIR}/src/thread/pthread_testcancel.c
#    ${MUSL_SOURCE_DIR}/src/thread/sem_destroy.c
#    ${MUSL_SOURCE_DIR}/src/thread/sem_getvalue.c
#    ${MUSL_SOURCE_DIR}/src/thread/sem_init.c
#    ${MUSL_SOURCE_DIR}/src/thread/sem_open.c
#    ${MUSL_SOURCE_DIR}/src/thread/sem_post.c
#    ${MUSL_SOURCE_DIR}/src/thread/sem_timedwait.c
#    ${MUSL_SOURCE_DIR}/src/thread/sem_trywait.c
#    ${MUSL_SOURCE_DIR}/src/thread/sem_unlink.c
#    ${MUSL_SOURCE_DIR}/src/thread/sem_wait.c
#    ${MUSL_SOURCE_DIR}/src/thread/synccall.c
#    ${MUSL_SOURCE_DIR}/src/thread/thrd_create.c
#    ${MUSL_SOURCE_DIR}/src/thread/thrd_exit.c
#    ${MUSL_SOURCE_DIR}/src/thread/thrd_join.c
#    ${MUSL_SOURCE_DIR}/src/thread/thrd_sleep.c
#    ${MUSL_SOURCE_DIR}/src/thread/thrd_yield.c
#    ${MUSL_SOURCE_DIR}/src/thread/tls.c
#    ${MUSL_SOURCE_DIR}/src/thread/tss_create.c
#    ${MUSL_SOURCE_DIR}/src/thread/tss_delete.c
#    ${MUSL_SOURCE_DIR}/src/thread/tss_set.c
#    ${MUSL_SOURCE_DIR}/src/thread/vmlock.c
#    ${MUSL_SOURCE_DIR}/src/time/__map_file.c
#    ${MUSL_SOURCE_DIR}/src/time/__month_to_secs.c
#    ${MUSL_SOURCE_DIR}/src/time/__secs_to_tm.c
#    ${MUSL_SOURCE_DIR}/src/time/__tm_to_secs.c
#    ${MUSL_SOURCE_DIR}/src/time/__tz.c
#    ${MUSL_SOURCE_DIR}/src/time/__year_to_secs.c
#    ${MUSL_SOURCE_DIR}/src/time/asctime.c
#    ${MUSL_SOURCE_DIR}/src/time/asctime_r.c
#    ${MUSL_SOURCE_DIR}/src/time/clock.c
#    ${MUSL_SOURCE_DIR}/src/time/clock_getcpuclockid.c
#    ${MUSL_SOURCE_DIR}/src/time/clock_getres.c
#    ${MUSL_SOURCE_DIR}/src/time/clock_gettime.c
#    ${MUSL_SOURCE_DIR}/src/time/clock_nanosleep.c
#    ${MUSL_SOURCE_DIR}/src/time/clock_settime.c
#    ${MUSL_SOURCE_DIR}/src/time/ctime.c
#    ${MUSL_SOURCE_DIR}/src/time/ctime_r.c
#    ${MUSL_SOURCE_DIR}/src/time/difftime.c
#    ${MUSL_SOURCE_DIR}/src/time/ftime.c
#    ${MUSL_SOURCE_DIR}/src/time/getdate.c
#    ${MUSL_SOURCE_DIR}/src/time/gettimeofday.c
#    ${MUSL_SOURCE_DIR}/src/time/gmtime.c
#    ${MUSL_SOURCE_DIR}/src/time/gmtime_r.c
#    ${MUSL_SOURCE_DIR}/src/time/localtime.c
#    ${MUSL_SOURCE_DIR}/src/time/localtime_r.c
#    ${MUSL_SOURCE_DIR}/src/time/mktime.c
#    ${MUSL_SOURCE_DIR}/src/time/nanosleep.c
#    ${MUSL_SOURCE_DIR}/src/time/strftime.c
#    ${MUSL_SOURCE_DIR}/src/time/strptime.c
#    ${MUSL_SOURCE_DIR}/src/time/time.c
#    ${MUSL_SOURCE_DIR}/src/time/timegm.c
#    ${MUSL_SOURCE_DIR}/src/time/timer_create.c
#    ${MUSL_SOURCE_DIR}/src/time/timer_delete.c
#    ${MUSL_SOURCE_DIR}/src/time/timer_getoverrun.c
#    ${MUSL_SOURCE_DIR}/src/time/timer_gettime.c
#    ${MUSL_SOURCE_DIR}/src/time/timer_settime.c
#    ${MUSL_SOURCE_DIR}/src/time/times.c
#    ${MUSL_SOURCE_DIR}/src/time/timespec_get.c
#    ${MUSL_SOURCE_DIR}/src/time/utime.c
#    ${MUSL_SOURCE_DIR}/src/time/wcsftime.c
#    ${MUSL_SOURCE_DIR}/src/unistd/_exit.c
#    ${MUSL_SOURCE_DIR}/src/unistd/access.c
#    ${MUSL_SOURCE_DIR}/src/unistd/acct.c
#    ${MUSL_SOURCE_DIR}/src/unistd/alarm.c
#    ${MUSL_SOURCE_DIR}/src/unistd/chdir.c
#    ${MUSL_SOURCE_DIR}/src/unistd/chown.c
#    ${MUSL_SOURCE_DIR}/src/unistd/close.c
#    ${MUSL_SOURCE_DIR}/src/unistd/ctermid.c
#    ${MUSL_SOURCE_DIR}/src/unistd/dup.c
#    ${MUSL_SOURCE_DIR}/src/unistd/dup2.c
#    ${MUSL_SOURCE_DIR}/src/unistd/dup3.c
#    ${MUSL_SOURCE_DIR}/src/unistd/faccessat.c
#    ${MUSL_SOURCE_DIR}/src/unistd/fchdir.c
#    ${MUSL_SOURCE_DIR}/src/unistd/fchown.c
#    ${MUSL_SOURCE_DIR}/src/unistd/fchownat.c
#    ${MUSL_SOURCE_DIR}/src/unistd/fdatasync.c
#    ${MUSL_SOURCE_DIR}/src/unistd/fsync.c
#    ${MUSL_SOURCE_DIR}/src/unistd/ftruncate.c
#    ${MUSL_SOURCE_DIR}/src/unistd/getcwd.c
#    ${MUSL_SOURCE_DIR}/src/unistd/getegid.c
#    ${MUSL_SOURCE_DIR}/src/unistd/geteuid.c
#    ${MUSL_SOURCE_DIR}/src/unistd/getgid.c
#    ${MUSL_SOURCE_DIR}/src/unistd/getgroups.c
#    ${MUSL_SOURCE_DIR}/src/unistd/gethostname.c
#    ${MUSL_SOURCE_DIR}/src/unistd/getlogin.c
#    ${MUSL_SOURCE_DIR}/src/unistd/getlogin_r.c
#    ${MUSL_SOURCE_DIR}/src/unistd/getpgid.c
#    ${MUSL_SOURCE_DIR}/src/unistd/getpgrp.c
#    ${MUSL_SOURCE_DIR}/src/unistd/getpid.c
#    ${MUSL_SOURCE_DIR}/src/unistd/getppid.c
#    ${MUSL_SOURCE_DIR}/src/unistd/getsid.c
#    ${MUSL_SOURCE_DIR}/src/unistd/getuid.c
#    ${MUSL_SOURCE_DIR}/src/unistd/isatty.c
#    ${MUSL_SOURCE_DIR}/src/unistd/lchown.c
#    ${MUSL_SOURCE_DIR}/src/unistd/link.c
#    ${MUSL_SOURCE_DIR}/src/unistd/linkat.c
#    ${MUSL_SOURCE_DIR}/src/unistd/lseek.c
#    ${MUSL_SOURCE_DIR}/src/unistd/nice.c
#    ${MUSL_SOURCE_DIR}/src/unistd/pause.c
#    ${MUSL_SOURCE_DIR}/src/unistd/pipe.c
#    ${MUSL_SOURCE_DIR}/src/unistd/pipe2.c
#    ${MUSL_SOURCE_DIR}/src/unistd/posix_close.c
#    ${MUSL_SOURCE_DIR}/src/unistd/pread.c
#    ${MUSL_SOURCE_DIR}/src/unistd/preadv.c
#    ${MUSL_SOURCE_DIR}/src/unistd/pwrite.c
#    ${MUSL_SOURCE_DIR}/src/unistd/pwritev.c
#    ${MUSL_SOURCE_DIR}/src/unistd/read.c
#    ${MUSL_SOURCE_DIR}/src/unistd/readlink.c
#    ${MUSL_SOURCE_DIR}/src/unistd/readlinkat.c
#    ${MUSL_SOURCE_DIR}/src/unistd/readv.c
#    ${MUSL_SOURCE_DIR}/src/unistd/renameat.c
#    ${MUSL_SOURCE_DIR}/src/unistd/rmdir.c
#    ${MUSL_SOURCE_DIR}/src/unistd/setegid.c
#    ${MUSL_SOURCE_DIR}/src/unistd/seteuid.c
#    ${MUSL_SOURCE_DIR}/src/unistd/setgid.c
#    ${MUSL_SOURCE_DIR}/src/unistd/setpgid.c
#    ${MUSL_SOURCE_DIR}/src/unistd/setpgrp.c
#    ${MUSL_SOURCE_DIR}/src/unistd/setregid.c
#    ${MUSL_SOURCE_DIR}/src/unistd/setresgid.c
#    ${MUSL_SOURCE_DIR}/src/unistd/setresuid.c
#    ${MUSL_SOURCE_DIR}/src/unistd/setreuid.c
#    ${MUSL_SOURCE_DIR}/src/unistd/setsid.c
#    ${MUSL_SOURCE_DIR}/src/unistd/setuid.c
#    ${MUSL_SOURCE_DIR}/src/unistd/setxid.c
#    ${MUSL_SOURCE_DIR}/src/unistd/sleep.c
#    ${MUSL_SOURCE_DIR}/src/unistd/symlink.c
#    ${MUSL_SOURCE_DIR}/src/unistd/symlinkat.c
#    ${MUSL_SOURCE_DIR}/src/unistd/sync.c
#    ${MUSL_SOURCE_DIR}/src/unistd/tcgetpgrp.c
#    ${MUSL_SOURCE_DIR}/src/unistd/tcsetpgrp.c
#    ${MUSL_SOURCE_DIR}/src/unistd/truncate.c
#    ${MUSL_SOURCE_DIR}/src/unistd/ttyname.c
#    ${MUSL_SOURCE_DIR}/src/unistd/ttyname_r.c
#    ${MUSL_SOURCE_DIR}/src/unistd/ualarm.c
#    ${MUSL_SOURCE_DIR}/src/unistd/unlink.c
#    ${MUSL_SOURCE_DIR}/src/unistd/unlinkat.c
#    ${MUSL_SOURCE_DIR}/src/unistd/usleep.c
#    ${MUSL_SOURCE_DIR}/src/unistd/write.c
#    ${MUSL_SOURCE_DIR}/src/unistd/writev.c
)

set(MUSL_ASM_SOURCES
)
set_source_files_properties(${MUSL_ASM_SOURCES} PROPERTIES LANGUAGE C)

file(MAKE_DIRECTORY ${MUSL_BINARY_DIR}/obj/include/bits/)

add_custom_command(OUTPUT ${MUSL_BINARY_DIR}/obj/include/bits/alltypes.h
    COMMAND sed -f ${MUSL_SOURCE_DIR}/tools/mkalltypes.sed ${MUSL_SOURCE_DIR}/arch/x86_64/bits/alltypes.h.in ${MUSL_SOURCE_DIR}/include/alltypes.h.in > ${MUSL_BINARY_DIR}/obj/include/bits/alltypes.h)

add_custom_command(OUTPUT ${MUSL_BINARY_DIR}/obj/include/bits/syscall.h
    COMMAND sed -n -e s/__NR_/SYS_/p < ${MUSL_SOURCE_DIR}/arch/x86_64/bits/syscall.h.in > ${MUSL_BINARY_DIR}/obj/include/bits/syscall_continuation.h && cat ${MUSL_SOURCE_DIR}/arch/x86_64/bits/syscall.h.in ${MUSL_BINARY_DIR}/obj/include/bits/syscall_continuation.h > ${MUSL_BINARY_DIR}/obj/include/bits/syscall.h)


set(MUSL_SOURCES
    ${MUSL_C_SOURCES}
    ${MUSL_ASM_SOURCES}
    ${MUSL_BINARY_DIR}/obj/include/bits/alltypes.h
    ${MUSL_BINARY_DIR}/obj/include/bits/syscall.h)

add_library(musl ${MUSL_SOURCES})

target_include_directories(musl SYSTEM PRIVATE
    $<BUILD_INTERFACE:${MUSL_SOURCE_DIR}/arch/x86_64>
    $<BUILD_INTERFACE:${MUSL_SOURCE_DIR}/arch/generic>
    $<BUILD_INTERFACE:${MUSL_SOURCE_DIR}/obj/src/internal>
    $<BUILD_INTERFACE:${MUSL_SOURCE_DIR}/src/include>
    $<BUILD_INTERFACE:${MUSL_SOURCE_DIR}/src/internal>
    $<BUILD_INTERFACE:${MUSL_BINARY_DIR}/obj/include>
    $<BUILD_INTERFACE:${MUSL_SOURCE_DIR}/obj/include>
    $<BUILD_INTERFACE:${MUSL_SOURCE_DIR}/include>
)

target_compile_options(musl PRIVATE
    -std=c99 -nostdinc -ffreestanding -fexcess-precision=standard -frounding-math -Wa,--noexecstack
    -D_XOPEN_SOURCE=700)

target_compile_options(musl PRIVATE
    -D__DEFINED_max_align_t
    -D_LIBCPP_HAS_MUSL_LIBC)

install(
    TARGETS musl
    EXPORT global
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
)
