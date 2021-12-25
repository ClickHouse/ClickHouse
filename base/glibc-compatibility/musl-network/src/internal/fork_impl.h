#include <features.h>

extern hidden volatile int *const __at_quick_exit_lockptr;
extern hidden volatile int *const __atexit_lockptr;
extern hidden volatile int *const __dlerror_lockptr;
extern hidden volatile int *const __gettext_lockptr;
extern hidden volatile int *const __locale_lockptr;
extern hidden volatile int *const __random_lockptr;
extern hidden volatile int *const __sem_open_lockptr;
extern hidden volatile int *const __stdio_ofl_lockptr;
extern hidden volatile int *const __syslog_lockptr;
extern hidden volatile int *const __timezone_lockptr;

extern hidden volatile int *const __bump_lockptr;

extern hidden volatile int *const __vmlock_lockptr;

hidden void __malloc_atfork(int);
hidden void __ldso_atfork(int);
