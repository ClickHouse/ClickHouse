#ifndef GCC_CONFIG_H
#define GCC_CONFIG_H
#ifdef GENERATOR_FILE
#error config.h is for the host, not build, machine.
#endif
#include "auto-host.h"
#ifdef IN_GCC
# include "ansidecl.h"
#endif
#endif /* GCC_CONFIG_H */
