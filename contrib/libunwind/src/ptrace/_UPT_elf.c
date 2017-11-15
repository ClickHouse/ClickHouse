/* We need to get a separate copy of the ELF-code into
   libunwind-ptrace since it cannot (and must not) have any ELF
   dependencies on libunwind.  */
#include "libunwind_i.h"        /* get ELFCLASS defined */
#include "../elfxx.c"
