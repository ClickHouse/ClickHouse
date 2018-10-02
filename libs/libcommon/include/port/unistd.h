#pragma once

// https://stackoverflow.com/questions/341817/is-there-a-replacement-for-unistd-h-for-windows-visual-c

#ifdef _MSC_VER
    #include <io.h>
#else
    #include <unistd.h>
#endif

#include "ssize_t.h"
