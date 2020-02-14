#pragma once

#ifdef _MSC_VER
    #include <basetsd.h>
    typedef SSIZE_T ssize_t;
#else
    #include <sys/types.h>
#endif
