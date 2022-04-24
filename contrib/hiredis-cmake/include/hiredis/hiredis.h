#if __has_include(<hiredis.h>) // maybe bundled
#    include_next <hiredis.h>
#else // system
#    include_next <hiredis/hiredis.h>
#endif

