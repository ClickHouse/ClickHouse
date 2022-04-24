#if __has_include(<async.h>) // maybe bundled
#    include_next <async.h>
#else // system
#    include_next <hiredis/async.h>
#endif

