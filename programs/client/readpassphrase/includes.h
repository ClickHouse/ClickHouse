#pragma once

/* #undef HAVE_READPASSPHRASE */

#if !defined(HAVE_READPASSPHRASE)
#    ifndef _PATH_TTY
#        define _PATH_TTY "/dev/tty"
#    endif
#endif
