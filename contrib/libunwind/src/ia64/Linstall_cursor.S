#define UNW_LOCAL_ONLY
#include "Ginstall_cursor.S"
#ifdef __linux__
	/* We do not need executable stack.  */
	.section	.note.GNU-stack,"",@progbits
#endif
