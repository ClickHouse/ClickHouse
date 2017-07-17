#ifndef UNW_REMOTE_ONLY

/*
 * Create a special unwind-table entry which makes it easy for an
 * unwinder to locate the dynamic registration list.  The special
 * entry covers address range [0-0) and is therefore guaranteed to be
 * the first in the unwind-table.
 */
	.global _U_dyn_info_list
	.hidden _U_dyn_info_list

	.section .IA_64.unwind_info,"a","progbits"
.info:	data8 (1<<48) | 1	/* v1, length==1 (8-byte word) */
	data8 0		/* 8 empty .prologue directives (nops) */
	data8 0		/* personality routine (ignored) */
	string "dyn-list"	/* lsda */
	data8 @gprel(_U_dyn_info_list)

	.section .IA_64.unwind, "a", "progbits"
	data8 0, 0, @segrel(.info)

#endif
#ifdef __linux__
	/* We do not need executable stack.  */
	.section	.note.GNU-stack,"",@progbits
#endif
