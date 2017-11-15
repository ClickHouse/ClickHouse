	/* Dummy implementation for now.  */

	.global _UI_siglongjmp_cont
	.global _UI_longjmp_cont

_UI_siglongjmp_cont:
_UI_longjmp_cont:
	ret
#ifdef __linux__
 /* We do not need executable stack.  */
 .section  .note.GNU-stack,"",%progbits
#endif
