	.global _UI_setcontext

_UI_setcontext:
        blr

#ifdef __linux__
        /* We do not need executable stack.  */
        .section        .note.GNU-stack,"",@progbits
#endif
