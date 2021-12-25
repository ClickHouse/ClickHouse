1:	int $128
	ret

.data
.align 4
.hidden __sysinfo
.global __sysinfo
__sysinfo:
	.long 1b
