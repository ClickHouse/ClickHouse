// __syscall_cp_asm(&self->cancel, nr, u, v, w, x, y, z)
//                  x0             x1  x2 x3 x4 x5 x6 x7

// syscall(nr, u, v, w, x, y, z)
//         x8  x0 x1 x2 x3 x4 x5

.global __cp_begin
.hidden __cp_begin
.global __cp_end
.hidden __cp_end
.global __cp_cancel
.hidden __cp_cancel
.hidden __cancel
.global __syscall_cp
.hidden __syscall_cp
.type __syscall_cp,%function
__syscall_cp_asm:
__cp_begin:
	ldr w0,[x0]
	cbnz w0,__cp_cancel
	mov x8,x1
	mov x0,x2
	mov x1,x3
	mov x2,x4
	mov x3,x5
	mov x4,x6
	mov x5,x7
	svc 0
__cp_end:
	ret
__cp_cancel:
	b __cancel
