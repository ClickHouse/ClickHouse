.global __syscall
.hidden __syscall
.type __syscall,%function
__syscall:
	uxtw x8,w0
	mov x0,x1
	mov x1,x2
	mov x2,x3
	mov x3,x4
	mov x4,x5
	mov x5,x6
	mov x6,x7
	svc 0
	ret
