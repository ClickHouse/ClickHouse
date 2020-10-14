; This file is generated from a similarly-named Perl script in the BoringSSL
; source tree. Do not edit by hand.

default	rel
%define XMMWORD
%define YMMWORD
%define ZMMWORD

%ifdef BORINGSSL_PREFIX
%include "boringssl_prefix_symbols_nasm.inc"
%endif
section	.text code align=64










global	abi_test_trampoline
ALIGN	16
abi_test_trampoline:
$L$abi_test_trampoline_seh_begin:










	sub	rsp,344

$L$abi_test_trampoline_seh_prolog_alloc:
	mov	QWORD[112+rsp],rbx

$L$abi_test_trampoline_seh_prolog_rbx:
	mov	QWORD[120+rsp],rbp

$L$abi_test_trampoline_seh_prolog_rbp:
	mov	QWORD[128+rsp],rdi

$L$abi_test_trampoline_seh_prolog_rdi:
	mov	QWORD[136+rsp],rsi

$L$abi_test_trampoline_seh_prolog_rsi:
	mov	QWORD[144+rsp],r12

$L$abi_test_trampoline_seh_prolog_r12:
	mov	QWORD[152+rsp],r13

$L$abi_test_trampoline_seh_prolog_r13:
	mov	QWORD[160+rsp],r14

$L$abi_test_trampoline_seh_prolog_r14:
	mov	QWORD[168+rsp],r15

$L$abi_test_trampoline_seh_prolog_r15:
	movdqa	XMMWORD[176+rsp],xmm6

$L$abi_test_trampoline_seh_prolog_xmm6:
	movdqa	XMMWORD[192+rsp],xmm7

$L$abi_test_trampoline_seh_prolog_xmm7:
	movdqa	XMMWORD[208+rsp],xmm8

$L$abi_test_trampoline_seh_prolog_xmm8:
	movdqa	XMMWORD[224+rsp],xmm9

$L$abi_test_trampoline_seh_prolog_xmm9:
	movdqa	XMMWORD[240+rsp],xmm10

$L$abi_test_trampoline_seh_prolog_xmm10:
	movdqa	XMMWORD[256+rsp],xmm11

$L$abi_test_trampoline_seh_prolog_xmm11:
	movdqa	XMMWORD[272+rsp],xmm12

$L$abi_test_trampoline_seh_prolog_xmm12:
	movdqa	XMMWORD[288+rsp],xmm13

$L$abi_test_trampoline_seh_prolog_xmm13:
	movdqa	XMMWORD[304+rsp],xmm14

$L$abi_test_trampoline_seh_prolog_xmm14:
	movdqa	XMMWORD[320+rsp],xmm15

$L$abi_test_trampoline_seh_prolog_xmm15:
$L$abi_test_trampoline_seh_prolog_end:
	mov	rbx,QWORD[rdx]
	mov	rbp,QWORD[8+rdx]
	mov	rdi,QWORD[16+rdx]
	mov	rsi,QWORD[24+rdx]
	mov	r12,QWORD[32+rdx]
	mov	r13,QWORD[40+rdx]
	mov	r14,QWORD[48+rdx]
	mov	r15,QWORD[56+rdx]
	movdqa	xmm6,XMMWORD[64+rdx]
	movdqa	xmm7,XMMWORD[80+rdx]
	movdqa	xmm8,XMMWORD[96+rdx]
	movdqa	xmm9,XMMWORD[112+rdx]
	movdqa	xmm10,XMMWORD[128+rdx]
	movdqa	xmm11,XMMWORD[144+rdx]
	movdqa	xmm12,XMMWORD[160+rdx]
	movdqa	xmm13,XMMWORD[176+rdx]
	movdqa	xmm14,XMMWORD[192+rdx]
	movdqa	xmm15,XMMWORD[208+rdx]

	mov	QWORD[88+rsp],rcx
	mov	QWORD[96+rsp],rdx




	mov	r10,r8
	mov	r11,r9
	dec	r11
	js	NEAR $L$args_done
	mov	rcx,QWORD[r10]
	add	r10,8
	dec	r11
	js	NEAR $L$args_done
	mov	rdx,QWORD[r10]
	add	r10,8
	dec	r11
	js	NEAR $L$args_done
	mov	r8,QWORD[r10]
	add	r10,8
	dec	r11
	js	NEAR $L$args_done
	mov	r9,QWORD[r10]
	add	r10,8
	lea	rax,[32+rsp]
$L$args_loop:
	dec	r11
	js	NEAR $L$args_done






	mov	QWORD[104+rsp],r11
	mov	r11,QWORD[r10]
	mov	QWORD[rax],r11
	mov	r11,QWORD[104+rsp]

	add	r10,8
	add	rax,8
	jmp	NEAR $L$args_loop

$L$args_done:
	mov	rax,QWORD[88+rsp]
	mov	r10,QWORD[384+rsp]
	test	r10,r10
	jz	NEAR $L$no_unwind


	pushfq
	or	QWORD[rsp],0x100
	popfq



	nop
global	abi_test_unwind_start
abi_test_unwind_start:

	call	rax
global	abi_test_unwind_return
abi_test_unwind_return:




	pushfq
	and	QWORD[rsp],-0x101
	popfq
global	abi_test_unwind_stop
abi_test_unwind_stop:

	jmp	NEAR $L$call_done

$L$no_unwind:
	call	rax

$L$call_done:

	mov	rdx,QWORD[96+rsp]
	mov	QWORD[rdx],rbx
	mov	QWORD[8+rdx],rbp
	mov	QWORD[16+rdx],rdi
	mov	QWORD[24+rdx],rsi
	mov	QWORD[32+rdx],r12
	mov	QWORD[40+rdx],r13
	mov	QWORD[48+rdx],r14
	mov	QWORD[56+rdx],r15
	movdqa	XMMWORD[64+rdx],xmm6
	movdqa	XMMWORD[80+rdx],xmm7
	movdqa	XMMWORD[96+rdx],xmm8
	movdqa	XMMWORD[112+rdx],xmm9
	movdqa	XMMWORD[128+rdx],xmm10
	movdqa	XMMWORD[144+rdx],xmm11
	movdqa	XMMWORD[160+rdx],xmm12
	movdqa	XMMWORD[176+rdx],xmm13
	movdqa	XMMWORD[192+rdx],xmm14
	movdqa	XMMWORD[208+rdx],xmm15
	mov	rbx,QWORD[112+rsp]

	mov	rbp,QWORD[120+rsp]

	mov	rdi,QWORD[128+rsp]

	mov	rsi,QWORD[136+rsp]

	mov	r12,QWORD[144+rsp]

	mov	r13,QWORD[152+rsp]

	mov	r14,QWORD[160+rsp]

	mov	r15,QWORD[168+rsp]

	movdqa	xmm6,XMMWORD[176+rsp]

	movdqa	xmm7,XMMWORD[192+rsp]

	movdqa	xmm8,XMMWORD[208+rsp]

	movdqa	xmm9,XMMWORD[224+rsp]

	movdqa	xmm10,XMMWORD[240+rsp]

	movdqa	xmm11,XMMWORD[256+rsp]

	movdqa	xmm12,XMMWORD[272+rsp]

	movdqa	xmm13,XMMWORD[288+rsp]

	movdqa	xmm14,XMMWORD[304+rsp]

	movdqa	xmm15,XMMWORD[320+rsp]

	add	rsp,344



	DB	0F3h,0C3h		;repret

$L$abi_test_trampoline_seh_end:


global	abi_test_clobber_rax
ALIGN	16
abi_test_clobber_rax:
	xor	rax,rax
	DB	0F3h,0C3h		;repret


global	abi_test_clobber_rbx
ALIGN	16
abi_test_clobber_rbx:
	xor	rbx,rbx
	DB	0F3h,0C3h		;repret


global	abi_test_clobber_rcx
ALIGN	16
abi_test_clobber_rcx:
	xor	rcx,rcx
	DB	0F3h,0C3h		;repret


global	abi_test_clobber_rdx
ALIGN	16
abi_test_clobber_rdx:
	xor	rdx,rdx
	DB	0F3h,0C3h		;repret


global	abi_test_clobber_rdi
ALIGN	16
abi_test_clobber_rdi:
	xor	rdi,rdi
	DB	0F3h,0C3h		;repret


global	abi_test_clobber_rsi
ALIGN	16
abi_test_clobber_rsi:
	xor	rsi,rsi
	DB	0F3h,0C3h		;repret


global	abi_test_clobber_rbp
ALIGN	16
abi_test_clobber_rbp:
	xor	rbp,rbp
	DB	0F3h,0C3h		;repret


global	abi_test_clobber_r8
ALIGN	16
abi_test_clobber_r8:
	xor	r8,r8
	DB	0F3h,0C3h		;repret


global	abi_test_clobber_r9
ALIGN	16
abi_test_clobber_r9:
	xor	r9,r9
	DB	0F3h,0C3h		;repret


global	abi_test_clobber_r10
ALIGN	16
abi_test_clobber_r10:
	xor	r10,r10
	DB	0F3h,0C3h		;repret


global	abi_test_clobber_r11
ALIGN	16
abi_test_clobber_r11:
	xor	r11,r11
	DB	0F3h,0C3h		;repret


global	abi_test_clobber_r12
ALIGN	16
abi_test_clobber_r12:
	xor	r12,r12
	DB	0F3h,0C3h		;repret


global	abi_test_clobber_r13
ALIGN	16
abi_test_clobber_r13:
	xor	r13,r13
	DB	0F3h,0C3h		;repret


global	abi_test_clobber_r14
ALIGN	16
abi_test_clobber_r14:
	xor	r14,r14
	DB	0F3h,0C3h		;repret


global	abi_test_clobber_r15
ALIGN	16
abi_test_clobber_r15:
	xor	r15,r15
	DB	0F3h,0C3h		;repret


global	abi_test_clobber_xmm0
ALIGN	16
abi_test_clobber_xmm0:
	pxor	xmm0,xmm0
	DB	0F3h,0C3h		;repret


global	abi_test_clobber_xmm1
ALIGN	16
abi_test_clobber_xmm1:
	pxor	xmm1,xmm1
	DB	0F3h,0C3h		;repret


global	abi_test_clobber_xmm2
ALIGN	16
abi_test_clobber_xmm2:
	pxor	xmm2,xmm2
	DB	0F3h,0C3h		;repret


global	abi_test_clobber_xmm3
ALIGN	16
abi_test_clobber_xmm3:
	pxor	xmm3,xmm3
	DB	0F3h,0C3h		;repret


global	abi_test_clobber_xmm4
ALIGN	16
abi_test_clobber_xmm4:
	pxor	xmm4,xmm4
	DB	0F3h,0C3h		;repret


global	abi_test_clobber_xmm5
ALIGN	16
abi_test_clobber_xmm5:
	pxor	xmm5,xmm5
	DB	0F3h,0C3h		;repret


global	abi_test_clobber_xmm6
ALIGN	16
abi_test_clobber_xmm6:
	pxor	xmm6,xmm6
	DB	0F3h,0C3h		;repret


global	abi_test_clobber_xmm7
ALIGN	16
abi_test_clobber_xmm7:
	pxor	xmm7,xmm7
	DB	0F3h,0C3h		;repret


global	abi_test_clobber_xmm8
ALIGN	16
abi_test_clobber_xmm8:
	pxor	xmm8,xmm8
	DB	0F3h,0C3h		;repret


global	abi_test_clobber_xmm9
ALIGN	16
abi_test_clobber_xmm9:
	pxor	xmm9,xmm9
	DB	0F3h,0C3h		;repret


global	abi_test_clobber_xmm10
ALIGN	16
abi_test_clobber_xmm10:
	pxor	xmm10,xmm10
	DB	0F3h,0C3h		;repret


global	abi_test_clobber_xmm11
ALIGN	16
abi_test_clobber_xmm11:
	pxor	xmm11,xmm11
	DB	0F3h,0C3h		;repret


global	abi_test_clobber_xmm12
ALIGN	16
abi_test_clobber_xmm12:
	pxor	xmm12,xmm12
	DB	0F3h,0C3h		;repret


global	abi_test_clobber_xmm13
ALIGN	16
abi_test_clobber_xmm13:
	pxor	xmm13,xmm13
	DB	0F3h,0C3h		;repret


global	abi_test_clobber_xmm14
ALIGN	16
abi_test_clobber_xmm14:
	pxor	xmm14,xmm14
	DB	0F3h,0C3h		;repret


global	abi_test_clobber_xmm15
ALIGN	16
abi_test_clobber_xmm15:
	pxor	xmm15,xmm15
	DB	0F3h,0C3h		;repret





global	abi_test_bad_unwind_wrong_register
ALIGN	16
abi_test_bad_unwind_wrong_register:

$L$abi_test_bad_unwind_wrong_register_seh_begin:
	push	r12

$L$abi_test_bad_unwind_wrong_register_seh_push_r13:



	nop
	pop	r12

	DB	0F3h,0C3h		;repret
$L$abi_test_bad_unwind_wrong_register_seh_end:







global	abi_test_bad_unwind_temporary
ALIGN	16
abi_test_bad_unwind_temporary:

$L$abi_test_bad_unwind_temporary_seh_begin:
	push	r12

$L$abi_test_bad_unwind_temporary_seh_push_r12:

	mov	rax,r12
	inc	rax
	mov	QWORD[rsp],rax



	mov	QWORD[rsp],r12


	pop	r12

	DB	0F3h,0C3h		;repret
$L$abi_test_bad_unwind_temporary_seh_end:







global	abi_test_get_and_clear_direction_flag
abi_test_get_and_clear_direction_flag:
	pushfq
	pop	rax
	and	rax,0x400
	shr	rax,10
	cld
	DB	0F3h,0C3h		;repret





global	abi_test_set_direction_flag
abi_test_set_direction_flag:
	std
	DB	0F3h,0C3h		;repret






global	abi_test_bad_unwind_epilog
ALIGN	16
abi_test_bad_unwind_epilog:
$L$abi_test_bad_unwind_epilog_seh_begin:
	push	r12
$L$abi_test_bad_unwind_epilog_seh_push_r12:

	nop


	pop	r12
	nop
	DB	0F3h,0C3h		;repret
$L$abi_test_bad_unwind_epilog_seh_end:

section	.pdata rdata align=4
ALIGN	4

	DD	$L$abi_test_trampoline_seh_begin wrt ..imagebase
	DD	$L$abi_test_trampoline_seh_end wrt ..imagebase
	DD	$L$abi_test_trampoline_seh_info wrt ..imagebase

	DD	$L$abi_test_bad_unwind_wrong_register_seh_begin wrt ..imagebase
	DD	$L$abi_test_bad_unwind_wrong_register_seh_end wrt ..imagebase
	DD	$L$abi_test_bad_unwind_wrong_register_seh_info wrt ..imagebase

	DD	$L$abi_test_bad_unwind_temporary_seh_begin wrt ..imagebase
	DD	$L$abi_test_bad_unwind_temporary_seh_end wrt ..imagebase
	DD	$L$abi_test_bad_unwind_temporary_seh_info wrt ..imagebase

	DD	$L$abi_test_bad_unwind_epilog_seh_begin wrt ..imagebase
	DD	$L$abi_test_bad_unwind_epilog_seh_end wrt ..imagebase
	DD	$L$abi_test_bad_unwind_epilog_seh_info wrt ..imagebase

section	.xdata rdata align=8
ALIGN	8
$L$abi_test_trampoline_seh_info:

DB	1
DB	$L$abi_test_trampoline_seh_prolog_end-$L$abi_test_trampoline_seh_begin
DB	38
DB	0
DB	$L$abi_test_trampoline_seh_prolog_xmm15-$L$abi_test_trampoline_seh_begin
DB	248
	DW	20
DB	$L$abi_test_trampoline_seh_prolog_xmm14-$L$abi_test_trampoline_seh_begin
DB	232
	DW	19
DB	$L$abi_test_trampoline_seh_prolog_xmm13-$L$abi_test_trampoline_seh_begin
DB	216
	DW	18
DB	$L$abi_test_trampoline_seh_prolog_xmm12-$L$abi_test_trampoline_seh_begin
DB	200
	DW	17
DB	$L$abi_test_trampoline_seh_prolog_xmm11-$L$abi_test_trampoline_seh_begin
DB	184
	DW	16
DB	$L$abi_test_trampoline_seh_prolog_xmm10-$L$abi_test_trampoline_seh_begin
DB	168
	DW	15
DB	$L$abi_test_trampoline_seh_prolog_xmm9-$L$abi_test_trampoline_seh_begin
DB	152
	DW	14
DB	$L$abi_test_trampoline_seh_prolog_xmm8-$L$abi_test_trampoline_seh_begin
DB	136
	DW	13
DB	$L$abi_test_trampoline_seh_prolog_xmm7-$L$abi_test_trampoline_seh_begin
DB	120
	DW	12
DB	$L$abi_test_trampoline_seh_prolog_xmm6-$L$abi_test_trampoline_seh_begin
DB	104
	DW	11
DB	$L$abi_test_trampoline_seh_prolog_r15-$L$abi_test_trampoline_seh_begin
DB	244
	DW	21
DB	$L$abi_test_trampoline_seh_prolog_r14-$L$abi_test_trampoline_seh_begin
DB	228
	DW	20
DB	$L$abi_test_trampoline_seh_prolog_r13-$L$abi_test_trampoline_seh_begin
DB	212
	DW	19
DB	$L$abi_test_trampoline_seh_prolog_r12-$L$abi_test_trampoline_seh_begin
DB	196
	DW	18
DB	$L$abi_test_trampoline_seh_prolog_rsi-$L$abi_test_trampoline_seh_begin
DB	100
	DW	17
DB	$L$abi_test_trampoline_seh_prolog_rdi-$L$abi_test_trampoline_seh_begin
DB	116
	DW	16
DB	$L$abi_test_trampoline_seh_prolog_rbp-$L$abi_test_trampoline_seh_begin
DB	84
	DW	15
DB	$L$abi_test_trampoline_seh_prolog_rbx-$L$abi_test_trampoline_seh_begin
DB	52
	DW	14
DB	$L$abi_test_trampoline_seh_prolog_alloc-$L$abi_test_trampoline_seh_begin
DB	1
	DW	43


ALIGN	8
$L$abi_test_bad_unwind_wrong_register_seh_info:
DB	1
DB	$L$abi_test_bad_unwind_wrong_register_seh_push_r13-$L$abi_test_bad_unwind_wrong_register_seh_begin
DB	1
DB	0

DB	$L$abi_test_bad_unwind_wrong_register_seh_push_r13-$L$abi_test_bad_unwind_wrong_register_seh_begin
DB	208

ALIGN	8
$L$abi_test_bad_unwind_temporary_seh_info:
DB	1
DB	$L$abi_test_bad_unwind_temporary_seh_push_r12-$L$abi_test_bad_unwind_temporary_seh_begin
DB	1
DB	0

DB	$L$abi_test_bad_unwind_temporary_seh_push_r12-$L$abi_test_bad_unwind_temporary_seh_begin
DB	192

ALIGN	8
$L$abi_test_bad_unwind_epilog_seh_info:
DB	1
DB	$L$abi_test_bad_unwind_epilog_seh_push_r12-$L$abi_test_bad_unwind_epilog_seh_begin
DB	1
DB	0

DB	$L$abi_test_bad_unwind_epilog_seh_push_r12-$L$abi_test_bad_unwind_epilog_seh_begin
DB	192
