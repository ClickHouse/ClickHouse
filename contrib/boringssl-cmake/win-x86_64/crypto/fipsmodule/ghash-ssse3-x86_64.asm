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







global	gcm_gmult_ssse3
ALIGN	16
gcm_gmult_ssse3:

$L$gmult_seh_begin:
	sub	rsp,40
$L$gmult_seh_allocstack:
	movdqa	XMMWORD[rsp],xmm6
$L$gmult_seh_save_xmm6:
	movdqa	XMMWORD[16+rsp],xmm10
$L$gmult_seh_save_xmm10:
$L$gmult_seh_prolog_end:
	movdqu	xmm0,XMMWORD[rcx]
	movdqa	xmm10,XMMWORD[$L$reverse_bytes]
	movdqa	xmm2,XMMWORD[$L$low4_mask]


DB	102,65,15,56,0,194


	movdqa	xmm1,xmm2
	pandn	xmm1,xmm0
	psrld	xmm1,4
	pand	xmm0,xmm2




	pxor	xmm2,xmm2
	pxor	xmm3,xmm3
	mov	rax,5
$L$oop_row_1:
	movdqa	xmm4,XMMWORD[rdx]
	lea	rdx,[16+rdx]


	movdqa	xmm6,xmm2
DB	102,15,58,15,243,1
	movdqa	xmm3,xmm6
	psrldq	xmm2,1




	movdqa	xmm5,xmm4
DB	102,15,56,0,224
DB	102,15,56,0,233


	pxor	xmm2,xmm5



	movdqa	xmm5,xmm4
	psllq	xmm5,60
	movdqa	xmm6,xmm5
	pslldq	xmm6,8
	pxor	xmm3,xmm6


	psrldq	xmm5,8
	pxor	xmm2,xmm5
	psrlq	xmm4,4
	pxor	xmm2,xmm4

	sub	rax,1
	jnz	NEAR $L$oop_row_1



	pxor	xmm2,xmm3
	psrlq	xmm3,1
	pxor	xmm2,xmm3
	psrlq	xmm3,1
	pxor	xmm2,xmm3
	psrlq	xmm3,5
	pxor	xmm2,xmm3
	pxor	xmm3,xmm3
	mov	rax,5
$L$oop_row_2:
	movdqa	xmm4,XMMWORD[rdx]
	lea	rdx,[16+rdx]


	movdqa	xmm6,xmm2
DB	102,15,58,15,243,1
	movdqa	xmm3,xmm6
	psrldq	xmm2,1




	movdqa	xmm5,xmm4
DB	102,15,56,0,224
DB	102,15,56,0,233


	pxor	xmm2,xmm5



	movdqa	xmm5,xmm4
	psllq	xmm5,60
	movdqa	xmm6,xmm5
	pslldq	xmm6,8
	pxor	xmm3,xmm6


	psrldq	xmm5,8
	pxor	xmm2,xmm5
	psrlq	xmm4,4
	pxor	xmm2,xmm4

	sub	rax,1
	jnz	NEAR $L$oop_row_2



	pxor	xmm2,xmm3
	psrlq	xmm3,1
	pxor	xmm2,xmm3
	psrlq	xmm3,1
	pxor	xmm2,xmm3
	psrlq	xmm3,5
	pxor	xmm2,xmm3
	pxor	xmm3,xmm3
	mov	rax,6
$L$oop_row_3:
	movdqa	xmm4,XMMWORD[rdx]
	lea	rdx,[16+rdx]


	movdqa	xmm6,xmm2
DB	102,15,58,15,243,1
	movdqa	xmm3,xmm6
	psrldq	xmm2,1




	movdqa	xmm5,xmm4
DB	102,15,56,0,224
DB	102,15,56,0,233


	pxor	xmm2,xmm5



	movdqa	xmm5,xmm4
	psllq	xmm5,60
	movdqa	xmm6,xmm5
	pslldq	xmm6,8
	pxor	xmm3,xmm6


	psrldq	xmm5,8
	pxor	xmm2,xmm5
	psrlq	xmm4,4
	pxor	xmm2,xmm4

	sub	rax,1
	jnz	NEAR $L$oop_row_3



	pxor	xmm2,xmm3
	psrlq	xmm3,1
	pxor	xmm2,xmm3
	psrlq	xmm3,1
	pxor	xmm2,xmm3
	psrlq	xmm3,5
	pxor	xmm2,xmm3
	pxor	xmm3,xmm3

DB	102,65,15,56,0,210
	movdqu	XMMWORD[rcx],xmm2


	pxor	xmm0,xmm0
	pxor	xmm1,xmm1
	pxor	xmm2,xmm2
	pxor	xmm3,xmm3
	pxor	xmm4,xmm4
	pxor	xmm5,xmm5
	pxor	xmm6,xmm6
	movdqa	xmm6,XMMWORD[rsp]
	movdqa	xmm10,XMMWORD[16+rsp]
	add	rsp,40
	DB	0F3h,0C3h		;repret
$L$gmult_seh_end:








global	gcm_ghash_ssse3
ALIGN	16
gcm_ghash_ssse3:
$L$ghash_seh_begin:

	sub	rsp,56
$L$ghash_seh_allocstack:
	movdqa	XMMWORD[rsp],xmm6
$L$ghash_seh_save_xmm6:
	movdqa	XMMWORD[16+rsp],xmm10
$L$ghash_seh_save_xmm10:
	movdqa	XMMWORD[32+rsp],xmm11
$L$ghash_seh_save_xmm11:
$L$ghash_seh_prolog_end:
	movdqu	xmm0,XMMWORD[rcx]
	movdqa	xmm10,XMMWORD[$L$reverse_bytes]
	movdqa	xmm11,XMMWORD[$L$low4_mask]


	and	r9,-16



DB	102,65,15,56,0,194


	pxor	xmm3,xmm3
$L$oop_ghash:

	movdqu	xmm1,XMMWORD[r8]
DB	102,65,15,56,0,202
	pxor	xmm0,xmm1


	movdqa	xmm1,xmm11
	pandn	xmm1,xmm0
	psrld	xmm1,4
	pand	xmm0,xmm11




	pxor	xmm2,xmm2

	mov	rax,5
$L$oop_row_4:
	movdqa	xmm4,XMMWORD[rdx]
	lea	rdx,[16+rdx]


	movdqa	xmm6,xmm2
DB	102,15,58,15,243,1
	movdqa	xmm3,xmm6
	psrldq	xmm2,1




	movdqa	xmm5,xmm4
DB	102,15,56,0,224
DB	102,15,56,0,233


	pxor	xmm2,xmm5



	movdqa	xmm5,xmm4
	psllq	xmm5,60
	movdqa	xmm6,xmm5
	pslldq	xmm6,8
	pxor	xmm3,xmm6


	psrldq	xmm5,8
	pxor	xmm2,xmm5
	psrlq	xmm4,4
	pxor	xmm2,xmm4

	sub	rax,1
	jnz	NEAR $L$oop_row_4



	pxor	xmm2,xmm3
	psrlq	xmm3,1
	pxor	xmm2,xmm3
	psrlq	xmm3,1
	pxor	xmm2,xmm3
	psrlq	xmm3,5
	pxor	xmm2,xmm3
	pxor	xmm3,xmm3
	mov	rax,5
$L$oop_row_5:
	movdqa	xmm4,XMMWORD[rdx]
	lea	rdx,[16+rdx]


	movdqa	xmm6,xmm2
DB	102,15,58,15,243,1
	movdqa	xmm3,xmm6
	psrldq	xmm2,1




	movdqa	xmm5,xmm4
DB	102,15,56,0,224
DB	102,15,56,0,233


	pxor	xmm2,xmm5



	movdqa	xmm5,xmm4
	psllq	xmm5,60
	movdqa	xmm6,xmm5
	pslldq	xmm6,8
	pxor	xmm3,xmm6


	psrldq	xmm5,8
	pxor	xmm2,xmm5
	psrlq	xmm4,4
	pxor	xmm2,xmm4

	sub	rax,1
	jnz	NEAR $L$oop_row_5



	pxor	xmm2,xmm3
	psrlq	xmm3,1
	pxor	xmm2,xmm3
	psrlq	xmm3,1
	pxor	xmm2,xmm3
	psrlq	xmm3,5
	pxor	xmm2,xmm3
	pxor	xmm3,xmm3
	mov	rax,6
$L$oop_row_6:
	movdqa	xmm4,XMMWORD[rdx]
	lea	rdx,[16+rdx]


	movdqa	xmm6,xmm2
DB	102,15,58,15,243,1
	movdqa	xmm3,xmm6
	psrldq	xmm2,1




	movdqa	xmm5,xmm4
DB	102,15,56,0,224
DB	102,15,56,0,233


	pxor	xmm2,xmm5



	movdqa	xmm5,xmm4
	psllq	xmm5,60
	movdqa	xmm6,xmm5
	pslldq	xmm6,8
	pxor	xmm3,xmm6


	psrldq	xmm5,8
	pxor	xmm2,xmm5
	psrlq	xmm4,4
	pxor	xmm2,xmm4

	sub	rax,1
	jnz	NEAR $L$oop_row_6



	pxor	xmm2,xmm3
	psrlq	xmm3,1
	pxor	xmm2,xmm3
	psrlq	xmm3,1
	pxor	xmm2,xmm3
	psrlq	xmm3,5
	pxor	xmm2,xmm3
	pxor	xmm3,xmm3
	movdqa	xmm0,xmm2


	lea	rdx,[((-256))+rdx]


	lea	r8,[16+r8]
	sub	r9,16
	jnz	NEAR $L$oop_ghash


DB	102,65,15,56,0,194
	movdqu	XMMWORD[rcx],xmm0


	pxor	xmm0,xmm0
	pxor	xmm1,xmm1
	pxor	xmm2,xmm2
	pxor	xmm3,xmm3
	pxor	xmm4,xmm4
	pxor	xmm5,xmm5
	pxor	xmm6,xmm6
	movdqa	xmm6,XMMWORD[rsp]
	movdqa	xmm10,XMMWORD[16+rsp]
	movdqa	xmm11,XMMWORD[32+rsp]
	add	rsp,56
	DB	0F3h,0C3h		;repret
$L$ghash_seh_end:



ALIGN	16


$L$reverse_bytes:
DB	15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0

$L$low4_mask:
	DQ	0x0f0f0f0f0f0f0f0f,0x0f0f0f0f0f0f0f0f
section	.pdata rdata align=4
ALIGN	4
	DD	$L$gmult_seh_begin wrt ..imagebase
	DD	$L$gmult_seh_end wrt ..imagebase
	DD	$L$gmult_seh_info wrt ..imagebase

	DD	$L$ghash_seh_begin wrt ..imagebase
	DD	$L$ghash_seh_end wrt ..imagebase
	DD	$L$ghash_seh_info wrt ..imagebase

section	.xdata rdata align=8
ALIGN	8
$L$gmult_seh_info:
DB	1
DB	$L$gmult_seh_prolog_end-$L$gmult_seh_begin
DB	5
DB	0

DB	$L$gmult_seh_save_xmm10-$L$gmult_seh_begin
DB	168
	DW	1

DB	$L$gmult_seh_save_xmm6-$L$gmult_seh_begin
DB	104
	DW	0

DB	$L$gmult_seh_allocstack-$L$gmult_seh_begin
DB	66

ALIGN	8
$L$ghash_seh_info:
DB	1
DB	$L$ghash_seh_prolog_end-$L$ghash_seh_begin
DB	7
DB	0

DB	$L$ghash_seh_save_xmm11-$L$ghash_seh_begin
DB	184
	DW	2

DB	$L$ghash_seh_save_xmm10-$L$ghash_seh_begin
DB	168
	DW	1

DB	$L$ghash_seh_save_xmm6-$L$ghash_seh_begin
DB	104
	DW	0

DB	$L$ghash_seh_allocstack-$L$ghash_seh_begin
DB	98
