.text	
.section	.rodata
.align	16
SM4_FK:
.long	0xa3b1bac6, 0x56aa3350, 0x677d9197, 0xb27022dc

.align	16
SM4_CK:
.long	0x00070E15, 0x1C232A31, 0x383F464D, 0x545B6269
.long	0x70777E85, 0x8C939AA1, 0xA8AFB6BD, 0xC4CBD2D9
.long	0xE0E7EEF5, 0xFC030A11, 0x181F262D, 0x343B4249
.long	0x50575E65, 0x6C737A81, 0x888F969D, 0xA4ABB2B9
.long	0xC0C7CED5, 0xDCE3EAF1, 0xF8FF060D, 0x141B2229
.long	0x30373E45, 0x4C535A61, 0x686F767D, 0x848B9299
.long	0xA0A7AEB5, 0xBCC3CAD1, 0xD8DFE6ED, 0xF4FB0209
.long	0x10171E25, 0x2C333A41, 0x484F565D, 0x646B7279

IN_SHUFB:
.byte	0x03, 0x02, 0x01, 0x00, 0x07, 0x06, 0x05, 0x04
.byte	0x0b, 0x0a, 0x09, 0x08, 0x0f, 0x0e, 0x0d, 0x0c
.byte	0x03, 0x02, 0x01, 0x00, 0x07, 0x06, 0x05, 0x04
.byte	0x0b, 0x0a, 0x09, 0x08, 0x0f, 0x0e, 0x0d, 0x0c

OUT_SHUFB:
.byte	0x0f, 0x0e, 0x0d, 0x0c, 0x0b, 0x0a, 0x09, 0x08
.byte	0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x00
.byte	0x0f, 0x0e, 0x0d, 0x0c, 0x0b, 0x0a, 0x09, 0x08
.byte	0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x00

.text	







.globl	hw_x86_64_sm4_set_key
.type	hw_x86_64_sm4_set_key,@function
.align	32
hw_x86_64_sm4_set_key:
.cfi_startproc	
.byte	243,15,30,250

	pushq	%rbp
.cfi_adjust_cfa_offset	8
.cfi_offset	%rbp,-16

.Lossl_hw_x86_64_sm4_set_key_seh_prolog_end:

	vmovdqu	(%rdi),%xmm0
	vpshufb	IN_SHUFB(%rip),%xmm0,%xmm0
	vpxor	SM4_FK(%rip),%xmm0,%xmm0

	vmovdqu	SM4_CK(%rip),%xmm1
	vsm4key4	%xmm1,%xmm0,%xmm0
	vmovdqu	%xmm0,(%rsi)
	vmovdqu	SM4_CK + 16(%rip),%xmm1
	vsm4key4	%xmm1,%xmm0,%xmm0
	vmovdqu	%xmm0,16(%rsi)
	vmovdqu	SM4_CK + 32(%rip),%xmm1
	vsm4key4	%xmm1,%xmm0,%xmm0
	vmovdqu	%xmm0,32(%rsi)
	vmovdqu	SM4_CK + 48(%rip),%xmm1
	vsm4key4	%xmm1,%xmm0,%xmm0
	vmovdqu	%xmm0,48(%rsi)
	vmovdqu	SM4_CK + 64(%rip),%xmm1
	vsm4key4	%xmm1,%xmm0,%xmm0
	vmovdqu	%xmm0,64(%rsi)
	vmovdqu	SM4_CK + 80(%rip),%xmm1
	vsm4key4	%xmm1,%xmm0,%xmm0
	vmovdqu	%xmm0,80(%rsi)
	vmovdqu	SM4_CK + 96(%rip),%xmm1
	vsm4key4	%xmm1,%xmm0,%xmm0
	vmovdqu	%xmm0,96(%rsi)
	vmovdqu	SM4_CK + 112(%rip),%xmm1
	vsm4key4	%xmm1,%xmm0,%xmm0
	vmovdqu	%xmm0,112(%rsi)

	vpxor	%xmm0,%xmm0,%xmm0
	movl	$1,%eax
	popq	%rbp
.cfi_adjust_cfa_offset	-8
.cfi_restore	%rbp
	.byte	0xf3,0xc3
.cfi_endproc	



.globl	hw_x86_64_sm4_encrypt
.type	hw_x86_64_sm4_encrypt,@function
.align	32
hw_x86_64_sm4_encrypt:
.cfi_startproc	
.byte	243,15,30,250

	pushq	%rbp
.cfi_adjust_cfa_offset	8
.cfi_offset	%rbp,-16

.Lossl_hw_x86_64_sm4_encrypt_seh_prolog_end:

	vmovdqu	(%rdi),%xmm0
	vpshufb	IN_SHUFB(%rip),%xmm0,%xmm0


	movq	%rdx,%r10

	vsm4rnds4	(%r10),%xmm0,%xmm0
	vsm4rnds4	16(%r10),%xmm0,%xmm0
	vsm4rnds4	32(%r10),%xmm0,%xmm0
	vsm4rnds4	48(%r10),%xmm0,%xmm0
	vsm4rnds4	64(%r10),%xmm0,%xmm0
	vsm4rnds4	80(%r10),%xmm0,%xmm0
	vsm4rnds4	96(%r10),%xmm0,%xmm0
	vsm4rnds4	112(%r10),%xmm0,%xmm0

	vpshufb	OUT_SHUFB(%rip),%xmm0,%xmm0
	vmovdqu	%xmm0,(%rsi)
	vpxor	%xmm0,%xmm0,%xmm0
	popq	%rbp
.cfi_adjust_cfa_offset	-8
.cfi_restore	%rbp
	.byte	0xf3,0xc3
.cfi_endproc	



.globl	hw_x86_64_sm4_decrypt
.type	hw_x86_64_sm4_decrypt,@function
.align	32
hw_x86_64_sm4_decrypt:
.cfi_startproc	
.byte	243,15,30,250

	pushq	%rbp
.cfi_adjust_cfa_offset	8
.cfi_offset	%rbp,-16

.Lossl_hw_x86_64_sm4_decrypt_seh_prolog_end:

	vmovdqu	(%rdi),%xmm0
	vpshufb	IN_SHUFB(%rip),%xmm0,%xmm0

	vmovdqu	112(%rdx),%xmm1
	vpshufd	$27,%xmm1,%xmm1
	vsm4rnds4	%xmm1,%xmm0,%xmm0
	vmovdqu	96(%rdx),%xmm1
	vpshufd	$27,%xmm1,%xmm1
	vsm4rnds4	%xmm1,%xmm0,%xmm0
	vmovdqu	80(%rdx),%xmm1
	vpshufd	$27,%xmm1,%xmm1
	vsm4rnds4	%xmm1,%xmm0,%xmm0
	vmovdqu	64(%rdx),%xmm1
	vpshufd	$27,%xmm1,%xmm1
	vsm4rnds4	%xmm1,%xmm0,%xmm0
	vmovdqu	48(%rdx),%xmm1
	vpshufd	$27,%xmm1,%xmm1
	vsm4rnds4	%xmm1,%xmm0,%xmm0
	vmovdqu	32(%rdx),%xmm1
	vpshufd	$27,%xmm1,%xmm1
	vsm4rnds4	%xmm1,%xmm0,%xmm0
	vmovdqu	16(%rdx),%xmm1
	vpshufd	$27,%xmm1,%xmm1
	vsm4rnds4	%xmm1,%xmm0,%xmm0
	vmovdqu	(%rdx),%xmm1
	vpshufd	$27,%xmm1,%xmm1
	vsm4rnds4	%xmm1,%xmm0,%xmm0

	vpshufb	OUT_SHUFB(%rip),%xmm0,%xmm0
	vmovdqu	%xmm0,(%rsi)
	vpxor	%xmm0,%xmm0,%xmm0
	vpxor	%xmm1,%xmm1,%xmm1
	popq	%rbp
.cfi_adjust_cfa_offset	-8
.cfi_restore	%rbp
	.byte	0xf3,0xc3
.cfi_endproc	
