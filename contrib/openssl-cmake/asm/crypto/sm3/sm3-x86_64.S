.data	
.align	16
SHUFF_MASK:
.byte	3, 2, 1, 0, 7, 6, 5, 4, 11, 10, 9, 8, 15, 14, 13, 12

.text	








.globl	ossl_hwsm3_block_data_order
.type	ossl_hwsm3_block_data_order,@function
.align	32
ossl_hwsm3_block_data_order:
.cfi_startproc	
.byte	243,15,30,250

	pushq	%rbp
.cfi_adjust_cfa_offset	8
.cfi_offset	%rbp,-16
.cfi_def_cfa_register	%rbp
.Lossl_hwsm3_block_data_order_seh_setfp:

.Lossl_hwsm3_block_data_order_seh_prolog_end:
	orq	%rdx,%rdx
	je	.done_hash




	vmovdqu	(%rdi),%xmm6
	vmovdqu	16(%rdi),%xmm7

	vpshufd	$0x1B,%xmm6,%xmm0
	vpshufd	$0x1B,%xmm7,%xmm1
	vpunpckhqdq	%xmm0,%xmm1,%xmm6
	vpunpcklqdq	%xmm0,%xmm1,%xmm7
	vpsrld	$9,%xmm7,%xmm2
	vpslld	$23,%xmm7,%xmm3
	vpxor	%xmm3,%xmm2,%xmm1
	vpsrld	$19,%xmm7,%xmm4
	vpslld	$13,%xmm7,%xmm5
	vpxor	%xmm5,%xmm4,%xmm0

	vpblendd	$0x3,%xmm0,%xmm1,%xmm7

	vmovdqa	SHUFF_MASK(%rip),%xmm12

.align	32
.block_loop:
	vmovdqa	%xmm6,%xmm10
	vmovdqa	%xmm7,%xmm11


	vmovdqu	(%rsi),%xmm2
	vmovdqu	16(%rsi),%xmm3
	vmovdqu	32(%rsi),%xmm4
	vmovdqu	48(%rsi),%xmm5
	vpshufb	%xmm12,%xmm2,%xmm2
	vpshufb	%xmm12,%xmm3,%xmm3
	vpshufb	%xmm12,%xmm4,%xmm4
	vpshufb	%xmm12,%xmm5,%xmm5

	vpalignr	$12,%xmm3,%xmm4,%xmm8
	vpsrldq	$4,%xmm5,%xmm9
	vsm3msg1	%xmm2,%xmm9,%xmm8
	vpalignr	$12,%xmm2,%xmm3,%xmm9
	vpalignr	$8,%xmm4,%xmm5,%xmm1
	vsm3msg2	%xmm1,%xmm9,%xmm8
	vpunpcklqdq	%xmm3,%xmm2,%xmm1
	vsm3rnds2	$0,%xmm1,%xmm6,%xmm7
	vpunpckhqdq	%xmm3,%xmm2,%xmm1
	vsm3rnds2	$2,%xmm1,%xmm7,%xmm6
	vmovdqa	%xmm8,%xmm2
	vpalignr	$12,%xmm4,%xmm5,%xmm8
	vpsrldq	$4,%xmm2,%xmm9
	vsm3msg1	%xmm3,%xmm9,%xmm8
	vpalignr	$12,%xmm3,%xmm4,%xmm9
	vpalignr	$8,%xmm5,%xmm2,%xmm1
	vsm3msg2	%xmm1,%xmm9,%xmm8
	vpunpcklqdq	%xmm4,%xmm3,%xmm1
	vsm3rnds2	$4,%xmm1,%xmm6,%xmm7
	vpunpckhqdq	%xmm4,%xmm3,%xmm1
	vsm3rnds2	$6,%xmm1,%xmm7,%xmm6
	vmovdqa	%xmm8,%xmm3
	vpalignr	$12,%xmm5,%xmm2,%xmm8
	vpsrldq	$4,%xmm3,%xmm9
	vsm3msg1	%xmm4,%xmm9,%xmm8
	vpalignr	$12,%xmm4,%xmm5,%xmm9
	vpalignr	$8,%xmm2,%xmm3,%xmm1
	vsm3msg2	%xmm1,%xmm9,%xmm8
	vpunpcklqdq	%xmm5,%xmm4,%xmm1
	vsm3rnds2	$8,%xmm1,%xmm6,%xmm7
	vpunpckhqdq	%xmm5,%xmm4,%xmm1
	vsm3rnds2	$10,%xmm1,%xmm7,%xmm6
	vmovdqa	%xmm8,%xmm4
	vpalignr	$12,%xmm2,%xmm3,%xmm8
	vpsrldq	$4,%xmm4,%xmm9
	vsm3msg1	%xmm5,%xmm9,%xmm8
	vpalignr	$12,%xmm5,%xmm2,%xmm9
	vpalignr	$8,%xmm3,%xmm4,%xmm1
	vsm3msg2	%xmm1,%xmm9,%xmm8
	vpunpcklqdq	%xmm2,%xmm5,%xmm1
	vsm3rnds2	$12,%xmm1,%xmm6,%xmm7
	vpunpckhqdq	%xmm2,%xmm5,%xmm1
	vsm3rnds2	$14,%xmm1,%xmm7,%xmm6
	vmovdqa	%xmm8,%xmm5
	vpalignr	$12,%xmm3,%xmm4,%xmm8
	vpsrldq	$4,%xmm5,%xmm9
	vsm3msg1	%xmm2,%xmm9,%xmm8
	vpalignr	$12,%xmm2,%xmm3,%xmm9
	vpalignr	$8,%xmm4,%xmm5,%xmm1
	vsm3msg2	%xmm1,%xmm9,%xmm8
	vpunpcklqdq	%xmm3,%xmm2,%xmm1
	vsm3rnds2	$16,%xmm1,%xmm6,%xmm7
	vpunpckhqdq	%xmm3,%xmm2,%xmm1
	vsm3rnds2	$18,%xmm1,%xmm7,%xmm6
	vmovdqa	%xmm8,%xmm2
	vpalignr	$12,%xmm4,%xmm5,%xmm8
	vpsrldq	$4,%xmm2,%xmm9
	vsm3msg1	%xmm3,%xmm9,%xmm8
	vpalignr	$12,%xmm3,%xmm4,%xmm9
	vpalignr	$8,%xmm5,%xmm2,%xmm1
	vsm3msg2	%xmm1,%xmm9,%xmm8
	vpunpcklqdq	%xmm4,%xmm3,%xmm1
	vsm3rnds2	$20,%xmm1,%xmm6,%xmm7
	vpunpckhqdq	%xmm4,%xmm3,%xmm1
	vsm3rnds2	$22,%xmm1,%xmm7,%xmm6
	vmovdqa	%xmm8,%xmm3
	vpalignr	$12,%xmm5,%xmm2,%xmm8
	vpsrldq	$4,%xmm3,%xmm9
	vsm3msg1	%xmm4,%xmm9,%xmm8
	vpalignr	$12,%xmm4,%xmm5,%xmm9
	vpalignr	$8,%xmm2,%xmm3,%xmm1
	vsm3msg2	%xmm1,%xmm9,%xmm8
	vpunpcklqdq	%xmm5,%xmm4,%xmm1
	vsm3rnds2	$24,%xmm1,%xmm6,%xmm7
	vpunpckhqdq	%xmm5,%xmm4,%xmm1
	vsm3rnds2	$26,%xmm1,%xmm7,%xmm6
	vmovdqa	%xmm8,%xmm4
	vpalignr	$12,%xmm2,%xmm3,%xmm8
	vpsrldq	$4,%xmm4,%xmm9
	vsm3msg1	%xmm5,%xmm9,%xmm8
	vpalignr	$12,%xmm5,%xmm2,%xmm9
	vpalignr	$8,%xmm3,%xmm4,%xmm1
	vsm3msg2	%xmm1,%xmm9,%xmm8
	vpunpcklqdq	%xmm2,%xmm5,%xmm1
	vsm3rnds2	$28,%xmm1,%xmm6,%xmm7
	vpunpckhqdq	%xmm2,%xmm5,%xmm1
	vsm3rnds2	$30,%xmm1,%xmm7,%xmm6
	vmovdqa	%xmm8,%xmm5
	vpalignr	$12,%xmm3,%xmm4,%xmm8
	vpsrldq	$4,%xmm5,%xmm9
	vsm3msg1	%xmm2,%xmm9,%xmm8
	vpalignr	$12,%xmm2,%xmm3,%xmm9
	vpalignr	$8,%xmm4,%xmm5,%xmm1
	vsm3msg2	%xmm1,%xmm9,%xmm8
	vpunpcklqdq	%xmm3,%xmm2,%xmm1
	vsm3rnds2	$32,%xmm1,%xmm6,%xmm7
	vpunpckhqdq	%xmm3,%xmm2,%xmm1
	vsm3rnds2	$34,%xmm1,%xmm7,%xmm6
	vmovdqa	%xmm8,%xmm2
	vpalignr	$12,%xmm4,%xmm5,%xmm8
	vpsrldq	$4,%xmm2,%xmm9
	vsm3msg1	%xmm3,%xmm9,%xmm8
	vpalignr	$12,%xmm3,%xmm4,%xmm9
	vpalignr	$8,%xmm5,%xmm2,%xmm1
	vsm3msg2	%xmm1,%xmm9,%xmm8
	vpunpcklqdq	%xmm4,%xmm3,%xmm1
	vsm3rnds2	$36,%xmm1,%xmm6,%xmm7
	vpunpckhqdq	%xmm4,%xmm3,%xmm1
	vsm3rnds2	$38,%xmm1,%xmm7,%xmm6
	vmovdqa	%xmm8,%xmm3
	vpalignr	$12,%xmm5,%xmm2,%xmm8
	vpsrldq	$4,%xmm3,%xmm9
	vsm3msg1	%xmm4,%xmm9,%xmm8
	vpalignr	$12,%xmm4,%xmm5,%xmm9
	vpalignr	$8,%xmm2,%xmm3,%xmm1
	vsm3msg2	%xmm1,%xmm9,%xmm8
	vpunpcklqdq	%xmm5,%xmm4,%xmm1
	vsm3rnds2	$40,%xmm1,%xmm6,%xmm7
	vpunpckhqdq	%xmm5,%xmm4,%xmm1
	vsm3rnds2	$42,%xmm1,%xmm7,%xmm6
	vmovdqa	%xmm8,%xmm4
	vpalignr	$12,%xmm2,%xmm3,%xmm8
	vpsrldq	$4,%xmm4,%xmm9
	vsm3msg1	%xmm5,%xmm9,%xmm8
	vpalignr	$12,%xmm5,%xmm2,%xmm9
	vpalignr	$8,%xmm3,%xmm4,%xmm1
	vsm3msg2	%xmm1,%xmm9,%xmm8
	vpunpcklqdq	%xmm2,%xmm5,%xmm1
	vsm3rnds2	$44,%xmm1,%xmm6,%xmm7
	vpunpckhqdq	%xmm2,%xmm5,%xmm1
	vsm3rnds2	$46,%xmm1,%xmm7,%xmm6
	vmovdqa	%xmm8,%xmm5
	vpalignr	$12,%xmm3,%xmm4,%xmm8
	vpsrldq	$4,%xmm5,%xmm9
	vsm3msg1	%xmm2,%xmm9,%xmm8
	vpalignr	$12,%xmm2,%xmm3,%xmm9
	vpalignr	$8,%xmm4,%xmm5,%xmm1
	vsm3msg2	%xmm1,%xmm9,%xmm8
	vpunpcklqdq	%xmm3,%xmm2,%xmm1
	vsm3rnds2	$48,%xmm1,%xmm6,%xmm7
	vpunpckhqdq	%xmm3,%xmm2,%xmm1
	vsm3rnds2	$50,%xmm1,%xmm7,%xmm6
	vmovdqa	%xmm8,%xmm2
	vpunpcklqdq	%xmm4,%xmm3,%xmm1
	vsm3rnds2	$52,%xmm1,%xmm6,%xmm7
	vpunpckhqdq	%xmm4,%xmm3,%xmm1
	vsm3rnds2	$54,%xmm1,%xmm7,%xmm6
	vpunpcklqdq	%xmm5,%xmm4,%xmm1
	vsm3rnds2	$56,%xmm1,%xmm6,%xmm7
	vpunpckhqdq	%xmm5,%xmm4,%xmm1
	vsm3rnds2	$58,%xmm1,%xmm7,%xmm6
	vpunpcklqdq	%xmm2,%xmm5,%xmm1
	vsm3rnds2	$60,%xmm1,%xmm6,%xmm7
	vpunpckhqdq	%xmm2,%xmm5,%xmm1
	vsm3rnds2	$62,%xmm1,%xmm7,%xmm6

	vpxor	%xmm10,%xmm6,%xmm6
	vpxor	%xmm11,%xmm7,%xmm7
	addq	$64,%rsi
	decq	%rdx
	jnz	.block_loop


	vpslld	$9,%xmm7,%xmm2
	vpsrld	$23,%xmm7,%xmm3
	vpxor	%xmm3,%xmm2,%xmm1
	vpslld	$19,%xmm7,%xmm4
	vpsrld	$13,%xmm7,%xmm5
	vpxor	%xmm5,%xmm4,%xmm0
	vpblendd	$0x3,%xmm0,%xmm1,%xmm7
	vpshufd	$0x1B,%xmm6,%xmm0
	vpshufd	$0x1B,%xmm7,%xmm1

	vpunpcklqdq	%xmm1,%xmm0,%xmm6
	vpunpckhqdq	%xmm1,%xmm0,%xmm7

	vmovdqu	%xmm6,(%rdi)
	vmovdqu	%xmm7,16(%rdi)
.done_hash:

	popq	%rbp
.cfi_restore	%rbp
	.byte	0xf3,0xc3
.cfi_endproc	
