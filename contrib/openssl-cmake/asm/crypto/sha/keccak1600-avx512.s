.text

.type	__KeccakF1600,@function
.align	32
__KeccakF1600:
	lea		iotas(%rip),%r10
	mov		$12,%eax
	jmp		.Loop_avx512

.align	32
.Loop_avx512:
	######################################### Theta, even round
	vmovdqa64	%zmm0,%zmm5		# put aside original A00
	vpternlogq	$0x96,%zmm2,%zmm1,%zmm0	# and use it as "C00"
	vpternlogq	$0x96,%zmm4,%zmm3,%zmm0

	vprolq		$1,%zmm0,%zmm6
	vpermq		%zmm0,%zmm13,%zmm0
	vpermq		%zmm6,%zmm16,%zmm6

	vpternlogq	$0x96,%zmm0,%zmm6,%zmm5	# T[0] is original A00
	vpternlogq	$0x96,%zmm0,%zmm6,%zmm1
	vpternlogq	$0x96,%zmm0,%zmm6,%zmm2
	vpternlogq	$0x96,%zmm0,%zmm6,%zmm3
	vpternlogq	$0x96,%zmm0,%zmm6,%zmm4

	######################################### Rho
	vprolvq		%zmm22,%zmm5,%zmm0	# T[0] is original A00
	vprolvq		%zmm23,%zmm1,%zmm1
	vprolvq		%zmm24,%zmm2,%zmm2
	vprolvq		%zmm25,%zmm3,%zmm3
	vprolvq		%zmm26,%zmm4,%zmm4

	######################################### Pi
	vpermq		%zmm0,%zmm17,%zmm0
	vpermq		%zmm1,%zmm18,%zmm1
	vpermq		%zmm2,%zmm19,%zmm2
	vpermq		%zmm3,%zmm20,%zmm3
	vpermq		%zmm4,%zmm21,%zmm4

	######################################### Chi
	vmovdqa64	%zmm0,%zmm5
	vmovdqa64	%zmm1,%zmm6
	vpternlogq	$0xD2,%zmm2,%zmm1,%zmm0
	vpternlogq	$0xD2,%zmm3,%zmm2,%zmm1
	vpternlogq	$0xD2,%zmm4,%zmm3,%zmm2
	vpternlogq	$0xD2,%zmm5,%zmm4,%zmm3
	vpternlogq	$0xD2,%zmm6,%zmm5,%zmm4

	######################################### Iota
	vpxorq		(%r10),%zmm0,%zmm0{%k1}
	lea		16(%r10),%r10

	######################################### Harmonize rounds
	vpblendmq	%zmm2,%zmm1,%zmm6{%k2}
	vpblendmq	%zmm3,%zmm2,%zmm7{%k2}
	vpblendmq	%zmm4,%zmm3,%zmm8{%k2}
	 vpblendmq	%zmm1,%zmm0,%zmm5{%k2}
	vpblendmq	%zmm0,%zmm4,%zmm9{%k2}

	vpblendmq	%zmm3,%zmm6,%zmm6{%k3}
	vpblendmq	%zmm4,%zmm7,%zmm7{%k3}
	 vpblendmq	%zmm2,%zmm5,%zmm5{%k3}
	vpblendmq	%zmm0,%zmm8,%zmm8{%k3}
	vpblendmq	%zmm1,%zmm9,%zmm9{%k3}

	vpblendmq	%zmm4,%zmm6,%zmm6{%k4}
	 vpblendmq	%zmm3,%zmm5,%zmm5{%k4}
	vpblendmq	%zmm0,%zmm7,%zmm7{%k4}
	vpblendmq	%zmm1,%zmm8,%zmm8{%k4}
	vpblendmq	%zmm2,%zmm9,%zmm9{%k4}

	vpblendmq	%zmm4,%zmm5,%zmm5{%k5}
	vpblendmq	%zmm0,%zmm6,%zmm6{%k5}
	vpblendmq	%zmm1,%zmm7,%zmm7{%k5}
	vpblendmq	%zmm2,%zmm8,%zmm8{%k5}
	vpblendmq	%zmm3,%zmm9,%zmm9{%k5}

	#vpermq		%zmm5,%zmm33,%zmm0	# doesn't actually change order
	vpermq		%zmm6,%zmm13,%zmm1
	vpermq		%zmm7,%zmm14,%zmm2
	vpermq		%zmm8,%zmm15,%zmm3
	vpermq		%zmm9,%zmm16,%zmm4

	######################################### Theta, odd round
	vmovdqa64	%zmm5,%zmm0		# real A00
	vpternlogq	$0x96,%zmm2,%zmm1,%zmm5	# C00 is %zmm5's alias
	vpternlogq	$0x96,%zmm4,%zmm3,%zmm5

	vprolq		$1,%zmm5,%zmm6
	vpermq		%zmm5,%zmm13,%zmm5
	vpermq		%zmm6,%zmm16,%zmm6

	vpternlogq	$0x96,%zmm5,%zmm6,%zmm0
	vpternlogq	$0x96,%zmm5,%zmm6,%zmm3
	vpternlogq	$0x96,%zmm5,%zmm6,%zmm1
	vpternlogq	$0x96,%zmm5,%zmm6,%zmm4
	vpternlogq	$0x96,%zmm5,%zmm6,%zmm2

	######################################### Rho
	vprolvq		%zmm27,%zmm0,%zmm0
	vprolvq		%zmm30,%zmm3,%zmm6
	vprolvq		%zmm28,%zmm1,%zmm7
	vprolvq		%zmm31,%zmm4,%zmm8
	vprolvq		%zmm29,%zmm2,%zmm9

	 vpermq		%zmm0,%zmm16,%zmm10
	 vpermq		%zmm0,%zmm15,%zmm11

	######################################### Iota
	vpxorq		-8(%r10),%zmm0,%zmm0{%k1}

	######################################### Pi
	vpermq		%zmm6,%zmm14,%zmm1
	vpermq		%zmm7,%zmm16,%zmm2
	vpermq		%zmm8,%zmm13,%zmm3
	vpermq		%zmm9,%zmm15,%zmm4

	######################################### Chi
	vpternlogq	$0xD2,%zmm11,%zmm10,%zmm0

	vpermq		%zmm6,%zmm13,%zmm12
	#vpermq		%zmm6,%zmm33,%zmm6
	vpternlogq	$0xD2,%zmm6,%zmm12,%zmm1

	vpermq		%zmm7,%zmm15,%zmm5
	vpermq		%zmm7,%zmm14,%zmm7
	vpternlogq	$0xD2,%zmm7,%zmm5,%zmm2

	#vpermq		%zmm8,%zmm33,%zmm8
	vpermq		%zmm8,%zmm16,%zmm6
	vpternlogq	$0xD2,%zmm6,%zmm8,%zmm3

	vpermq		%zmm9,%zmm14,%zmm5
	vpermq		%zmm9,%zmm13,%zmm9
	vpternlogq	$0xD2,%zmm9,%zmm5,%zmm4

	dec		%eax
	jnz		.Loop_avx512

	ret
.size	__KeccakF1600,.-__KeccakF1600
.globl	SHA3_absorb
.type	SHA3_absorb,@function
.align	32
SHA3_absorb:
	mov	%rsp,%r11

	lea	-320(%rsp),%rsp
	and	$-64,%rsp

	lea	96(%rdi),%rdi
	lea	96(%rsi),%rsi
	lea	128(%rsp),%r9

	lea		theta_perm(%rip),%r8

	kxnorw		%k6,%k6,%k6
	kshiftrw	$15,%k6,%k1
	kshiftrw	$11,%k6,%k6
	kshiftlw	$1,%k1,%k2
	kshiftlw	$2,%k1,%k3
	kshiftlw	$3,%k1,%k4
	kshiftlw	$4,%k1,%k5

	#vmovdqa64	64*0(%r8),%zmm33
	vmovdqa64	64*1(%r8),%zmm13
	vmovdqa64	64*2(%r8),%zmm14
	vmovdqa64	64*3(%r8),%zmm15
	vmovdqa64	64*4(%r8),%zmm16

	vmovdqa64	64*5(%r8),%zmm27
	vmovdqa64	64*6(%r8),%zmm28
	vmovdqa64	64*7(%r8),%zmm29
	vmovdqa64	64*8(%r8),%zmm30
	vmovdqa64	64*9(%r8),%zmm31

	vmovdqa64	64*10(%r8),%zmm22
	vmovdqa64	64*11(%r8),%zmm23
	vmovdqa64	64*12(%r8),%zmm24
	vmovdqa64	64*13(%r8),%zmm25
	vmovdqa64	64*14(%r8),%zmm26

	vmovdqa64	64*15(%r8),%zmm17
	vmovdqa64	64*16(%r8),%zmm18
	vmovdqa64	64*17(%r8),%zmm19
	vmovdqa64	64*18(%r8),%zmm20
	vmovdqa64	64*19(%r8),%zmm21

	vmovdqu64	40*0-96(%rdi),%zmm0{%k6}{z}
	vpxorq		%zmm5,%zmm5,%zmm5
	vmovdqu64	40*1-96(%rdi),%zmm1{%k6}{z}
	vmovdqu64	40*2-96(%rdi),%zmm2{%k6}{z}
	vmovdqu64	40*3-96(%rdi),%zmm3{%k6}{z}
	vmovdqu64	40*4-96(%rdi),%zmm4{%k6}{z}

	vmovdqa64	%zmm5,0*64-128(%r9)	# zero transfer area on stack
	vmovdqa64	%zmm5,1*64-128(%r9)
	vmovdqa64	%zmm5,2*64-128(%r9)
	vmovdqa64	%zmm5,3*64-128(%r9)
	vmovdqa64	%zmm5,4*64-128(%r9)
	jmp		.Loop_absorb_avx512

.align	32
.Loop_absorb_avx512:
	mov		%rcx,%rax
	sub		%rcx,%rdx
	jc		.Ldone_absorb_avx512

	shr		$3,%eax
	mov	8*0-96(%rsi),%r8
	mov	%r8,0-128(%r9)
	dec	%eax
	jz	.Labsorved_avx512
	mov	8*1-96(%rsi),%r8
	mov	%r8,8-128(%r9)
	dec	%eax
	jz	.Labsorved_avx512
	mov	8*2-96(%rsi),%r8
	mov	%r8,16-128(%r9)
	dec	%eax
	jz	.Labsorved_avx512
	mov	8*3-96(%rsi),%r8
	mov	%r8,24-128(%r9)
	dec	%eax
	jz	.Labsorved_avx512
	mov	8*4-96(%rsi),%r8
	mov	%r8,32-128(%r9)
	dec	%eax
	jz	.Labsorved_avx512
	mov	8*5-96(%rsi),%r8
	mov	%r8,64-128(%r9)
	dec	%eax
	jz	.Labsorved_avx512
	mov	8*6-96(%rsi),%r8
	mov	%r8,72-128(%r9)
	dec	%eax
	jz	.Labsorved_avx512
	mov	8*7-96(%rsi),%r8
	mov	%r8,80-128(%r9)
	dec	%eax
	jz	.Labsorved_avx512
	mov	8*8-96(%rsi),%r8
	mov	%r8,88-128(%r9)
	dec	%eax
	jz	.Labsorved_avx512
	mov	8*9-96(%rsi),%r8
	mov	%r8,96-128(%r9)
	dec	%eax
	jz	.Labsorved_avx512
	mov	8*10-96(%rsi),%r8
	mov	%r8,128-128(%r9)
	dec	%eax
	jz	.Labsorved_avx512
	mov	8*11-96(%rsi),%r8
	mov	%r8,136-128(%r9)
	dec	%eax
	jz	.Labsorved_avx512
	mov	8*12-96(%rsi),%r8
	mov	%r8,144-128(%r9)
	dec	%eax
	jz	.Labsorved_avx512
	mov	8*13-96(%rsi),%r8
	mov	%r8,152-128(%r9)
	dec	%eax
	jz	.Labsorved_avx512
	mov	8*14-96(%rsi),%r8
	mov	%r8,160-128(%r9)
	dec	%eax
	jz	.Labsorved_avx512
	mov	8*15-96(%rsi),%r8
	mov	%r8,192-128(%r9)
	dec	%eax
	jz	.Labsorved_avx512
	mov	8*16-96(%rsi),%r8
	mov	%r8,200-128(%r9)
	dec	%eax
	jz	.Labsorved_avx512
	mov	8*17-96(%rsi),%r8
	mov	%r8,208-128(%r9)
	dec	%eax
	jz	.Labsorved_avx512
	mov	8*18-96(%rsi),%r8
	mov	%r8,216-128(%r9)
	dec	%eax
	jz	.Labsorved_avx512
	mov	8*19-96(%rsi),%r8
	mov	%r8,224-128(%r9)
	dec	%eax
	jz	.Labsorved_avx512
	mov	8*20-96(%rsi),%r8
	mov	%r8,256-128(%r9)
	dec	%eax
	jz	.Labsorved_avx512
	mov	8*21-96(%rsi),%r8
	mov	%r8,264-128(%r9)
	dec	%eax
	jz	.Labsorved_avx512
	mov	8*22-96(%rsi),%r8
	mov	%r8,272-128(%r9)
	dec	%eax
	jz	.Labsorved_avx512
	mov	8*23-96(%rsi),%r8
	mov	%r8,280-128(%r9)
	dec	%eax
	jz	.Labsorved_avx512
	mov	8*24-96(%rsi),%r8
	mov	%r8,288-128(%r9)
	dec	%eax
	jz	.Labsorved_avx512
.Labsorved_avx512:
	lea	(%rsi,%rcx),%rsi

	vpxorq	64*0-128(%r9),%zmm0,%zmm0
	vpxorq	64*1-128(%r9),%zmm1,%zmm1
	vpxorq	64*2-128(%r9),%zmm2,%zmm2
	vpxorq	64*3-128(%r9),%zmm3,%zmm3
	vpxorq	64*4-128(%r9),%zmm4,%zmm4

	call	__KeccakF1600

	jmp	.Loop_absorb_avx512

.align	32
.Ldone_absorb_avx512:
	vmovdqu64	%zmm0,40*0-96(%rdi){%k6}
	vmovdqu64	%zmm1,40*1-96(%rdi){%k6}
	vmovdqu64	%zmm2,40*2-96(%rdi){%k6}
	vmovdqu64	%zmm3,40*3-96(%rdi){%k6}
	vmovdqu64	%zmm4,40*4-96(%rdi){%k6}

	vzeroupper

	lea	(%r11),%rsp
	lea	(%rdx,%rcx),%rax		# return value
	ret
.size	SHA3_absorb,.-SHA3_absorb

.globl	SHA3_squeeze
.type	SHA3_squeeze,@function
.align	32
SHA3_squeeze:
	mov	%rsp,%r11

	lea	96(%rdi),%rdi
	cmp	%rcx,%rdx
	jbe	.Lno_output_extension_avx512

	lea		theta_perm(%rip),%r8

	kxnorw		%k6,%k6,%k6
	kshiftrw	$15,%k6,%k1
	kshiftrw	$11,%k6,%k6
	kshiftlw	$1,%k1,%k2
	kshiftlw	$2,%k1,%k3
	kshiftlw	$3,%k1,%k4
	kshiftlw	$4,%k1,%k5

	#vmovdqa64	64*0(%r8),%zmm33
	vmovdqa64	64*1(%r8),%zmm13
	vmovdqa64	64*2(%r8),%zmm14
	vmovdqa64	64*3(%r8),%zmm15
	vmovdqa64	64*4(%r8),%zmm16

	vmovdqa64	64*5(%r8),%zmm27
	vmovdqa64	64*6(%r8),%zmm28
	vmovdqa64	64*7(%r8),%zmm29
	vmovdqa64	64*8(%r8),%zmm30
	vmovdqa64	64*9(%r8),%zmm31

	vmovdqa64	64*10(%r8),%zmm22
	vmovdqa64	64*11(%r8),%zmm23
	vmovdqa64	64*12(%r8),%zmm24
	vmovdqa64	64*13(%r8),%zmm25
	vmovdqa64	64*14(%r8),%zmm26

	vmovdqa64	64*15(%r8),%zmm17
	vmovdqa64	64*16(%r8),%zmm18
	vmovdqa64	64*17(%r8),%zmm19
	vmovdqa64	64*18(%r8),%zmm20
	vmovdqa64	64*19(%r8),%zmm21

	vmovdqu64	40*0-96(%rdi),%zmm0{%k6}{z}
	vmovdqu64	40*1-96(%rdi),%zmm1{%k6}{z}
	vmovdqu64	40*2-96(%rdi),%zmm2{%k6}{z}
	vmovdqu64	40*3-96(%rdi),%zmm3{%k6}{z}
	vmovdqu64	40*4-96(%rdi),%zmm4{%k6}{z}

.Lno_output_extension_avx512:
	shr	$3,%rcx
	lea	-96(%rdi),%r9
	mov	%rcx,%rax
	jmp	.Loop_squeeze_avx512

.align	32
.Loop_squeeze_avx512:
	cmp	$8,%rdx
	jb	.Ltail_squeeze_avx512

	mov	(%r9),%r8
	lea	8(%r9),%r9
	mov	%r8,(%rsi)
	lea	8(%rsi),%rsi
	sub	$8,%rdx		# len -= 8
	jz	.Ldone_squeeze_avx512

	sub	$1,%rax		# bsz--
	jnz	.Loop_squeeze_avx512

	#vpermq		%zmm16,%zmm16,%zmm15
	#vpermq		%zmm15,%zmm16,%zmm14
	#vpermq		%zmm15,%zmm15,%zmm13

	call		__KeccakF1600

	vmovdqu64	%zmm0,40*0-96(%rdi){%k6}
	vmovdqu64	%zmm1,40*1-96(%rdi){%k6}
	vmovdqu64	%zmm2,40*2-96(%rdi){%k6}
	vmovdqu64	%zmm3,40*3-96(%rdi){%k6}
	vmovdqu64	%zmm4,40*4-96(%rdi){%k6}

	lea	-96(%rdi),%r9
	mov	%rcx,%rax
	jmp	.Loop_squeeze_avx512

.Ltail_squeeze_avx512:
	mov	%rsi,%rdi
	mov	%r9,%rsi
	mov	%rdx,%rcx
	.byte	0xf3,0xa4		# rep movsb

.Ldone_squeeze_avx512:
	vzeroupper

	lea	(%r11),%rsp
	ret
.size	SHA3_squeeze,.-SHA3_squeeze

.section .rodata
.align	64
theta_perm:
	.quad	0, 1, 2, 3, 4, 5, 6, 7		# [not used]
	.quad	4, 0, 1, 2, 3, 5, 6, 7
	.quad	3, 4, 0, 1, 2, 5, 6, 7
	.quad	2, 3, 4, 0, 1, 5, 6, 7
	.quad	1, 2, 3, 4, 0, 5, 6, 7

rhotates1:
	.quad	0,  44, 43, 21, 14, 0, 0, 0	# [0][0] [1][1] [2][2] [3][3] [4][4]
	.quad	18, 1,  6,  25, 8,  0, 0, 0	# [4][0] [0][1] [1][2] [2][3] [3][4]
	.quad	41, 2,	62, 55, 39, 0, 0, 0	# [3][0] [4][1] [0][2] [1][3] [2][4]
	.quad	3,  45, 61, 28, 20, 0, 0, 0	# [2][0] [3][1] [4][2] [0][3] [1][4]
	.quad	36, 10, 15, 56, 27, 0, 0, 0	# [1][0] [2][1] [3][2] [4][3] [0][4]

rhotates0:
	.quad	 0,  1, 62, 28, 27, 0, 0, 0
	.quad	36, 44,  6, 55, 20, 0, 0, 0
	.quad	 3, 10, 43, 25, 39, 0, 0, 0
	.quad	41, 45, 15, 21,  8, 0, 0, 0
	.quad	18,  2, 61, 56, 14, 0, 0, 0

pi0_perm:
	.quad	0, 3, 1, 4, 2, 5, 6, 7
	.quad	1, 4, 2, 0, 3, 5, 6, 7
	.quad	2, 0, 3, 1, 4, 5, 6, 7
	.quad	3, 1, 4, 2, 0, 5, 6, 7
	.quad	4, 2, 0, 3, 1, 5, 6, 7


iotas:
	.quad	0x0000000000000001
	.quad	0x0000000000008082
	.quad	0x800000000000808a
	.quad	0x8000000080008000
	.quad	0x000000000000808b
	.quad	0x0000000080000001
	.quad	0x8000000080008081
	.quad	0x8000000000008009
	.quad	0x000000000000008a
	.quad	0x0000000000000088
	.quad	0x0000000080008009
	.quad	0x000000008000000a
	.quad	0x000000008000808b
	.quad	0x800000000000008b
	.quad	0x8000000000008089
	.quad	0x8000000000008003
	.quad	0x8000000000008002
	.quad	0x8000000000000080
	.quad	0x000000000000800a
	.quad	0x800000008000000a
	.quad	0x8000000080008081
	.quad	0x8000000000008080
	.quad	0x0000000080000001
	.quad	0x8000000080008008

.asciz	"Keccak-1600 absorb and squeeze for AVX-512F, CRYPTOGAMS by <appro@openssl.org>"
