.text

.type	__KeccakF1600,@function
.align	32
__KeccakF1600:
	lea		rhotates_left+96(%rip),%r8
	lea		rhotates_right+96(%rip),%r9
	lea		iotas(%rip),%r10
	mov		$24,%eax
	jmp		.Loop_avx2

.align	32
.Loop_avx2:
	######################################### Theta
	vpshufd		$0b01001110,%ymm2,%ymm13
	vpxor		%ymm3,%ymm5,%ymm12
	vpxor		%ymm6,%ymm4,%ymm9
	vpxor		%ymm1,%ymm12,%ymm12
	vpxor		%ymm9,%ymm12,%ymm12		# C[1..4]

	vpermq		$0b10010011,%ymm12,%ymm11
	vpxor		%ymm2,%ymm13,%ymm13
	vpermq		$0b01001110,%ymm13,%ymm7

	vpsrlq		$63,%ymm12,%ymm8
	vpaddq		%ymm12,%ymm12,%ymm9
	vpor		%ymm9,%ymm8,%ymm8	# ROL64(C[1..4],1)

	vpermq		$0b00111001,%ymm8,%ymm15
	vpxor		%ymm11,%ymm8,%ymm14
	vpermq		$0b00000000,%ymm14,%ymm14	# D[0..0] = ROL64(C[1],1) ^ C[4]

	vpxor		%ymm0,%ymm13,%ymm13
	vpxor		%ymm7,%ymm13,%ymm13		# C[0..0]

	vpsrlq		$63,%ymm13,%ymm7
	vpaddq		%ymm13,%ymm13,%ymm8
	vpor		%ymm7,%ymm8,%ymm8	# ROL64(C[0..0],1)

	vpxor		%ymm14,%ymm2,%ymm2		# ^= D[0..0]
	vpxor		%ymm14,%ymm0,%ymm0		# ^= D[0..0]

	vpblendd	$0b11000000,%ymm8,%ymm15,%ymm15
	vpblendd	$0b00000011,%ymm13,%ymm11,%ymm11
	vpxor		%ymm11,%ymm15,%ymm15		# D[1..4] = ROL64(C[2..4,0),1) ^ C[0..3]

	######################################### Rho + Pi + pre-Chi shuffle
	vpsllvq		0*32-96(%r8),%ymm2,%ymm10
	vpsrlvq		0*32-96(%r9),%ymm2,%ymm2
	vpor		%ymm10,%ymm2,%ymm2

	 vpxor		%ymm15,%ymm3,%ymm3		# ^= D[1..4] from Theta
	vpsllvq		2*32-96(%r8),%ymm3,%ymm11
	vpsrlvq		2*32-96(%r9),%ymm3,%ymm3
	vpor		%ymm11,%ymm3,%ymm3

	 vpxor		%ymm15,%ymm4,%ymm4		# ^= D[1..4] from Theta
	vpsllvq		3*32-96(%r8),%ymm4,%ymm12
	vpsrlvq		3*32-96(%r9),%ymm4,%ymm4
	vpor		%ymm12,%ymm4,%ymm4

	 vpxor		%ymm15,%ymm5,%ymm5		# ^= D[1..4] from Theta
	vpsllvq		4*32-96(%r8),%ymm5,%ymm13
	vpsrlvq		4*32-96(%r9),%ymm5,%ymm5
	vpor		%ymm13,%ymm5,%ymm5

	 vpxor		%ymm15,%ymm6,%ymm6		# ^= D[1..4] from Theta
	 vpermq		$0b10001101,%ymm2,%ymm10	# %ymm2 -> future %ymm3
	 vpermq		$0b10001101,%ymm3,%ymm11	# %ymm3 -> future %ymm4
	vpsllvq		5*32-96(%r8),%ymm6,%ymm14
	vpsrlvq		5*32-96(%r9),%ymm6,%ymm8
	vpor		%ymm14,%ymm8,%ymm8	# %ymm6 -> future %ymm1

	 vpxor		%ymm15,%ymm1,%ymm1		# ^= D[1..4] from Theta
	 vpermq		$0b00011011,%ymm4,%ymm12	# %ymm4 -> future %ymm5
	 vpermq		$0b01110010,%ymm5,%ymm13	# %ymm5 -> future %ymm6
	vpsllvq		1*32-96(%r8),%ymm1,%ymm15
	vpsrlvq		1*32-96(%r9),%ymm1,%ymm9
	vpor		%ymm15,%ymm9,%ymm9	# %ymm1 -> future %ymm2

	######################################### Chi
	vpsrldq		$8,%ymm8,%ymm14
	vpandn		%ymm14,%ymm8,%ymm7	# tgting  [0][0] [0][0] [0][0] [0][0]

	vpblendd	$0b00001100,%ymm13,%ymm9,%ymm3	#               [4][4] [2][0]
	vpblendd	$0b00001100,%ymm9,%ymm11,%ymm15	#               [4][0] [2][1]
	 vpblendd	$0b00001100,%ymm11,%ymm10,%ymm5	#               [4][2] [2][4]
	 vpblendd	$0b00001100,%ymm10,%ymm9,%ymm14	#               [4][3] [2][0]
	vpblendd	$0b00110000,%ymm11,%ymm3,%ymm3	#        [1][3] [4][4] [2][0]
	vpblendd	$0b00110000,%ymm12,%ymm15,%ymm15	#        [1][4] [4][0] [2][1]
	 vpblendd	$0b00110000,%ymm9,%ymm5,%ymm5	#        [1][0] [4][2] [2][4]
	 vpblendd	$0b00110000,%ymm13,%ymm14,%ymm14	#        [1][1] [4][3] [2][0]
	vpblendd	$0b11000000,%ymm12,%ymm3,%ymm3	# [3][2] [1][3] [4][4] [2][0]
	vpblendd	$0b11000000,%ymm13,%ymm15,%ymm15	# [3][3] [1][4] [4][0] [2][1]
	 vpblendd	$0b11000000,%ymm13,%ymm5,%ymm5	# [3][3] [1][0] [4][2] [2][4]
	 vpblendd	$0b11000000,%ymm11,%ymm14,%ymm14	# [3][4] [1][1] [4][3] [2][0]
	vpandn		%ymm15,%ymm3,%ymm3		# tgting  [3][1] [1][2] [4][3] [2][4]
	 vpandn		%ymm14,%ymm5,%ymm5		# tgting  [3][2] [1][4] [4][1] [2][3]

	vpblendd	$0b00001100,%ymm9,%ymm12,%ymm6	#               [4][0] [2][3]
	vpblendd	$0b00001100,%ymm12,%ymm10,%ymm15	#               [4][1] [2][4]
	 vpxor		%ymm10,%ymm3,%ymm3
	vpblendd	$0b00110000,%ymm10,%ymm6,%ymm6	#        [1][2] [4][0] [2][3]
	vpblendd	$0b00110000,%ymm11,%ymm15,%ymm15	#        [1][3] [4][1] [2][4]
	 vpxor		%ymm12,%ymm5,%ymm5
	vpblendd	$0b11000000,%ymm11,%ymm6,%ymm6	# [3][4] [1][2] [4][0] [2][3]
	vpblendd	$0b11000000,%ymm9,%ymm15,%ymm15	# [3][0] [1][3] [4][1] [2][4]
	vpandn		%ymm15,%ymm6,%ymm6		# tgting  [3][3] [1][1] [4][4] [2][2]
	vpxor		%ymm13,%ymm6,%ymm6

	  vpermq	$0b00011110,%ymm8,%ymm4		# [0][1] [0][2] [0][4] [0][3]
	  vpblendd	$0b00110000,%ymm0,%ymm4,%ymm15	# [0][1] [0][0] [0][4] [0][3]
	  vpermq	$0b00111001,%ymm8,%ymm1		# [0][1] [0][4] [0][3] [0][2]
	  vpblendd	$0b11000000,%ymm0,%ymm1,%ymm1	# [0][0] [0][4] [0][3] [0][2]
	  vpandn	%ymm15,%ymm1,%ymm1		# tgting  [0][4] [0][3] [0][2] [0][1]

	vpblendd	$0b00001100,%ymm12,%ymm11,%ymm2	#               [4][1] [2][1]
	vpblendd	$0b00001100,%ymm11,%ymm13,%ymm14	#               [4][2] [2][2]
	vpblendd	$0b00110000,%ymm13,%ymm2,%ymm2	#        [1][1] [4][1] [2][1]
	vpblendd	$0b00110000,%ymm10,%ymm14,%ymm14	#        [1][2] [4][2] [2][2]
	vpblendd	$0b11000000,%ymm10,%ymm2,%ymm2	# [3][1] [1][1] [4][1] [2][1]
	vpblendd	$0b11000000,%ymm12,%ymm14,%ymm14	# [3][2] [1][2] [4][2] [2][2]
	vpandn		%ymm14,%ymm2,%ymm2		# tgting  [3][0] [1][0] [4][0] [2][0]
	vpxor		%ymm9,%ymm2,%ymm2

	 vpermq		$0b00000000,%ymm7,%ymm7	# [0][0] [0][0] [0][0] [0][0]
	 vpermq		$0b00011011,%ymm3,%ymm3	# post-Chi shuffle
	 vpermq		$0b10001101,%ymm5,%ymm5
	 vpermq		$0b01110010,%ymm6,%ymm6

	vpblendd	$0b00001100,%ymm10,%ymm13,%ymm4	#               [4][3] [2][2]
	vpblendd	$0b00001100,%ymm13,%ymm12,%ymm14	#               [4][4] [2][3]
	vpblendd	$0b00110000,%ymm12,%ymm4,%ymm4	#        [1][4] [4][3] [2][2]
	vpblendd	$0b00110000,%ymm9,%ymm14,%ymm14	#        [1][0] [4][4] [2][3]
	vpblendd	$0b11000000,%ymm9,%ymm4,%ymm4	# [3][0] [1][4] [4][3] [2][2]
	vpblendd	$0b11000000,%ymm10,%ymm14,%ymm14	# [3][1] [1][0] [4][4] [2][3]
	vpandn		%ymm14,%ymm4,%ymm4		# tgting  [3][4] [1][3] [4][2] [2][1]

	vpxor		%ymm7,%ymm0,%ymm0
	vpxor		%ymm8,%ymm1,%ymm1
	vpxor		%ymm11,%ymm4,%ymm4

	######################################### Iota
	vpxor		(%r10),%ymm0,%ymm0
	lea		32(%r10),%r10

	dec		%eax
	jnz		.Loop_avx2

	ret
.size	__KeccakF1600,.-__KeccakF1600
.globl	SHA3_absorb
.type	SHA3_absorb,@function
.align	32
SHA3_absorb:
	mov	%rsp,%r11

	lea	-240(%rsp),%rsp
	and	$-32,%rsp

	lea	96(%rdi),%rdi
	lea	96(%rsi),%rsi
	lea	96(%rsp),%r10

	vzeroupper

	vpbroadcastq	-96(%rdi),%ymm0	# load A[5][5]
	vmovdqu		8+32*0-96(%rdi),%ymm1
	vmovdqu		8+32*1-96(%rdi),%ymm2
	vmovdqu		8+32*2-96(%rdi),%ymm3
	vmovdqu		8+32*3-96(%rdi),%ymm4
	vmovdqu		8+32*4-96(%rdi),%ymm5
	vmovdqu		8+32*5-96(%rdi),%ymm6

	vpxor		%ymm7,%ymm7,%ymm7
	vmovdqa		%ymm7,32*2-96(%r10)	# zero transfer area on stack
	vmovdqa		%ymm7,32*3-96(%r10)
	vmovdqa		%ymm7,32*4-96(%r10)
	vmovdqa		%ymm7,32*5-96(%r10)
	vmovdqa		%ymm7,32*6-96(%r10)

.Loop_absorb_avx2:
	mov		%rcx,%rax
	sub		%rcx,%rdx
	jc		.Ldone_absorb_avx2

	shr		$3,%eax
	vpbroadcastq	0-96(%rsi),%ymm7
	vmovdqu		8-96(%rsi),%ymm8
	sub		$4,%eax
	dec	%eax
	jz	.Labsorved_avx2
	mov	8*5-96(%rsi),%r8
	mov	%r8,80-96(%r10)
	dec	%eax
	jz	.Labsorved_avx2
	mov	8*6-96(%rsi),%r8
	mov	%r8,192-96(%r10)
	dec	%eax
	jz	.Labsorved_avx2
	mov	8*7-96(%rsi),%r8
	mov	%r8,104-96(%r10)
	dec	%eax
	jz	.Labsorved_avx2
	mov	8*8-96(%rsi),%r8
	mov	%r8,144-96(%r10)
	dec	%eax
	jz	.Labsorved_avx2
	mov	8*9-96(%rsi),%r8
	mov	%r8,184-96(%r10)
	dec	%eax
	jz	.Labsorved_avx2
	mov	8*10-96(%rsi),%r8
	mov	%r8,64-96(%r10)
	dec	%eax
	jz	.Labsorved_avx2
	mov	8*11-96(%rsi),%r8
	mov	%r8,128-96(%r10)
	dec	%eax
	jz	.Labsorved_avx2
	mov	8*12-96(%rsi),%r8
	mov	%r8,200-96(%r10)
	dec	%eax
	jz	.Labsorved_avx2
	mov	8*13-96(%rsi),%r8
	mov	%r8,176-96(%r10)
	dec	%eax
	jz	.Labsorved_avx2
	mov	8*14-96(%rsi),%r8
	mov	%r8,120-96(%r10)
	dec	%eax
	jz	.Labsorved_avx2
	mov	8*15-96(%rsi),%r8
	mov	%r8,88-96(%r10)
	dec	%eax
	jz	.Labsorved_avx2
	mov	8*16-96(%rsi),%r8
	mov	%r8,96-96(%r10)
	dec	%eax
	jz	.Labsorved_avx2
	mov	8*17-96(%rsi),%r8
	mov	%r8,168-96(%r10)
	dec	%eax
	jz	.Labsorved_avx2
	mov	8*18-96(%rsi),%r8
	mov	%r8,208-96(%r10)
	dec	%eax
	jz	.Labsorved_avx2
	mov	8*19-96(%rsi),%r8
	mov	%r8,152-96(%r10)
	dec	%eax
	jz	.Labsorved_avx2
	mov	8*20-96(%rsi),%r8
	mov	%r8,72-96(%r10)
	dec	%eax
	jz	.Labsorved_avx2
	mov	8*21-96(%rsi),%r8
	mov	%r8,160-96(%r10)
	dec	%eax
	jz	.Labsorved_avx2
	mov	8*22-96(%rsi),%r8
	mov	%r8,136-96(%r10)
	dec	%eax
	jz	.Labsorved_avx2
	mov	8*23-96(%rsi),%r8
	mov	%r8,112-96(%r10)
	dec	%eax
	jz	.Labsorved_avx2
	mov	8*24-96(%rsi),%r8
	mov	%r8,216-96(%r10)
.Labsorved_avx2:
	lea	(%rsi,%rcx),%rsi

	vpxor	%ymm7,%ymm0,%ymm0
	vpxor	%ymm8,%ymm1,%ymm1
	vpxor	32*2-96(%r10),%ymm2,%ymm2
	vpxor	32*3-96(%r10),%ymm3,%ymm3
	vpxor	32*4-96(%r10),%ymm4,%ymm4
	vpxor	32*5-96(%r10),%ymm5,%ymm5
	vpxor	32*6-96(%r10),%ymm6,%ymm6

	call	__KeccakF1600

	lea	96(%rsp),%r10
	jmp	.Loop_absorb_avx2

.Ldone_absorb_avx2:
	vmovq	%xmm0,-96(%rdi)
	vmovdqu	%ymm1,8+32*0-96(%rdi)
	vmovdqu	%ymm2,8+32*1-96(%rdi)
	vmovdqu	%ymm3,8+32*2-96(%rdi)
	vmovdqu	%ymm4,8+32*3-96(%rdi)
	vmovdqu	%ymm5,8+32*4-96(%rdi)
	vmovdqu	%ymm6,8+32*5-96(%rdi)

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
	shr	$3,%rcx

	vzeroupper

	vpbroadcastq	-96(%rdi),%ymm0
	vpxor		%ymm7,%ymm7,%ymm7
	vmovdqu		8+32*0-96(%rdi),%ymm1
	vmovdqu		8+32*1-96(%rdi),%ymm2
	vmovdqu		8+32*2-96(%rdi),%ymm3
	vmovdqu		8+32*3-96(%rdi),%ymm4
	vmovdqu		8+32*4-96(%rdi),%ymm5
	vmovdqu		8+32*5-96(%rdi),%ymm6

	mov	%rcx,%rax

.Loop_squeeze_avx2:
	mov	0-96(%rdi),%r8
	sub	$8,%rdx
	jc	.Ltail_squeeze_avx2
	mov	%r8,(%rsi)
	lea	8(%rsi),%rsi
	je	.Ldone_squeeze_avx2
	dec	%eax
	je	.Lextend_output_avx2
	mov	32-120(%rdi),%r8
	sub	$8,%rdx
	jc	.Ltail_squeeze_avx2
	mov	%r8,(%rsi)
	lea	8(%rsi),%rsi
	je	.Ldone_squeeze_avx2
	dec	%eax
	je	.Lextend_output_avx2
	mov	40-120(%rdi),%r8
	sub	$8,%rdx
	jc	.Ltail_squeeze_avx2
	mov	%r8,(%rsi)
	lea	8(%rsi),%rsi
	je	.Ldone_squeeze_avx2
	dec	%eax
	je	.Lextend_output_avx2
	mov	48-120(%rdi),%r8
	sub	$8,%rdx
	jc	.Ltail_squeeze_avx2
	mov	%r8,(%rsi)
	lea	8(%rsi),%rsi
	je	.Ldone_squeeze_avx2
	dec	%eax
	je	.Lextend_output_avx2
	mov	56-120(%rdi),%r8
	sub	$8,%rdx
	jc	.Ltail_squeeze_avx2
	mov	%r8,(%rsi)
	lea	8(%rsi),%rsi
	je	.Ldone_squeeze_avx2
	dec	%eax
	je	.Lextend_output_avx2
	mov	80-120(%rdi),%r8
	sub	$8,%rdx
	jc	.Ltail_squeeze_avx2
	mov	%r8,(%rsi)
	lea	8(%rsi),%rsi
	je	.Ldone_squeeze_avx2
	dec	%eax
	je	.Lextend_output_avx2
	mov	192-120(%rdi),%r8
	sub	$8,%rdx
	jc	.Ltail_squeeze_avx2
	mov	%r8,(%rsi)
	lea	8(%rsi),%rsi
	je	.Ldone_squeeze_avx2
	dec	%eax
	je	.Lextend_output_avx2
	mov	104-120(%rdi),%r8
	sub	$8,%rdx
	jc	.Ltail_squeeze_avx2
	mov	%r8,(%rsi)
	lea	8(%rsi),%rsi
	je	.Ldone_squeeze_avx2
	dec	%eax
	je	.Lextend_output_avx2
	mov	144-120(%rdi),%r8
	sub	$8,%rdx
	jc	.Ltail_squeeze_avx2
	mov	%r8,(%rsi)
	lea	8(%rsi),%rsi
	je	.Ldone_squeeze_avx2
	dec	%eax
	je	.Lextend_output_avx2
	mov	184-120(%rdi),%r8
	sub	$8,%rdx
	jc	.Ltail_squeeze_avx2
	mov	%r8,(%rsi)
	lea	8(%rsi),%rsi
	je	.Ldone_squeeze_avx2
	dec	%eax
	je	.Lextend_output_avx2
	mov	64-120(%rdi),%r8
	sub	$8,%rdx
	jc	.Ltail_squeeze_avx2
	mov	%r8,(%rsi)
	lea	8(%rsi),%rsi
	je	.Ldone_squeeze_avx2
	dec	%eax
	je	.Lextend_output_avx2
	mov	128-120(%rdi),%r8
	sub	$8,%rdx
	jc	.Ltail_squeeze_avx2
	mov	%r8,(%rsi)
	lea	8(%rsi),%rsi
	je	.Ldone_squeeze_avx2
	dec	%eax
	je	.Lextend_output_avx2
	mov	200-120(%rdi),%r8
	sub	$8,%rdx
	jc	.Ltail_squeeze_avx2
	mov	%r8,(%rsi)
	lea	8(%rsi),%rsi
	je	.Ldone_squeeze_avx2
	dec	%eax
	je	.Lextend_output_avx2
	mov	176-120(%rdi),%r8
	sub	$8,%rdx
	jc	.Ltail_squeeze_avx2
	mov	%r8,(%rsi)
	lea	8(%rsi),%rsi
	je	.Ldone_squeeze_avx2
	dec	%eax
	je	.Lextend_output_avx2
	mov	120-120(%rdi),%r8
	sub	$8,%rdx
	jc	.Ltail_squeeze_avx2
	mov	%r8,(%rsi)
	lea	8(%rsi),%rsi
	je	.Ldone_squeeze_avx2
	dec	%eax
	je	.Lextend_output_avx2
	mov	88-120(%rdi),%r8
	sub	$8,%rdx
	jc	.Ltail_squeeze_avx2
	mov	%r8,(%rsi)
	lea	8(%rsi),%rsi
	je	.Ldone_squeeze_avx2
	dec	%eax
	je	.Lextend_output_avx2
	mov	96-120(%rdi),%r8
	sub	$8,%rdx
	jc	.Ltail_squeeze_avx2
	mov	%r8,(%rsi)
	lea	8(%rsi),%rsi
	je	.Ldone_squeeze_avx2
	dec	%eax
	je	.Lextend_output_avx2
	mov	168-120(%rdi),%r8
	sub	$8,%rdx
	jc	.Ltail_squeeze_avx2
	mov	%r8,(%rsi)
	lea	8(%rsi),%rsi
	je	.Ldone_squeeze_avx2
	dec	%eax
	je	.Lextend_output_avx2
	mov	208-120(%rdi),%r8
	sub	$8,%rdx
	jc	.Ltail_squeeze_avx2
	mov	%r8,(%rsi)
	lea	8(%rsi),%rsi
	je	.Ldone_squeeze_avx2
	dec	%eax
	je	.Lextend_output_avx2
	mov	152-120(%rdi),%r8
	sub	$8,%rdx
	jc	.Ltail_squeeze_avx2
	mov	%r8,(%rsi)
	lea	8(%rsi),%rsi
	je	.Ldone_squeeze_avx2
	dec	%eax
	je	.Lextend_output_avx2
	mov	72-120(%rdi),%r8
	sub	$8,%rdx
	jc	.Ltail_squeeze_avx2
	mov	%r8,(%rsi)
	lea	8(%rsi),%rsi
	je	.Ldone_squeeze_avx2
	dec	%eax
	je	.Lextend_output_avx2
	mov	160-120(%rdi),%r8
	sub	$8,%rdx
	jc	.Ltail_squeeze_avx2
	mov	%r8,(%rsi)
	lea	8(%rsi),%rsi
	je	.Ldone_squeeze_avx2
	dec	%eax
	je	.Lextend_output_avx2
	mov	136-120(%rdi),%r8
	sub	$8,%rdx
	jc	.Ltail_squeeze_avx2
	mov	%r8,(%rsi)
	lea	8(%rsi),%rsi
	je	.Ldone_squeeze_avx2
	dec	%eax
	je	.Lextend_output_avx2
	mov	112-120(%rdi),%r8
	sub	$8,%rdx
	jc	.Ltail_squeeze_avx2
	mov	%r8,(%rsi)
	lea	8(%rsi),%rsi
	je	.Ldone_squeeze_avx2
	dec	%eax
	je	.Lextend_output_avx2
	mov	216-120(%rdi),%r8
	sub	$8,%rdx
	jc	.Ltail_squeeze_avx2
	mov	%r8,(%rsi)
	lea	8(%rsi),%rsi
	je	.Ldone_squeeze_avx2
	dec	%eax
	je	.Lextend_output_avx2
	mov	-120(%rdi),%r8
.Lextend_output_avx2:
	call	__KeccakF1600

	vmovq	%xmm0,-96(%rdi)
	vmovdqu	%ymm1,8+32*0-96(%rdi)
	vmovdqu	%ymm2,8+32*1-96(%rdi)
	vmovdqu	%ymm3,8+32*2-96(%rdi)
	vmovdqu	%ymm4,8+32*3-96(%rdi)
	vmovdqu	%ymm5,8+32*4-96(%rdi)
	vmovdqu	%ymm6,8+32*5-96(%rdi)

	mov	%rcx,%rax
	jmp	.Loop_squeeze_avx2


.Ltail_squeeze_avx2:
	add	$8,%rdx
.Loop_tail_avx2:
	mov	%r8b,(%rsi)
	lea	1(%rsi),%rsi
	shr	$8,%r8
	dec	%rdx
	jnz	.Loop_tail_avx2

.Ldone_squeeze_avx2:
	vzeroupper

	lea	(%r11),%rsp
	ret
.size	SHA3_squeeze,.-SHA3_squeeze

.section .rodata
.align	64
rhotates_left:
	.quad	3,	18,	36,	41	# [2][0] [4][0] [1][0] [3][0]
	.quad	1,	62,	28,	27	# [0][1] [0][2] [0][3] [0][4]
	.quad	45,	6,	56,	39	# [3][1] [1][2] [4][3] [2][4]
	.quad	10,	61,	55,	8	# [2][1] [4][2] [1][3] [3][4]
	.quad	2,	15,	25,	20	# [4][1] [3][2] [2][3] [1][4]
	.quad	44,	43,	21,	14	# [1][1] [2][2] [3][3] [4][4]
rhotates_right:
	.quad	64-3,	64-18,	64-36,	64-41
	.quad	64-1,	64-62,	64-28,	64-27
	.quad	64-45,	64-6,	64-56,	64-39
	.quad	64-10,	64-61,	64-55,	64-8
	.quad	64-2,	64-15,	64-25,	64-20
	.quad	64-44,	64-43,	64-21,	64-14
iotas:
	.quad	0x0000000000000001, 0x0000000000000001, 0x0000000000000001, 0x0000000000000001
	.quad	0x0000000000008082, 0x0000000000008082, 0x0000000000008082, 0x0000000000008082
	.quad	0x800000000000808a, 0x800000000000808a, 0x800000000000808a, 0x800000000000808a
	.quad	0x8000000080008000, 0x8000000080008000, 0x8000000080008000, 0x8000000080008000
	.quad	0x000000000000808b, 0x000000000000808b, 0x000000000000808b, 0x000000000000808b
	.quad	0x0000000080000001, 0x0000000080000001, 0x0000000080000001, 0x0000000080000001
	.quad	0x8000000080008081, 0x8000000080008081, 0x8000000080008081, 0x8000000080008081
	.quad	0x8000000000008009, 0x8000000000008009, 0x8000000000008009, 0x8000000000008009
	.quad	0x000000000000008a, 0x000000000000008a, 0x000000000000008a, 0x000000000000008a
	.quad	0x0000000000000088, 0x0000000000000088, 0x0000000000000088, 0x0000000000000088
	.quad	0x0000000080008009, 0x0000000080008009, 0x0000000080008009, 0x0000000080008009
	.quad	0x000000008000000a, 0x000000008000000a, 0x000000008000000a, 0x000000008000000a
	.quad	0x000000008000808b, 0x000000008000808b, 0x000000008000808b, 0x000000008000808b
	.quad	0x800000000000008b, 0x800000000000008b, 0x800000000000008b, 0x800000000000008b
	.quad	0x8000000000008089, 0x8000000000008089, 0x8000000000008089, 0x8000000000008089
	.quad	0x8000000000008003, 0x8000000000008003, 0x8000000000008003, 0x8000000000008003
	.quad	0x8000000000008002, 0x8000000000008002, 0x8000000000008002, 0x8000000000008002
	.quad	0x8000000000000080, 0x8000000000000080, 0x8000000000000080, 0x8000000000000080
	.quad	0x000000000000800a, 0x000000000000800a, 0x000000000000800a, 0x000000000000800a
	.quad	0x800000008000000a, 0x800000008000000a, 0x800000008000000a, 0x800000008000000a
	.quad	0x8000000080008081, 0x8000000080008081, 0x8000000080008081, 0x8000000080008081
	.quad	0x8000000000008080, 0x8000000000008080, 0x8000000000008080, 0x8000000000008080
	.quad	0x0000000080000001, 0x0000000080000001, 0x0000000080000001, 0x0000000080000001
	.quad	0x8000000080008008, 0x8000000080008008, 0x8000000080008008, 0x8000000080008008

.asciz	"Keccak-1600 absorb and squeeze for AVX2, CRYPTOGAMS by <appro@openssl.org>"
