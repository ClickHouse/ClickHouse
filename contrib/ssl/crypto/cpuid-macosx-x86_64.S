#include "x86_arch.h"

.private_extern	_OPENSSL_cpuid_setup
.mod_init_func
	.p2align	3
	.quad	_OPENSSL_cpuid_setup


.private_extern	_OPENSSL_ia32cap_P

.text	

.globl	_OPENSSL_atomic_add

.p2align	4
_OPENSSL_atomic_add:
	movl	(%rdi),%eax
L$spin:	leaq	(%rsi,%rax,1),%r8
.byte	0xf0		
	cmpxchgl	%r8d,(%rdi)
	jne	L$spin
	movl	%r8d,%eax
.byte	0x48,0x98	
	.byte	0xf3,0xc3


.globl	_OPENSSL_ia32_cpuid

.p2align	4
_OPENSSL_ia32_cpuid:
	movq	%rbx,%r8

	xorl	%eax,%eax
	cpuid
	movl	%eax,%r11d

	xorl	%eax,%eax
	cmpl	$1970169159,%ebx
	setne	%al
	movl	%eax,%r9d
	cmpl	$1231384169,%edx
	setne	%al
	orl	%eax,%r9d
	cmpl	$1818588270,%ecx
	setne	%al
	orl	%eax,%r9d
	jz	L$intel

	cmpl	$1752462657,%ebx
	setne	%al
	movl	%eax,%r10d
	cmpl	$1769238117,%edx
	setne	%al
	orl	%eax,%r10d
	cmpl	$1145913699,%ecx
	setne	%al
	orl	%eax,%r10d
	jnz	L$intel


	movl	$2147483648,%eax
	cpuid
	cmpl	$2147483649,%eax
	jb	L$intel
	movl	%eax,%r10d
	movl	$2147483649,%eax
	cpuid
	andl	$IA32CAP_MASK1_AMD_XOP,%r9d
	orl	$1,%r9d

	cmpl	$2147483656,%r10d
	jb	L$intel

	movl	$2147483656,%eax
	cpuid
	movzbq	%cl,%r10
	incq	%r10

	movl	$1,%eax
	cpuid
	btl	$IA32CAP_BIT0_HT,%edx
	jnc	L$generic
	shrl	$16,%ebx
	cmpb	%r10b,%bl
	ja	L$generic
	xorl	$IA32CAP_MASK0_HT,%edx
	jmp	L$generic

L$intel:
	cmpl	$4,%r11d
	movl	$-1,%r10d
	jb	L$nocacheinfo

	movl	$4,%eax
	movl	$0,%ecx
	cpuid
	movl	%eax,%r10d
	shrl	$14,%r10d
	andl	$4095,%r10d

L$nocacheinfo:
	movl	$1,%eax
	cpuid

	andl	$(~(IA32CAP_MASK0_INTELP4 | IA32CAP_MASK0_INTEL)),%edx
	cmpl	$0,%r9d
	jne	L$notintel

	orl	$IA32CAP_MASK0_INTEL,%edx
	andb	$15,%ah
	cmpb	$15,%ah
	jne	L$notintel

	orl	$IA32CAP_MASK0_INTELP4,%edx
L$notintel:
	btl	$IA32CAP_BIT0_HT,%edx
	jnc	L$generic
	xorl	$IA32CAP_MASK0_HT,%edx
	cmpl	$0,%r10d
	je	L$generic

	orl	$IA32CAP_MASK0_HT,%edx
	shrl	$16,%ebx
	cmpb	$1,%bl
	ja	L$generic
	xorl	$IA32CAP_MASK0_HT,%edx

L$generic:
	andl	$IA32CAP_MASK1_AMD_XOP,%r9d
	andl	$(~IA32CAP_MASK1_AMD_XOP),%ecx
	orl	%ecx,%r9d

	movl	%edx,%r10d
	btl	$IA32CAP_BIT1_OSXSAVE,%r9d
	jnc	L$clear_avx
	xorl	%ecx,%ecx
.byte	0x0f,0x01,0xd0		
	andl	$6,%eax
	cmpl	$6,%eax
	je	L$done
L$clear_avx:
	movl	$(~(IA32CAP_MASK1_AVX | IA32CAP_MASK1_FMA3 | IA32CAP_MASK1_AMD_XOP)),%eax
	andl	%eax,%r9d
L$done:
	shlq	$32,%r9
	movl	%r10d,%eax
	movq	%r8,%rbx
	orq	%r9,%rax
	.byte	0xf3,0xc3

.globl	_OPENSSL_wipe_cpu

.p2align	4
_OPENSSL_wipe_cpu:
	pxor	%xmm0,%xmm0
	pxor	%xmm1,%xmm1
	pxor	%xmm2,%xmm2
	pxor	%xmm3,%xmm3
	pxor	%xmm4,%xmm4
	pxor	%xmm5,%xmm5
	pxor	%xmm6,%xmm6
	pxor	%xmm7,%xmm7
	pxor	%xmm8,%xmm8
	pxor	%xmm9,%xmm9
	pxor	%xmm10,%xmm10
	pxor	%xmm11,%xmm11
	pxor	%xmm12,%xmm12
	pxor	%xmm13,%xmm13
	pxor	%xmm14,%xmm14
	pxor	%xmm15,%xmm15
	xorq	%rcx,%rcx
	xorq	%rdx,%rdx
	xorq	%rsi,%rsi
	xorq	%rdi,%rdi
	xorq	%r8,%r8
	xorq	%r9,%r9
	xorq	%r10,%r10
	xorq	%r11,%r11
	leaq	8(%rsp),%rax
	.byte	0xf3,0xc3

