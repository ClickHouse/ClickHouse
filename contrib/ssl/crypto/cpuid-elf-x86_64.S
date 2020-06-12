#include "x86_arch.h"

.hidden	OPENSSL_cpuid_setup
.section	.init
	call	OPENSSL_cpuid_setup


.hidden	OPENSSL_ia32cap_P

.text	

.globl	OPENSSL_atomic_add
.type	OPENSSL_atomic_add,@function
.align	16
OPENSSL_atomic_add:
	movl	(%rdi),%eax
.Lspin:	leaq	(%rsi,%rax,1),%r8
.byte	0xf0		
	cmpxchgl	%r8d,(%rdi)
	jne	.Lspin
	movl	%r8d,%eax
.byte	0x48,0x98	
	.byte	0xf3,0xc3
.size	OPENSSL_atomic_add,.-OPENSSL_atomic_add

.globl	OPENSSL_ia32_cpuid
.type	OPENSSL_ia32_cpuid,@function
.align	16
OPENSSL_ia32_cpuid:
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
	jz	.Lintel

	cmpl	$1752462657,%ebx
	setne	%al
	movl	%eax,%r10d
	cmpl	$1769238117,%edx
	setne	%al
	orl	%eax,%r10d
	cmpl	$1145913699,%ecx
	setne	%al
	orl	%eax,%r10d
	jnz	.Lintel


	movl	$2147483648,%eax
	cpuid
	cmpl	$2147483649,%eax
	jb	.Lintel
	movl	%eax,%r10d
	movl	$2147483649,%eax
	cpuid
	andl	$IA32CAP_MASK1_AMD_XOP,%r9d
	orl	$1,%r9d

	cmpl	$2147483656,%r10d
	jb	.Lintel

	movl	$2147483656,%eax
	cpuid
	movzbq	%cl,%r10
	incq	%r10

	movl	$1,%eax
	cpuid
	btl	$IA32CAP_BIT0_HT,%edx
	jnc	.Lgeneric
	shrl	$16,%ebx
	cmpb	%r10b,%bl
	ja	.Lgeneric
	xorl	$IA32CAP_MASK0_HT,%edx
	jmp	.Lgeneric

.Lintel:
	cmpl	$4,%r11d
	movl	$-1,%r10d
	jb	.Lnocacheinfo

	movl	$4,%eax
	movl	$0,%ecx
	cpuid
	movl	%eax,%r10d
	shrl	$14,%r10d
	andl	$4095,%r10d

.Lnocacheinfo:
	movl	$1,%eax
	cpuid

	andl	$(~(IA32CAP_MASK0_INTELP4 | IA32CAP_MASK0_INTEL)),%edx
	cmpl	$0,%r9d
	jne	.Lnotintel

	orl	$IA32CAP_MASK0_INTEL,%edx
	andb	$15,%ah
	cmpb	$15,%ah
	jne	.Lnotintel

	orl	$IA32CAP_MASK0_INTELP4,%edx
.Lnotintel:
	btl	$IA32CAP_BIT0_HT,%edx
	jnc	.Lgeneric
	xorl	$IA32CAP_MASK0_HT,%edx
	cmpl	$0,%r10d
	je	.Lgeneric

	orl	$IA32CAP_MASK0_HT,%edx
	shrl	$16,%ebx
	cmpb	$1,%bl
	ja	.Lgeneric
	xorl	$IA32CAP_MASK0_HT,%edx

.Lgeneric:
	andl	$IA32CAP_MASK1_AMD_XOP,%r9d
	andl	$(~IA32CAP_MASK1_AMD_XOP),%ecx
	orl	%ecx,%r9d

	movl	%edx,%r10d
	btl	$IA32CAP_BIT1_OSXSAVE,%r9d
	jnc	.Lclear_avx
	xorl	%ecx,%ecx
.byte	0x0f,0x01,0xd0		
	andl	$6,%eax
	cmpl	$6,%eax
	je	.Ldone
.Lclear_avx:
	movl	$(~(IA32CAP_MASK1_AVX | IA32CAP_MASK1_FMA3 | IA32CAP_MASK1_AMD_XOP)),%eax
	andl	%eax,%r9d
.Ldone:
	shlq	$32,%r9
	movl	%r10d,%eax
	movq	%r8,%rbx
	orq	%r9,%rax
	.byte	0xf3,0xc3
.size	OPENSSL_ia32_cpuid,.-OPENSSL_ia32_cpuid
.globl	OPENSSL_wipe_cpu
.type	OPENSSL_wipe_cpu,@function
.align	16
OPENSSL_wipe_cpu:
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
.size	OPENSSL_wipe_cpu,.-OPENSSL_wipe_cpu
#if defined(HAVE_GNU_STACK)
.section .note.GNU-stack,"",%progbits
#endif
