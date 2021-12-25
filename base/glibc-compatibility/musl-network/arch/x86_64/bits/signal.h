#if defined(_POSIX_SOURCE) || defined(_POSIX_C_SOURCE) \
 || defined(_XOPEN_SOURCE) || defined(_GNU_SOURCE) || defined(_BSD_SOURCE)

#if defined(_XOPEN_SOURCE) || defined(_GNU_SOURCE) || defined(_BSD_SOURCE)
#define MINSIGSTKSZ 2048
#define SIGSTKSZ 8192
#endif

#ifdef _GNU_SOURCE
enum { REG_R8 = 0 };
#define REG_R8 REG_R8
enum { REG_R9 = 1 };
#define REG_R9 REG_R9
enum { REG_R10 = 2 };
#define REG_R10 REG_R10
enum { REG_R11 = 3 };
#define REG_R11 REG_R11
enum { REG_R12 = 4 };
#define REG_R12 REG_R12
enum { REG_R13 = 5 };
#define REG_R13 REG_R13
enum { REG_R14 = 6 };
#define REG_R14 REG_R14
enum { REG_R15 = 7 };
#define REG_R15 REG_R15
enum { REG_RDI = 8 };
#define REG_RDI REG_RDI
enum { REG_RSI = 9 };
#define REG_RSI REG_RSI
enum { REG_RBP = 10 };
#define REG_RBP REG_RBP
enum { REG_RBX = 11 };
#define REG_RBX REG_RBX
enum { REG_RDX = 12 };
#define REG_RDX REG_RDX
enum { REG_RAX = 13 };
#define REG_RAX REG_RAX
enum { REG_RCX = 14 };
#define REG_RCX REG_RCX
enum { REG_RSP = 15 };
#define REG_RSP REG_RSP
enum { REG_RIP = 16 };
#define REG_RIP REG_RIP
enum { REG_EFL = 17 };
#define REG_EFL REG_EFL
enum { REG_CSGSFS = 18 };
#define REG_CSGSFS REG_CSGSFS
enum { REG_ERR = 19 };
#define REG_ERR REG_ERR
enum { REG_TRAPNO = 20 };
#define REG_TRAPNO REG_TRAPNO
enum { REG_OLDMASK = 21 };
#define REG_OLDMASK REG_OLDMASK
enum { REG_CR2 = 22 };
#define REG_CR2 REG_CR2
#endif

#if defined(_GNU_SOURCE) || defined(_BSD_SOURCE)
typedef long long greg_t, gregset_t[23];
typedef struct _fpstate {
	unsigned short cwd, swd, ftw, fop;
	unsigned long long rip, rdp;
	unsigned mxcsr, mxcr_mask;
	struct {
		unsigned short significand[4], exponent, padding[3];
	} _st[8];
	struct {
		unsigned element[4];
	} _xmm[16];
	unsigned padding[24];
} *fpregset_t;
struct sigcontext {
	unsigned long r8, r9, r10, r11, r12, r13, r14, r15;
	unsigned long rdi, rsi, rbp, rbx, rdx, rax, rcx, rsp, rip, eflags;
	unsigned short cs, gs, fs, __pad0;
	unsigned long err, trapno, oldmask, cr2;
	struct _fpstate *fpstate;
	unsigned long __reserved1[8];
};
typedef struct {
	gregset_t gregs;
	fpregset_t fpregs;
	unsigned long long __reserved1[8];
} mcontext_t;
#else
typedef struct {
	unsigned long __space[32];
} mcontext_t;
#endif

struct sigaltstack {
	void *ss_sp;
	int ss_flags;
	size_t ss_size;
};

typedef struct __ucontext {
	unsigned long uc_flags;
	struct __ucontext *uc_link;
	stack_t uc_stack;
	mcontext_t uc_mcontext;
	sigset_t uc_sigmask;
	unsigned long __fpregs_mem[64];
} ucontext_t;

#define SA_NOCLDSTOP  1
#define SA_NOCLDWAIT  2
#define SA_SIGINFO    4
#define SA_ONSTACK    0x08000000
#define SA_RESTART    0x10000000
#define SA_NODEFER    0x40000000
#define SA_RESETHAND  0x80000000
#define SA_RESTORER   0x04000000

#endif

#define SIGHUP    1
#define SIGINT    2
#define SIGQUIT   3
#define SIGILL    4
#define SIGTRAP   5
#define SIGABRT   6
#define SIGIOT    SIGABRT
#define SIGBUS    7
#define SIGFPE    8
#define SIGKILL   9
#define SIGUSR1   10
#define SIGSEGV   11
#define SIGUSR2   12
#define SIGPIPE   13
#define SIGALRM   14
#define SIGTERM   15
#define SIGSTKFLT 16
#define SIGCHLD   17
#define SIGCONT   18
#define SIGSTOP   19
#define SIGTSTP   20
#define SIGTTIN   21
#define SIGTTOU   22
#define SIGURG    23
#define SIGXCPU   24
#define SIGXFSZ   25
#define SIGVTALRM 26
#define SIGPROF   27
#define SIGWINCH  28
#define SIGIO     29
#define SIGPOLL   29
#define SIGPWR    30
#define SIGSYS    31
#define SIGUNUSED SIGSYS

#define _NSIG 65

