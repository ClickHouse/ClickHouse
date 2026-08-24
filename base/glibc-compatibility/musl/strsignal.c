#include <signal.h>
#include <string.h>

#if (SIGHUP == 1) && (SIGINT == 2) && (SIGQUIT == 3) && (SIGILL == 4) \
 && (SIGTRAP == 5) && (SIGABRT == 6) && (SIGBUS == 7) && (SIGFPE == 8) \
 && (SIGKILL == 9) && (SIGUSR1 == 10) && (SIGSEGV == 11) && (SIGUSR2 == 12) \
 && (SIGPIPE == 13) && (SIGALRM == 14) && (SIGTERM == 15) && (SIGSTKFLT == 16) \
 && (SIGCHLD == 17) && (SIGCONT == 18) && (SIGSTOP == 19) && (SIGTSTP == 20) \
 && (SIGTTIN == 21) && (SIGTTOU == 22) && (SIGURG == 23) && (SIGXCPU == 24) \
 && (SIGXFSZ == 25) && (SIGVTALRM == 26) && (SIGPROF == 27) && (SIGWINCH == 28) \
 && (SIGPOLL == 29) && (SIGPWR == 30) && (SIGSYS == 31)

#define sigmap(x) x

#else

static const char map[] = {
	[SIGHUP]    = 1,
	[SIGINT]    = 2,
	[SIGQUIT]   = 3,
	[SIGILL]    = 4,
	[SIGTRAP]   = 5,
	[SIGABRT]   = 6,
	[SIGBUS]    = 7,
	[SIGFPE]    = 8,
	[SIGKILL]   = 9,
	[SIGUSR1]   = 10,
	[SIGSEGV]   = 11,
	[SIGUSR2]   = 12,
	[SIGPIPE]   = 13,
	[SIGALRM]   = 14,
	[SIGTERM]   = 15,
#if defined(SIGSTKFLT)
	[SIGSTKFLT] = 16,
#elif defined(SIGEMT)
	[SIGEMT]    = 16,
#endif
	[SIGCHLD]   = 17,
	[SIGCONT]   = 18,
	[SIGSTOP]   = 19,
	[SIGTSTP]   = 20,
	[SIGTTIN]   = 21,
	[SIGTTOU]   = 22,
	[SIGURG]    = 23,
	[SIGXCPU]   = 24,
	[SIGXFSZ]   = 25,
	[SIGVTALRM] = 26,
	[SIGPROF]   = 27,
	[SIGWINCH]  = 28,
	[SIGPOLL]   = 29,
	[SIGPWR]    = 30,
	[SIGSYS]    = 31
};

#define sigmap(x) ((x) >= sizeof map ? (x) : map[(x)])

#endif

static const char strings[] =
	"Unknown signal\0"
	"Hangup\0"
	"Interrupt\0"
	"Quit\0"
	"Illegal instruction\0"
	"Trace/breakpoint trap\0"
	"Aborted\0"
	"Bus error\0"
	"Arithmetic exception\0"
	"Killed\0"
	"User defined signal 1\0"
	"Segmentation fault\0"
	"User defined signal 2\0"
	"Broken pipe\0"
	"Alarm clock\0"
	"Terminated\0"
#if defined(SIGSTKFLT)
	"Stack fault\0"
#elif defined(SIGEMT)
	"Emulator trap\0"
#else
	"Unknown signal\0"
#endif
	"Child process status\0"
	"Continued\0"
	"Stopped (signal)\0"
	"Stopped\0"
	"Stopped (tty input)\0"
	"Stopped (tty output)\0"
	"Urgent I/O condition\0"
	"CPU time limit exceeded\0"
	"File size limit exceeded\0"
	"Virtual timer expired\0"
	"Profiling timer expired\0"
	"Window changed\0"
	"I/O possible\0"
	"Power failure\0"
	"Bad system call\0"
	"RT32"
	"\0RT33\0RT34\0RT35\0RT36\0RT37\0RT38\0RT39\0RT40"
	"\0RT41\0RT42\0RT43\0RT44\0RT45\0RT46\0RT47\0RT48"
	"\0RT49\0RT50\0RT51\0RT52\0RT53\0RT54\0RT55\0RT56"
	"\0RT57\0RT58\0RT59\0RT60\0RT61\0RT62\0RT63\0RT64"
#if _NSIG > 65
	"\0RT65\0RT66\0RT67\0RT68\0RT69\0RT70\0RT71\0RT72"
	"\0RT73\0RT74\0RT75\0RT76\0RT77\0RT78\0RT79\0RT80"
	"\0RT81\0RT82\0RT83\0RT84\0RT85\0RT86\0RT87\0RT88"
	"\0RT89\0RT90\0RT91\0RT92\0RT93\0RT94\0RT95\0RT96"
	"\0RT97\0RT98\0RT99\0RT100\0RT101\0RT102\0RT103\0RT104"
	"\0RT105\0RT106\0RT107\0RT108\0RT109\0RT110\0RT111\0RT112"
	"\0RT113\0RT114\0RT115\0RT116\0RT117\0RT118\0RT119\0RT120"
	"\0RT121\0RT122\0RT123\0RT124\0RT125\0RT126\0RT127\0RT128"
#endif
	"";

char *strsignal(int signum)
{
	const char *s = strings;

	signum = sigmap(signum);
	if (signum - 1U >= _NSIG-1) signum = 0;

	for (; signum--; s++) for (; *s; s++);

	return (char *)s;
}
