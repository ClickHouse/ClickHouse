#include <features.h>

struct k_sigaction {
	void (*handler)(int);
	unsigned long flags;
	void (*restorer)(void);
	unsigned mask[2];
};

hidden void __restore_rt();
#define __restore __restore_rt
