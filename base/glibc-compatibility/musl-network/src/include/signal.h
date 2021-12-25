#ifndef SIGNAL_H
#define SIGNAL_H

#include "../../include/signal.h"

hidden int __sigaction(int, const struct sigaction *, struct sigaction *);

hidden void __block_all_sigs(void *);
hidden void __block_app_sigs(void *);
hidden void __restore_sigs(void *);

hidden void __get_handler_set(sigset_t *);

#endif
