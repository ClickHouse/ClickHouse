#ifndef LOCK_H
#define LOCK_H

hidden void __lock(volatile int *);
hidden void __unlock(volatile int *);
#define LOCK(x) __lock(x)
#define UNLOCK(x) __unlock(x)

#endif
