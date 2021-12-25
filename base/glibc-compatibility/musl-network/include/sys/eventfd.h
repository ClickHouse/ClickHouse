#ifndef _SYS_EVENTFD_H
#define _SYS_EVENTFD_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <fcntl.h>

typedef uint64_t eventfd_t;

#define EFD_SEMAPHORE 1
#define EFD_CLOEXEC O_CLOEXEC
#define EFD_NONBLOCK O_NONBLOCK

int eventfd(unsigned int, int);
int eventfd_read(int, eventfd_t *);
int eventfd_write(int, eventfd_t);


#ifdef __cplusplus
}
#endif

#endif /* sys/eventfd.h */
