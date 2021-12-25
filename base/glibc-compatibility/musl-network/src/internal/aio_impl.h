#ifndef AIO_IMPL_H
#define AIO_IMPL_H

extern hidden volatile int __aio_fut;

extern hidden int __aio_close(int);
extern hidden void __aio_atfork(int);

#endif
