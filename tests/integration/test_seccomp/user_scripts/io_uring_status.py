#!/usr/bin/python3

# Reports what a process the server started gets from the three `io_uring` system calls, as a single
# row of three fields, each `0` or the name of the errno:
#  - `io_uring_setup` of a one-entry ring with zeroed parameters, which a kernel with `io_uring`
#    makes (the ring is closed at once);
#  - `io_uring_enter` and `io_uring_register` on a descriptor that is not open, which such a kernel
#    rejects with `EBADF`. Not -1: `io_uring_register` takes that for a registration without a ring.
# A filter that refuses the calls answers `ENOSYS` to all three, before the kernel looks at the
# arguments. A kernel built without `io_uring` answers `ENOSYS` too, so on such a host the
# assertions for the nodes that allow the calls fail rather than pass by accident.

import ctypes
import errno
import os

# The same numbers on every architecture.
IO_URING_SETUP = 425
IO_URING_ENTER = 426
IO_URING_REGISTER = 427
# `sizeof(struct io_uring_params)`.
IO_URING_PARAMS_SIZE = 120
NOT_A_DESCRIPTOR = 2**31 - 1

libc = ctypes.CDLL(None, use_errno=True)
libc.syscall.restype = ctypes.c_long


def syscall_result(number, *arguments):
    ctypes.set_errno(0)
    result = libc.syscall(ctypes.c_long(number), *arguments)
    if result == -1:
        return None, errno.errorcode.get(ctypes.get_errno(), str(ctypes.get_errno()))
    return result, "0"


if __name__ == "__main__":
    params = ctypes.create_string_buffer(IO_URING_PARAMS_SIZE)
    ring, setup_result = syscall_result(IO_URING_SETUP, ctypes.c_uint(1), params)
    if ring is not None:
        os.close(ring)

    _, enter_result = syscall_result(
        IO_URING_ENTER,
        ctypes.c_int(NOT_A_DESCRIPTOR),
        ctypes.c_uint(0),
        ctypes.c_uint(0),
        ctypes.c_uint(0),
        ctypes.c_void_p(None),
        ctypes.c_size_t(0),
    )
    _, register_result = syscall_result(
        IO_URING_REGISTER,
        ctypes.c_int(NOT_A_DESCRIPTOR),
        ctypes.c_uint(0),
        ctypes.c_void_p(None),
        ctypes.c_uint(0),
    )

    print("\t".join([setup_result, enter_result, register_result]))
