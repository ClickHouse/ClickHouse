#!/usr/bin/python3

# Reports what `perf_event_open` does in a process the server started, as a single field: `0` if it
# opened a software event counting this thread's own CPU time, or the name of the error. A kernel
# with performance events answers `0`, or `EACCES` or `EPERM` where `perf_event_paranoid` forbids
# the event, but never `ENOSYS`: that is the answer of the seccomp policy when the
# `seccomp_allow_perf_events` server setting is `false`.

import ctypes
import errno
import os
import platform
import struct

PERF_EVENT_OPEN_SYSCALL_NUMBERS = {"x86_64": 298, "aarch64": 241}
PERF_TYPE_SOFTWARE = 1
PERF_COUNT_SW_TASK_CLOCK = 1
# `PERF_ATTR_SIZE_VER0`, the size of the first `struct perf_event_attr`, which every kernel accepts.
PERF_ATTR_SIZE = 64
# `disabled`, `exclude_kernel` and `exclude_hv`, in the bit field at offset 40.
PERF_ATTR_FLAGS = 1 << 0 | 1 << 5 | 1 << 6

if __name__ == "__main__":
    attr = ctypes.create_string_buffer(PERF_ATTR_SIZE)
    struct.pack_into("=II", attr, 0, PERF_TYPE_SOFTWARE, PERF_ATTR_SIZE)
    struct.pack_into("=Q", attr, 8, PERF_COUNT_SW_TASK_CLOCK)
    struct.pack_into("=Q", attr, 40, PERF_ATTR_FLAGS)

    libc = ctypes.CDLL(None, use_errno=True)
    libc.syscall.restype = ctypes.c_long
    libc.syscall.argtypes = [
        ctypes.c_long,
        ctypes.c_void_p,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_ulong,
    ]
    ctypes.set_errno(0)
    # This thread, on any CPU, in no group.
    fd = libc.syscall(
        PERF_EVENT_OPEN_SYSCALL_NUMBERS[platform.machine()],
        ctypes.addressof(attr),
        0,
        -1,
        -1,
        0,
    )
    if fd != -1:
        os.close(fd)
        print("0")
    else:
        print(errno.errorcode.get(ctypes.get_errno(), str(ctypes.get_errno())))
