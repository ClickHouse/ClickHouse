#!/usr/bin/env python3

# Runs a command under a seccomp filter that refuses the `seccomp` system call with `EPERM` and
# allows everything else, the way a restrictive container runtime would.

import ctypes
import os
import platform
import sys

PR_SET_NO_NEW_PRIVS = 38
PR_SET_SECCOMP = 22
SECCOMP_MODE_FILTER = 2
SECCOMP_RET_ALLOW = 0x7FFF0000
SECCOMP_RET_ERRNO = 0x00050000
EPERM = 1

BPF_LD_W_ABS = 0x20
BPF_JMP_JEQ_K = 0x15
BPF_RET_K = 0x06

SECCOMP_SYSCALL_NUMBER = {"x86_64": 317, "aarch64": 277}[platform.machine()]


class SockFilter(ctypes.Structure):
    _fields_ = [
        ("code", ctypes.c_uint16),
        ("jt", ctypes.c_uint8),
        ("jf", ctypes.c_uint8),
        ("k", ctypes.c_uint32),
    ]


class SockFprog(ctypes.Structure):
    _fields_ = [("len", ctypes.c_uint16), ("filter", ctypes.POINTER(SockFilter))]


program = (SockFilter * 4)(
    # The system call number is the first field of `struct seccomp_data`.
    SockFilter(BPF_LD_W_ABS, 0, 0, 0),
    SockFilter(BPF_JMP_JEQ_K, 0, 1, SECCOMP_SYSCALL_NUMBER),
    SockFilter(BPF_RET_K, 0, 0, SECCOMP_RET_ERRNO | EPERM),
    SockFilter(BPF_RET_K, 0, 0, SECCOMP_RET_ALLOW),
)
fprog = SockFprog(len(program), program)

libc = ctypes.CDLL(None, use_errno=True)
if libc.prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0:
    raise OSError(ctypes.get_errno(), "prctl(PR_SET_NO_NEW_PRIVS)")
if libc.prctl(PR_SET_SECCOMP, SECCOMP_MODE_FILTER, ctypes.byref(fprog), 0, 0) != 0:
    raise OSError(ctypes.get_errno(), "prctl(PR_SET_SECCOMP)")

os.execv(sys.argv[1], sys.argv[1:])
