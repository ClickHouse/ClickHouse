#!/usr/bin/python3

# Reports what a process the server started sees of the policy, as a single row of ten fields:
#  - the seccomp mode it inherited across `fork` and `execve`: 0 is no filter, 2 is a BPF filter;
#  - what `getxattr`, which the policy does not allow, does here. Without a filter the attribute is
#    simply missing (`ENODATA`); under a filter the call is refused with `ENOSYS`, whatever the
#    mode, so that the NSS modules of the host, which probe for it, carry on;
#  - what a `clone` that asks for a user namespace does. The policy refuses it by its flags, so the
#    `errno` mode answers `EPERM`. The flags also name a combination the kernel rejects on its own
#    with `EINVAL`, which is what makes the probe safe to run: no process is created either way;
#  - what `clone3` does. Its arguments are in a structure a filter cannot read, so the call is
#    refused as a whole, with `ENOSYS`. Without a filter the kernel gets to look at that structure,
#    and the null pointer passed here gives `EFAULT`;
#  - whether making a thread still works, which is the point of answering `ENOSYS` above rather
#    than `EPERM`: the libc has to fall back to `clone`;
#  - what `mknod` of a character device does. The policy refuses it by its file type, so the `errno`
#    mode answers `EPERM`. The path is in a directory that does not exist, which is what makes the
#    probe safe to run and tells the two answers apart: without a filter the kernel looks the path
#    up before it checks for `CAP_MKNOD`, and answers `ENOENT`;
#  - whether `mkfifo`, which is `mknod` of a FIFO, still works: executable user defined functions
#    pass their arguments through FIFOs;
#  - what `socket` of a packet socket does, in both of its spellings, joined by a comma: `AF_PACKET`,
#    and the obsolete `AF_INET` of the type `SOCK_PACKET`, which the kernel turns into `AF_PACKET`.
#    The policy refuses both with `EAFNOSUPPORT`, whatever the mode. Without a filter the kernel
#    makes the socket (`0`) for a process with `CAP_NET_RAW`, and answers `EPERM` otherwise;
#  - the same two through `socketpair`. Without a filter the kernel answers `EOPNOTSUPP`, because
#    packet sockets do not pair, or `EPERM` without `CAP_NET_RAW`;
#  - whether the families the server uses still work: `OK`, or the family and the error of the first
#    one that does not.
#
# With the argument `sockets`, only the last three fields are printed, so that the probe can run in
# the `trap` mode, where the `clone` and `mknod` probes would take it down with `SIGSYS`.

import ctypes
import errno
import os
import platform
import shutil
import socket
import stat
import sys
import tempfile
import threading

CLONE_FS = 0x00000200
CLONE_NEWUSER = 0x10000000

# `clone` is numbered per architecture. `clone3` is new enough to have the same number everywhere.
CLONE_SYSCALL_NUMBERS = {"x86_64": 56, "aarch64": 220}
CLONE3_SYSCALL_NUMBER = 435
# `sizeof(struct clone_args)` of the kernel that introduced `clone3`. Anything smaller than that is
# rejected with `EINVAL` before the structure itself is read.
CLONE_ARGS_SIZE = 64
# The obsolete packet socket type, for which Python has no constant.
SOCK_PACKET = 10


def syscall_result(number, *arguments):
    libc = ctypes.CDLL(None, use_errno=True)
    libc.syscall.restype = ctypes.c_long
    libc.syscall.argtypes = [ctypes.c_long] * (1 + len(arguments))
    ctypes.set_errno(0)
    if libc.syscall(number, *arguments) != -1:
        return "0"
    return errno.errorcode.get(ctypes.get_errno(), str(ctypes.get_errno()))


def socket_result(make):
    try:
        made = make()
    except OSError as e:
        return errno.errorcode.get(e.errno, str(e.errno))
    for s in made if isinstance(made, tuple) else (made,):
        s.close()
    return "0"


def socket_fields():
    packet_socket_result = ",".join(
        socket_result(lambda: socket.socket(family, kind))
        for family, kind in [
            (socket.AF_PACKET, socket.SOCK_RAW),
            (socket.AF_INET, SOCK_PACKET),
        ]
    )
    packet_socketpair_result = ",".join(
        socket_result(lambda: socket.socketpair(family, kind))
        for family, kind in [
            (socket.AF_PACKET, socket.SOCK_DGRAM),
            (socket.AF_INET, SOCK_PACKET),
        ]
    )

    used_families_result = "OK"
    for name, make in [
        ("AF_UNIX", lambda: socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)),
        ("AF_INET", lambda: socket.socket(socket.AF_INET, socket.SOCK_STREAM)),
        ("AF_NETLINK", lambda: socket.socket(socket.AF_NETLINK, socket.SOCK_RAW, 0)),
        (
            "AF_UNIX_pair",
            lambda: socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM),
        ),
    ]:
        result = socket_result(make)
        if result != "0":
            used_families_result = f"{name}:{result}"
            break

    return [packet_socket_result, packet_socketpair_result, used_families_result]


if __name__ == "__main__":
    if sys.argv[1:] == ["sockets"]:
        print("\t".join(socket_fields()))
        sys.exit(0)

    mode = "unknown"
    with open("/proc/self/status", "r") as status:
        for line in status:
            if line.startswith("Seccomp:"):
                mode = line.split(":", 1)[1].strip()

    try:
        os.getxattr("/etc/hostname", "user.nonexistent")
        getxattr_result = "0"
    except OSError as e:
        getxattr_result = errno.errorcode.get(e.errno, str(e.errno))

    clone_result = syscall_result(
        CLONE_SYSCALL_NUMBERS[platform.machine()], CLONE_NEWUSER | CLONE_FS, 0, 0, 0, 0
    )
    clone3_result = syscall_result(CLONE3_SYSCALL_NUMBER, 0, CLONE_ARGS_SIZE)

    try:
        thread = threading.Thread(target=lambda: None)
        thread.start()
        thread.join()
        thread_result = "OK"
    except RuntimeError as e:
        # One field, so that the row still parses and the assertion is the one that fails.
        thread_result = str(e).replace(" ", "_")

    try:
        os.mknod(
            "/nonexistent_seccomp_probe/null", stat.S_IFCHR | 0o600, os.makedev(1, 3)
        )
        mknod_device_result = "0"
    except OSError as e:
        mknod_device_result = errno.errorcode.get(e.errno, str(e.errno))

    directory = tempfile.mkdtemp()
    try:
        os.mkfifo(os.path.join(directory, "fifo"))
        mkfifo_result = "0"
    except OSError as e:
        mkfifo_result = errno.errorcode.get(e.errno, str(e.errno))
    finally:
        shutil.rmtree(directory)

    print(
        "\t".join(
            [
                mode,
                getxattr_result,
                clone_result,
                clone3_result,
                thread_result,
                mknod_device_result,
                mkfifo_result,
            ]
            + socket_fields()
        )
    )
