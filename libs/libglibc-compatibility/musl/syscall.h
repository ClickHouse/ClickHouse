#pragma once

typedef long syscall_arg_t;

__attribute__((visibility("hidden")))
long __syscall_ret(unsigned long);

__attribute__((visibility("hidden")))
long __syscall(syscall_arg_t, ...);
