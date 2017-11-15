/* Copyright (C) 2004 Hewlett-Packard Co.
     Contributed by David Mosberger-Tang <davidm@hpl.hp.com>.

This file is part of libunwind.

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.  */

#if defined __linux__
#define UC_MCONTEXT_GREGS_R8    0x28
#define UC_MCONTEXT_GREGS_R9    0x30
#define UC_MCONTEXT_GREGS_R10   0x38
#define UC_MCONTEXT_GREGS_R11   0x40
#define UC_MCONTEXT_GREGS_R12   0x48
#define UC_MCONTEXT_GREGS_R13   0x50
#define UC_MCONTEXT_GREGS_R14   0x58
#define UC_MCONTEXT_GREGS_R15   0x60
#define UC_MCONTEXT_GREGS_RDI   0x68
#define UC_MCONTEXT_GREGS_RSI   0x70
#define UC_MCONTEXT_GREGS_RBP   0x78
#define UC_MCONTEXT_GREGS_RBX   0x80
#define UC_MCONTEXT_GREGS_RDX   0x88
#define UC_MCONTEXT_GREGS_RAX   0x90
#define UC_MCONTEXT_GREGS_RCX   0x98
#define UC_MCONTEXT_GREGS_RSP   0xa0
#define UC_MCONTEXT_GREGS_RIP   0xa8
#define UC_MCONTEXT_FPREGS_PTR  0x1a8
#define UC_MCONTEXT_FPREGS_MEM  0xe0
#define UC_SIGMASK              0x128
#define FPREGS_OFFSET_MXCSR     0x18
#elif defined __FreeBSD__
#define UC_SIGMASK              0x0
#define UC_MCONTEXT_GREGS_RDI   0x18
#define UC_MCONTEXT_GREGS_RSI   0x20
#define UC_MCONTEXT_GREGS_RDX   0x28
#define UC_MCONTEXT_GREGS_RCX   0x30
#define UC_MCONTEXT_GREGS_R8    0x38
#define UC_MCONTEXT_GREGS_R9    0x40
#define UC_MCONTEXT_GREGS_RAX   0x48
#define UC_MCONTEXT_GREGS_RBX   0x50
#define UC_MCONTEXT_GREGS_RBP   0x58
#define UC_MCONTEXT_GREGS_R10   0x60
#define UC_MCONTEXT_GREGS_R11   0x68
#define UC_MCONTEXT_GREGS_R12   0x70
#define UC_MCONTEXT_GREGS_R13   0x78
#define UC_MCONTEXT_GREGS_R14   0x80
#define UC_MCONTEXT_GREGS_R15   0x88
#define UC_MCONTEXT_FS          0x94
#define UC_MCONTEXT_GS          0x96
#define UC_MCONTEXT_FLAGS       0xa0
#define UC_MCONTEXT_ES          0xa4
#define UC_MCONTEXT_DS          0xa6
#define UC_MCONTEXT_GREGS_RIP   0xb0
#define UC_MCONTEXT_CS          0xb8
#define UC_MCONTEXT_RFLAGS      0xc0
#define UC_MCONTEXT_GREGS_RSP   0xc8
#define UC_MCONTEXT_SS          0xd0
#define UC_MCONTEXT_MC_LEN      0xd8
#define UC_MCONTEXT_FPFORMAT    0xe0
#define UC_MCONTEXT_OWNEDFP     0xe8
#define UC_MCONTEXT_FPSTATE     0xf0
#define UC_MCONTEXT_FPOWNED_FPU 0x20001
#define UC_MCONTEXT_FPFMT_XMM   0x10002
#define UC_MCONTEXT_MC_LEN_VAL  0x320

#endif
