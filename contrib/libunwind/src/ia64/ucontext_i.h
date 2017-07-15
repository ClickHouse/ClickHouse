/* Copyright (C) 2002 Hewlett-Packard Co.
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

/* Constants shared between setcontext() and getcontext().  Don't
   install this header file.  */

#define SIG_BLOCK       0
#define SIG_UNBLOCK     1
#define SIG_SETMASK     2

#define IA64_SC_FLAG_SYNCHRONOUS_BIT    63

#define SC_FLAGS 0x000
#define SC_NAT  0x008
#define SC_BSP  0x048
#define SC_RNAT 0x050
#define SC_UNAT 0x060
#define SC_FPSR 0x068
#define SC_PFS  0x070
#define SC_LC   0x078
#define SC_PR   0x080
#define SC_BR   0x088
#define SC_GR   0x0c8
#define SC_FR   0x1d0
#define SC_MASK 0x9d0


#define rTMP    r10
#define rPOS    r11
#define rCPOS   r14
#define rNAT    r15
#define rFLAGS  r16

#define rB5     r18
#define rB4     r19
#define rB3     r20
#define rB2     r21
#define rB1     r22
#define rB0     r23
#define rRSC    r24
#define rBSP    r25
#define rRNAT   r26
#define rUNAT   r27
#define rFPSR   r28
#define rPFS    r29
#define rLC     r30
#define rPR     r31
