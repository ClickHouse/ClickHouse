/*
  Copyright 2011, 2012 Kristian Nielsen and Monty Program Ab
            2016 MariaDB Corporation AB

  This file is free software; you can redistribute it and/or
  modify it under the terms of the GNU Lesser General Public
  License as published by the Free Software Foundation; either
  version 2.1 of the License, or (at your option) any later version.

  This library is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  Lesser General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this.  If not, see <http://www.gnu.org/licenses/>.
*/

/*
  Implementation of async context spawning using Posix ucontext and
  swapcontext().
*/

#include "ma_global.h"
#include "ma_string.h"
#include "ma_context.h"

#ifdef HAVE_VALGRIND
#include <valgrind/valgrind.h>
#endif

#ifdef MY_CONTEXT_USE_UCONTEXT
/*
  The makecontext() only allows to pass integers into the created context :-(
  We want to pass pointers, so we do it this kinda hackish way.
  Anyway, it should work everywhere, and at least it does not break strict
  aliasing.
*/
union pass_void_ptr_as_2_int {
  int a[2];
  void *p;
};

/*
  We use old-style function definition here, as this is passed to
  makecontext(). And the type of the makecontext() argument does not match
  the actual type (as the actual type can differ from call to call).
*/
static void
my_context_spawn_internal(i0, i1)
int i0, i1;
{
  int err;
  struct my_context *c;
  union pass_void_ptr_as_2_int u;

  u.a[0]= i0;
  u.a[1]= i1;
  c= (struct my_context *)u.p;

  (*c->user_func)(c->user_data);
  c->active= 0;
  err= setcontext(&c->base_context);
  fprintf(stderr, "Aieie, setcontext() failed: %d (errno=%d)\n", err, errno);
}


int
my_context_continue(struct my_context *c)
{
  int err;

  if (!c->active)
    return 0;

  err= swapcontext(&c->base_context, &c->spawned_context);
  if (err)
  {
    fprintf(stderr, "Aieie, swapcontext() failed: %d (errno=%d)\n",
            err, errno);
    return -1;
  }

  return c->active;
}


int
my_context_spawn(struct my_context *c, void (*f)(void *), void *d)
{
  int err;
  union pass_void_ptr_as_2_int u;

  err= getcontext(&c->spawned_context);
  if (err)
    return -1;
  c->spawned_context.uc_stack.ss_sp= c->stack;
  c->spawned_context.uc_stack.ss_size= c->stack_size;
  c->spawned_context.uc_link= NULL;
  c->user_func= f;
  c->user_data= d;
  c->active= 1;
  u.p= c;
  makecontext(&c->spawned_context, my_context_spawn_internal, 2,
              u.a[0], u.a[1]);

  return my_context_continue(c);
}


int
my_context_yield(struct my_context *c)
{
  int err;

  if (!c->active)
    return -1;

  err= swapcontext(&c->spawned_context, &c->base_context);
  if (err)
    return -1;
  return 0;
}

int
my_context_init(struct my_context *c, size_t stack_size)
{
#if SIZEOF_CHARP > SIZEOF_INT*2
#error Error: Unable to store pointer in 2 ints on this architecture
#endif

  memset(c, 0, sizeof(*c));
  if (!(c->stack= malloc(stack_size)))
    return -1;                                  /* Out of memory */
  c->stack_size= stack_size;
#ifdef HAVE_VALGRIND
  c->valgrind_stack_id=
    VALGRIND_STACK_REGISTER(c->stack, ((unsigned char *)(c->stack))+stack_size);
#endif
  return 0;
}

void
my_context_destroy(struct my_context *c)
{
  if (c->stack)
  {
#ifdef HAVE_VALGRIND
    VALGRIND_STACK_DEREGISTER(c->valgrind_stack_id);
#endif
    free(c->stack);
  }
}

#endif  /* MY_CONTEXT_USE_UCONTEXT */


#ifdef MY_CONTEXT_USE_X86_64_GCC_ASM
/*
  GCC-amd64 implementation of my_context.

  This is slightly optimized in the common case where we never yield
  (eg. fetch next row and it is already fully received in buffer). In this
  case we do not need to restore registers at return (though we still need to
  save them as we cannot know if we will yield or not in advance).
*/

#include <stdint.h>
#include <stdlib.h>

/*
  Layout of saved registers etc.
  Since this is accessed through gcc inline assembler, it is simpler to just
  use numbers than to try to define nice constants or structs.

   0    0   %rsp
   1    8   %rbp
   2   16   %rbx
   3   24   %r12
   4   32   %r13
   5   40   %r14
   6   48   %r15
   7   56   %rip for done
   8   64   %rip for yield/continue
*/

int
my_context_spawn(struct my_context *c, void (*f)(void *), void *d)
{
  int ret;

  /*
    There are 6 callee-save registers we need to save and restore when
    suspending and continuing, plus stack pointer %rsp and instruction pointer
    %rip.

    However, if we never suspend, the user-supplied function will in any case
    restore the 6 callee-save registers, so we can avoid restoring them in
    this case.
  */
  __asm__ __volatile__
    (
     "movq %%rsp, (%[save])\n\t"
     "movq %[stack], %%rsp\n\t"
#if __GNUC__ >= 4 && __GNUC_MINOR__ >= 4 && !defined(__INTEL_COMPILER)
     /*
       This emits a DWARF DW_CFA_undefined directive to make the return address
       undefined. This indicates that this is the top of the stack frame, and
       helps tools that use DWARF stack unwinding to obtain stack traces.
       (I use numeric constant to avoid a dependency on libdwarf includes).
     */
     ".cfi_escape 0x07, 16\n\t"
#endif
     "movq %%rbp, 8(%[save])\n\t"
     "movq %%rbx, 16(%[save])\n\t"
     "movq %%r12, 24(%[save])\n\t"
     "movq %%r13, 32(%[save])\n\t"
     "movq %%r14, 40(%[save])\n\t"
     "movq %%r15, 48(%[save])\n\t"
     "leaq 1f(%%rip), %%rax\n\t"
     "leaq 2f(%%rip), %%rcx\n\t"
     "movq %%rax, 56(%[save])\n\t"
     "movq %%rcx, 64(%[save])\n\t"
     /*
       Constraint below puts the argument to the user function into %rdi, as
       needed for the calling convention.
     */
     "callq *%[f]\n\t"
     "jmpq *56(%[save])\n"
     /*
       Come here when operation is done.
       We do not need to restore callee-save registers, as the called function
       will do this for us if needed.
     */
     "1:\n\t"
     "movq (%[save]), %%rsp\n\t"
     "xorl %[ret], %[ret]\n\t"
     "jmp 3f\n"
     /* Come here when operation was suspended. */
     "2:\n\t"
     "movl $1, %[ret]\n"
     "3:\n"
     : [ret] "=a" (ret),
       [f] "+S" (f),
       /* Need this in %rdi to follow calling convention. */
       [d] "+D" (d)
     : [stack] "a" (c->stack_top),
       /* Need this in callee-save register to preserve in function call. */
       [save] "b" (&c->save[0])
     : "rcx", "rdx", "r8", "r9", "r10", "r11", "memory", "cc"
  );

  return ret;
}

int
my_context_continue(struct my_context *c)
{
  int ret;

  __asm__ __volatile__
    (
     "movq (%[save]), %%rax\n\t"
     "movq %%rsp, (%[save])\n\t"
     "movq %%rax, %%rsp\n\t"
     "movq 8(%[save]), %%rax\n\t"
     "movq %%rbp, 8(%[save])\n\t"
     "movq %%rax, %%rbp\n\t"
     "movq 24(%[save]), %%rax\n\t"
     "movq %%r12, 24(%[save])\n\t"
     "movq %%rax, %%r12\n\t"
     "movq 32(%[save]), %%rax\n\t"
     "movq %%r13, 32(%[save])\n\t"
     "movq %%rax, %%r13\n\t"
     "movq 40(%[save]), %%rax\n\t"
     "movq %%r14, 40(%[save])\n\t"
     "movq %%rax, %%r14\n\t"
     "movq 48(%[save]), %%rax\n\t"
     "movq %%r15, 48(%[save])\n\t"
     "movq %%rax, %%r15\n\t"

     "leaq 1f(%%rip), %%rax\n\t"
     "leaq 2f(%%rip), %%rcx\n\t"
     "movq %%rax, 56(%[save])\n\t"
     "movq 64(%[save]), %%rax\n\t"
     "movq %%rcx, 64(%[save])\n\t"

     "movq 16(%[save]), %%rcx\n\t"
     "movq %%rbx, 16(%[save])\n\t"
     "movq %%rcx, %%rbx\n\t"

     "jmpq *%%rax\n"
     /*
       Come here when operation is done.
       Be sure to use the same callee-save register for %[save] here and in
       my_context_spawn(), so we preserve the value correctly at this point.
     */
     "1:\n\t"
     "movq (%[save]), %%rsp\n\t"
     "movq 8(%[save]), %%rbp\n\t"
     /* %rbx is preserved from my_context_spawn() in this case. */
     "movq 24(%[save]), %%r12\n\t"
     "movq 32(%[save]), %%r13\n\t"
     "movq 40(%[save]), %%r14\n\t"
     "movq 48(%[save]), %%r15\n\t"
     "xorl %[ret], %[ret]\n\t"
     "jmp 3f\n"
     /* Come here when operation is suspended. */
     "2:\n\t"
     "movl $1, %[ret]\n"
     "3:\n"
     : [ret] "=a" (ret)
     : /* Need this in callee-save register to preserve in function call. */
       [save] "b" (&c->save[0])
     : "rcx", "rdx", "rsi", "rdi", "r8", "r9", "r10", "r11", "memory", "cc"
        );

  return ret;
}

int
my_context_yield(struct my_context *c)
{
  uint64_t *save= &c->save[0];
  __asm__ __volatile__
    (
     "movq (%[save]), %%rax\n\t"
     "movq %%rsp, (%[save])\n\t"
     "movq %%rax, %%rsp\n\t"
     "movq 8(%[save]), %%rax\n\t"
     "movq %%rbp, 8(%[save])\n\t"
     "movq %%rax, %%rbp\n\t"
     "movq 16(%[save]), %%rax\n\t"
     "movq %%rbx, 16(%[save])\n\t"
     "movq %%rax, %%rbx\n\t"
     "movq 24(%[save]), %%rax\n\t"
     "movq %%r12, 24(%[save])\n\t"
     "movq %%rax, %%r12\n\t"
     "movq 32(%[save]), %%rax\n\t"
     "movq %%r13, 32(%[save])\n\t"
     "movq %%rax, %%r13\n\t"
     "movq 40(%[save]), %%rax\n\t"
     "movq %%r14, 40(%[save])\n\t"
     "movq %%rax, %%r14\n\t"
     "movq 48(%[save]), %%rax\n\t"
     "movq %%r15, 48(%[save])\n\t"
     "movq %%rax, %%r15\n\t"
     "movq 64(%[save]), %%rax\n\t"
     "leaq 1f(%%rip), %%rcx\n\t"
     "movq %%rcx, 64(%[save])\n\t"

     "jmpq *%%rax\n"

     "1:\n"
     : [save] "+D" (save)
     :
     : "rax", "rcx", "rdx", "rsi", "r8", "r9", "r10", "r11", "memory", "cc"
     );
  return 0;
}

int
my_context_init(struct my_context *c, size_t stack_size)
{
  memset(c, 0, sizeof(*c));

  if (!(c->stack_bot= malloc(stack_size)))
    return -1;                                  /* Out of memory */
  /*
    The x86_64 ABI specifies 16-byte stack alignment.
    Also put two zero words at the top of the stack.
  */
  c->stack_top= (void *)
    (( ((intptr)c->stack_bot + stack_size) & ~(intptr)0xf) - 16);
  memset(c->stack_top, 0, 16);

#ifdef HAVE_VALGRIND
  c->valgrind_stack_id=
    VALGRIND_STACK_REGISTER(c->stack_bot, c->stack_top);
#endif
  return 0;
}

void
my_context_destroy(struct my_context *c)
{
  if (c->stack_bot)
  {
    free(c->stack_bot);
#ifdef HAVE_VALGRIND
    VALGRIND_STACK_DEREGISTER(c->valgrind_stack_id);
#endif
  }
}

#endif  /* MY_CONTEXT_USE_X86_64_GCC_ASM */


#ifdef MY_CONTEXT_USE_I386_GCC_ASM
/*
  GCC-i386 implementation of my_context.

  This is slightly optimized in the common case where we never yield
  (eg. fetch next row and it is already fully received in buffer). In this
  case we do not need to restore registers at return (though we still need to
  save them as we cannot know if we will yield or not in advance).
*/

#include <stdint.h>
#include <stdlib.h>

/*
  Layout of saved registers etc.
  Since this is accessed through gcc inline assembler, it is simpler to just
  use numbers than to try to define nice constants or structs.

   0    0   %esp
   1    4   %ebp
   2    8   %ebx
   3   12   %esi
   4   16   %edi
   5   20   %eip for done
   6   24   %eip for yield/continue
*/

int
my_context_spawn(struct my_context *c, void (*f)(void *), void *d)
{
  int ret;

  /*
    There are 4 callee-save registers we need to save and restore when
    suspending and continuing, plus stack pointer %esp and instruction pointer
    %eip.

    However, if we never suspend, the user-supplied function will in any case
    restore the 4 callee-save registers, so we can avoid restoring them in
    this case.
  */
  __asm__ __volatile__
    (
     "movl %%esp, (%[save])\n\t"
     "movl %[stack], %%esp\n\t"
#if __GNUC__ >= 4 && __GNUC_MINOR__ >= 4 && !defined(__INTEL_COMPILER)
     /*
       This emits a DWARF DW_CFA_undefined directive to make the return address
       undefined. This indicates that this is the top of the stack frame, and
       helps tools that use DWARF stack unwinding to obtain stack traces.
       (I use numeric constant to avoid a dependency on libdwarf includes).
     */
     ".cfi_escape 0x07, 8\n\t"
#endif
     /* Push the parameter on the stack. */
     "pushl %[d]\n\t"
     "movl %%ebp, 4(%[save])\n\t"
     "movl %%ebx, 8(%[save])\n\t"
     "movl %%esi, 12(%[save])\n\t"
     "movl %%edi, 16(%[save])\n\t"
     /* Get label addresses in -fPIC-compatible way (no pc-relative on 32bit) */
     "call 1f\n"
     "1:\n\t"
     "popl %%eax\n\t"
     "addl $(2f-1b), %%eax\n\t"
     "movl %%eax, 20(%[save])\n\t"
     "addl $(3f-2f), %%eax\n\t"
     "movl %%eax, 24(%[save])\n\t"
     "call *%[f]\n\t"
     "jmp *20(%[save])\n"
     /*
       Come here when operation is done.
       We do not need to restore callee-save registers, as the called function
       will do this for us if needed.
     */
     "2:\n\t"
     "movl (%[save]), %%esp\n\t"
     "xorl %[ret], %[ret]\n\t"
     "jmp 4f\n"
     /* Come here when operation was suspended. */
     "3:\n\t"
     "movl $1, %[ret]\n"
     "4:\n"
     : [ret] "=a" (ret),
       [f] "+c" (f),
       [d] "+d" (d)
     : [stack] "a" (c->stack_top),
       /* Need this in callee-save register to preserve across function call. */
       [save] "D" (&c->save[0])
     : "memory", "cc"
  );

  return ret;
}

int
my_context_continue(struct my_context *c)
{
  int ret;

  __asm__ __volatile__
    (
     "movl (%[save]), %%eax\n\t"
     "movl %%esp, (%[save])\n\t"
     "movl %%eax, %%esp\n\t"
     "movl 4(%[save]), %%eax\n\t"
     "movl %%ebp, 4(%[save])\n\t"
     "movl %%eax, %%ebp\n\t"
     "movl 8(%[save]), %%eax\n\t"
     "movl %%ebx, 8(%[save])\n\t"
     "movl %%eax, %%ebx\n\t"
     "movl 12(%[save]), %%eax\n\t"
     "movl %%esi, 12(%[save])\n\t"
     "movl %%eax, %%esi\n\t"

     "movl 24(%[save]), %%eax\n\t"
     "call 1f\n"
     "1:\n\t"
     "popl %%ecx\n\t"
     "addl $(2f-1b), %%ecx\n\t"
     "movl %%ecx, 20(%[save])\n\t"
     "addl $(3f-2f), %%ecx\n\t"
     "movl %%ecx, 24(%[save])\n\t"

     /* Must restore %edi last as it is also our %[save] register. */
     "movl 16(%[save]), %%ecx\n\t"
     "movl %%edi, 16(%[save])\n\t"
     "movl %%ecx, %%edi\n\t"

     "jmp *%%eax\n"
     /*
       Come here when operation is done.
       Be sure to use the same callee-save register for %[save] here and in
       my_context_spawn(), so we preserve the value correctly at this point.
     */
     "2:\n\t"
     "movl (%[save]), %%esp\n\t"
     "movl 4(%[save]), %%ebp\n\t"
     "movl 8(%[save]), %%ebx\n\t"
     "movl 12(%[save]), %%esi\n\t"
     "movl 16(%[save]), %%edi\n\t"
     "xorl %[ret], %[ret]\n\t"
     "jmp 4f\n"
     /* Come here when operation is suspended. */
     "3:\n\t"
     "movl $1, %[ret]\n"
     "4:\n"
     : [ret] "=a" (ret)
     : /* Need this in callee-save register to preserve in function call. */
       [save] "D" (&c->save[0])
     : "ecx", "edx", "memory", "cc"
        );

  return ret;
}

int
my_context_yield(struct my_context *c)
{
  uint64_t *save= &c->save[0];
  __asm__ __volatile__
    (
     "movl (%[save]), %%eax\n\t"
     "movl %%esp, (%[save])\n\t"
     "movl %%eax, %%esp\n\t"
     "movl 4(%[save]), %%eax\n\t"
     "movl %%ebp, 4(%[save])\n\t"
     "movl %%eax, %%ebp\n\t"
     "movl 8(%[save]), %%eax\n\t"
     "movl %%ebx, 8(%[save])\n\t"
     "movl %%eax, %%ebx\n\t"
     "movl 12(%[save]), %%eax\n\t"
     "movl %%esi, 12(%[save])\n\t"
     "movl %%eax, %%esi\n\t"
     "movl 16(%[save]), %%eax\n\t"
     "movl %%edi, 16(%[save])\n\t"
     "movl %%eax, %%edi\n\t"

     "movl 24(%[save]), %%eax\n\t"
     "call 1f\n"
     "1:\n\t"
     "popl %%ecx\n\t"
     "addl $(2f-1b), %%ecx\n\t"
     "movl %%ecx, 24(%[save])\n\t"

     "jmp *%%eax\n"

     "2:\n"
     : [save] "+d" (save)
     :
     : "eax", "ecx", "memory", "cc"
     );
  return 0;
}

int
my_context_init(struct my_context *c, size_t stack_size)
{
  memset(c, 0, sizeof(*c));
  if (!(c->stack_bot= malloc(stack_size)))
    return -1;                                  /* Out of memory */
  c->stack_top= (void *)
    (( ((intptr)c->stack_bot + stack_size) & ~(intptr)0xf) - 16);
  memset(c->stack_top, 0, 16);

#ifdef HAVE_VALGRIND
  c->valgrind_stack_id=
    VALGRIND_STACK_REGISTER(c->stack_bot, c->stack_top);
#endif
  return 0;
}

void
my_context_destroy(struct my_context *c)
{
  if (c->stack_bot)
  {
    free(c->stack_bot);
#ifdef HAVE_VALGRIND
    VALGRIND_STACK_DEREGISTER(c->valgrind_stack_id);
#endif
  }
}

#endif  /* MY_CONTEXT_USE_I386_GCC_ASM */


#ifdef MY_CONTEXT_USE_WIN32_FIBERS
int
my_context_yield(struct my_context *c)
{
  c->return_value= 1;
  SwitchToFiber(c->app_fiber);
  return 0;
}


static void WINAPI
my_context_trampoline(void *p)
{
  struct my_context *c= (struct my_context *)p;
  /*
    Reuse the Fiber by looping infinitely, each time we are scheduled we
    spawn the appropriate function and switch back when it is done.

    This way we avoid the overhead of CreateFiber() for every asynchroneous
    operation.
  */
  for(;;)
  {
    (*(c->user_func))(c->user_arg);
    c->return_value= 0;
    SwitchToFiber(c->app_fiber);
  }
}

int
my_context_init(struct my_context *c, size_t stack_size)
{
  memset(c, 0, sizeof(*c));
  c->lib_fiber= CreateFiber(stack_size, my_context_trampoline, c);
  if (c->lib_fiber)
    return 0;
  return -1;
}

void
my_context_destroy(struct my_context *c)
{
  if (c->lib_fiber)
  {
    DeleteFiber(c->lib_fiber);
    c->lib_fiber= NULL;
  }
}

int
my_context_spawn(struct my_context *c, void (*f)(void *), void *d)
{
  c->user_func= f;
  c->user_arg= d;
  return my_context_continue(c);
}

int
my_context_continue(struct my_context *c)
{
  void *current_fiber=  IsThreadAFiber() ? GetCurrentFiber() : ConvertThreadToFiber(c);
  c->app_fiber= current_fiber;
  SwitchToFiber(c->lib_fiber);
  return c->return_value;
}

#endif  /* MY_CONTEXT_USE_WIN32_FIBERS */

#ifdef MY_CONTEXT_DISABLE
int
my_context_continue(struct my_context *c)
{
  return -1;
}


int
my_context_spawn(struct my_context *c, void (*f)(void *), void *d)
{
  return -1;
}


int
my_context_yield(struct my_context *c)
{
  return -1;
}

int
my_context_init(struct my_context *c, size_t stack_size)
{
  return -1;                                  /* Out of memory */
}

void
my_context_destroy(struct my_context *c)
{
}

#endif
