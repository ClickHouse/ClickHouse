/* libunwind - a platform-independent unwind library
   Copyright (C) 2002-2004 Hewlett-Packard Co
        Contributed by David Mosberger-Tang <davidm@hpl.hp.com>

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

/* This file defines the runtime-support routines for dynamically
generated code.  Even though it is implemented as part of libunwind,
it is logically separate from the interface to perform the actual
unwinding.  In particular, this interface is always used in the
context of the unwind target, whereas the rest of the unwind API is
used in context of the process that is doing the unwind (which may be
a debugger running on another machine, for example).

Note that the data-structures declared here server a dual purpose:
when a program registers a dynamically generated procedure, it uses
these structures directly.  On the other hand, with remote-unwinding,
the data-structures are read from the remote process's memory and
translated into internalized versions.  To facilitate remote-access,
the following rules should be followed in declaring these structures:

 (1) Declare a member as a pointer only if the the information the
     member points to needs to be internalized as well (e.g., a
     string representing a procedure name should be declared as
     "const char *", but the instruction pointer should be declared
     as unw_word_t).

 (2) Provide sufficient padding to ensure that no implicit padding
     will be needed on any of the supported target architectures.  For
     the time being, padding data structures with the assumption that
     sizeof (unw_word_t) == 8 should be sufficient.  (Note: it's not
     impossible to internalize structures with internal padding, but
     it does make the process a bit harder).

 (3) Don't declare members that contain bitfields or floating-point
     values.

 (4) Don't declare members with enumeration types.  Declare them as
     int32_t instead.  */

typedef enum
  {
    UNW_DYN_STOP = 0,           /* end-of-unwind-info marker */
    UNW_DYN_SAVE_REG,           /* save register to another register */
    UNW_DYN_SPILL_FP_REL,       /* frame-pointer-relative register spill */
    UNW_DYN_SPILL_SP_REL,       /* stack-pointer-relative register spill */
    UNW_DYN_ADD,                /* add constant value to a register */
    UNW_DYN_POP_FRAMES,         /* drop one or more stack frames */
    UNW_DYN_LABEL_STATE,        /* name the current state */
    UNW_DYN_COPY_STATE,         /* set the region's entry-state */
    UNW_DYN_ALIAS               /* get unwind info from an alias */
  }
unw_dyn_operation_t;

typedef enum
  {
    UNW_INFO_FORMAT_DYNAMIC,            /* unw_dyn_proc_info_t */
    UNW_INFO_FORMAT_TABLE,              /* unw_dyn_table_t */
    UNW_INFO_FORMAT_REMOTE_TABLE,       /* unw_dyn_remote_table_t */
    UNW_INFO_FORMAT_ARM_EXIDX,          /* ARM specific unwind info */
    UNW_INFO_FORMAT_IP_OFFSET,          /* Like UNW_INFO_FORMAT_REMOTE_TABLE, but
                                           table entries are considered
                                           relative to di->start_ip, rather
                                           than di->segbase */
  }
unw_dyn_info_format_t;

typedef struct unw_dyn_op
  {
    int8_t tag;                         /* what operation? */
    int8_t qp;                          /* qualifying predicate register */
    int16_t reg;                        /* what register */
    int32_t when;                       /* when does it take effect? */
    unw_word_t val;                     /* auxiliary value */
  }
unw_dyn_op_t;

typedef struct unw_dyn_region_info
  {
    struct unw_dyn_region_info *next;   /* linked list of regions */
    int32_t insn_count;                 /* region length (# of instructions) */
    uint32_t op_count;                  /* length of op-array */
    unw_dyn_op_t op[1];                 /* variable-length op-array */
  }
unw_dyn_region_info_t;

typedef struct unw_dyn_proc_info
  {
    unw_word_t name_ptr;        /* address of human-readable procedure name */
    unw_word_t handler;         /* address of personality routine */
    uint32_t flags;
    int32_t pad0;
    unw_dyn_region_info_t *regions;
  }
unw_dyn_proc_info_t;

typedef struct unw_dyn_table_info
  {
    unw_word_t name_ptr;        /* addr. of table name (e.g., library name) */
    unw_word_t segbase;         /* segment base */
    unw_word_t table_len;       /* must be a multiple of sizeof(unw_word_t)! */
    unw_word_t *table_data;
  }
unw_dyn_table_info_t;

typedef struct unw_dyn_remote_table_info
  {
    unw_word_t name_ptr;        /* addr. of table name (e.g., library name) */
    unw_word_t segbase;         /* segment base */
    unw_word_t table_len;       /* must be a multiple of sizeof(unw_word_t)! */
    unw_word_t table_data;
  }
unw_dyn_remote_table_info_t;

typedef struct unw_dyn_info
  {
    /* doubly-linked list of dyn-info structures: */
    struct unw_dyn_info *next;
    struct unw_dyn_info *prev;
    unw_word_t start_ip;        /* first IP covered by this entry */
    unw_word_t end_ip;          /* first IP NOT covered by this entry */
    unw_word_t gp;              /* global-pointer in effect for this entry */
    int32_t format;             /* real type: unw_dyn_info_format_t */
    int32_t pad;
    union
      {
        unw_dyn_proc_info_t pi;
        unw_dyn_table_info_t ti;
        unw_dyn_remote_table_info_t rti;
      }
    u;
  }
unw_dyn_info_t;

typedef struct unw_dyn_info_list
  {
    uint32_t version;
    uint32_t generation;
    unw_dyn_info_t *first;
  }
unw_dyn_info_list_t;

/* Return the size (in bytes) of an unw_dyn_region_info_t structure that can
   hold OP_COUNT ops.  */
#define _U_dyn_region_info_size(op_count)                               \
        ((char *) (((unw_dyn_region_info_t *) NULL)->op + (op_count))   \
         - (char *) NULL)

/* Register the unwind info for a single procedure.
   This routine is NOT signal-safe.  */
extern void _U_dyn_register (unw_dyn_info_t *);

/* Cancel the unwind info for a single procedure.
   This routine is NOT signal-safe.  */
extern void _U_dyn_cancel (unw_dyn_info_t *);


/* Convenience routines.  */

#define _U_dyn_op(_tag, _qp, _when, _reg, _val)                         \
        ((unw_dyn_op_t) { (_tag), (_qp), (_reg), (_when), (_val) })

#define _U_dyn_op_save_reg(op, qp, when, reg, dst)                      \
        (*(op) = _U_dyn_op (UNW_DYN_SAVE_REG, (qp), (when), (reg), (dst)))

#define _U_dyn_op_spill_fp_rel(op, qp, when, reg, offset)               \
        (*(op) = _U_dyn_op (UNW_DYN_SPILL_FP_REL, (qp), (when), (reg),  \
                            (offset)))

#define _U_dyn_op_spill_sp_rel(op, qp, when, reg, offset)               \
        (*(op) = _U_dyn_op (UNW_DYN_SPILL_SP_REL, (qp), (when), (reg),  \
                            (offset)))

#define _U_dyn_op_add(op, qp, when, reg, value)                         \
        (*(op) = _U_dyn_op (UNW_DYN_ADD, (qp), (when), (reg), (value)))

#define _U_dyn_op_pop_frames(op, qp, when, num_frames)                  \
        (*(op) = _U_dyn_op (UNW_DYN_POP_FRAMES, (qp), (when), 0, (num_frames)))

#define _U_dyn_op_label_state(op, label)                                \
        (*(op) = _U_dyn_op (UNW_DYN_LABEL_STATE, _U_QP_TRUE, -1, 0, (label)))

#define _U_dyn_op_copy_state(op, label)                                 \
        (*(op) = _U_dyn_op (UNW_DYN_COPY_STATE, _U_QP_TRUE, -1, 0, (label)))

#define _U_dyn_op_alias(op, qp, when, addr)                             \
        (*(op) = _U_dyn_op (UNW_DYN_ALIAS, (qp), (when), 0, (addr)))

#define _U_dyn_op_stop(op)                                              \
        (*(op) = _U_dyn_op (UNW_DYN_STOP, _U_QP_TRUE, -1, 0, 0))

/* The target-dependent qualifying predicate which is always TRUE.  On
   IA-64, that's p0 (0), on non-predicated architectures, the value is
   ignored.  */
#define _U_QP_TRUE      _U_TDEP_QP_TRUE
