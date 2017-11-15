/*
 * Copyright (C) 1998, 1999, 2002, 2003, 2005 Hewlett-Packard Co
 *      David Mosberger-Tang <davidm@hpl.hp.com>
 *
 * Register stack engine related helper functions.  This file may be
 * used in applications, so be careful about the name-space and give
 * some consideration to non-GNU C compilers (though __inline__ is
 * fine).
 */
#ifndef RSE_H
#define RSE_H

#include <libunwind.h>

static inline uint64_t
rse_slot_num (uint64_t addr)
{
        return (addr >> 3) & 0x3f;
}

/*
 * Return TRUE if ADDR is the address of an RNAT slot.
 */
static inline uint64_t
rse_is_rnat_slot (uint64_t addr)
{
        return rse_slot_num (addr) == 0x3f;
}

/*
 * Returns the address of the RNAT slot that covers the slot at
 * address SLOT_ADDR.
 */
static inline uint64_t
rse_rnat_addr (uint64_t slot_addr)
{
        return slot_addr | (0x3f << 3);
}

/*
 * Calculate the number of registers in the dirty partition starting at
 * BSPSTORE and ending at BSP.  This isn't simply (BSP-BSPSTORE)/8
 * because every 64th slot stores ar.rnat.
 */
static inline uint64_t
rse_num_regs (uint64_t bspstore, uint64_t bsp)
{
        uint64_t slots = (bsp - bspstore) >> 3;

        return slots - (rse_slot_num(bspstore) + slots)/0x40;
}

/*
 * The inverse of the above: given bspstore and the number of
 * registers, calculate ar.bsp.
 */
static inline uint64_t
rse_skip_regs (uint64_t addr, long num_regs)
{
        long delta = rse_slot_num(addr) + num_regs;

        if (num_regs < 0)
                delta -= 0x3e;
        return addr + ((num_regs + delta/0x3f) << 3);
}

#endif /* RSE_H */
