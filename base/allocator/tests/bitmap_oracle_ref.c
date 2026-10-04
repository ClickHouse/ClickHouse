/* The reference: jemalloc's own bitmap (`bitmap.h` inline functions and `bitmap.c`). */

#include "jemalloc/internal/jemalloc_preamble.h"
#include "jemalloc/internal/bitmap.h"

static bitmap_info_t ref_info;

static void info_fields(const bitmap_info_t * info, size_t * out, int all_levels)
{
    out[0] = info->nbits;
#ifdef BITMAP_USE_TREE
    out[1] = info->nlevels;
    unsigned nlevels = all_levels ? BITMAP_MAX_LEVELS : info->nlevels;
    for (unsigned l = 0; l <= nlevels; l++)
        out[2 + l] = info->levels[l].group_offset;
#else
    (void)all_levels;
    out[1] = info->ngroups;
#endif
}

int ref_bitmap_use_tree(void)
{
#ifdef BITMAP_USE_TREE
    return 1;
#else
    return 0;
#endif
}

size_t ref_bitmap_groups_max(void)
{
    return BITMAP_GROUPS_MAX;
}

size_t ref_bitmap_maxbits(void)
{
    return BITMAP_MAXBITS;
}

/* Fields of BITMAP_INFO_INITIALIZER(nbits) (all levels). */
void ref_bitmap_info_initializer(size_t nbits, size_t * out)
{
    bitmap_info_t info = BITMAP_INFO_INITIALIZER(nbits);
    info_fields(&info, out, 1);
}

/* Fields of bitmap_info_init (only the used levels). */
void ref_bitmap_info_init(size_t nbits, size_t * out)
{
    bitmap_info_t info;
    bitmap_info_init(&info, nbits);
    info_fields(&info, out, 0);
}

/* Selects the bitmap info used by the following operations. */
void ref_bitmap_select(size_t nbits)
{
    bitmap_info_t info = BITMAP_INFO_INITIALIZER(nbits);
    ref_info = info;
}

size_t ref_bitmap_size(void)
{
    return bitmap_size(&ref_info);
}

void ref_bitmap_init(bitmap_t * bitmap, bool fill)
{
    bitmap_init(bitmap, &ref_info, fill);
}

bool ref_bitmap_full(bitmap_t * bitmap)
{
    return bitmap_full(bitmap, &ref_info);
}

bool ref_bitmap_get(bitmap_t * bitmap, size_t bit)
{
    return bitmap_get(bitmap, &ref_info, bit);
}

void ref_bitmap_set(bitmap_t * bitmap, size_t bit)
{
    bitmap_set(bitmap, &ref_info, bit);
}

void ref_bitmap_unset(bitmap_t * bitmap, size_t bit)
{
    bitmap_unset(bitmap, &ref_info, bit);
}

size_t ref_bitmap_sfu(bitmap_t * bitmap)
{
    return bitmap_sfu(bitmap, &ref_info);
}

size_t ref_bitmap_ffu(bitmap_t * bitmap, size_t min_bit)
{
    return bitmap_ffu(bitmap, &ref_info, min_bit);
}
