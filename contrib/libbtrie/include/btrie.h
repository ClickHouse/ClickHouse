#pragma once

#if defined (__cplusplus)
extern "C" {
#endif

#include <stdlib.h>
#include <stdint.h>

/**
 * In btrie, each leaf means one bit in ip tree.
 * Left means 0, and right means 1.
 */

#define BTRIE_NULL   (uintptr_t) -1
#define MAX_PAGES    1024 * 16

typedef struct btrie_node_s btrie_node_t;

struct btrie_node_s {
    btrie_node_t  *right;
    btrie_node_t  *left;
    btrie_node_t  *parent;
    uintptr_t         value;
};


typedef struct btrie_s {
    btrie_node_t  *root;

    btrie_node_t  *free;    /* free list of btrie */
    char             *start;
    size_t            size;

    /*
     * memory pool.
     * memory management(esp free) will be so easy by using this facility.
     */
    char             *pools[MAX_PAGES];
    size_t            len;
} btrie_t;


/**
 * Create an empty btrie
 *
 * @Return:
 * An ip radix_tree created.
 * NULL if creation failed.
 */

btrie_t *btrie_create();

/**
 * Destroy the ip radix_tree
 *
 * @Return:
 * OK if deletion succeed.
 * ERROR if error occurs while deleting.
 */
int btrie_destroy(btrie_t *tree);

/**
 * Count the nodes in the radix tree.
 */
size_t btrie_count(btrie_t *tree);

/**
 * Return the allocated number of bytes.
 */
size_t btrie_allocated(btrie_t *tree);


/**
 * Add an ipv4 into btrie
 *
 * @Args:
 * key: ip address
 * mask: key's mask
 * value: value of this IP, may be NULL.
 *
 * @Return:
 * OK for success.
 * ERROR for failure.
 */
int btrie_insert(btrie_t *tree, uint32_t key, uint32_t mask,
    uintptr_t value);


/**
 * Delete an ipv4 from btrie
 *
 * @Args:
 *
 * @Return:
 * OK for success.
 * ERROR for failure.
 */
int btrie_delete(btrie_t *tree, uint32_t key, uint32_t mask);


/**
 * Find an ipv4 from btrie
 *

 * @Args:
 *
 * @Return:
 * Value if succeed.
 * NULL if failed.
 */
uintptr_t btrie_find(btrie_t *tree, uint32_t key);


/**
 * Add an ipv6 into btrie
 *
 * @Args:
 * key: ip address
 * mask: key's mask
 * value: value of this IP, may be NULL.
 *
 * @Return:
 * OK for success.
 * ERROR for failure.
 */
int btrie_insert_a6(btrie_t *tree, const uint8_t *key, const uint8_t *mask,
          uintptr_t value);

/**
 * Delete an ipv6 from btrie
 *
 * @Args:
 *
 * @Return:
 * OK for success.
 * ERROR for failure.
 */
int btrie_delete_a6(btrie_t *tree, const uint8_t *key, const uint8_t *mask);

/**
 * Find an ipv6 from btrie
 *

 * @Args:
 *
 * @Return:
 * Value if succeed.
 * NULL if failed.
 */
uintptr_t btrie_find_a6(btrie_t *tree, const uint8_t *key);

#if defined (__cplusplus)
}
#endif