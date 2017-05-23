#include <stdlib.h>
#include <string.h>
#include <btrie.h>

#define PAGE_SIZE 4096


static btrie_node_t *
btrie_alloc(btrie_t *tree)
{
    btrie_node_t  *p;

    if (tree->free) {
        p = tree->free;
        tree->free = tree->free->right;
        return p;
    }

    if (tree->size < sizeof(btrie_node_t)) {
        tree->start = (char *) calloc(sizeof(char), PAGE_SIZE);
        if (tree->start == NULL) {
            return NULL;
        }

        tree->pools[tree->len++] = tree->start;
        tree->size = PAGE_SIZE;
    }

    p = (btrie_node_t *) tree->start;

    tree->start += sizeof(btrie_node_t);
    tree->size -= sizeof(btrie_node_t);

    return p;
}


btrie_t *
btrie_create()
{
    btrie_t *tree = (btrie_t *) malloc(sizeof(btrie_t));
    if (tree == NULL) {
        return NULL;
    }

    tree->free  = NULL;
    tree->start = NULL;
    tree->size  = 0;
    memset(tree->pools, 0, sizeof(btrie_t *) * MAX_PAGES);
    tree->len = 0;

    tree->root = btrie_alloc(tree);
    if (tree->root == NULL) {
        return NULL;
    }

    tree->root->right  = NULL;
    tree->root->left   = NULL;
    tree->root->parent = NULL;
    tree->root->value  = BTRIE_NULL;

    return tree;
}

static size_t
subtree_weight(btrie_node_t *node)
{
    size_t weight = 1;
    if (node->left) {
        weight += subtree_weight(node->left);
    }
    if (node->right) {
        weight += subtree_weight(node->right);
    }
    return weight;
}

size_t
btrie_count(btrie_t *tree)
{
    if (tree->root == NULL) {
        return 0;
    }

    return subtree_weight(tree->root);
}

size_t
btrie_allocated(btrie_t *tree)
{
    return tree->len * PAGE_SIZE;
}


int
btrie_insert(btrie_t *tree, uint32_t key, uint32_t mask,
    uintptr_t value)
{
    uint32_t          bit;
    btrie_node_t  *node, *next;

    bit = 0x80000000;

    node = tree->root;
    next = tree->root;

    while (bit & mask) {
        if (key & bit) {
            next = node->right;

        } else {
            next = node->left;
        }

        if (next == NULL) {
            break;
        }

        bit >>= 1;
        node = next;
    }

    if (next) {
        if (node->value != BTRIE_NULL) {
            return -1;
        }

        node->value = value;
        return 0;
    }

    while (bit & mask) {
        next = btrie_alloc(tree);
        if (next == NULL) {
            return -1;
        }

        next->right = NULL;
        next->left = NULL;
        next->parent = node;
        next->value = BTRIE_NULL;

        if (key & bit) {
            node->right = next;

        } else {
            node->left = next;
        }

        bit >>= 1;
        node = next;
    }

    node->value = value;

    return 0;
}


int
btrie_delete(btrie_t *tree, uint32_t key, uint32_t mask)
{
    uint32_t          bit;
    btrie_node_t  *node;

    bit = 0x80000000;
    node = tree->root;

    while (node && (bit & mask)) {
        if (key & bit) {
            node = node->right;

        } else {
            node = node->left;
        }

        bit >>= 1;
    }

    if (node == NULL) {
        return -1;
    }

    if (node->right || node->left) {
        if (node->value != BTRIE_NULL) {
            node->value = BTRIE_NULL;
            return 0;
        }

        return -1;
    }

    for ( ;; ) {
        if (node->parent->right == node) {
            node->parent->right = NULL;

        } else {
            node->parent->left = NULL;
        }

        node->right = tree->free;
        tree->free = node;

        node = node->parent;

        if (node->right || node->left) {
            break;
        }

        if (node->value != BTRIE_NULL) {
            break;
        }

        if (node->parent == NULL) {
            break;
        }
    }

    return 0;
}


uintptr_t
btrie_find(btrie_t *tree, uint32_t key)
{
    uint32_t          bit;
    uintptr_t         value;
    btrie_node_t  *node;

    bit = 0x80000000;
    value = BTRIE_NULL;
    node = tree->root;

    while (node) {
        if (node->value != BTRIE_NULL) {
            value = node->value;
        }

        if (key & bit) {
            node = node->right;

        } else {
            node = node->left;
        }

        bit >>= 1;
    }

    return value;
}


int
btrie_insert_a6(btrie_t *tree, const uint8_t *key, const uint8_t *mask,
    uintptr_t value)
{
    uint8_t             bit;
    unsigned int        i;
    btrie_node_t  *node, *next;

    i = 0;
    bit = 0x80;

    node = tree->root;
    next = tree->root;

    while (bit & mask[i]) {
        if (key[i] & bit) {
            next = node->right;

        } else {
            next = node->left;
        }

        if (next == NULL) {
            break;
        }

        bit >>= 1;
        node = next;

        if (bit == 0) {
            if (++i == 16) {
                break;
            }

            bit = 0x80;
        }
    }

    if (next) {
        if (node->value != BTRIE_NULL) {
            return -1;
        }

        node->value = value;
        return 0;
    }

    while (bit & mask[i]) {
        next = btrie_alloc(tree);
        if (next == NULL) {
            return -1;
        }

        next->right = NULL;
        next->left = NULL;
        next->parent = node;
        next->value = BTRIE_NULL;

        if (key[i] & bit) {
            node->right = next;

        } else {
            node->left = next;
        }

        bit >>= 1;
        node = next;

        if (bit == 0) {
            if (++i == 16) {
                break;
            }

            bit = 0x80;
        }
    }

    node->value = value;

    return 0;
}


int
btrie_delete_a6(btrie_t *tree, const uint8_t *key, const uint8_t *mask)
{
    uint8_t             bit;
    unsigned int        i;
    btrie_node_t  *node;

    i = 0;
    bit = 0x80;
    node = tree->root;

    while (node && (bit & mask[i])) {
        if (key[i] & bit) {
            node = node->right;

        } else {
            node = node->left;
        }

        bit >>= 1;

        if (bit == 0) {
            if (++i == 16) {
                break;
            }

            bit = 0x80;
        }
    }

    if (node == NULL) {
        return -1;
    }

    if (node->right || node->left) {
        if (node->value != BTRIE_NULL) {
            node->value = BTRIE_NULL;
            return 0;
        }

        return -1;
    }

    for ( ;; ) {
        if (node->parent->right == node) {
            node->parent->right = NULL;

        } else {
            node->parent->left = NULL;
        }

        node->right = tree->free;
        tree->free = node;

        node = node->parent;

        if (node->right || node->left) {
            break;
        }

        if (node->value != BTRIE_NULL) {
            break;
        }

        if (node->parent == NULL) {
            break;
        }
    }

    return 0;
}


uintptr_t
btrie_find_a6(btrie_t *tree, const uint8_t *key)
{
    uint8_t             bit;
    uintptr_t          value;
    unsigned int        i;
    btrie_node_t  *node;

    i = 0;
    bit = 0x80;
    value = BTRIE_NULL;
    node = tree->root;

    while (node) {
        if (node->value != BTRIE_NULL) {
            value = node->value;
        }

        if (key[i] & bit) {
            node = node->right;

        } else {
            node = node->left;
        }

        bit >>= 1;

        if (bit == 0) {
            i++;
            bit = 0x80;
        }
    }

    return value;
}


int
btrie_destroy(btrie_t *tree)
{
    size_t    i;


    /* free memory pools */
    for (i = 0; i < tree->len; i++) {
        free(tree->pools[i]);
    }

    free(tree);

    return 0;
}
