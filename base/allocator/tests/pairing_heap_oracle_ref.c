/* The reference: jemalloc's own pairing heap (`ph.h`) instantiated on a simple node type. */

#include "jemalloc/internal/jemalloc_preamble.h"
#include "jemalloc/internal/ph.h"

#define REF_MAX_NODES 4096
#define REF_QUEUE_SIZE 32

typedef struct ref_node_s ref_node_t;
struct ref_node_s
{
    uint64_t key;
    int id;
    phn_link_t link;
};

ph_structs(ref_heap, ref_node_t, REF_QUEUE_SIZE)

static int ref_cmp(const ref_node_t * a, const ref_node_t * b)
{
    return (a->key > b->key) - (a->key < b->key);
}

ph_gen(static, ref_heap, ref_node_t, link, ref_cmp)

static ref_node_t ref_nodes[REF_MAX_NODES];
static ref_heap_t ref_heap;
static ref_heap_enumerate_helper_t ref_helper;

static int ref_id(void * node)
{
    return node == NULL ? -1 : ((ref_node_t *)node)->id;
}

void ref_ph_init(int n, const uint64_t * keys)
{
    for (int i = 0; i < n; i++)
    {
        ref_nodes[i].key = keys[i];
        ref_nodes[i].id = i;
    }
    ref_heap_new(&ref_heap);
}

void ref_ph_insert(int id)
{
    ref_heap_insert(&ref_heap, &ref_nodes[id]);
}

int ref_ph_empty(void)
{
    return ref_heap_empty(&ref_heap);
}

int ref_ph_first(void)
{
    return ref_id(ref_heap_first(&ref_heap));
}

int ref_ph_any(void)
{
    return ref_id(ref_heap_any(&ref_heap));
}

int ref_ph_remove_first(void)
{
    return ref_id(ref_heap_remove_first(&ref_heap));
}

int ref_ph_remove_any(void)
{
    return ref_id(ref_heap_remove_any(&ref_heap));
}

void ref_ph_remove(int id)
{
    ref_heap_remove(&ref_heap, &ref_nodes[id]);
}

/* Returns the number of enumerated nodes written to `out`. */
int ref_ph_enumerate(uint16_t max_visit_num, uint16_t max_queue_size, int * out)
{
    int count = 0;
    ref_heap_enumerate_prepare(&ref_heap, &ref_helper, max_visit_num, max_queue_size);
    ref_node_t * node;
    while ((node = ref_heap_enumerate_next(&ref_heap, &ref_helper)) != NULL)
        out[count++] = node->id;
    return count;
}

int ref_ph_root(void)
{
    return ref_id(ref_heap.ph.root);
}

size_t ref_ph_auxcount(void)
{
    return ref_heap.ph.auxcount;
}

/* The links of a node, as ids: prev, next, lchild. */
void ref_ph_links(int id, int * out)
{
    out[0] = ref_id(ref_nodes[id].link.prev);
    out[1] = ref_id(ref_nodes[id].link.next);
    out[2] = ref_id(ref_nodes[id].link.lchild);
}
