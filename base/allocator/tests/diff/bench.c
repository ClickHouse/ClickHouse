/*
 * Allocation microbenchmarks, built against the reference jemalloc and against the new allocator to compare
 * performance. Prints one line per benchmark: name, threads, nanoseconds per operation (best of several runs).
 *
 * Usage: bench [filter]
 */

#define _GNU_SOURCE
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <jemalloc/jemalloc.h>

static uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static inline uint64_t xorshift(uint64_t * state)
{
    uint64_t x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    return *state = x;
}

static volatile uintptr_t sink;

struct bench_args
{
    int kind;
    size_t iterations;
    uint64_t seed;
};

enum
{
    PAIR_16,
    PAIR_100,
    PAIR_1000,
    PAIR_SIZED_100,
    BATCH_SMALL,
    RANDOM_SMALL,
    RANDOM_MIXED,
    REALLOC_GROW,
    CALLOC_SMALL,
    LARGE_PAIR,
    NUM_KINDS
};

static const char * const kind_names[] = {"pair_16", "pair_100", "pair_1000", "pair_sized_100", "batch_small", "random_small",
                                          "random_mixed", "realloc_grow", "calloc_small", "large_pair"};

enum { BATCH = 1000 };

static void * bench_thread(void * arg)
{
    struct bench_args * args = arg;
    uint64_t rng = args->seed;
    void * batch[BATCH];
    size_t n = args->iterations;
    switch (args->kind)
    {
        case PAIR_16:
            for (size_t i = 0; i < n; ++i)
            {
                void * p = je_malloc(16);
                sink += (uintptr_t)p;
                je_free(p);
            }
            break;
        case PAIR_100:
            for (size_t i = 0; i < n; ++i)
            {
                void * p = je_malloc(100);
                sink += (uintptr_t)p;
                je_free(p);
            }
            break;
        case PAIR_1000:
            for (size_t i = 0; i < n; ++i)
            {
                void * p = je_malloc(1000);
                sink += (uintptr_t)p;
                je_free(p);
            }
            break;
        case PAIR_SIZED_100:
            for (size_t i = 0; i < n; ++i)
            {
                void * p = je_malloc(100);
                sink += (uintptr_t)p;
                je_sdallocx(p, 100, 0);
            }
            break;
        case BATCH_SMALL:
            for (size_t i = 0; i < n; i += BATCH)
            {
                for (int j = 0; j < BATCH; ++j)
                    batch[j] = je_malloc(64);
                for (int j = 0; j < BATCH; ++j)
                    je_free(batch[j]);
            }
            break;
        case RANDOM_SMALL:
            for (int j = 0; j < BATCH; ++j)
                batch[j] = NULL;
            for (size_t i = 0; i < n; ++i)
            {
                size_t j = xorshift(&rng) % BATCH;
                je_free(batch[j]);
                batch[j] = je_malloc((xorshift(&rng) % 1024) + 1);
            }
            for (int j = 0; j < BATCH; ++j)
                je_free(batch[j]);
            break;
        case RANDOM_MIXED:
            for (int j = 0; j < BATCH; ++j)
                batch[j] = NULL;
            for (size_t i = 0; i < n; ++i)
            {
                size_t j = xorshift(&rng) % BATCH;
                je_free(batch[j]);
                uint64_t r = xorshift(&rng);
                size_t size = (r % 64 == 0) ? (size_t)(r >> 40) % (1 << 20) + 1 : (size_t)(r >> 40) % 8192 + 1;
                batch[j] = je_malloc(size);
            }
            for (int j = 0; j < BATCH; ++j)
                je_free(batch[j]);
            break;
        case REALLOC_GROW:
            for (size_t i = 0; i < n; i += 64)
            {
                void * p = NULL;
                for (int j = 1; j <= 64; ++j)
                    p = je_realloc(p, (size_t)j * 96);
                je_free(p);
            }
            break;
        case CALLOC_SMALL:
            for (size_t i = 0; i < n; ++i)
            {
                void * p = je_calloc(1, 200);
                sink += (uintptr_t)p;
                je_free(p);
            }
            break;
        case LARGE_PAIR:
            for (size_t i = 0; i < n; ++i)
            {
                void * p = je_malloc(300000 + (i & 7) * 4096);
                sink += (uintptr_t)p;
                je_free(p);
            }
            break;
    }
    return NULL;
}

static double run(int kind, int nthreads, size_t iterations)
{
    pthread_t threads[64];
    struct bench_args args[64];
    uint64_t start = now_ns();
    for (int t = 0; t < nthreads; ++t)
    {
        args[t].kind = kind;
        args[t].iterations = iterations;
        args[t].seed = 0x9e3779b97f4a7c15ULL * (uint64_t)(t + 1);
        pthread_create(&threads[t], NULL, bench_thread, &args[t]);
    }
    for (int t = 0; t < nthreads; ++t)
        pthread_join(threads[t], NULL);
    uint64_t elapsed = now_ns() - start;
    return (double)elapsed / (double)iterations;
}

int main(int argc, char ** argv)
{
    const char * filter = argc > 1 ? argv[1] : NULL;
    static const int thread_counts[] = {1, 4, 16};
    for (int kind = 0; kind < NUM_KINDS; ++kind)
    {
        if (filter && !strstr(kind_names[kind], filter))
            continue;
        size_t iterations = (kind == LARGE_PAIR) ? 200000 : (kind == REALLOC_GROW ? 2000000 : 10000000);
        for (size_t t = 0; t < sizeof(thread_counts) / sizeof(thread_counts[0]); ++t)
        {
            double best = 1e30;
            for (int rep = 0; rep < 5; ++rep)
            {
                double ns = run(kind, thread_counts[t], iterations);
                if (ns < best)
                    best = ns;
            }
            printf("%-16s threads=%-3d %8.2f ns/op\n", kind_names[kind], thread_counts[t], best);
            fflush(stdout);
        }
    }
    return 0;
}
