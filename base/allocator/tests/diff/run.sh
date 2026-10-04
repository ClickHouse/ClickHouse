#!/usr/bin/env bash
# Differential test: run the same deterministic workload against the reference jemalloc and the new allocator and
# compare the outputs.
#
# Usage: run.sh <reference lib_jemalloc.a> <libunwind.a> <new allocator build dir> [work dir] [sections...]
#
# Environment: EXTRA_CONF is appended to the configuration of every variant (e.g. EXTRA_CONF=prof:false);
# VARIANTS_ONLY selects variants by number (e.g. VARIANTS_ONLY="1 5"); CPUS is the initial CPU affinity (default 0;
# with several CPUs the allocator boots with per-CPU arenas, and the `percpu`/`threads` sections pin themselves).
#
# The reference library and the new allocator must be built with the same page size.

set -euo pipefail

REFERENCE_LIB=$1
UNWIND_LIB=$2
NEW_BUILD_DIR=$3
WORK_DIR=${4:-$(pwd)/tmp/diff}
shift 4 || true
SECTIONS=("${@:-all}")

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
ALLOCATOR_DIR=$(cd "$SCRIPT_DIR/../.." && pwd)
REPO_DIR=$(cd "$ALLOCATOR_DIR/../.." && pwd)

mkdir -p "$WORK_DIR"

CC=${CC:-clang}
CFLAGS=(-O2 -g -DJEMALLOC_NO_RENAME)

"$CC" "${CFLAGS[@]}" -I"$REPO_DIR/contrib/jemalloc-cmake/include" "$SCRIPT_DIR/driver.c" \
    "$REFERENCE_LIB" "$UNWIND_LIB" -lpthread -ldl -lm -o "$WORK_DIR/driver_ref"

"$CC" "${CFLAGS[@]}" -I"$ALLOCATOR_DIR/include" "$SCRIPT_DIR/driver.c" \
    "$NEW_BUILD_DIR/lib_allocator.a" "$NEW_BUILD_DIR/lib_allocator_core.a" "$UNWIND_LIB" -lpthread -ldl -lm \
    -o "$WORK_DIR/driver_new"

# Time-dependent behavior is disabled: background threads, time-based decay, and the time-gated tcache GC (by making
# the GC event interval huge).
DETERMINISTIC="background_thread:false,tcache_gc_incr_bytes:1152921504606846976"

VARIANTS=(
    "$DETERMINISTIC,dirty_decay_ms:-1,muzzy_decay_ms:-1"
    "$DETERMINISTIC,dirty_decay_ms:0,muzzy_decay_ms:0"
    "$DETERMINISTIC,dirty_decay_ms:-1,muzzy_decay_ms:-1,tcache:false"
    "background_thread:false,dirty_decay_ms:-1,muzzy_decay_ms:-1,experimental_tcache_gc:false"
    "$DETERMINISTIC,dirty_decay_ms:-1,muzzy_decay_ms:-1,prof:false"
    "$DETERMINISTIC,dirty_decay_ms:-1,muzzy_decay_ms:-1,junk:true"
    "$DETERMINISTIC,dirty_decay_ms:-1,muzzy_decay_ms:-1,retain:false"
    "$DETERMINISTIC,dirty_decay_ms:-1,muzzy_decay_ms:-1,cache_oblivious:false,disable_large_size_classes:false"
    "$DETERMINISTIC,dirty_decay_ms:-1,muzzy_decay_ms:-1,narenas:3,percpu_arena:disabled,lg_extent_max_active_fit:10"
    "$DETERMINISTIC,dirty_decay_ms:-1,muzzy_decay_ms:-1,slab_sizes:1-4096:17|100-200:1,bin_shards:1-160:16"
    "$DETERMINISTIC,dirty_decay_ms:-1,muzzy_decay_ms:-1,tcache_max:1048576,lg_tcache_nslots_mul:2,tcache_nslots_small_min:7"
)

normalize()
{
    # Uptime is time-dependent. Heap profile backtraces contain code addresses that differ between the binaries.
    # The table-mode output of malloc_stats_print contains "(#/sec)" rate columns that depend on uptime: mask all
    # numbers there (the same values are compared exactly in the JSON output).
    sed -E -e 's/"uptime_ns":[0-9]+/"uptime_ns":N/g' -e 's/uptime: [0-9]+/uptime: N/' \
        -e '/^___ Begin jemalloc statistics ___$/,/^--- End jemalloc statistics ---$/{s/[0-9]+/N/g;s/ +/ /g}'
}

normalize_heap()
{
    sed -E -e 's/^@ .*/@ <backtrace>/' -e 's/^(  f: )[0-9]+/\1N/' -e '/^MAPPED_LIBRARIES:/,$d'
}

failed=0
index=0
for variant in "${VARIANTS[@]}"
do
    index=$((index + 1))
    if [ -n "${VARIANTS_ONLY:-}" ] && [[ " $VARIANTS_ONLY " != *" $index "* ]]
    then
        continue
    fi
    variant="$variant${EXTRA_CONF:+,$EXTRA_CONF}"
    for kind in ref new
    do
        (
            cd "$WORK_DIR"
            rm -f driver_prof.heap
            MALLOC_CONF="$variant" setarch -R taskset -c "${CPUS:-0}" "./driver_$kind" "${SECTIONS[@]}" 2>&1 | normalize > "out_${index}_$kind.txt" || true
            if [ -f driver_prof.heap ]
            then
                normalize_heap < driver_prof.heap > "heap_${index}_$kind.txt"
            else
                : > "heap_${index}_$kind.txt"
            fi
        )
    done
    if cmp -s "$WORK_DIR/out_${index}_ref.txt" "$WORK_DIR/out_${index}_new.txt" && cmp -s "$WORK_DIR/heap_${index}_ref.txt" "$WORK_DIR/heap_${index}_new.txt"
    then
        echo "OK   [$index] $variant"
    else
        echo "DIFF [$index] $variant"
        diff "$WORK_DIR/out_${index}_ref.txt" "$WORK_DIR/out_${index}_new.txt" | head -20 || true
        diff "$WORK_DIR/heap_${index}_ref.txt" "$WORK_DIR/heap_${index}_new.txt" | head -10 || true
        failed=1
    fi
done

exit $failed
