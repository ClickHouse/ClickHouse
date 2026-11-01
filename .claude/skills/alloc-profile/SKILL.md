---
name: alloc-profile
description: Analyze a jemalloc (or other) allocation profile in collapsed stack format. Use when the user wants to analyze memory allocations, find top allocators, or understand memory usage patterns from a .collapsed profile file.
argument-hint: [path-to-profile.collapsed]
disable-model-invocation: false
allowed-tools: Task, Bash(ls:*), Bash(find:*), Bash(wc:*), Bash(python3:*)
---

# Allocation Profile Analysis Skill

Analyze an allocation profile file in collapsed stack format (as produced by jemalloc, async-profiler, perf). Each line has the form:

```
frame1;frame2;...;frameN VALUE
```

where `VALUE` is the number of bytes (or samples, depending on the profiler) attributed to that stack trace.

## Arguments

- `$0` (optional): Path to the `.collapsed` file. If not provided, search for `.collapsed` files in the current directory and ask the user to choose.

## Step 1 — Locate the profile file

**Use Task tool with `subagent_type=Bash`** to locate the file:

If `$ARGUMENTS` is provided, use it directly. Otherwise, run:
```bash
find . -maxdepth 3 -name "*.collapsed" -o -name "*.folded" | sort -t_ -k1,1
```
Report the candidates to the user and ask with `AskUserQuestion`:
- **Question**: "Which profile file do you want to analyze?"
- Options: one per found file (show filename and size), plus "Other — enter path manually"

Once the file path is known, pass it to all subsequent steps.

## Step 2 — Parallel initial analysis

**Launch the following three Task agents IN PARALLEL** (single message, three tool calls) all with `run_in_background: true`.
**Then call TaskOutput for ALL three agents** (also in parallel, single message) before proceeding to Step 3.
Do NOT start Step 3 until every agent has finished.

**Fallback:** If any agent fails (e.g., reports lacking Bash permission), re-run its Python script directly using the Bash tool in the main context.

### Agent A — Summary statistics (`subagent_type=Bash`)

Run this Python script to compute summary statistics:

```python
python3 - <<'EOF'
import sys, os, re
filepath = "PATH_TO_FILE"  # substituted by skill
lines = open(filepath).read().splitlines()
traces = []
for line in lines:
    line = line.strip()
    if not line:
        continue
    parts = line.rsplit(' ', 1)
    if len(parts) != 2:
        continue
    try:
        traces.append((int(parts[1]), parts[0]))
    except ValueError:
        continue

total = sum(v for v, _ in traces)
traces.sort(reverse=True)

# Noise filters — keep in sync with Agent C
JEMALLOC_PREFIXES = (
    "prof_backtrace", "prof_alloc_prep", "prof_tctx", "prof_",
    "imalloc", "ialloc", "irallocx", "imallocx",
    "arena_malloc", "arena_palloc", "arena_ralloc", "arena_",
    "tcache_alloc", "tcache_",
    "large_malloc", "large_palloc",
    "chunk_alloc", "huge_malloc", "huge_palloc",
    "je_malloc", "je_calloc", "je_realloc", "je_rallocx", "je_mallocx",
    "je_posix_memalign", "je_aligned_alloc",
    "malloc_default", "calloc",
)
ALLOC_SUBSTRINGS = (
    "operator new", "operator new[]",
    "__libcpp_operator_new",
    "__libc_malloc", "__libc_calloc", "_int_malloc",
    "posix_memalign", "aligned_alloc",
    "do_rallocx", "do_mallocx",
    "mi_malloc", "mi_calloc",
    "__cxx_global_var_init", "__cxa_thread_atexit_impl",
    "DB::Memory<", "Memory::newImpl", "Allocator<false", "Allocator<true",
    "allocNoTrack",
    "PODArrayBase::realloc", "PODArrayBase::alloc",
    "CRYPTO_malloc",
    "std::__detail::_Hash_node", "std::_Rb_tree",
    "std::vector<", "std::string::",
    # STL and PODArray wrappers — noise for leaf analysis
    "std::__1::",
    "DB::PODArrayBase",
)

def is_noise(frame):
    return (any(frame.startswith(p) for p in JEMALLOC_PREFIXES) or
            any(s in frame for s in ALLOC_SUBSTRINGS))

def shorten(frame):
    return re.sub(r'<[^>]{40,}>', '<...>', frame)

print(f"=== SUMMARY ===")
print(f"File: {filepath}")
print(f"Total allocated: {total:,} bytes  ({total/1024/1024:.2f} MB)  ({total/1024/1024/1024:.3f} GB)")
print(f"Unique stack traces: {len(traces)}")
print()
print("=== TOP 25 STACK TRACES ===")
for i, (v, stack) in enumerate(traces[:25], 1):
    frames = [f for f in stack.split(';') if f]
    meaningful = [f for f in frames if not is_noise(f)]
    tail_frames = meaningful[-4:] if meaningful else frames[-4:]
    tail = ' <- '.join(shorten(f) for f in reversed(tail_frames))
    print(f"{i:>3}. {v/1024/1024:>8.2f} MB  ({100*v/total:>5.1f}%)  {tail[:120]}")
print()
print("=== FULL STACKS FOR TOP 10 ===")
for i, (v, stack) in enumerate(traces[:10], 1):
    frames = [f for f in stack.split(';') if f]
    print(f"\n--- #{i}: {v/1024/1024:.2f} MB ({100*v/total:.1f}%) ---")
    for depth, frame in enumerate(reversed(frames), 1):
        noise_mark = "  [noise]" if is_noise(frame) else ""
        print(f"  [{depth:>2}] {shorten(frame)}{noise_mark}")
EOF
```

### Agent B — Outermost meaningful frame aggregation (`subagent_type=Bash`)

Run this Python script to aggregate by the outermost (shallowest) meaningful frame — the operation that initiated the allocation. This answers "why did this allocation happen?" (e.g., loading data parts, executing a query, loading a dictionary), complementing Agent C which answers "what code allocated?":

```python
python3 - <<'EOF'
import sys, re
from collections import defaultdict
filepath = "PATH_TO_FILE"  # substituted by skill

lines = open(filepath).read().splitlines()
traces = []
for line in lines:
    line = line.strip()
    if not line:
        continue
    parts = line.rsplit(' ', 1)
    if len(parts) != 2:
        continue
    try:
        traces.append((int(parts[1]), parts[0]))
    except ValueError:
        continue

total = sum(v for v, _ in traces)

# Frames to skip when looking for the outermost meaningful frame:
# thread pool scaffolding, libc entry points, raw addresses, lambda wrappers
SKIP_OUTER = (
    "0000", "_start", "__libc_start", "__GI___clone",
    "start_thread", "clone3",
    "ThreadPoolImpl", "ThreadFromGlobalPool",
    "std::__1::__function", "std::__1::__invoke",
    "decltype", "void std::__1::__function",
    "std::__1::__packaged_task_function",
    "DB::ThreadPool", "DB::GlobalThreadPool",
    "DB::threadFunction",
    "BaseDaemon", "SignalListener",
    "Poco::ThreadImpl::runnableEntry",
    "Poco::PooledThread::run",
    "main",
    "DB::Server::run",
    "Poco::Util::Application::run",
)

def is_skip_outer(frame):
    return any(frame.startswith(p) for p in SKIP_OUTER) or frame.startswith("(")

def shorten(frame):
    # Collapse long templates, preserve (anonymous namespace), strip args
    s = re.sub(r'<[^>]{40,}>', '<...>', frame)
    s = s.replace('(anonymous namespace)', '{anon}')
    s = re.sub(r'\(.*', '', s)
    s = s.replace('{anon}', '(anonymous namespace)')
    return s[:120]

by_outer = defaultdict(int)

for v, stack in traces:
    frames = [f for f in stack.split(';') if f]
    outer = None
    for f in frames:
        if not f or is_skip_outer(f):
            continue
        outer = f
        break
    if outer is None:
        outer = frames[0] if frames else "(unknown)"
    by_outer[shorten(outer)] += v

print("=== TOP 25 OUTERMOST MEANINGFUL FRAMES (operation that initiated allocation) ===")
for fn, v in sorted(by_outer.items(), key=lambda x: -x[1])[:25]:
    mb = v / 1024 / 1024
    pct = 100 * v / total
    bar = "\u2588" * int(pct / 2)
    print(f"  {mb:>10.2f} MB  {pct:>5.1f}%  {bar:<20}  {fn}")
EOF
```

### Agent C — Leaf (allocating) function aggregation (`subagent_type=Bash`)

Run this Python script to aggregate by the deepest (innermost) frame — the actual allocation call:

```python
python3 - <<'EOF'
import sys, re
from collections import defaultdict
filepath = "PATH_TO_FILE"  # substituted by skill

lines = open(filepath).read().splitlines()
traces = []
for line in lines:
    line = line.strip()
    if not line:
        continue
    parts = line.rsplit(' ', 1)
    if len(parts) != 2:
        continue
    try:
        traces.append((int(parts[1]), parts[0]))
    except ValueError:
        continue

total = sum(v for v, _ in traces)

# Aggregate by last meaningful frame (the allocating function)
by_leaf = defaultdict(int)
by_caller = defaultdict(int)  # caller of the leaf

# jemalloc profiling infrastructure — always at the bottom of every stack
JEMALLOC_PREFIXES = (
    "prof_backtrace", "prof_alloc_prep", "prof_tctx", "prof_",
    "imalloc", "ialloc", "irallocx", "imallocx",
    "arena_malloc", "arena_palloc", "arena_ralloc", "arena_",
    "tcache_alloc", "tcache_",
    "large_malloc", "large_palloc",
    "chunk_alloc", "huge_malloc", "huge_palloc",
    "je_malloc", "je_calloc", "je_realloc", "je_rallocx", "je_mallocx",
    "je_posix_memalign", "je_aligned_alloc",
    "malloc_default", "calloc",
)
# libc / C++ allocator wrappers that add no information
ALLOC_SUBSTRINGS = (
    "operator new", "operator new[]",
    "__libcpp_operator_new",
    "__libc_malloc", "__libc_calloc", "_int_malloc",
    "posix_memalign", "aligned_alloc",
    "do_rallocx", "do_mallocx",
    "mi_malloc", "mi_calloc",
    # C++ static/thread-local initialization wrappers
    "__cxx_global_var_init", "__cxa_thread_atexit_impl",
    # ClickHouse allocator wrappers — informative only as callers, not as leaf
    "DB::Memory<", "Memory::newImpl", "Allocator<false", "Allocator<true",
    "allocNoTrack",
    "PODArrayBase::realloc", "PODArrayBase::alloc",
    # Third-party allocators
    "CRYPTO_malloc",
    # STL internals
    "std::__detail::_Hash_node", "std::_Rb_tree",
    "std::vector<", "std::string::",
    # STL and PODArray wrappers — noise for leaf analysis
    "std::__1::",
    "DB::PODArrayBase",
)

def is_noise(frame):
    return (any(frame.startswith(p) for p in JEMALLOC_PREFIXES) or
            any(s in frame for s in ALLOC_SUBSTRINGS))

def meaningful_leaf(frames):
    # Walk from innermost (last) frame upward, skipping allocator/profiling noise.
    # In jemalloc collapsed format frames are outermost-first, so the bottom of
    # the stack (profiling infra + raw allocators) is at the end of the list.
    for f in reversed(frames):
        if f and not is_noise(f):
            return f
    return frames[-1] if frames else "(unknown)"

def meaningful_caller(frames):
    """Second non-noise frame from the bottom."""
    found_leaf = False
    for f in reversed(frames):
        if f and not is_noise(f):
            if found_leaf:
                return f
            found_leaf = True
    return None

def shorten(frame):
    s = re.sub(r'<[^>]{40,}>', '<...>', frame)
    s = s.replace('(anonymous namespace)', '{anon}')
    s = re.sub(r'\(.*', '', s)
    s = s.replace('{anon}', '(anonymous namespace)')
    return s[:120]

for v, stack in traces:
    frames = [f for f in stack.split(';') if f]
    leaf = meaningful_leaf(frames)
    by_leaf[shorten(leaf)] += v
    caller = meaningful_caller(frames)
    if caller:
        by_caller[shorten(caller)] += v

print("=== TOP 25 ALLOCATING FUNCTIONS (first non-trivial frame from bottom) ===")
for label, bucket in [("Leaf (allocator call site)", by_leaf),
                       ("Caller of leaf", by_caller)]:
    print(f"\n--- {label} ---")
    for fn, v in sorted(bucket.items(), key=lambda x: -x[1])[:25]:
        mb = v / 1024 / 1024
        pct = 100 * v / total
        print(f"  {mb:>8.2f} MB  {pct:>5.1f}%  {fn}")
EOF
```

## Step 3 — Synthesize results and perform subsystem grouping

**MANDATORY: All three agents from Step 2 must have completed (TaskOutput returned) before this step.**

Using the outputs from Agent A (top stacks with full traces), Agent B (outermost frame — the initiating operation), and Agent C (leaf function — the allocating code), **you** (the main LLM) produce a structured report. Agent B gives you the "why" (what operation triggered allocations) and Agent C gives you the "how" (what code did the allocating). Combined with Agent A's full stacks, you can semantically group allocations into subsystems — e.g., `AggregatedDataVariants::init` called from `HashedDictionary::loadData` is "Dictionary Loading", not "Aggregation"; `Arena::addMemoryChunk` inside a merge pipeline is "Merges", not "Arena".

Your report should include:

1. **Summary statistics** (total, trace count)
2. **Top allocators table** (top 15 stack traces with readable short description)
3. **Subsystem breakdown with bar chart** — group every top-25 trace (and the leaf function data) into semantic subsystems based on the full call path context. Use categories like: Part Loading, Dictionary Loading, Query Execution, Backup & Restore, Merges & Mutations, File Cache, IO Buffers, Replication, System Logs, etc. Show an ASCII bar chart. Traces that don't fit neatly into a category go into "(other)".
4. **Top 3–5 actionable findings** — e.g.:
   - Which subsystem unexpectedly dominates
   - Any single allocation that is disproportionately large (>5% of total)
   - Repeated patterns (e.g., many system log types each reserving large buffers)
   - Signs of fragmentation or excessive reallocation (`do_rallocx` / `PODArray::realloc` heavy)
5. **Follow-up drill-down questions** the user may want to investigate

## Step 4 — Offer drill-down options

After presenting the summary, use `AskUserQuestion`:

**Question**: "What would you like to do next?"

- **Option 1: "Drill into a specific subsystem"**
  Description: "Show all stack traces for a chosen component (e.g., MergeTree, SystemLog)"
  → Ask which subsystem with a follow-up `AskUserQuestion`
  → **Launch Task (`subagent_type=Bash`) in the background** (`run_in_background: true`) with a Python script that:
    - Filters all traces whose stack contains any keyword matching the chosen subsystem
    - Sorts by value descending
    - Prints each trace as: `MB (pct%) | frame1 ← frame2 ← ... ← frameN`
    - Also prints the full call stack for the top 5 matches
    - Prints a sub-total for the subsystem
  → Use TaskOutput to wait, then pass output to a `general-purpose` Task agent for a concise summary

- **Option 2: "Show full stacks for top N traces"**
  Description: "Print complete call stacks for the largest N allocations"
  → Ask N with a follow-up `AskUserQuestion` (suggest 10 as default)
  → **Launch Task (`subagent_type=Bash`) in the background** with a Python script that:
    - Parses the file, sorts by value, takes top N
    - For each: prints rank, MB, %, and the full reversed call stack with depth indices
  → Use TaskOutput to wait, then pass output to a `general-purpose` Task agent for a concise narrative summary

- **Option 3: "Search for a keyword in stacks"**
  Description: "Filter traces containing a specific function or class name"
  → Ask for the keyword via `AskUserQuestion`
  → **Launch two Task agents in parallel** (`run_in_background: true`):
    - **Agent X (`subagent_type=Bash`)**: filter and aggregate all matching traces — sum total, count, top 20 by size, full stacks for top 5
    - **Agent Y (`subagent_type=Bash`)**: find related keywords by scanning all frames containing the keyword and extracting their neighboring frames (co-occurring functions), to suggest related call paths
  → Use TaskOutput (both) then pass combined output to a `general-purpose` Task agent for synthesis

- **Option 4: "Generate flamegraph SVG"**
  Description: "Render an SVG flamegraph using flamegraph.pl (must be installed)"
  → **Launch Task (`subagent_type=Bash`) in the background**:
    ```bash
    flamegraph.pl --title "Allocation Profile" --countname bytes --width 1800 \
      PATH_TO_FILE > /tmp/alloc_flamegraph.svg
    ```
  → Use TaskOutput to wait for completion
  → Report the output path `/tmp/alloc_flamegraph.svg` and remind user to open it in a browser

- **Option 5: "Done"**
  Description: "Exit without further analysis"

**IMPORTANT:** For every drill-down option (1–4):
- Always run the analysis inside a Task subagent — never process the file in the main context
- Always run the Bash analysis task in the background with `run_in_background: true` and wait with TaskOutput
- Always pass raw output through a `general-purpose` Task agent for a concise, human-readable summary before showing it to the user

Repeat drill-down (return to the `AskUserQuestion`) until user selects "Done".

## Notes

- Values in collapsed format are **live (in-use) bytes** — jemalloc heap profiles track allocations minus deallocations, so values reflect currently live memory at the time of the dump
- High values directly indicate live memory pressure at those call sites
- Frames are listed **outermost (thread root) first**, innermost (allocator) last — the analysis scripts reverse this for readability
- Symbol names may be mangled if the binary lacks debug info; use `jeprof --demangle` or pipe through `c++filt`
- **ALWAYS use Task subagents for all analysis steps** — profile files can be hundreds of MB and must not be read into the main context
- All Python analysis scripts are self-contained and can be run directly with `python3 -`

## Examples

- `/alloc-profile` — Find `.collapsed` files and prompt for selection
- `/alloc-profile jemalloc-profile-2026-02-19T13-08-59-825Z.collapsed` — Analyze a specific file
- `/alloc-profile /tmp/prod-heap-dump.collapsed` — Analyze an absolute path
