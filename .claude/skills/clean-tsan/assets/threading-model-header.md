# Threading Model

This file is maintained by RCA and Fix subagents across iterations. Read it before analysis to understand what is already known. Update it with new discoveries. When a fix changes an invariant, update the relevant entry rather than appending. If an earlier entry was wrong, correct it.

**Per-class structure:** Add a `## ClassName` section for each class involved in TSan alerts. Under each class, use these subsections as needed (omit empty ones):

- **Mutexes**: mutex name and type, what fields/state it guards, which threads acquire it
- **Lock Ordering**: which mutex must be acquired before which (e.g., `MutexA` → `MutexB` means A is always acquired before B — never the reverse)
- **Cross-Class Calls Under Lock**: method holds mutex X while calling into another class (which acquires mutex Y)
- **Atomics**: field name, type, access semantics
- **Thread Roles**: which named thread types call which entry-point methods
- **Condition Variables**: name, wait predicate, who signals it
- **Known Safe Patterns**: things that look like races but are provably safe (and why)

**Conventions:** Use actual mutex names from the code (e.g., `AllocationQueue::mutex`), not abstract labels from TSan output like M0, M1, T123. One line per item. Keep entries factual — no speculation.
