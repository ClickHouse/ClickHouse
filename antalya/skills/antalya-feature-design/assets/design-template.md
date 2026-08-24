# Feature Design: [Feature Name]

**Status:** draft | under review | accepted | implemented
**Author(s):**
**Reviewers:**
**Related issues/PRs:**
**Last updated:**

---

## 1. Problem Definition

### Motivation
What problem does this feature solve for users? Why now? Quote user reports, incidents, or concrete
workloads where possible — avoid generic statements like "users want X".

### Requirements
- A series of numbered requirements for the feature. 

### Non-requirements
What is explicitly out of scope. Naming non-requirements up front prevents scope creep during review.

### Constraints
Compatibility requirements (older clients, existing on-disk formats, upstream ClickHouse parity),
deadlines, licensing, platform restrictions.

### References
URLs of background documents, GitHub issues, and pull requests that have
information relevant to the design.

---

## 2. Functional specification

### User-facing behavior
Describe the feature strictly from the user's perspective — what they type, what they see back.
No implementation details here.

### SQL syntax / API
Concrete examples. Wrap SQL keywords, function names, setting names, and engine names in inline
code blocks (e.g. `MergeTree`, `SELECT`, `max_threads`).

```sql
-- Example 1: basic usage
SELECT ...

-- Example 2: edge case
...
```

### Settings
Relevant settings, presented in tabular form. 

| Setting | Scope | Default | Range | Description |
| --- | --- | --- | --- | --- |
| `setting_name` | server / user / query | ... | ... | ... |

Note which settings are new vs. changes to existing ones, and whether defaults change behavior
for existing workloads.

### System tables / metrics / log messages / observability
New or changed rows in `system.settings`, `system.metrics`, `system.events`, `system.errors`,
`system.*_log`. Any new `ProfileEvents` or `CurrentMetrics. Highlight data that can provide
observability information to ClickHouse administrators. 

### Error behavior
What exceptions are thrown, with which error codes, on which inputs. Say "throws exception" rather
than "crashes" — logical errors don't crash release builds.

### Backward compatibility
Highlight compatibility issues, for example:
- Older clients talking to a new server
- New clients talking to an older server
- Mixed-version clusters (replication, distributed queries)
- On-disk format changes (mark version, metadata version)
- Default changes that alter existing query behavior

---

## 3. Implementation

This section may be omitted by the user. In this case the section should left empty
and replaced with a messsage like the following. 

"The specification only covers user-visible behavior, hence this section is omitted."

Optionally, also include references to other documents, GitHub issues, or code if 
available and relevant. 

### Architecture overview
One paragraph, plus a diagram if it helps. Where does the feature live in the codebase, and how
does it connect to existing subsystems (parser → analyzer → planner → executor → storage)?

### Key design decisions
Highlight prominent aspects of the design that an would be useful to implemntors or maintainers. 

### Concurrency / locking
Threads involved, locks held, ordering constraints. If none, state that explicitly.

### Storage format changes
Mark/metadata version bumps, migration path, how older parts are read after upgrade, how newer
parts are rejected by older servers.

### Performance
- Expected overhead in the hot path (CPU, memory, I/O)
- Which benchmarks will exercise this (e.g. specific `tests/performance/` cases)
- Memory ownership and lifetime of any new allocations

### Alternatives considered
Short list of approaches you rejected and why. This is the single most useful section for
reviewers — it shows you explored the design space.

### Open questions
Things you don't yet have an answer to. Better to name them than to paper over them.

---

## 4. Test plan

### Functional tests — `tests/queries/0_stateless`
List planned test files. Prefer adding new tests over extending existing ones. 

- Golden path: basic feature works on simple input
- Edge cases: empty input, single row, boundary values, max sizes
- Error cases: invalid input produces the expected exception with the expected error code
- Settings interactions: feature off (default), feature on, interaction with related settings
- Compatibility: works with older parts / mixed formats, if relevant

### Integration tests — `tests/integration`
Required if the feature touches replication, keeper, distributed queries, S3, auth, Kafka, etc.
Invoke with `python -m ci.praktika run "integration" --test <selectors>`.

### Performance tests — `tests/performance`
Required if the feature is in the hot path.

### Manual verification
Any checks that aren't automated (UI, metrics dashboards, upgrade scenarios).

### Rollout / risk
Deployment risk, whether a feature flag is warranted (default off? default on?), the existing
tests that guard against regressions, and what to watch in production after rollout.
