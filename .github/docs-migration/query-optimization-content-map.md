# Query optimization content migration ledger

This temporary ledger tracks the query optimization rewrite for DOC-895. Remove it after every item is either verified in the new guide set or intentionally removed with an approved rationale.

## Sources and preservation baseline

- Original source commit: `feaa46568da37868728e59aa27c9b8fa696db4d7`
- Original source file: `docs/guides/clickhouse/performance-and-monitoring/query-optimization.mdx`
- Original inventory: 17 SQL blocks, 13 response blocks, and 1 diagram
- Linear issue: DOC-895
- Structure PR: https://github.com/ClickHouse/ClickHouse/pull/111281

The original source commit is the preservation baseline. Later drafts can inform wording and structure, but they do not replace this baseline when checking for content loss.

## Status and treatment values

Status:

- **Unmapped**: no destination has been selected.
- **Mapped**: a destination exists, but the content has not been ported.
- **Ported**: the content is present at the destination, but has not been verified.
- **Verified**: the content is present, accurate, and tested or reviewed as appropriate.
- **Intentionally removed**: the content will not be retained. This status requires an approved rationale in the notes column.

Treatment:

- **Preserve**: retain the explanation and explicitly called-out message as closely as possible during the IA pass.
- **Adapt**: retain the information while changing its presentation or context.
- **Replace**: supersede the original with more accurate or useful guidance.
- **New**: add content required by DOC-895 or review feedback.

An outline heading counts as **Mapped**, not **Ported**. A code example does not become **Verified** until it has been run successfully and its documented output has been checked.

## Original content map

| ID | Original content or artifact | Destination | Treatment | Status | Verification or notes |
| --- | --- | --- | --- | --- | --- |
| O-01 | Introduction: optimization should begin by understanding query performance | Query optimization overview: **Start with a measured problem** | Preserve | Ported | Compare the full original message with the hub copy. |
| O-02 | General considerations: query speed depends on data volume, schema, query construction, and available resources | Query optimization overview: **How ClickHouse processes a query** | Preserve | Ported | Confirm that none of the causal explanation was dropped. |
| O-03 | NYC Taxi dataset introduction and setup | Shared dataset snippet used by the diagnostic, bottleneck, and worked-example guides | Adapt | Ported | Verify database creation, table selection, load commands, and row count end to end. |
| O-04 | Dataset setup SQL blocks and responses | Shared dataset snippet and worked optimization example | Preserve | Mapped | Account for every original SQL and response block; remove duplication only after the shared snippet is verified. |
| O-05 | Why `system.query_log` is the starting point for investigating slow queries | Diagnose slow queries: **How it works** | Preserve | Ported | Check explanation against the original, not only the new outline. |
| O-06 | Query-log query for finding slow candidates | Diagnose slow queries: **Find recurring slow-query patterns** | Adapt | Mapped | The final query must be executable for its stated deployment scope. |
| O-07 | Query-log output showing candidate queries and resource measurements | Diagnose slow queries: candidate-query example output | Preserve | Mapped | Keep useful context; make long output expandable if needed. Verify output against the query. |
| O-08 | Query-log fields and examples for duration, rows read, bytes read, memory, CPU, and filesystem cache activity | Diagnose slow queries: **Review duration and resource usage** | Preserve | Mapped | Confirm every retained field exists and its interpretation is accurate. |
| O-09 | Comparing rows read with total table rows | Diagnose slow queries: **Compare rows read with table size** | Preserve | Mapped | Keep distinct from the matching-row comparison in the bottleneck guide. |
| O-10 | `EXPLAIN indexes = 1` explanation, SQL, and output | Diagnose slow queries: **Inspect index use** | Preserve | Mapped | Run the example and verify parts and granules in the output. |
| O-11 | `EXPLAIN PIPELINE` explanation, SQL, and output | Diagnose slow queries: **Inspect the query pipeline** | Preserve | Mapped | Run the example and verify the documented pipeline. |
| O-12 | Methodology: identify, isolate, test, measure, and repeat | Isolate query bottlenecks: introduction, **How it works**, and baseline sections | Adapt | Ported | Compare the full methodology prose and callouts for lost cautions or instructions. |
| O-13 | Caching guidance used to make comparisons meaningful | Isolate query bottlenecks: **Establish a repeatable baseline** | Preserve | Ported | Verify settings, scope, and restoration guidance. |
| O-14 | Query optimization workflow diagram | Isolate query bottlenecks: baseline/workflow area | Preserve | Ported | Check that surrounding prose still explains the diagram naturally. |
| O-15 | Guidance for handling isolated slow runs and recurring query patterns | Diagnose slow queries and Isolate query bottlenecks | Adapt | Ported | Ensure the two pages are independently useful without repeating the full explanation. |
| O-16 | `Nullable` explanation and storage tradeoff | Optimization approaches: **Avoid unnecessary `Nullable` columns**; executable example in Worked optimization example | Preserve | Mapped | Heading-only coverage is not a port. Retain the explanation, SQL, and measured result. |
| O-17 | `Nullable` schema SQL and response | Worked optimization example: **Avoid unnecessary `Nullable` columns** | Preserve | Mapped | Execute and verify. |
| O-18 | `LowCardinality` explanation and suitable-cardinality guidance | Optimization approaches: **Use `LowCardinality` where appropriate**; executable example in Worked optimization example | Preserve | Mapped | Retain qualifications, not only the recommendation. |
| O-19 | `LowCardinality` SQL and response | Worked optimization example: **Use `LowCardinality` where appropriate** | Preserve | Mapped | Execute and verify. |
| O-20 | Data-type sizing explanation | Optimization approaches: **Choose appropriate data types**; executable example in Worked optimization example | Preserve | Mapped | Preserve the reason smaller suitable types reduce footprint. |
| O-21 | Data-type inspection SQL and response | Worked optimization example: **Choose appropriate data types** | Preserve | Mapped | Reconcile the `vendor_id` conversion concern raised in review. |
| O-22 | Applying schema optimizations with a replacement table | Worked optimization example: **Apply the schema changes** | Preserve | Mapped | Verify the DDL, insert, row count, and naming continuity. |
| O-23 | Post-schema-change benchmark SQL and response | Worked optimization example: **Compare the results** | Preserve | Mapped | Run under documented conditions and verify output. |
| O-24 | Why primary keys and ordering keys affect data skipping | Optimization approaches: **Choose an effective ordering key**; Worked optimization example: **Optimize the primary key** | Preserve | Mapped | Preserve the system-mechanics explanation and terminology. |
| O-25 | Primary-key selection principles | Optimization approaches: **Choose an effective ordering key** | Preserve | Mapped | Retain qualifications about column order and filtering patterns. |
| O-26 | Primary-key experiment DDL and data load | Worked optimization example: **Apply the primary-key change** | Preserve | Mapped | Execute and verify. |
| O-27 | Final benchmark query, response, and comparison after primary-key change | Worked optimization example: **Compare the results** | Preserve | Mapped | Execute and verify all stated improvements. |
| O-28 | Next-step links for analyzer, query profiling, partitioning, and data-skipping indexes | Overview and Optimization approaches: **Next steps** | Adapt | Mapped | Retain useful onward paths and remove only obsolete or duplicate links. |

## DOC-895 additions

| ID | Required addition | Destination | Treatment | Status | Verification or notes |
| --- | --- | --- | --- | --- | --- |
| N-01 | Cluster-wide query-log access with `clusterAllReplicas('default', merge(system, '^query_log'))` | Diagnose slow queries: cluster-wide query-log tab | New | Mapped | Verify syntax and explain why the merged table expression includes rotated `query_log_N` tables. |
| N-02 | Local-node query-log alternative | Diagnose slow queries: local/cluster tabs | New | Mapped | Ensure both tabs are copyable and clearly scoped. |
| N-03 | `skip_unavailable_shards` during autoscaling or unavailable replicas | Diagnose slow queries: cluster-wide query-log guidance | New | Mapped | State that successful results can be incomplete; verify syntax and deployment applicability. |
| N-04 | Bounded time ranges for query-log searches | Diagnose slow queries: candidate-query SQL and explanation | New | Mapped | Verify the query uses an explicit, practical range. |
| N-05 | Three-run method: original query, grouped `count`, ungrouped `count` | Isolate query bottlenecks: **Simplify the query in stages** | New | Ported | End-to-end execution validation is still required. |
| N-06 | Compare representative durations to distinguish scan/filter, grouping, and final computation | Isolate query bottlenecks: **Interpret the differences** | New | Ported | Validate that conclusions are described as diagnostic estimates. |
| N-07 | Compare `read_rows` with matching rows returned by ungrouped `count` | Isolate query bottlenecks: **Compare rows read with matching rows** | New | Ported | Verify semantics for single-table, joined, and distributed queries. |
| N-08 | Guidance for similar durations across all three runs | Isolate query bottlenecks: interpretation table | New | Ported | Ensure the row offers a useful next investigation rather than a generic retry instruction. |
| N-09 | Validate suspected bottlenecks with query logs, profile events, and `EXPLAIN` | Isolate query bottlenecks: **Validate the suspected bottleneck** | New | Ported | Confirm all recommended observations are accessible and accurately named. |
| N-10 | Docs AI inputs: query, table DDL, and query profile events | Diagnose or Isolate guide validation section | New | Mapped | Do not publish a reliability percentage without support. Tell readers to verify recommendations. |
| N-11 | Framework: minimize data footprint | Overview and Optimization approaches | New | Mapped | Detailed guidance still needs to be ported; the current outline is not complete. |
| N-12 | Framework: filter efficiently | Overview and Optimization approaches | New | Mapped | Connect evidence to ordering keys, pruning, and skipping indexes. |
| N-13 | Framework: move repeatable work to ingestion | Overview and Optimization approaches | New | Mapped | Add materialized-view and purpose-built-table guidance with appropriate tradeoffs. |
| N-14 | Bottleneck-to-intervention decision table | Optimization approaches: **Match the evidence to an intervention** | New | Mapped | Validate that each intervention follows from observable evidence. |
| N-15 | Projections-at-scale caution | Optimization approaches: **Use projections selectively** | New | Mapped | Fact-check the performance claim and frame projections without presenting them as generally problematic. |
| N-16 | Prefer fewer projections or separate tables where justified at scale | Optimization approaches: projections and purpose-built tables | New | Mapped | Explain the decision boundary and verify with a subject-matter reviewer. |
| N-17 | Apply and validate one optimization at a time | Optimization approaches and Worked optimization example | New | Mapped | Include the measurement loop and rollback or comparison expectations. |

## Reviewer feedback and reproducibility fixes

| ID | Feedback or fix | Destination | Status | Verification or notes |
| --- | --- | --- | --- | --- |
| R-01 | Make the overview the first visible page in navigation | Navigation | Ported | Verify rendered navigation order. |
| R-02 | Present the workflow with `Steps` | Query optimization overview | Ported | Verify rendered layout. |
| R-03 | Use numbered vertical guide cards | Query optimization overview | Ported | Verify rendered layout and link order. |
| R-04 | Do not add legacy-anchor compatibility UI | Structure PR | Verified | The component was removed. Existing links should route to the hub or current destinations. |
| R-05 | Avoid linked stubs that remove usable production content | Entire guide set | Mapped | Block final merge until all preservation items are Ported or intentionally removed. |
| R-06 | Create and select the `nyc_taxi` database in setup | Shared dataset snippet | Ported | Execute setup from a clean instance. |
| R-07 | Generate a representative workload before querying logs | Diagnose slow queries | Mapped | Add executable workload generation and verify that queries appear in `system.query_log`. |
| R-08 | Keep queries and displayed outputs consistent | Diagnose and Worked optimization guides | Mapped | Run every example and capture output from the same query and schema. |
| R-09 | Make long output expandable | Diagnose and Worked optimization guides | Mapped | Apply consistently without hiding required instructions. |
| R-10 | Resolve the `vendor_id` type-conversion inconsistency | Worked optimization example | Mapped | Verify the source type, target type, and insertion behavior. |
| R-11 | Keep each guide useful when opened directly | All guides | Mapped | Check prerequisites, context, and next steps for standalone use. |
| R-12 | Preserve explanations and explicitly called-out messages during the IA pass | Entire guide set | Mapped | Compare prose and callouts against the pinned source before content-level removals. |

## Verification checklist

- [ ] Every original section has a destination or an approved removal rationale.
- [ ] All 17 original SQL blocks are accounted for by a preserved, adapted, replaced, or intentionally removed artifact.
- [ ] All 13 original response blocks are accounted for.
- [ ] The original workflow diagram and its explanation are accounted for.
- [ ] Every current guide has complete frontmatter and explicit anchors on every heading.
- [ ] Every internal link and navigation entry resolves.
- [ ] Each guide is understandable when opened directly.
- [ ] Dataset setup succeeds on a clean local ClickHouse instance.
- [ ] Workload generation produces the query-log records used by the diagnostic examples.
- [ ] Local-node and cluster-wide query-log examples have been syntax-checked and scope-checked.
- [ ] The three-run bottleneck method has been executed end to end.
- [ ] Every documented query output matches the query and schema that produced it.
- [ ] Claims about settings, projections, materialized views, query-log fields, and distributed behavior have been fact-checked.
- [ ] A final prose comparison confirms that explanations and warnings were not silently lost.
- [ ] This temporary ledger is removed after all remaining items are Verified or Intentionally removed.
