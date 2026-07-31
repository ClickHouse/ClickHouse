---
name: breaking-change-check
description: Use when a change rejects previously-accepted input, removes or gates a user-visible mechanism, flips a user-visible default, or carries the "Backward Incompatible Change" changelog category - before creating or approving the PR, and before backporting such a change to a release branch.
argument-hint: "[PR-number or branch-name or diff-spec]"
---

# Breaking Change Check

A restriction that is correct in isolation can still break a documented product flow. This check makes the documented surface explicit before the change ships. It exists because PR #106855 restricted `role_arn`-based S3 access, which `docs/en/sql-reference/table-functions/s3.md` promises to ClickHouse Cloud customers as the "Secure S3" flow; nobody cross-checked the docs, the backports shipped in a routine patch upgrade, five customer escalations followed in one day, and the only remediation was a downgrade.

## Step 1: enumerate the restricted identifiers

From the diff, list every user-visible identifier whose acceptance, semantics, or default changes:

- settings and their defaults
- SQL syntax elements: function names, clauses, parameters (e.g. `extra_credentials(role_arn = ...)`)
- table-function and engine arguments, named-collection keys
- credential mechanisms, URL schemes, format names, error codes users match on

## Step 2: cross-reference the documentation

For each identifier:

```bash
grep -rn "<identifier>" docs/en/
gh search code --repo ClickHouse/clickhouse-docs "<identifier>"   # Cloud + current docs site
```

Read every hit and answer: does this documented flow still work after the change? Do not stop at "the identifier appears somewhere"; the question is whether the promise in that doc still holds.

## Step 3: quantify usage for Cloud-relevant restrictions

If any affected flow is documented for ClickHouse Cloud, the restriction must not reach a Cloud release branch before fleet usage is quantified (a loghouse `query_log` scan counting distinct services that used the restricted pattern in the last 30 days). This runs on the private side; if you cannot run it, request it explicitly in the PR description instead of skipping it.

## Step 4: write the "Documented behavior impact" section

Add to the PR description:

- each affected documented flow, with a link to the doc that promises it
- the migration path for users of that flow
- the doc updates included in this PR, or an explicit statement of who updates them and when

If a documented flow breaks and the PR neither updates the doc nor states the plan, that is a blocker: do not create or approve the PR in that state.

## Red flags

| Thought | Reality |
|---|---|
| "The restriction is intentional and security-motivated; docs are a follow-up" | The docs promise the old behavior to users today. Intent does not inform them. |
| "The changelog entry covers it" | The changelog describes the new behavior; it does not find or fix the existing docs that promise the old one. |
| "Cloud config will handle it" | Verify with the actual profile/config in hand. The #106855 Cloud rollout pinned the gate setting to 0 and broke the documented flow anyway. |
| "The tests were adapted to the new behavior" | A test rewritten to assert the new refusal is the alarm being silenced, not coverage. The old assertion was the documented contract. |
| "The identifier is niche, nobody uses it" | That is a measurable claim. Measure it (Step 3) or drop it. |
