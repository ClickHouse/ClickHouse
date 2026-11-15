# External PR Gate Solution

This note describes the external PR approval flow and the fix for false
cancellation of approved runs.

## Goal

For external PRs:

- do not enqueue CI immediately
- require maintainer approval for the exact head SHA
- still cancel older runs as soon as a new push arrives

## Flow

### 1. `pull_request.opened` / `reopened`

Lambda:

1. computes `event_ts = time.time()`
2. builds workflow payload
3. creates an approval gate check for `head_sha`
4. stores approval state in S3
5. does not enqueue CI yet

Saved approval state contains the workflow payload that may later be enqueued:

```json
{
  "status": "awaiting|approved",
  "head_sha": "<pr head sha>",
  "approval_check_id": 123,
  "workflow": {
    "action": "opened|synchronize|reopened",
    "event_ts": 1783353375.2960708,
    "head_sha": "<pr head sha>",
    "...": "..."
  }
}
```

### 2. `pull_request.synchronize`

Lambda:

1. computes `event_ts = time.time()`
2. builds workflow payload for the new PR head
3. immediately writes a PR-scoped cancel marker
4. then handles approval logic for the new head

Cancel marker:

```text
pr/<pr>/cancel-before-<scope>
```

Body:

```json
{
  "ts": 1783353375.3261397,
  "head_sha": "<new pr head sha>"
}
```

Meaning:

- cancel older runs in the same orchestrator scope
- but do not cancel a run for this same `head_sha`

### 3. Maintainer clicks Approve

GitHub sends `check_run.requested_action`.

Lambda:

1. validates the approval check external id
2. verifies maintainer permissions
3. loads approval state from S3
4. verifies the clicked check still matches saved `approval_check_id` and `head_sha`
5. marks the gate check successful
6. enqueues the saved workflow payload

Important:

- approval does not create a new logical PR event
- it only allows later enqueue of the already saved workflow payload

## Why `event_ts` exists

`event_ts` is only used to answer:

```text
is this run older than the newest PR event in this scope?
```

Old logic was:

```text
cancel if cancel_before.ts > run.event_ts
```

## The bug

False cancel became possible because:

1. external PR `synchronize` saved workflow payload with `event_ts = T1`
2. lambda wrote PR-scoped cancel marker with `ts = T2`
3. approval happened later
4. lambda enqueued the saved workflow from step 1
5. orchestrator compared current marker against saved `event_ts`

If logic used only `ts`, then a later PR-scoped marker could make the approved
current-head run look older, even though it was the actual head run.

This is mainly a delayed-enqueue problem:

- approval stores workflow state and reuses it later
- `cancel-before` is mutable PR-scoped state

## Fix

Current rule:

```text
cancel if:
  cancel_before.ts > run.event_ts
  AND cancel_before.head_sha != run.head_sha
```

This keeps the original timestamp behavior and adds a SHA guard.

## Why the SHA check is an extra check

In the ideal path, it is probably redundant:

- one `synchronize` webhook invocation
- same `event_ts` written into both workflow payload and cancel marker
- the freshly created run cannot cancel itself

But in the real system, the extra SHA check is useful defensive logic because:

- approval delays enqueue
- saved workflow state may be enqueued later
- PR-scoped cancel state may already have been rewritten

So the SHA check is best viewed as:

- not the primary mechanism
- an extra safety check that prevents false cancellation of the current head

## Result

After the fix:

- old SHA runs are cancelled immediately on new push
- external PR approval still gates CI
- approved runs for the current head SHA are not falsely cancelled by the
  current PR-level marker
