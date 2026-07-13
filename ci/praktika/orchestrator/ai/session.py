"""AI session management — the durable log across ephemeral CI runs.

The orchestrator process is per-sha and short-lived (new push → new SQS message
→ new ``run_id``). An AI fix-loop is not: it spans many CI runs (edit → push →
rerun → observe → edit …). ``SessionManager`` is the bridge — it owns the
persistent log and the notion of a **round** (one AI problem-solving episode)
that outlives any single run.

Hierarchy:

    PR     (pr key)        session.json   — rounds/runs index, cumulative cost, budget
     Round (round_id)      rounds/<id>.json — goal, status, run_ids, edits, cost
      Run  (run_id / sha)  runs/<id>/run.json — event, outcomes, cost
       Turn                runs/<id>/turns   — append-only advisor turns

Round boundaries are **implicit**: a round opens automatically the first time a
run reports a failure with no round already open, and closes when a later run
goes green (resolved). Continuity across runs is recovered from
``session.json`` (``open_round_id``); a future AI-authored-push check (commit
trailer / author) refines "continue the round" vs "human took over".

Storage is the blob layer (``SessionStore``, S3 in CI) plus a pluggable
``index`` (a queryable row store; ``LoggingIndex`` for now, CIDB later).
"""
import dataclasses
import time
import uuid
from dataclasses import dataclass, field
from typing import List, Optional

from praktika.settings import Settings

from .store import make_store
from .trace import TraceLogger


def _slug(text):
    return "".join(c if c.isalnum() or c in "-._" else "_" for c in str(text))


def _empty_usage():
    return {"turns": 0, "input_tokens": 0, "output_tokens": 0, "cost_usd": 0.0}


def _add_usage(dst, usage):
    dst["turns"] += 1
    dst["input_tokens"] += usage.input_tokens
    dst["output_tokens"] += usage.output_tokens
    dst["cost_usd"] += usage.cost_usd


def _load(cls, d):
    """Build a dataclass from a dict, ignoring unknown keys (forward-compat)."""
    names = {f.name for f in dataclasses.fields(cls)}
    return cls(**{k: v for k, v in (d or {}).items() if k in names})


# --------------------------------------------------------------- manifests


@dataclass
class RunManifest:
    run_id: str
    sha: str = ""
    event: dict = field(default_factory=dict)
    round_id: Optional[str] = None
    started_at: float = 0.0
    ended_at: Optional[float] = None
    conclusion: Optional[str] = None  # success / failure / cancelled / error
    job_outcomes: List[dict] = field(default_factory=list)
    usage: dict = field(default_factory=_empty_usage)


@dataclass
class RoundManifest:
    round_id: str
    pr: str = ""
    opened_at: float = 0.0
    status: str = "open"  # open / resolved / abandoned / superseded
    goal: str = ""
    trigger: dict = field(default_factory=dict)
    run_ids: List[str] = field(default_factory=list)
    edits: List[dict] = field(default_factory=list)  # [{edit_id, commit_sha, files, ts}]
    usage: dict = field(default_factory=_empty_usage)
    closed_at: Optional[float] = None
    outcome: str = ""


@dataclass
class SessionManifest:
    repo: str = ""
    pr: str = ""
    open_round_id: Optional[str] = None
    round_ids: List[str] = field(default_factory=list)
    run_ids: List[str] = field(default_factory=list)
    usage: dict = field(default_factory=_empty_usage)
    budget: dict = field(default_factory=dict)  # {token_cap, max_rounds}


# --------------------------------------------------------------- index (stub)


class LoggingIndex:
    """Minimal queryable-index stand-in: prints a row per event.

    The real implementation writes one row per turn/round to CIDB (ClickHouse)
    so cost and decision flow are queryable across PRs. Kept as a stub here so
    the interface is exercised without a live database.
    """

    def record_turn(self, row):
        print(
            f"[AI idx] turn pr={row.get('pr')} round={row.get('round_id')} "
            f"run={row.get('run_id')} provider={row.get('provider')} "
            f"cost=${row.get('cost_usd', 0.0):.4f} decisions={row.get('decision_types')}"
        )

    def record_round(self, row):
        print(
            f"[AI idx] round pr={row.get('pr')} round={row.get('round_id')} "
            f"status={row.get('status')} cost=${row.get('cost_usd', 0.0):.4f}"
        )


# --------------------------------------------------------------- manager


class SessionManager:
    """Owns the durable AI log for one PR and the current run within it."""

    def __init__(self, repo, pr, store, ai_config=None, index=None, console=None):
        self.repo = repo
        self.pr = _slug(pr)
        self.store = store
        self.ai_config = ai_config
        self.index = index or LoggingIndex()
        self.console = console or TraceLogger(run_id=None)
        self._base = f"ai-sessions/pr/{self.pr}"
        self.session = self._load_session()
        self._run = None  # RunManifest for the current run
        self._round = None  # RoundManifest for the open round, if any

    @classmethod
    def from_event(cls, event, run_id, local_mode, ai_config=None, console=None):
        repo = event.get("repo", "")
        pr = event.get("pr_number") or f"branch-{event.get('head_ref', '')}"
        store = make_store(local_mode)
        return cls(repo, pr, store, ai_config=ai_config, console=console)

    # ------------------------------------------------------------ keys

    def _session_key(self):
        return f"{self._base}/session.json"

    def _round_key(self, round_id):
        return f"{self._base}/rounds/{round_id}.json"

    def _run_key(self, run_id):
        return f"{self._base}/runs/{run_id}/run.json"

    def _turns_stream(self, run_id):
        return f"{self._base}/runs/{run_id}/turns"

    def _edit_key(self, run_id, edit_id):
        return f"{self._base}/runs/{run_id}/edits/{edit_id}.patch"

    # ------------------------------------------------------------ load/persist

    def _load_session(self):
        data = self.store.read_json(self._session_key())
        if data:
            return _load(SessionManifest, data)
        return SessionManifest(
            repo=self.repo,
            pr=self.pr,
            budget={
                "token_cap": int(getattr(self.ai_config, "pr_token_cap", 0) or 0),
                "max_rounds": int(
                    getattr(self.ai_config, "max_rounds", 0) or 0
                ),
            },
        )

    def _persist_session(self):
        self.store.write_json(self._session_key(), dataclasses.asdict(self.session))

    def _persist_run(self):
        if self._run is not None:
            self.store.write_json(self._run_key(self._run.run_id), dataclasses.asdict(self._run))

    def _persist_round(self):
        if self._round is not None:
            self.store.write_json(self._round_key(self._round.round_id), dataclasses.asdict(self._round))

    # ------------------------------------------------------------ lifecycle

    def begin_run(self, run_id, sha, event):
        """Register the current CI run and attach it to the open round (if any).

        Recovering ``open_round_id`` from the persisted session is how a fresh
        per-sha orchestrator rejoins an in-flight AI round started by an earlier
        run on the same PR.
        """
        self._run = RunManifest(run_id=run_id, sha=sha, event=event, started_at=time.time())
        if run_id not in self.session.run_ids:
            self.session.run_ids.append(run_id)
        if self.session.open_round_id:
            self._round = _load(
                RoundManifest, self.store.read_json(self._round_key(self.session.open_round_id))
            )
            if self._round is not None:
                self._run.round_id = self._round.round_id
                if run_id not in self._round.run_ids:
                    self._round.run_ids.append(run_id)
                self._persist_round()
                print(
                    f"[AI sess] run {run_id} rejoins open round {self._round.round_id} "
                    f"(pr={self.pr}, iter {len(self._round.run_ids)})"
                )
        self._persist_run()
        self._persist_session()
        return self._run

    def _open_round_implicit(self, trigger):
        """Open a round automatically on the first failure with none open."""
        round_id = uuid.uuid4().hex[:12]
        failing = [c.get("name") for c in trigger.get("failing", [])]
        self._round = RoundManifest(
            round_id=round_id,
            pr=self.pr,
            opened_at=time.time(),
            goal=f"Investigate failure(s): {', '.join(failing) or 'unknown'}",
            trigger=trigger,
            run_ids=[self._run.run_id] if self._run else [],
        )
        self.session.open_round_id = round_id
        self.session.round_ids.append(round_id)
        if self._run is not None:
            self._run.round_id = round_id
        self._persist_round()
        self._persist_run()
        self._persist_session()
        self.index.record_round(
            {"pr": self.pr, "round_id": round_id, "status": "open", "cost_usd": 0.0}
        )
        print(f"[AI sess] opened round {round_id} (pr={self.pr}) goal={self._round.goal!r}")
        return self._round

    def observe_turn(self, observation, turn):
        """Record one advisor turn: implicit round open, append, roll up cost.

        This is the single entry point the OrchestratorAI calls per workflow update.
        """
        failing = [c for c in observation.changed if c.get("status") in ("failure", "cancelled")]
        if failing and self._round is None and self.session.open_round_id is None:
            self._open_round_implicit({"run_id": self._run.run_id if self._run else None,
                                       "sha": self._run.sha if self._run else "",
                                       "failing": failing})

        record = {
            "ts": time.time(),
            "run_id": self._run.run_id if self._run else None,
            "round_id": self._round.round_id if self._round else None,
            "changed": observation.changed,
            "observation": observation.to_dict(),
            **turn.to_dict(),
        }
        if self._run is not None:
            self.store.append_event(self._turns_stream(self._run.run_id), record)
            _add_usage(self._run.usage, turn.usage)
        if self._round is not None:
            _add_usage(self._round.usage, turn.usage)
        _add_usage(self.session.usage, turn.usage)

        # Console one-liner (live) + index row (queryable).
        self.console.record(observation, turn)
        self.index.record_turn(
            {
                "pr": self.pr,
                "round_id": self._round.round_id if self._round else None,
                "run_id": self._run.run_id if self._run else None,
                "provider": turn.usage.provider,
                "model": turn.usage.model,
                "cost_usd": turn.usage.cost_usd,
                "decision_types": [d.get("type") for d in turn.decision],
            }
        )

        self._persist_run()
        self._persist_round()
        self._persist_session()

    def current_round_id(self):
        """The open round's id (for commit trailers), or "" when none is open."""
        if self._round is not None:
            return self._round.round_id
        return self.session.open_round_id or ""

    def record_edit(self, patch_text, commit_sha="", files=None):
        """Record an AI-produced edit (diff + resulting commit) under the round.

        Not used by the no-op mock; this is the seam a real provider writes to
        once it can edit and push. Requires an open round.
        """
        if self._round is None or self._run is None:
            print("[AI sess] record_edit ignored: no open round")
            return None
        edit_id = uuid.uuid4().hex[:12]
        self.store.write_text(self._edit_key(self._run.run_id, edit_id), patch_text or "")
        entry = {"edit_id": edit_id, "commit_sha": commit_sha, "files": files or [], "ts": time.time()}
        self._round.edits.append(entry)
        self._persist_round()
        print(f"[AI sess] recorded edit {edit_id} ({len(files or [])} files) -> {commit_sha[:12]}")
        return edit_id

    def finalize_run(self, conclusion, job_outcomes=None):
        """Close out the current run; auto-resolve the round on a green run."""
        if self._run is None:
            return
        self._run.ended_at = time.time()
        self._run.conclusion = conclusion
        if job_outcomes is not None:
            self._run.job_outcomes = job_outcomes
        self._persist_run()
        if self._round is not None and conclusion == "success":
            self.close_round("resolved", outcome=f"run {self._run.run_id} green")
        self._persist_session()

    def close_round(self, status, outcome=""):
        if self._round is None:
            return
        self._round.status = status
        self._round.outcome = outcome
        self._round.closed_at = time.time()
        self._persist_round()
        self.session.open_round_id = None
        self._persist_session()
        self.index.record_round(
            {
                "pr": self.pr,
                "round_id": self._round.round_id,
                "status": status,
                "cost_usd": self._round.usage.get("cost_usd", 0.0),
            }
        )
        print(f"[AI sess] closed round {self._round.round_id} status={status} ({outcome})")
        self._round = None

    # ------------------------------------------------------------ budget (stub)

    def can_continue_round(self):
        """Cheap budget gate. Returns (ok, reason). Lenient stub for now."""
        cap = int(self.session.budget.get("token_cap", 0) or 0)
        spent = int(self.session.usage.get("input_tokens", 0) or 0) + int(
            self.session.usage.get("output_tokens", 0) or 0
        )
        if cap and spent >= cap:
            return False, f"PR token cap reached ({spent} >= {cap})"
        max_rounds = int(
            self.session.budget.get(
                "max_rounds", self.session.budget.get("round_max_iterations", 0)
            )
            or 0
        )
        if self._round is not None and max_rounds and len(self._round.run_ids) >= max_rounds:
            return False, (
                f"round iteration cap reached "
                f"({len(self._round.run_ids)} >= {max_rounds})"
            )
        return True, ""

    # ------------------------------------------------------------ fetch

    def pr_log(self):
        """Entire log for the PR: session + every round + every run manifest."""
        return {
            "session": dataclasses.asdict(self.session),
            "rounds": [self.store.read_json(self._round_key(r)) for r in self.session.round_ids],
            "runs": [self.store.read_json(self._run_key(r)) for r in self.session.run_ids],
        }

    def round_log(self, round_id):
        """Entire log for one round: its manifest, runs, turns, and edits."""
        rnd = self.store.read_json(self._round_key(round_id))
        if not rnd:
            return None
        runs = []
        for run_id in rnd.get("run_ids", []):
            runs.append(
                {
                    "run": self.store.read_json(self._run_key(run_id)),
                    "turns": self.store.read_events(self._turns_stream(run_id)),
                }
            )
        return {"round": rnd, "runs": runs}

    def round_context_for_prompt(self, round_id):
        """Compact memory of a round, to prepend to the next run's prompt.

        Across CI reruns the AI is a fresh process; this is how it remembers
        what it already tried so it does not repeat failed fixes. Deliberately
        small (goal, prior edits, last reasoning per run) — that compactness is
        why turns/edits are stored as structured manifests rather than raw
        artifact dumps.
        """
        log = self.round_log(round_id)
        if not log:
            return None
        rnd = log["round"]
        attempts = []
        for entry in log["runs"]:
            run = entry["run"] or {}
            turns = entry["turns"]
            last_reasoning = turns[-1].get("reasoning", "") if turns else ""
            attempts.append(
                {
                    "sha": run.get("sha", ""),
                    "conclusion": run.get("conclusion"),
                    "last_reasoning": last_reasoning,
                }
            )
        return {
            "goal": rnd.get("goal", ""),
            "status": rnd.get("status"),
            "edits": [{"commit": e.get("commit_sha", ""), "files": e.get("files", [])} for e in rnd.get("edits", [])],
            "attempts": attempts,
        }

    def cost_summary(self):
        """Cumulative cost for the PR and per round (budget/analytics seam)."""
        return {
            "pr": self.session.usage,
            "budget": self.session.budget,
            "rounds": {
                r: (self.store.read_json(self._round_key(r)) or {}).get("usage", {})
                for r in self.session.round_ids
            },
        }
