"""Console output + cost accounting for AI advisor turns.

Persistence of turns moved to ``SessionManager`` + ``SessionStore`` (durable,
per-PR/round/run). ``TraceLogger`` is now purely the live console view — a
compact one-liner per turn (visible in ``journalctl -fu praktika-controller``)
and an end-of-run total. ``UsageLedger`` accumulates the per-process totals.
"""
from .provider import Usage


class UsageLedger:
    """Running totals of AI usage across a single orchestrator process."""

    def __init__(self):
        self.turns = 0
        self.input_tokens = 0
        self.output_tokens = 0
        self.cost_usd = 0.0

    def add(self, usage: Usage):
        self.turns += 1
        self.input_tokens += usage.input_tokens
        self.output_tokens += usage.output_tokens
        self.cost_usd += usage.cost_usd

    def to_dict(self):
        return {
            "turns": self.turns,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "cost_usd": self.cost_usd,
        }


class TraceLogger:
    """Stdout view of advisor turns."""

    def __init__(self, run_id=None):
        self.run_id = run_id
        self._turn_no = 0

    def record(self, observation, turn):
        self._turn_no += 1
        changed = ",".join(
            f"{c.get('name')}:{c.get('status')}" for c in observation.changed
        )
        u = turn.usage
        reasoning = (turn.reasoning or "").replace("\n", " ")[:120]
        decisions = ",".join(d.get("type", "?") for d in turn.decision) or "-"
        err = f" error={turn.error}" if turn.error else ""
        print(
            f"[AI   ] turn={self._turn_no} changed=[{changed}] "
            f"provider={u.provider or '?'} model={u.model or '?'} "
            f"decision=[{decisions}] reasoning='{reasoning}' "
            f"tokens={u.input_tokens}/{u.output_tokens} cost=${u.cost_usd:.4f}{err}"
        )

    def summary(self, ledger: UsageLedger):
        print(
            f"[AI   ] run total: turns={ledger.turns} "
            f"tokens={ledger.input_tokens}/{ledger.output_tokens} "
            f"cost=${ledger.cost_usd:.4f}"
        )
