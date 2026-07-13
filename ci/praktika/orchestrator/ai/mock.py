"""Mock AI provider — makes no real decision, but exercises the interfaces.

It performs no model call and produces no actionable change (zero cost, no
edits), so the orchestrator behaviour is unchanged. Like the real providers it
reacts to **failures only** (``on_job_failure``); the other lifecycle hooks stay
the inherited no-ops. On a failure it *looks at* the observation and returns a
structured, non-actionable ``decision`` plus a reasoning string, and prints a
line — enough to verify the advisor → session → store/index flow end to end with
no real model.
"""
from .provider import AIProvider, Turn, Usage


class MockProvider(AIProvider):
    name = "mock"

    def on_job_failure(self, observation) -> Turn:
        failing = [
            c for c in observation.changed if c.get("status") in ("failure", "cancelled")
        ]
        # A no-op "note" decision plus a (still non-actionable) "propose_fix"
        # marker for the failing jobs. Nothing here is ever applied.
        decision = [{"type": "note", "summary": observation.summary}]
        if failing:
            decision.append(
                {"type": "propose_fix", "jobs": [c.get("name") for c in failing]}
            )
            reasoning = (
                f"noop(mock): would investigate {len(failing)} failing job(s): "
                + ", ".join(c.get("name", "?") for c in failing)
            )
        else:
            reasoning = "noop(mock): no failures to act on"

        print(
            f"[AI mock] on_job_failure: changed={len(observation.changed)} "
            f"failing={len(failing)} -> decision={[d['type'] for d in decision]}"
        )
        return Turn(
            reasoning=reasoning,
            decision=decision,
            usage=Usage(provider=self.name, model=self.model),
        )
