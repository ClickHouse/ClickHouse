"""Run budget from the environment: a short deterministic run per PR, a long one in fuzz mode.

| Variable                             | PR default | Meaning                                                    |
|--------------------------------------|------------|------------------------------------------------------------|
| `FOREIGN_WRITER_FUZZ_SEEDS`          | 1          | histories per writer                                       |
| `FOREIGN_WRITER_FUZZ_BASE_SEED`      | 0          | first seed; `random` draws one (printed in the log)        |
| `FOREIGN_WRITER_FUZZ_SEED_LIST`      |            | explicit comma-separated seeds, overrides the two above    |
| `FOREIGN_WRITER_FUZZ_OPS`            | 6          | operations per history                                     |
| `FOREIGN_WRITER_FUZZ_TIME_BUDGET`    | 30         | seconds per writer, checked between operations             |
| `FOREIGN_WRITER_FUZZ_SHRINK_SECONDS` | 120        | seconds to shrink a failing history, 0 to disable          |
| `FOREIGN_WRITER_FUZZ_WRITERS`        | all        | comma-separated writer names                               |
| `FOREIGN_WRITER_FUZZ_KNOWN_ISSUES`   | 0          | 1 opens the known-issue gates (see `known_issues.py`)      |
| `FOREIGN_WRITER_FUZZ_REPLAY`         |            | path to a history JSON for `test_replay`                   |
"""

import os
import random
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class Budget:
    seeds: List[int]
    ops: int
    time_budget: float
    shrink_seconds: float
    writers: Optional[List[str]]
    replay: Optional[str]

    @property
    def pytest_timeout(self):
        # Seeds are only started while the budget lasts, but one history and its shrinking can overrun it.
        return int(self.time_budget + self.shrink_seconds + 600)


def _int(name, default):
    return int(os.environ.get(name, default))


def load_budget():
    seed_list = os.environ.get("FOREIGN_WRITER_FUZZ_SEED_LIST", "").strip()
    if seed_list:
        seeds = [int(s) for s in seed_list.split(",") if s.strip()]
    else:
        base = os.environ.get("FOREIGN_WRITER_FUZZ_BASE_SEED", "0").strip()
        base = (
            random.SystemRandom().randrange(1, 10**9) if base == "random" else int(base)
        )
        seeds = [base + i for i in range(_int("FOREIGN_WRITER_FUZZ_SEEDS", 1))]
    writers = os.environ.get("FOREIGN_WRITER_FUZZ_WRITERS", "").strip()
    return Budget(
        seeds=seeds,
        ops=_int("FOREIGN_WRITER_FUZZ_OPS", 6),
        time_budget=float(os.environ.get("FOREIGN_WRITER_FUZZ_TIME_BUDGET", 30)),
        shrink_seconds=float(os.environ.get("FOREIGN_WRITER_FUZZ_SHRINK_SECONDS", 120)),
        writers=[w.strip() for w in writers.split(",") if w.strip()] or None,
        replay=os.environ.get("FOREIGN_WRITER_FUZZ_REPLAY") or None,
    )
