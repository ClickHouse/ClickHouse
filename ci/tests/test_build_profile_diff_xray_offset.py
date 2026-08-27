"""
Tests that `Build profile diff` does not report the known debug-info offset of
the stripped binary as a change of the pull request.

The check's headline size row compares the pull request's `clickhouse-stripped`
against the official master `Build (arm_release)`, and the two are compiled with
different debug-info flags: a pull request build passes
`-DDISABLE_ALL_DEBUG_SYMBOLS=1` and the official build does not
(ci/jobs/build_clickhouse.py). `strip --strip-debug` does not undo that,
because debug info reaches the code itself through XRay: whether a loop-free
function is instrumented is decided by counting MachineInstrs with the debug
pseudo-instructions included (`MICount += MBB.size()` in
llvm/lib/CodeGen/XRayInstrumentation.cpp), so with `-g` thousands of functions
just under the 200-instruction threshold get entry/exit sleds that the pull
request build never emits.

Measured on master 9d8eed34c114 against pull request 116614 (a Web UI change):
11028 extra instrumented functions in the official build, 3,206,880 bytes of a
746,549,896-byte stripped binary - `xray_instr_map` +925,024, `xray_fn_idx`
+176,448, `.text` +989,952 of sled NOPs, `.symtab` +1,059,096 and `.strtab`
+39,895 for the `.Lxray_*` and `$d` symbols the new per-function sections add.
The sizes below are those two real builds.

The compiler side is fixed in llvm/llvm-project#219100.

Every pull request therefore used to show a "-3.06 MiB" headline row that no
pull request could influence. These tests pin the suppression, and - just as
importantly - pin that it never swallows a delta pointing the other way or one
large enough to be flagged.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.build_profile_diff_job import (
    BINARY_SIG_BYTES,
    HEADLINE_BINARIES,
    XRAY_DEBUG_OFFSET_RATIO,
    Side,
    compare_binaries,
)

# The measured official master build of programs/clickhouse-stripped.
MASTER_SIZE = 746549896
# The measured pull request build of the same file: 3,206,880 bytes smaller.
PR_SIZE = 743343016

BINARY = HEADLINE_BINARIES[0]


class FakeDb:
    """Answers `compare_binaries`'s single size query with canned rows."""

    def __init__(self, pr_size, base_size):
        self.pr_size = pr_size
        self.base_size = base_size
        self.queries = []

    def query(self, query):
        self.queries.append(query)
        if self.pr_size is None and self.base_size is None:
            return []
        return [
            {
                "file": BINARY,
                "pr_size": str(self.pr_size or 0),
                "base_size": str(self.base_size or 0),
            }
        ]


def side(pr_number, sha):
    return Side("date >= today() - 7", pr_number, sha, "2026-08-27 01:23:35", "i-1")


def compare(pr_size, base_size):
    db = FakeDb(pr_size, base_size)
    return compare_binaries(db, side(116614, "84f67fe3"), side(0, "9d8eed34"))


def test_measured_offset_is_not_shown():
    """The real -3.06 MiB delta of a no-op pull request is not reported."""
    section = compare(PR_SIZE, MASTER_SIZE)
    assert not section.significant
    assert not section.summary
    # No number, no table: neither the delta nor the two sizes are rendered.
    assert "MiB" not in section.body
    assert "| Binary |" not in section.body
    # But the binary is named, with the reason.
    assert "clickhouse-stripped" in section.body
    assert "XRay" in section.body


def test_offset_band_covers_the_measurement_with_headroom():
    """The band is wide enough for the measured offset and stays under 1%."""
    measured_ratio = (MASTER_SIZE - PR_SIZE) / MASTER_SIZE
    assert measured_ratio < XRAY_DEBUG_OFFSET_RATIO < 0.01
    assert XRAY_DEBUG_OFFSET_RATIO > 1.5 * measured_ratio


def test_a_bigger_binary_is_always_shown():
    """A growing binary cannot be the offset - the official build is the larger one."""
    section = compare(MASTER_SIZE + (1 << 20), MASTER_SIZE)
    assert "| Binary |" in section.body
    assert "+1.00 MiB" in section.body
    assert not section.significant


def test_a_shrink_beyond_the_band_is_shown():
    """More shrinkage than the offset explains is a real signal, even if unflagged."""
    delta = -int(MASTER_SIZE * XRAY_DEBUG_OFFSET_RATIO) - (1 << 20)
    assert abs(delta) < BINARY_SIG_BYTES  # deliberately below the flagging bar
    section = compare(MASTER_SIZE + delta, MASTER_SIZE)
    assert "| Binary |" in section.body
    assert not section.significant


def test_a_significant_shrink_is_never_hidden():
    """Significance is decided before the offset band, so a flagged row survives."""
    section = compare(MASTER_SIZE - (64 << 20), MASTER_SIZE)
    assert section.significant
    assert "| Binary |" in section.body
    assert "clickhouse-stripped: -64.00 MiB" in section.summary


def test_lost_pr_side_row_still_fails_the_check():
    """The offset path must not turn a lost size row into an all-green omission."""
    section = compare(None, None)
    assert section.significant
    assert "missing PR-side size data" in section.summary
