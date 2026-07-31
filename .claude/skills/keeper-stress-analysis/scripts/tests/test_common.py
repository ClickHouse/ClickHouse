"""Tests for the small pure functions in `_common`. Stdlib `unittest` only,
no extra deps. Run with:

    cd .claude/skills/keeper-stress-analysis/scripts
    python3 -m unittest tests.test_common -v

These two functions are the riskiest pieces of the rubric:

  - `classify` encodes the per-metric significance bands documented in
    `references/methodology.md`. A regression here changes every per-PR
    verdict.
  - `iso_week` underpins the PR-branch pool widening across year/W01/W52-53
    boundaries. A regression here silently shrinks the pool for any PR
    landing in the first or last ISO week of a year.
"""
import datetime
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from _common import classify, common_prefix, iso_week, parse_merged_at  # noqa: E402


class ClassifyBandsTest(unittest.TestCase):
    """Mirror the rubric in `references/methodology.md`. If these
    expectations and the doc drift apart, change one to match the other —
    the rubric is the binding contract.
    """

    # ---- rps (higher_better, 5/15) ----

    def test_rps_clean_within_band(self):
        # 5 % drop sits exactly on the clean/watch boundary; clean wins.
        self.assertEqual(classify("rps", 100.0, 95.0), "clean")

    def test_rps_watch_band(self):
        # 10 % drop is in watch (-5 %..-15 %).
        self.assertEqual(classify("rps", 100.0, 90.0), "watch")

    def test_rps_regression_below_watch(self):
        # 16 % drop crosses regression.
        self.assertEqual(classify("rps", 100.0, 84.0), "regression")

    def test_rps_clean_when_improved(self):
        # Higher is better — improvements stay clean.
        self.assertEqual(classify("rps", 100.0, 120.0), "clean")

    # ---- read_p99_ms (lower_better, 10/30 — wider band than rps) ----

    def test_read_p99_clean_within_wider_band(self):
        # +9 % rise on p99 is still clean (band is 10 %, not 5 %).
        self.assertEqual(classify("read_p99_ms", 10.0, 10.9), "clean")

    def test_read_p99_watch_band(self):
        # +20 % rise on p99 is watch (10 %..30 %).
        self.assertEqual(classify("read_p99_ms", 10.0, 12.0), "watch")

    def test_read_p99_regression_above_watch(self):
        # +35 % rise on p99 crosses regression.
        self.assertEqual(classify("read_p99_ms", 10.0, 13.5), "regression")

    # ---- peak_mem_gb (lower_better, 10/30) ----

    def test_peak_mem_watch_band(self):
        # +25 % memory rise is watch.
        self.assertEqual(classify("peak_mem_gb", 1.0, 1.25), "watch")

    # ---- error_pct (absolute PP, not relative %) ----

    def test_error_pct_small_rise_is_clean(self):
        # +0.04 PP rise is below the 0.05 PP noise threshold.
        self.assertEqual(classify("error_pct", 0.0, 0.04), "clean")

    def test_error_pct_mid_rise_is_watch(self):
        # +0.1 PP is in (0.05, 0.5) → watch.
        self.assertEqual(classify("error_pct", 0.0, 0.1), "watch")

    def test_error_pct_large_rise_is_regression(self):
        # +0.6 PP crosses regression.
        self.assertEqual(classify("error_pct", 0.0, 0.6), "regression")

    # ---- edge cases ----

    def test_no_data_when_pre_or_post_missing(self):
        self.assertEqual(classify("rps", None, 100.0), "no-data")
        self.assertEqual(classify("rps", 100.0, None), "no-data")

    def test_pre_zero_clean_when_post_also_zero(self):
        self.assertEqual(classify("rps", 0.0, 0.0), "clean")

    def test_pre_zero_watch_when_post_nonzero(self):
        self.assertEqual(classify("rps", 0.0, 5.0), "watch")


class IsoWeekTest(unittest.TestCase):
    """Verify the year-boundary cases that the post-`4692e7` widening
    fix relies on. A run on Mon 2026-01-05 (`2026-W02`) should look
    back to Mon 2025-12-29 (`2026-W01` per ISO calendar quirks) and
    forward to Mon 2026-01-12 (`2026-W03`); a run in `2025-W52` should
    cleanly cross into `2026-W01` etc.
    """

    @staticmethod
    def _day(y, m, d):
        return datetime.datetime(y, m, d, tzinfo=datetime.timezone.utc)

    def test_format_is_yyyy_dash_w_two_digits(self):
        self.assertEqual(iso_week(self._day(2026, 4, 1)), "2026-W14")

    def test_year_rollover_backward(self):
        # 2026-01-05 is 2026-W02 by ISO. -7 days lands 2025-12-29 which
        # ISO also calls 2026-W01 (Mon-of-week-that-contains-Jan-1).
        d = self._day(2026, 1, 5)
        self.assertEqual(iso_week(d), "2026-W02")
        self.assertEqual(iso_week(d - datetime.timedelta(days=7)), "2026-W01")
        self.assertEqual(iso_week(d - datetime.timedelta(days=14)), "2025-W52")

    def test_year_rollover_forward(self):
        # 2025-12-29 is the start of 2026-W01 by ISO.
        d = self._day(2025, 12, 29)
        self.assertEqual(iso_week(d), "2026-W01")
        self.assertEqual(iso_week(d + datetime.timedelta(days=7)), "2026-W02")

    def test_w53_year(self):
        # 2026 has 53 ISO weeks. 2026-12-31 is in W53.
        self.assertEqual(iso_week(self._day(2026, 12, 31)), "2026-W53")


class CommonPrefixTest(unittest.TestCase):
    """Used by `build_commit_diff`'s SHA-typo suggestion ranking. A bug here
    silently degrades suggestion quality without raising — catch it in tests.
    """

    def test_no_overlap_returns_empty(self):
        self.assertEqual(common_prefix("abc", "xyz"), "")

    def test_partial_overlap(self):
        self.assertEqual(common_prefix("fdf46xxx", "fdf46ee1"), "fdf46")

    def test_identical_strings(self):
        self.assertEqual(common_prefix("740b4a58", "740b4a58"), "740b4a58")

    def test_one_is_prefix_of_other(self):
        # First arg is the canonical "stored" string returned by the function.
        self.assertEqual(common_prefix("abcd", "ab"), "ab")
        self.assertEqual(common_prefix("ab", "abcd"), "ab")

    def test_empty_inputs(self):
        self.assertEqual(common_prefix("", "abc"), "")
        self.assertEqual(common_prefix("abc", ""), "")
        self.assertEqual(common_prefix("", ""), "")


class ParseMergedAtTest(unittest.TestCase):
    """`parse_merged_at` is the single guard for empty `mergedAt` in
    `pr_meta.tsv`. `gh pr list --state all` returns open PRs with empty
    `mergedAt`, so the loader must skip them with a clear message instead of
    aborting the pipeline with a `fromisoformat` exception.
    """

    def test_returns_none_for_empty_string(self):
        self.assertIsNone(parse_merged_at({"mergedAt": ""}))

    def test_returns_none_for_whitespace_only(self):
        self.assertIsNone(parse_merged_at({"mergedAt": "   "}))

    def test_returns_none_for_missing_key(self):
        self.assertIsNone(parse_merged_at({}))

    def test_parses_zulu_suffix(self):
        # `gh pr view` emits Z-suffixed UTC timestamps. The result must be
        # tz-aware so it compares cleanly to `KEEPER_SKILL_THRESHOLD`.
        dt = parse_merged_at({"mergedAt": "2026-04-15T10:23:00Z"})
        self.assertEqual(dt.year, 2026)
        self.assertEqual(dt.month, 4)
        self.assertEqual(dt.day, 15)
        self.assertEqual(dt.tzinfo, datetime.timezone.utc)


class ThresholdEndTest(unittest.TestCase):
    """The `KEEPER_SKILL_THRESHOLD_END` upper bound is the linchpin of the
    "between A and B" date-range workflow. The default must be far-future so
    single-arg invocations preserve byte-identical output; an explicit value
    must parse to the expected datetime so the in-window filter actually
    truncates.
    """

    def test_default_threshold_end_is_far_future(self):
        # Reload `_common` with no env var set — the default must be past any
        # `merged_dt` we'd ever see in practice. `9999-12-31` is the sentinel.
        import importlib
        import os

        prev = os.environ.pop("KEEPER_SKILL_THRESHOLD_END", None)
        try:
            import _common
            importlib.reload(_common)
            self.assertGreater(_common.KEEPER_SKILL_THRESHOLD_END.year, 2100)
        finally:
            if prev is not None:
                os.environ["KEEPER_SKILL_THRESHOLD_END"] = prev
            import _common
            importlib.reload(_common)

    def test_explicit_threshold_end_parses_to_utc_datetime(self):
        # Setting the env var must yield a tz-aware UTC datetime so it is
        # comparable to the tz-aware `merged_dt` values from `pr_meta.tsv`.
        import importlib
        import os

        prev = os.environ.get("KEEPER_SKILL_THRESHOLD_END")
        os.environ["KEEPER_SKILL_THRESHOLD_END"] = "2026-04-15"
        try:
            import _common
            importlib.reload(_common)
            self.assertEqual(_common.KEEPER_SKILL_THRESHOLD_END.year, 2026)
            self.assertEqual(_common.KEEPER_SKILL_THRESHOLD_END.month, 4)
            self.assertEqual(_common.KEEPER_SKILL_THRESHOLD_END.day, 15)
            self.assertEqual(_common.KEEPER_SKILL_THRESHOLD_END.tzinfo,
                             datetime.timezone.utc)
        finally:
            if prev is None:
                os.environ.pop("KEEPER_SKILL_THRESHOLD_END", None)
            else:
                os.environ["KEEPER_SKILL_THRESHOLD_END"] = prev
            import _common
            importlib.reload(_common)


if __name__ == "__main__":
    unittest.main(verbosity=2)
