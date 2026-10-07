#!/usr/bin/env python3

import argparse
import math
import statistics
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple

try:
    from scipy import stats
except ImportError:
    stats = None


def tsv_escape(value: object) -> str:
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace("\t", "\\t")
        .replace("\n", "\\n")
        .replace("\r", "")
    )


def tsv_unescape(value: str) -> str:
    result = []
    escaped = False
    for ch in value:
        if escaped:
            if ch == "t":
                result.append("\t")
            elif ch == "n":
                result.append("\n")
            elif ch == "\\":
                result.append("\\")
            else:
                result.append(ch)
            escaped = False
        elif ch == "\\":
            escaped = True
        else:
            result.append(ch)
    if escaped:
        result.append("\\")
    return "".join(result)


@dataclass
class PerfTsv:
    display_names: Dict[int, str] = field(default_factory=dict)
    samples_by_query: Dict[int, List[float]] = field(default_factory=lambda: defaultdict(list))


def parse_perf_tsv(path: str, conn_index: int) -> PerfTsv:
    parsed = PerfTsv()
    with open(path, "r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, 1):
            line = raw_line.rstrip("\n")
            if not line:
                continue

            fields = line.split("\t")
            tag = fields[0]

            if tag == "display-name":
                if len(fields) < 3:
                    warn(path, line_number, "ignoring malformed display-name row")
                    continue
                query_index = parse_int(fields[1], path, line_number, "query index")
                if query_index is None:
                    continue
                parsed.display_names[query_index] = tsv_unescape(fields[2])
                continue

            if tag == "query":
                if len(fields) < 5:
                    warn(path, line_number, "ignoring malformed query row")
                    continue
                query_index = parse_int(fields[1], path, line_number, "query index")
                row_conn_index = parse_int(fields[3], path, line_number, "connection index")
                elapsed = parse_float(fields[4], path, line_number, "elapsed seconds")
                if query_index is None or row_conn_index is None or elapsed is None:
                    continue
                if row_conn_index == conn_index:
                    parsed.samples_by_query[query_index].append(elapsed)

    return parsed


def warn(path: str, line_number: int, message: str) -> None:
    print(f"{path}:{line_number}: {message}", file=sys.stderr)


def parse_int(value: str, path: str, line_number: int, label: str) -> Optional[int]:
    try:
        return int(value)
    except ValueError:
        warn(path, line_number, f"ignoring row with invalid {label}: {value!r}")
        return None


def parse_float(value: str, path: str, line_number: int, label: str) -> Optional[float]:
    try:
        return float(value)
    except ValueError:
        warn(path, line_number, f"ignoring row with invalid {label}: {value!r}")
        return None


def median(samples: List[float]) -> Optional[float]:
    if not samples:
        return None
    return statistics.median(samples)


def welch_ttest_pvalue(left: List[float], right: List[float], min_samples: int) -> Optional[float]:
    if stats is None or len(left) < min_samples or len(right) < min_samples:
        return None
    p_value = stats.ttest_ind(left, right, equal_var=False).pvalue
    if p_value is None or math.isnan(p_value):
        return None
    return float(p_value)


def status_for(
    relative_change: Optional[float],
    p_value: Optional[float],
    min_relative_change: float,
    alpha: float,
) -> str:
    if relative_change is None:
        return "missing"
    if abs(relative_change) < min_relative_change:
        return "same"
    if p_value is None or p_value > alpha:
        return "unstable"
    return "faster" if relative_change < 0 else "slower"


def fmt_float(value: Optional[float]) -> str:
    if value is None:
        return ""
    return f"{value:.12g}"


def iter_index_matches(baseline: PerfTsv, contender: PerfTsv) -> Iterable[Tuple[str, int, Optional[int]]]:
    indexes = set(baseline.samples_by_query) | set(contender.samples_by_query)
    indexes |= set(baseline.display_names) | set(contender.display_names)
    for query_index in sorted(indexes):
        yield str(query_index), query_index, query_index


def display_name_index(parsed: PerfTsv) -> Dict[str, List[int]]:
    by_name: Dict[str, List[int]] = defaultdict(list)
    for query_index, display_name in parsed.display_names.items():
        by_name[display_name].append(query_index)
    return by_name


def iter_display_name_matches(
    baseline: PerfTsv, contender: PerfTsv
) -> Iterable[Tuple[str, Optional[int], Optional[int]]]:
    baseline_by_name = display_name_index(baseline)
    contender_by_name = display_name_index(contender)
    names = set(baseline_by_name) | set(contender_by_name)
    for display_name in sorted(names):
        baseline_indexes = baseline_by_name.get(display_name, [])
        contender_indexes = contender_by_name.get(display_name, [])
        baseline_index = baseline_indexes[0] if len(baseline_indexes) == 1 else None
        contender_index = contender_indexes[0] if len(contender_indexes) == 1 else None
        if baseline_index is None and contender_index is None:
            query_index = ""
        elif baseline_index is None:
            query_index = f"/{contender_index}"
        elif contender_index is None:
            query_index = f"{baseline_index}/"
        elif baseline_index == contender_index:
            query_index = str(baseline_index)
        else:
            query_index = f"{baseline_index}/{contender_index}"
        yield query_index, baseline_index, contender_index


def compare(args: argparse.Namespace) -> None:
    baseline = parse_perf_tsv(args.baseline, args.baseline_conn)
    contender = parse_perf_tsv(args.contender, args.contender_conn)

    print(
        "\t".join(
            [
                "query_index",
                "display_name",
                "baseline_runs",
                "contender_runs",
                "baseline_median",
                "contender_median",
                "relative_change",
                "p_value",
                "status",
            ]
        )
    )

    if args.match_by == "display-name":
        matches = iter_display_name_matches(baseline, contender)
    else:
        matches = iter_index_matches(baseline, contender)

    for query_index, baseline_index, contender_index in matches:
        baseline_samples = (
            baseline.samples_by_query.get(baseline_index, []) if baseline_index is not None else []
        )
        contender_samples = (
            contender.samples_by_query.get(contender_index, []) if contender_index is not None else []
        )
        baseline_median = median(baseline_samples)
        contender_median = median(contender_samples)

        if baseline_median is None or contender_median is None or baseline_median == 0:
            relative_change = None
            p_value = None
        else:
            relative_change = (contender_median - baseline_median) / baseline_median
            p_value = welch_ttest_pvalue(
                baseline_samples, contender_samples, args.min_samples_for_ttest
            )

        status = status_for(relative_change, p_value, args.min_relative_change, args.alpha)
        display_name = ""
        if baseline_index is not None:
            display_name = baseline.display_names.get(baseline_index, "")
        if not display_name and contender_index is not None:
            display_name = contender.display_names.get(contender_index, "")

        print(
            "\t".join(
                [
                    tsv_escape(query_index),
                    tsv_escape(display_name),
                    str(len(baseline_samples)),
                    str(len(contender_samples)),
                    fmt_float(baseline_median),
                    fmt_float(contender_median),
                    fmt_float(relative_change),
                    fmt_float(p_value),
                    status,
                ]
            )
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare two saved ClickHouse tests/performance/scripts/perf.py raw TSV outputs."
    )
    parser.add_argument("--baseline", required=True, help="Baseline perf.py raw TSV file.")
    parser.add_argument("--contender", required=True, help="Contender perf.py raw TSV file.")
    parser.add_argument(
        "--baseline-conn",
        type=int,
        default=0,
        help="Connection index to read from the baseline TSV. Default: 0.",
    )
    parser.add_argument(
        "--contender-conn",
        type=int,
        default=0,
        help="Connection index to read from the contender TSV. Default: 0.",
    )
    parser.add_argument(
        "--match-by",
        choices=("index", "display-name"),
        default="index",
        help="Match query rows by XML-expanded query index or display name. Default: index.",
    )
    parser.add_argument(
        "--min-relative-change",
        type=float,
        default=0.03,
        help="Classify smaller median changes as same. Default: 0.03.",
    )
    parser.add_argument(
        "--alpha",
        type=float,
        default=0.05,
        help="p-value threshold for faster/slower status when enough samples exist. Default: 0.05.",
    )
    parser.add_argument(
        "--min-samples-for-ttest",
        type=int,
        default=3,
        help="Minimum samples per side before emitting a Welch t-test p-value. Default: 3.",
    )
    return parser


def main() -> None:
    compare(build_parser().parse_args())


if __name__ == "__main__":
    main()
