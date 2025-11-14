#!/usr/bin/env python3
import argparse
import io
import os
import subprocess
import sys
import urllib.request

try:
    from unidiff import PatchSet
except Exception as exc:
    print(
        "Missing dependency 'unidiff' (pip install unidiff): {}".format(exc),
        file=sys.stderr,
    )
    sys.exit(2)


def fetch(url: str) -> bytes:
    with urllib.request.urlopen(url) as resp:
        return resp.read()


def parse_diff_to_csv(diff_bytes: bytes) -> str:
    patch = PatchSet(diff_bytes.decode("utf-8", errors="ignore"))
    out = io.StringIO()
    out.write("filename,line\n")
    exts = (".cpp", ".cc", ".cxx", ".c", ".hpp", ".hh", ".hxx", ".h", ".ipp")
    for f in patch:
        if not f.path.endswith(exts):
            continue
        for hunk in f:
            for line in hunk:
                if line.is_added:
                    out.write("{},{}\n".format(f.path, line.target_line_no))
    return out.getvalue()


def run_query(clickhouse_path: str, csv_payload: str) -> str:
    # Use the binary itself as the DWARF source
    ch_path_sql = clickhouse_path.replace("'", "\\'")
    query = (
        """
    SELECT
        groupUniqArray(empty(linkage_name)
            ? demangle(addressToSymbol(address))
            : demangle(linkage_name) AS symbol)
    FROM file('stdin', 'CSVWithNames', 'filename String, line UInt32') AS diff
    ASOF JOIN
    (
        SELECT
            decl_file,
            decl_line,
            linkage_name,
            ranges[1].1 AS address
        FROM file('{ch_path}', 'DWARF')
        WHERE (tag = 'subprogram') AND (notEmpty(linkage_name) OR address != 0) AND notEmpty(decl_file)
    ) AS binary
    ON basename(diff.filename) = basename(binary.decl_file) AND diff.line >= binary.decl_line
    FORMAT TSV
        """.format(
            ch_path=ch_path_sql
        )
    ).strip()

    proc = subprocess.run(
        [clickhouse_path, "local", "--query", query],
        input=csv_payload,
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        print(proc.stderr, file=sys.stderr)
        raise SystemExit(proc.returncode)
    return proc.stdout


def main() -> None:
    parser = argparse.ArgumentParser(
        description="List changed symbols for a PR by parsing diff and querying ClickHouse."
    )
    parser.add_argument("pr", help="PR number")
    parser.add_argument(
        "clickhouse_path",
        help='Path to clickhouse binary (will be executed as "clickhouse local")',
    )
    args = parser.parse_args()

    diff_url = "https://patch-diff.githubusercontent.com/raw/ClickHouse/ClickHouse/pull/{}.diff".format(
        args.pr
    )
    diff_bytes = fetch(diff_url)

    csv_payload = parse_diff_to_csv(diff_bytes)

    ch_path = os.path.abspath(args.clickhouse_path)
    if not os.path.exists(ch_path):
        print("ClickHouse binary not found at {}".format(ch_path), file=sys.stderr)
        sys.exit(1)
    if not os.access(ch_path, os.X_OK) or not os.path.isfile(ch_path):
        print(
            "ClickHouse path must be an executable file: {}".format(ch_path),
            file=sys.stderr,
        )
        sys.exit(1)

    output = run_query(ch_path, csv_payload)
    sys.stdout.write(output)


if __name__ == "__main__":
    main()
