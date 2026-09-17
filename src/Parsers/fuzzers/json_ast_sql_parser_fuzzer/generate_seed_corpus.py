#!/usr/bin/env python3
"""Builds the binary seed corpus of json_ast_sql_parser_fuzzer.

Every statement of seed_queries.sql (one per line, `--` comments skipped) is serialized to the
native JSON AST with `parseQueryToJSON` by `clickhouse local`, converted to the binary protobuf
form with `json_ast_seed_converter to-proto`, and written to the corpus directory. The converter
is then run backwards (`to-json`) and the two JSON documents are compared, so a schema that cannot
express some property of a seed fails loudly here instead of silently producing a lossy seed.

Usage:
    generate_seed_corpus.py --clickhouse build/programs/clickhouse \
        --converter build_fuzz/src/Parsers/fuzzers/json_ast_sql_parser_fuzzer/json_ast_seed_converter \
        [--queries seed_queries.sql] [--output tests/fuzz/json_ast_sql_parser_fuzzer.in]

Any `clickhouse` binary works for the first step; the converter has to come from a fuzzing build
(`-DENABLE_FUZZING=1`), because the protobuf schema is only compiled there.
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile


def read_queries(path):
    with open(path, encoding="utf-8") as file:
        for line in file:
            query = line.strip()
            if query and not query.startswith("--"):
                yield query


def slug(query, index):
    first_words = "_".join(re.findall(r"[A-Za-z]+", query)[:4]).lower()
    return f"{index:03d}_{first_words[:40]}"


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    here = os.path.dirname(os.path.abspath(__file__))
    repo = os.path.abspath(os.path.join(here, "..", "..", "..", ".."))
    parser.add_argument("--clickhouse", required=True, help="path to a clickhouse binary (used as `clickhouse local`)")
    parser.add_argument("--converter", required=True, help="path to json_ast_seed_converter from a fuzzing build")
    parser.add_argument("--queries", default=os.path.join(here, "seed_queries.sql"))
    parser.add_argument("--output", default=os.path.join(repo, "tests", "fuzz", "json_ast_sql_parser_fuzzer.in"))
    parser.add_argument("--keep-existing", action="store_true", help="do not delete the current content of --output")
    args = parser.parse_args()

    if not args.keep_existing and os.path.isdir(args.output):
        shutil.rmtree(args.output)
    os.makedirs(args.output, exist_ok=True)

    failures = 0
    written = 0
    with tempfile.TemporaryDirectory() as tmp:
        for index, query in enumerate(read_queries(args.queries), start=1):
            result = subprocess.run(
                [args.clickhouse, "local", "--param_q", query, "--query", "SELECT parseQueryToJSON({q:String}) FORMAT TSVRaw"],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                failures += 1
                print(f"parseQueryToJSON failed for: {query}\n  {result.stderr.strip()}", file=sys.stderr)
                continue
            json_text = result.stdout.strip()

            json_path = os.path.join(tmp, "seed.json")
            with open(json_path, "w", encoding="utf-8") as file:
                file.write(json_text)
            # No extension: libFuzzer corpus files conventionally have none, and `*.bin` is git-ignored.
            seed_path = os.path.join(args.output, slug(query, index))
            result = subprocess.run([args.converter, "to-proto", json_path, seed_path], capture_output=True, text=True)
            if result.returncode != 0:
                failures += 1
                print(f"json_ast_seed_converter failed for: {query}\n  {result.stderr.strip()}", file=sys.stderr)
                continue

            result = subprocess.run([args.converter, "to-json", seed_path], capture_output=True, text=True)
            if result.returncode != 0 or json.loads(result.stdout) != json.loads(json_text):
                failures += 1
                print(f"the seed does not round-trip through the protobuf schema: {query}", file=sys.stderr)
                os.remove(seed_path)
                continue
            written += 1

    print(f"wrote {written} seeds to {args.output}, {failures} failures")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
