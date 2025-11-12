#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch cppjieba hmm_model.utf8 and produce a C++ header with:
 - constexpr double Z = -3.14e+100;
 - start_prob as single-line std::array<double,4>
 - trans_prob as single-line std::array<std::array<double,4>,4>
 - emit_probs as single-line std::array<std::array<double,65536>,4> where
   entries not present in the source are output as Z (the symbol), and
   present entries use the original numeric literal from the file.
"""
import requests
import sys

URL = "https://raw.githubusercontent.com/yanyiwu/cppjieba/refs/heads/master/dict/hmm_model.utf8"

STATE_ORDER = ["B", "E", "M", "S"]
STATE_COUNT = 4
MAX_RUNE = 65536
Z_LITERAL = "Z"  # symbol to output for missing values
Z_VALUE = "-3.14e+100"  # numeric value used in original model (kept as comment)


def fetch_lines(url):
    r = requests.get(url)
    r.raise_for_status()
    # preserve original lines without altering spacing
    return r.text.splitlines()


def parse_model(lines):
    # parse start_prob (keep string tokens)
    start_prob = None
    for i, ln in enumerate(lines):
        if ln.strip().startswith("#prob_start"):
            # the next non-empty line is the start probs
            for j in range(i + 1, len(lines)):
                s = lines[j].strip()
                if s:
                    start_prob = s.split()
                    break
            break
    if start_prob is None:
        raise RuntimeError("start_prob not found")

    # parse trans_prob 4x4 (keep strings)
    trans_prob = []
    for i, ln in enumerate(lines):
        if ln.strip().startswith("#prob_trans"):
            # read next 4 non-empty lines
            k = i + 1
            while len(trans_prob) < STATE_COUNT and k < len(lines):
                s = lines[k].strip()
                if s:
                    trans_prob.append(s.split())
                k += 1
            break
    if len(trans_prob) != STATE_COUNT:
        raise RuntimeError("trans_prob not found or incomplete")

    # parse emit blocks for states B/E/M/S
    # mapping: state -> list of non-comment lines (each line may contain many items)
    emit_lines_for_state = {st: [] for st in STATE_ORDER}
    cur_state = None
    for ln in lines:
        s = ln.rstrip("\n")
        stripped = s.strip()
        # state header lines are like "#B" or "#E" etc.
        if (
            stripped.startswith("#")
            and stripped[1:] in STATE_ORDER
            and (
                stripped == f"#{stripped[1:]}"
                or stripped.startswith(f"#{stripped[1:]}:") == False
            )
        ):
            # set current state
            cur_state = stripped[1:]
            continue
        # skip other comment headers like #prob_emit etc.
        if stripped.startswith("#"):
            # If it's a line like "#B:" or "#E:..." (not seen in sample), ignore here
            continue
        if cur_state:
            # Only non-empty non-# lines that belong to current state
            if stripped:
                emit_lines_for_state[cur_state].append(stripped)

    # Build per-state emit dicts: codepoint -> prob_string
    # We'll keep the prob as the original string token (no float conversion)
    emit_maps = []
    for st in STATE_ORDER:
        mp = {}
        for ln in emit_lines_for_state.get(st, []):
            # items separated by commas, each item like "蘄:-11.015514"
            parts = ln.split(",")
            for item in parts:
                if not item:
                    continue
                # split on first colon only
                if ":" not in item:
                    continue
                rune, prob = item.split(":", 1)
                rune = rune.strip()
                prob = prob.strip()
                if rune == "":
                    continue
                # map rune character to codepoint; treat rune as one Unicode codepoint
                # If rune contains more than one UTF-16 code unit (surrogate pair) ord still works
                codepoint = ord(rune)
                mp[codepoint] = prob
        emit_maps.append(mp)

    # sanity check: emit_maps should have 4 entries
    if len(emit_maps) != STATE_COUNT:
        raise RuntimeError("emit maps incomplete")

    return start_prob, trans_prob, emit_maps


def format_cpp(start_prob, trans_prob, emit_maps, out_path):
    # Write single-line C++ definitions
    with open(out_path, "w", encoding="utf-8") as f:
        # start_prob single line
        f.write("static constexpr double start_prob[4] = {")
        f.write(",".join(start_prob))
        f.write("};\n")

        # trans_prob single line
        f.write("static constexpr double trans_prob[4][4] = {")
        rows = []
        for row in trans_prob:
            rows.append("{" + ",".join(row) + "}")
        f.write(",".join(rows))
        f.write("};\n")

        # emit_probs: each state a single-line array of 65536 entries.
        # For entries present in emit_maps we use the original prob string,
        # for missing entries we use the symbol Z (not numeric).
        f.write(
            f"static constexpr double emit_probs[4][{MAX_RUNE}] = {{"
        )
        state_chunks = []
        for s_idx, mp in enumerate(emit_maps):
            # build list of string tokens for this state
            # to avoid building huge intermediate lists of strings all at once in memory
            # we will stream join in chunks (but here we can build full string; memory ok)
            vals = []
            # iterate all codepoints 0..65535
            for cp in range(MAX_RUNE):
                if cp in mp:
                    vals.append(mp[cp])
                else:
                    vals.append(Z_LITERAL)
            state_chunks.append("{" + ",".join(vals) + "}")
        f.write(",".join(state_chunks))
        f.write("};\n")


def main():
    try:
        lines = fetch_lines(URL)
        start_prob, trans_prob, emit_maps = parse_model(lines)
        out_path = "jieba_hmm_model.dat"
        format_cpp(start_prob, trans_prob, emit_maps, out_path)
        print(f"Wrote {out_path}")
    except Exception as e:
        print("Error:", e, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
