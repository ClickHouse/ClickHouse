#!/usr/bin/env python3

import argparse
import os
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument("-i", "--input-dir")
parser.add_argument("-n", "--dry-run", action="store_true")
parser.add_argument("-t", "--table")
args = parser.parse_args()

broken_parts = []
parts_to_repair = []

subprocess.check_call(["./clickhouse", "client", "-q", "SYSTEM STOP MERGES {}".format(args.table)])

for file in os.listdir(args.input_dir):
    name_parts = file.split("_")
    is_part_name = len(name_parts) == 4 or len(name_parts) == 5

    if not is_part_name:
        continue

    try:
        res = subprocess.run(["./clickhouse", "restore-parts-serialization", "-n", "-i", os.path.join(args.input_dir, file)])
    except Exception as e:
        broken_parts.append(file)
        print("Failed to check part: {}, error: {}".format(file, e))
        continue

    if (res.returncode == 1):
        broken_parts.append(file)
        print("Part {} is broken and cannot be repaired".format(file))
    elif (res.returncode == 2):
        parts_to_repair.append(file)
    elif (res.returncode == 127):
        broken_parts.append(file)
        print("Failed to check part: {}".format(file))

    print()


print("Parts to repair: {}".format(parts_to_repair))
print("Broken parts: {}".format(broken_parts))

repaired_parts = []

if not args.dry_run and len(parts_to_repair) != 0:
    for part in parts_to_repair:
        print()
        is_active = subprocess.check_output(["./clickhouse", "client", "-q", "SELECT active FROM system.parts WHERE table = '{}' AND name = '{}'".format(args.table, part)]).decode("utf-8").strip()
        if is_active == "0":
            print("Part {} is not active, skipping".format(part))
            continue

        try:
            subprocess.check_call(["./clickhouse", "client", "ALTER TABLE {} DETACH PART '{}'".format(args.table, part)])
            subprocess.check_call(["./clickhouse", "restore-parts-serialization", "-i", os.path.join(args.input_dir, "detached", part)])
            subprocess.check_call(["rm", os.path.join(args.input_dir, "detached", part, "checksums.txt")])
            subprocess.check_call(["./clickhouse", "client", "ALTER TABLE {} ATTACH PART '{}'".format(args.table, part)])
            repaired_parts.append(part)
        except Exception as e:
            print("Failed to repair part: {}, error: {}".format(part, e))

    print()
    print("Parts repaired: {}".format(repaired_parts))
