"""Run the configured DWYU analyzer on complete, verified DwyuProcessSourceFile pairs."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import sys
import tempfile


def require(ok, message):
    if not ok:
        raise ValueError(message)


def relative(value):
    path = PurePosixPath(value)
    require(path.parts and not path.is_absolute() and ".." not in path.parts and str(path) == value,
            "invalid execroot path: " + value)
    return path


def checked(path, wanted):
    require(path.is_file(), "missing file: " + str(path))
    require(path.stat().st_size == wanted["size"], "size mismatch: " + str(path))
    with path.open("rb") as stream:
        require(hashlib.file_digest(stream, "sha256").hexdigest() == wanted["sha256"], "digest mismatch: " + str(path))


def run(plan_path, plan_digest, files_path, result):
    raw = plan_path.read_bytes()
    require(hashlib.sha256(raw).hexdigest() == plan_digest, "partition plan digest mismatch")
    plan = json.loads(raw)
    files = json.loads(files_path.read_bytes())
    require(plan["schema"] == 1 and files["arch"] == plan["arch"] and files["owner"] == plan["owner"]
            and files["exe"] == plan["exe"], "configured owner, architecture or executable mismatch")
    outputs = [p for values in plan["groups"].values() for p in values]
    pairs = [p for row in plan["roots"] for p in (row["public"], row["private"])]
    observations = set(pairs)
    require(Counter(outputs) == Counter(pairs) and len(observations) == len(pairs), "omitted or duplicate observation")
    require(all(row["public"] in plan["groups"][row["group"]] and row["private"] in plan["groups"][row["group"]]
                for row in plan["roots"]), "split public/private output pair")
    assigned = plan["inputs"]
    require(set(pairs) == {p for p, v in assigned.items() if v["group"] != "dwyu_analyzer_inputs"}, "unexpected or omitted PSF")
    require(all(assigned[p]["group"] == group for group, paths in plan["groups"].items() for p in paths), "wrong group")
    support = set(files["support"])
    require(support == {p for p, v in assigned.items() if v["group"] == "dwyu_analyzer_inputs"}, "original support files changed")
    required = set(plan["support"])
    exe = plan["exe"]
    tree = exe + ".runfiles"
    require(exe in required and required - {tree} <= support and not required.intersection(pairs), "incorrect analyzer inputs")
    blobs = files["blobs"]
    require(set(blobs) == {assigned[p]["sha256"] for p in pairs}, "missing or unexpected imported blob")
    with tempfile.TemporaryDirectory(prefix="dwyu-") as tmp:
        root = Path(tmp)

        def bind(name, src):
            to = root / relative(name)
            to.parent.mkdir(parents=True, exist_ok=True)
            to.symlink_to(src.resolve(strict=True))

        for name, item in assigned.items():
            require(re.fullmatch(r"[a-f0-9]{64}", item["sha256"]), "invalid artifact SHA-256")
            src = Path(relative(name)) if name in support else Path(relative(blobs[item["sha256"]]))
            checked(src, item)
            if name in observations or name in required:
                bind(name, src)
        if tree in required:
            supplied = Path(relative(tree))
            require(supplied.is_dir() and "runfiles" in plan, "missing original analyzer runfiles")
            actual = {f.relative_to(supplied).as_posix() for f in supplied.rglob("*") if f.is_file()}
            require(actual == set(plan["runfiles"]), "wrong analyzer runfiles")
            for f in actual:
                checked(supplied / relative(f), plan["runfiles"][f])
            bind(tree, supplied)
        argv = plan["argv"]
        require(argv and argv[0] == exe, "wrong analyzer argv")
        for param in plan["parameters"]:
            name = param["execPath"]
            require(argv.count("--param_file=" + name) + argv.count("@" + name) == 1, "unbound parameter file")
            require(all("\n" not in s and "\r" not in s for s in param["arguments"]), "invalid multiline parameter file")
            path = root / relative(name)
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("x") as f:
                f.write("".join(s + "\n" for s in param["arguments"]))
        out = root / relative(plan["report"])
        require(not out.exists(), "report aliases an input")
        out.parent.mkdir(parents=True, exist_ok=True)
        status = subprocess.run(argv, cwd=root, env=plan["environment"]).returncode
        if out.is_file():
            shutil.copyfile(out, result)
        elif status == 0:
            raise ValueError("original analyzer returned zero without a report")
        return status


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("plan", type=Path)
    parser.add_argument("sha256")
    parser.add_argument("files", type=Path)
    parser.add_argument("report", type=Path)
    args = parser.parse_args()
    try:
        sys.exit(run(args.plan, args.sha256, args.files, args.report))
    except (KeyError, OSError, ValueError) as error:
        sys.exit("DWYU partition: " + str(error))
