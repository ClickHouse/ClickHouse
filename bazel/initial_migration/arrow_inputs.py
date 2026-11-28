"""Assign Arrow SDK includes to the libraries in clickhouse_arrow.BUILD.bazel."""

from functools import cache
from pathlib import Path
import re

from initial_migration.prune_inherited_headers import SourceDocument
from initial_migration.repository_inputs import package_sources


ARROW = "@clickhouse_arrow//:"
FLATBUFFERS = "//bazel:arrow_flatbuffer_headers"
_FLATBUFFER_HEADERS = {f"generated/{n}_generated.h" for n in
                       ("File", "Message", "Schema", "SparseTensor", "Tensor", "feather")}
_INCLUDE = re.compile(rb'^[ \t]*#[ \t]*(?:include|include_next|import)[ \t]*[<"]((?:arrow|parquet|generated)/[^>"\n]+)[>"]', re.M)


@cache
def _providers(root, arch):
    """Index public hdrs only. Arrow's vendored FlatBuffers outputs are not public."""
    doc = SourceDocument(root / "bazel/dependencies/clickhouse_arrow.BUILD.bazel",
                         files=package_sources("contrib/arrow"))
    configured = {attrs["name"].value: attrs["out"].value
                  for _, rule in doc.source.rules() if getattr(rule.func, "id", None) == "expand_template"
                  for attrs in ({k.arg: k.value for k in rule.keywords},)}
    result = {}
    for _, rule in doc.source.rules():
        attrs = {k.arg: k.value for k in rule.keywords}
        if getattr(rule.func, "id", None) not in {"ch_cc_library", "cc_library"}:
            continue
        name = attrs["name"].value
        for label in doc.values(attrs.get("hdrs"), arch):
            path = label.removeprefix(":")
            path = configured.get(path, path)
            for directory in ("cpp/src/", "config/contrib/arrow-cmake/cpp/src/"):
                if path.startswith(directory):
                    key = path.removeprefix(directory)
                    if key in result and result[key] != ARROW + name:
                        raise ValueError(f"Multiple public Arrow owners for {key}")
                    result[key] = ARROW + name
    return result


@cache
def _includes(path):
    return tuple(s.decode() for s in _INCLUDE.findall(path.read_bytes().replace(b"\\\r\n", b"").replace(b"\\\n", b"")))


def foreign_arrow_dependencies(root, name, index, sources, arch):
    """Return (deps, implementation_deps) for a non-Arrow CMake compile group."""
    sources = [Path(p) for p in sources]
    if (name, index) == ("dbms", 0):
        # DBMS opens Arrow IPC files using these generated FlatBuffers definitions.
        sources.append(root / "src/Processors/Formats/Impl/ArrowIPC/FlatBuffersCommon.h")
    needed = set()
    for path in sources:
        # These CMake generators produce ORC's protobuf or C++ string tables.
        # They can be absent when importing a configured, unbuilt CMake tree.
        if not path.is_file() and (name, path.name) in {
                ("_orc", "orc_proto.pb.cc"),
                ("clickhouse_storages_system", "StorageSystemBuildOptions.generated.cpp"),
                ("clickhouse_storages_system", "StorageSystemLicenses.generated.cpp")}:
            continue
        for header in _includes(path):
            if header in _FLATBUFFER_HEADERS:
                needed.add(FLATBUFFERS)
            else:
                owner = _providers(root, arch).get(header)
                if owner is None:
                    raise ValueError(f"{path}: no public Arrow library owns {header}")
                needed.add(owner)
    if not needed:
        return (), ()
    if (name, index) == ("dbms", 0):
        # e06b87e9 added core and Parquet to DBMS's public deps after Flight.
        public = tuple(ARROW + lib for lib in ("_arrow_flight", "_arrow__compile_0", "_parquet")
                       if ARROW + lib in needed)
        return public, tuple(sorted(needed.difference(public)))
    if (name, index) == ("unit_tests_dbms", 0):
        # The executable links Parquet before its direct core header dep.
        return (), tuple(sorted(needed, key=lambda d: (d == ARROW + "_arrow__compile_0", d)))
    raise ValueError(f"Review public and private Arrow dependencies for {name} compile group {index}: {sorted(needed)}")
