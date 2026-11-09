#!/usr/bin/env python3
"""Prune local additional_compiler_inputs using verified native includes.

The default is a read-only preview. --write edits only external BUILD files.
cc_library.srcs translation units are not assumed available to other compile
actions. Shrink a glob only when every caller can discard the same files.
"""

import argparse
import ast
from collections import Counter, defaultdict, deque
from functools import cache
import json
from pathlib import Path
import posixpath
import re
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from initial_migration.native_cc_dependencies import DependencyGraph
from initial_migration.native_cc_inputs import FORCED, SPECIAL_REPOS, Sources, TUS
from initial_migration.native_public_includes import directory, list_elements
from initial_migration.native_textual_inputs import expanded, string_values
from initial_migration.prune_inherited_headers import ARCHES, CC_COMPILERS, Document, label
from initial_migration.repository_inputs import CC_HEADER_SUFFIXES, TEXTUAL_SOURCE_SUFFIXES, ROOT

LOCATION = re.compile(r"\$\((?:location|locations|execpath|execpaths|rootpath|rootpaths)\s+([^\s)]+)\)")
REMOVABLE = {
    "delete_duplicate_already_compile_input", "delete_no_incoming_include",
    "delete_unreferenced_build_metadata",
}


def build_filename(ref):
    """Recognize build instructions; C/C++ source templates remain inputs."""
    name = ref.rsplit("//:", 1)[-1].rsplit("/", 1)[-1].lower()
    if name.endswith(tuple((suffix + ".cmake").lower()
                           for suffix in (*TEXTUAL_SOURCE_SUFFIXES, *TUS))):
        return False
    return (name in {"cmakelists.txt", "makefile", "build", "build.bazel",
                     "workspace", "workspace.bazel", "module.bazel"}
            or name.endswith((".bzl", ".cmake")))


def glob_labels(doc, node, cpu, constants=()):
    """Files named by a recognized glob in the active CPU's input expression."""
    if isinstance(node, ast.Name) and node.id in doc.constants and node.id not in constants:
        return glob_labels(doc, doc.constants[node.id], cpu, (*constants, node.id))
    if isinstance(node, (ast.List, ast.Tuple)):
        return set().union(*(glob_labels(doc, item, cpu, constants) for item in node.elts))
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        return glob_labels(doc, node.left, cpu, constants) | glob_labels(doc, node.right, cpu, constants)
    if isinstance(node, ast.Call) and getattr(node.func, "id", "") == "glob":
        return doc.labels(node, cpu) if known_expression(doc, node, "srcs") else set()
    if (isinstance(node, ast.Call) and getattr(node.func, "id", "") == "select"
            and len(node.args) == 1 and isinstance(node.args[0], ast.Dict)):
        choices = node.args[0]
        allowed = {"@platforms//cpu:" + arch for arch in ARCHES} | {"//conditions:default"}
        if all(isinstance(key, ast.Constant) and key.value in allowed for key in choices.keys):
            table = {key.value: val for key, val in zip(choices.keys, choices.values)}
            return glob_labels(doc, table.get("@platforms//cpu:" + cpu,
                                              table.get("//conditions:default")), cpu, constants)
    return set()


def known_expression(doc, node, field, constants=()):
    """Never infer missing source or flag inputs from an opaque expression."""
    if node is None or isinstance(node, ast.Constant):
        return True
    if isinstance(node, ast.Attribute):
        return field == "copts" and string_values(doc, node, ARCHES[0]) is not None
    if isinstance(node, ast.Name):
        if node.id in doc.constants and node.id not in constants:
            return known_expression(doc, doc.constants[node.id], field, (*constants, node.id))
        return field == "copts" and node.id == "CLICKHOUSE_WARNINGS"
    if isinstance(node, (ast.List, ast.Tuple)):
        return all(known_expression(doc, part, field, constants) for part in node.elts)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        return (known_expression(doc, node.left, field, constants) and
                known_expression(doc, node.right, field, constants))
    if isinstance(node, ast.Call):
        kind = getattr(node.func, "id", "")
        if kind == "glob" and field == "srcs" and len(node.args) == 1:
            opts = {kw.arg: kw.value for kw in node.keywords}
            return (doc.glob_strings(node.args[0]) is not None and
                    not (set(opts) - {"exclude", "allow_empty"}) and
                    ("exclude" not in opts or doc.glob_strings(opts["exclude"]) is not None))
        if kind == "select" and len(node.args) == 1 and isinstance(node.args[0], ast.Dict):
            choices = node.args[0]
            allowed = {"@platforms//cpu:" + cpu for cpu in ARCHES} | {"//conditions:default"}
            return (all(isinstance(k, ast.Constant) and k.value in allowed for k in choices.keys) and
                    all(known_expression(doc, v, field, constants) for v in choices.values))
        return (field == "copts" and kind in {
            "repository_path", "repository_local_include", "llvm_toolchain_path", "cmake_prefix_maps"
        } and all(known_expression(doc, arg, field, constants) for arg in node.args))
    return False


def build_text(root, path, revision):
    if revision:
        return subprocess.check_output(
            ["git", "show", revision + ":" + path.relative_to(root).as_posix()], cwd=root).decode()
    return path.read_text()


class LocalAudit:
    def __init__(self, graph, sources):
        self.graph = graph
        self.sources = sources
        self.computed = Counter()
        self.forced = Counter()
        self.generated = Counter()
        # Check every scanned source repository, not just this library. That
        # includes #embed and assembly .include/.incbin collected in v3.
        self.literal_names = {
            posixpath.basename(operand).lower()
            for directives in sources.directives.values()
            for _, _, syntax, operand in directives if syntax != "computed"
        }

    @cache
    def forced_roots(self, target, cpu):
        doc, _, attrs = self.graph.rules[target]
        nodes = list_elements(attrs.get("copts"), doc, cpu)
        values = [directory(node, doc) for node in nodes]
        roots, unresolved = set(), []
        index = 0
        while index < len(values):
            option = values[index]
            index += 1
            if option is None:
                continue
            if option in FORCED:
                if index == len(values) or values[index] is None:
                    unresolved.append(option)
                    continue
                operand = values[index]
                index += 1
            else:
                prefix = next((name for name in sorted(FORCED, key=len, reverse=True)
                               if option.startswith(name + "=")), None)
                if prefix is None:
                    continue
                operand = option.removeprefix(prefix + "=")
            location = LOCATION.fullmatch(operand)
            if location:
                candidates = {label(location[1], doc.repo)}
            else:
                candidates = set(self.sources.include_candidates(target, operand, "quote"))
            roots.update(candidates)
            self.forced[target, cpu, option, operand] += 1
        return roots, unresolved

    def reachable(self, target, cpu, public):
        doc, _, attrs = self.graph.rules[target]
        generated, forced, unresolved = set(), set(), []
        if public:
            roots = self.graph.own_headers(target, cpu)
        else:
            raw_srcs = doc.labels(attrs.get("srcs"), cpu)
            roots = set(self.graph.archive_sources(target, cpu))
            generated = {ref for ref in raw_srcs if ref in self.graph.rules or (
                ref.endswith(TUS) and ref not in self.sources.directives)}
            forced, unresolved = self.forced_roots(target, cpu)
            roots |= forced
        pending = deque(roots)
        seen, incoming, computed = set(), {}, []
        for ref in forced:
            incoming[ref] = ("copts", 0, "-include", ref)
        while pending:
            source = pending.popleft()
            if source in seen:
                continue
            seen.add(source)
            for line, directive, syntax, operand in self.sources.directives.get(source, ()):
                if syntax == "computed":
                    computed.append((source, line, directive, operand))
                    continue
                matches = self.sources.include_candidates(source, operand, syntax)
                original = self.sources.source_copies.get(source, source)
                repo, separator, filename = original.partition("//:")
                relative = None
                if separator and syntax in {"quote", "apostrophe"} and directive != "#include_next":
                    relative = repo + "//:" + posixpath.normpath(
                        posixpath.join(posixpath.dirname(filename), operand))
                # An input's presence in a compiler action does not establish
                # a search directory. For example absl's <version> is not
                # absl/time/internal/cctz/testdata/version. Retain the native
                # repo root as the implicit quote search directory that Bazel
                # supplies to the compilation, as well as explicit includes.
                implicit = doc.repo + "//:" + posixpath.normpath(operand)
                reachable = {ref for ref in matches
                             if ref == relative or
                             (syntax in {"quote", "apostrophe"} and ref == implicit) or
                             self.graph.under_original_include(target, cpu, ref, operand)}
                for ref in reachable:
                    incoming.setdefault(ref, (source, line, directive, operand))
                    if ref not in seen:
                        pending.append(ref)
        return incoming, computed, generated, forced, unresolved

    @cache
    def target(self, target, cpu):
        doc, _, attrs = self.graph.rules[target]
        extras = {ref for ref in doc.labels(attrs.get("additional_compiler_inputs"), cpu)
                  if ref.startswith(doc.repo + "//:")}
        if not extras:
            return {}, {}
        native_headers = (doc.labels(attrs.get("hdrs"), cpu) |
                          doc.labels(attrs.get("textual_hdrs"), cpu) |
                          self.graph.visible(target, cpu))
        native_srcs = doc.labels(attrs.get("srcs"), cpu)
        present_at_every_compile = native_headers | {
            ref for ref in native_srcs if ref.endswith(CC_HEADER_SUFFIXES) and not ref.endswith(TUS)
        }
        forced, unresolved_forced = self.forced_roots(target, cpu)
        opaque = (not known_expression(doc, attrs.get("srcs"), "srcs") or
                  not known_expression(doc, attrs.get("copts"), "copts"))
        # No reachability calculation is needed when actual compiler inputs
        # already carry all additional inputs for every compilation action.
        unknowns = extras - present_at_every_compile
        if unknowns:
            pub, pub_computed, _, _, _ = self.reachable(target, cpu, True)
            priv, priv_computed, generated, forced, unresolved_forced = self.reachable(target, cpu, False)
        else:
            pub, priv, pub_computed, priv_computed = {}, {}, [], []
            generated = set()
        for filename in generated:
            self.generated[target, cpu, filename] += 1
        for source, line, directive, operand in set(pub_computed + priv_computed):
            self.computed[source, line, directive, operand] += 1
        local_globs = glob_labels(doc, attrs.get("additional_compiler_inputs"), cpu)
        # Includes can be selected through -D macros, -include, response
        # files, profile maps or linker/assembler options. Interpret complete
        # option expressions so a split repository_path + filename is kept.
        flags = [string_values(doc, attrs.get(field), cpu) for field in (
            "copts", "conlyopts", "cxxopts", "linkopts", "defines", "local_defines",
        )]
        known_flags = all(isinstance(values, list) and
                          all(isinstance(value, str) for value in values)
                          for values in flags)
        decisions, evidence = {}, {}
        for ref in sorted(extras):
            basename = posixpath.basename(ref.rsplit("//:", 1)[-1]).lower()
            if ref not in self.sources.inventory or ref in self.graph.rules:
                reason = "unscanned_repo_or_generated_input"
            elif ref in forced:
                reason = "forced_keep"
                evidence[ref] = ("copts", 0, "-include", ref)
            elif ref in present_at_every_compile:
                reason = "delete_duplicate_already_compile_input"
            elif ref in pub:
                reason = "publish_exact_textual_header"
                evidence[ref] = pub[ref]
            elif ref in priv:
                # Native cc_library srcs accepts ordinary noncompiled header
                # extensions as private headers; .c/.cc/.S would add compile
                # actions. Other private embedded texts keep explicit inputs.
                if ref.endswith(CC_HEADER_SUFFIXES) and not ref.endswith(TUS):
                    reason = "private_header_to_srcs"
                else:
                    reason = "private_textual_keep"
                evidence[ref] = priv[ref]
            elif (ref in local_globs and build_filename(ref) and known_flags
                  and basename not in self.literal_names
                  and not any(basename in value.lower() for values in flags for value in values)
                  and not unresolved_forced):
                # A checked-in CMake/Bazel/Make input which no scanned source
                # names and no target flag references is not a compiler input.
                # Leave ordinary data and all unknown source templates under
                # the stricter generated/computed-include rules below.
                reason = "delete_unreferenced_build_metadata"
            elif generated:
                reason = "unknown_generated_tu"
            elif opaque:
                reason = "unknown_source_or_copts_expression"
            elif pub_computed or priv_computed or unresolved_forced:
                # A computed include in any reachable repository can select a
                # header from this repository. The v3 receipt does not record
                # preprocessor macro expansion, so repo-only exclusion is unsafe.
                reason = "unresolved_computed_include"
            else:
                reason = "delete_no_incoming_include"
            decisions[ref] = reason
        return decisions, evidence


def verify_fields(before, after):
    """Verify expanded unrelated attributes, including every source select."""
    old_rules = [(call, {kw.arg: kw.value for kw in call.keywords})
                 for _, call in before.source.rules()]
    new_rules = [(call, {kw.arg: kw.value for kw in call.keywords})
                 for _, call in after.source.rules()]
    if len(old_rules) != len(new_rules):
        raise ValueError(str(before.path) + ": changed target count")
    for (old, old_attrs), (new, new_attrs) in zip(old_rules, new_rules, strict=True):
        kind = getattr(old.func, "id", "")
        skip = {"additional_compiler_inputs"} if kind in CC_COMPILERS else set()
        if (kind != getattr(new.func, "id", "") or
                set(old_attrs) - skip != set(new_attrs) - skip):
            raise ValueError(str(before.path) + ": changed unrelated rule attributes")
        for field in set(old_attrs) - skip:
            if expanded(old_attrs[field], before) != expanded(new_attrs[field], after):
                name = ast.literal_eval(old_attrs["name"])
                raise ValueError(f"{before.path}:{name}: changed {field}")
        if kind in CC_COMPILERS:
            for cpu in ARCHES:
                if before.values(old_attrs.get("srcs"), cpu) != after.values(new_attrs.get("srcs"), cpu):
                    raise ValueError(f"{before.path}:{old.lineno}: changed ordered {cpu} srcs")


class LocalRewrite:
    def __init__(self, doc, audit):
        self.doc, self.audit = doc, audit
        self.source = doc.source
        self.allowed = {}
        self.glob_removals = {}
        self.nodes = {}
        self.list_items = {}
        self.seen = set()
        self.stats = Counter()

    def vote(self, node, target, arches, native_extra):
        """Intersect permission from every use of a literal or glob node."""
        self.nodes[id(node)] = node
        eligible = bool(native_extra and arches and target is not None)
        if eligible:
            is_glob = isinstance(node, ast.Call)
            if is_glob and (self.doc.globs is None or not known_expression(self.doc, node, "srcs")):
                eligible = False
            else:
                for cpu in arches:
                    refs = (self.doc.labels(node, cpu) if is_glob else
                            {label(node.value, self.doc.repo)})
                    decisions, _ = self.audit.target(target, cpu)
                    removable = {ref for ref in refs if decisions.get(ref) in REMOVABLE}
                    if not is_glob:
                        # A second literal occurrence of an input must not
                        # inherit a removal authorized only for a compile glob.
                        removable = {ref for ref in removable
                                     if decisions[ref] != "delete_unreferenced_build_metadata"}
                    if is_glob:
                        prior = self.glob_removals.get(id(node), refs)
                        self.glob_removals[id(node)] = prior & removable
                    if not refs or not removable:
                        eligible = False
                        break
        self.allowed[id(node)] = self.allowed.get(id(node), True) and eligible

    def compact_glob(self, retained):
        """Return a short exact expression for retained package files."""
        globs = self.doc.globs
        candidates = set()
        for name in retained:
            directory, leaf = posixpath.split(name)
            suffix = posixpath.splitext(leaf)[1]
            # Do not invent a broad extensionless pattern for names such as
            # libc++ <version> or a library's extensionless public headers.
            if not suffix:
                continue
            while directory:
                candidates.add(directory + "/*" + suffix)
                candidates.add(directory + "/**/*" + suffix)
                directory = posixpath.dirname(directory)
            candidates.add("*" + suffix)
            candidates.add("**/*" + suffix)
        matches = [(pattern, globs.matching(pattern)) for pattern in candidates]
        matches = [(pattern, files) for pattern, files in matches
                   if len(files) > 1 and files <= retained]
        # Prefer the narrowest directory when two patterns name the same
        # files, e.g. src/roaring/*.c before **/*.c.
        matches.sort(key=lambda pair: (
            -len(pair[1]), -pair[0].split("*", 1)[0].count("/"),
            "**" in pair[0], len(pair[0]), pair[0],
        ))
        remaining, patterns = set(retained), []
        for pattern, files in matches:
            if len(files & remaining) > 1:
                patterns.append(pattern)
                remaining.difference_update(files)
        # Preserve a large mixed glob if an exact representation would dump
        # thousands of source labels into the BUILD file. Never add excludes.
        if len(remaining) > 32 or len(patterns) > 32:
            return None
        parts = []
        if patterns:
            parts.append("glob(" + json.dumps(sorted(patterns)) + ")")
        if remaining:
            parts.append(json.dumps(sorted(remaining)))
        return parts[0] if len(parts) == 1 else "(" + " + ".join(parts) + ")"

    @staticmethod
    def build_metadata(ref):
        """Classify removed build files for the audit report, never for votes."""
        name = ref.rsplit("//:", 1)[-1].rsplit("/", 1)[-1].lower()
        if name in {"cmakelists.txt", "cmakecache.txt"} or name.endswith(".cmake"):
            return "cmake"
        if (name in {"build", "build.bazel", "workspace", "workspace.bazel", "module.bazel"}
                or name.endswith((".bzl", ".bazel"))):
            return "bazel"
        if re.fullmatch(
                r"(?:readme|authors|contributing|how[_-]to[_-]update|changes|changelog|todo)"
                r"(?:\.(?:md|rst|txt))?", name):
            return "readme"
        return None

    def walk(self, node, target, arches, native_extra, constants=()):
        if node is None:
            return
        if isinstance(node, ast.Name) and node.id in self.doc.constants:
            if node.id in constants:
                raise ValueError("Recursive BUILD constant " + node.id)
            key = node.id, target, arches, native_extra
            if key not in self.seen:
                self.seen.add(key)
                self.walk(self.doc.constants[node.id], target, arches, native_extra, (*constants, node.id))
            return
        if isinstance(node, (ast.List, ast.Tuple)):
            for item in node.elts:
                if isinstance(item, ast.Constant) and isinstance(item.value, str):
                    self.list_items[id(item)] = node, item
                    self.vote(item, target, arches, native_extra)
                else:
                    self.walk(item, target, arches, native_extra, constants)
            return
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            self.walk(node.left, target, arches, native_extra, constants)
            self.walk(node.right, target, arches, native_extra, constants)
            return
        if isinstance(node, ast.Call) and getattr(node.func, "id", "") == "glob":
            self.vote(node, target, arches, native_extra)
            # Pattern strings and glob option strings are never input labels.
            return
        if (isinstance(node, ast.Call) and getattr(node.func, "id", "") == "select"
                and len(node.args) == 1 and isinstance(node.args[0], ast.Dict)):
            choices = node.args[0]
            keys = {k.value for k in choices.keys if isinstance(k, ast.Constant)}
            cpus = {"@platforms//cpu:" + cpu for cpu in ARCHES}
            cpu_only = len(keys) == len(choices.keys) and keys <= cpus | {"//conditions:default"}
            for key, branch in zip(choices.keys, choices.values):
                if not cpu_only:
                    # Do not use the guaranteed intersection of an unknown
                    # select to edit any of that select's individual branches.
                    self.walk(branch, target, arches, False, constants)
                    continue
                selected = tuple(cpu for cpu in arches
                                 if key.value == "@platforms//cpu:" + cpu or
                                 key.value == "//conditions:default" and
                                 "@platforms//cpu:" + cpu not in keys)
                self.walk(branch, target, selected, native_extra, constants)
            return
        # Other macros, expressions and non-native rule inputs cannot donate
        # their strings or shared BUILD constants to this edit.
        for child in ast.iter_child_nodes(node):
            self.walk(child, target, arches, False, constants)

    @staticmethod
    def empty(node, doc, constants=()):
        if isinstance(node, ast.Name) and node.id in doc.constants and node.id not in constants:
            return LocalRewrite.empty(doc.constants[node.id], doc, (*constants, node.id))
        if isinstance(node, (ast.List, ast.Tuple)):
            return not node.elts
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            return LocalRewrite.empty(node.left, doc, constants) and LocalRewrite.empty(node.right, doc, constants)
        # Keep even empty selects: removing one could mask a missing branch
        # for an unsupported configuration.
        return False

    def text(self):
        for stmt in self.source.module.body:
            if isinstance(stmt, ast.Assign):
                continue
            if (isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Call) and
                    getattr(stmt.value.func, "id", "") in CC_COMPILERS):
                call = stmt.value
                attrs = {kw.arg: kw.value for kw in call.keywords}
                name = attrs.get("name")
                if isinstance(name, ast.Constant) and isinstance(name.value, str):
                    target = label(":" + name.value, self.doc.repo)
                    for field, node in attrs.items():
                        self.walk(node, target, ARCHES, field == "additional_compiler_inputs")
                    for node in call.args:
                        self.walk(node, target, ARCHES, False)
                    continue
            self.walk(stmt, None, ARCHES, False)
        edits = []
        for ident, eligible in self.allowed.items():
            if not eligible:
                continue
            node = self.nodes[ident]
            if isinstance(node, ast.Call):
                removable = self.glob_removals[ident]
                if not removable:
                    continue
                original = self.doc.labels(node, ARCHES[0])
                retained_refs = original - removable
                if not retained_refs:
                    replacement = "[]"
                    self.stats["whole_globs"] += 1
                else:
                    prefix = self.doc.repo + "//:"
                    retained = {ref.removeprefix(prefix) for ref in retained_refs}
                    replacement = self.compact_glob(retained)
                    if replacement is None:
                        self.stats["partial_globs_too_large"] += 1
                        continue
                    self.stats["partial_globs"] += 1
                parsed = ast.parse(replacement, mode="eval").body
                for cpu in ARCHES:
                    # Sources checks the inventory digest. Verify the exact
                    # retained file set under that same repository inventory.
                    if self.doc.labels(parsed, cpu) != self.doc.labels(node, cpu) - removable:
                        raise ValueError(f"{self.doc.path}:{node.lineno}: glob changed retained {cpu} files")
                edits.append((*self.source.span(node), replacement))
                self.stats["glob_files_removed"] += len(removable)
                for ref in removable:
                    category = self.build_metadata(ref)
                    if category:
                        self.stats["glob_" + category + "_removed"] += 1
            else:
                container, item = self.list_items[ident]
                first, last = self.source.span(item)
                index = container.elts.index(item)
                next_pos = (self.source.span(container.elts[index + 1])[0]
                            if index + 1 < len(container.elts) else self.source.span(container)[1] - 1)
                comma = self.source.bytes[last:next_pos].find(b",")
                if comma >= 0:
                    last += comma + 1
                edits.append((first, last, ""))
                self.stats["literals"] += 1
        text = self.source.rewrite(edits)
        intermediate = Document(self.doc.path, self.audit.sources.inventories, text)
        edits = []
        for _, rule in intermediate.source.rules():
            if getattr(rule.func, "id", "") not in CC_COMPILERS:
                continue
            for kw in rule.keywords:
                if kw.arg == "additional_compiler_inputs" and self.empty(kw.value, intermediate):
                    first, last = intermediate.source.span(kw)
                    if intermediate.source.bytes[last:last + 1] == b",":
                        last += 1
                    edits.append((first, last, ""))
                    self.stats["empty_attributes"] += 1
        text = intermediate.source.rewrite(edits)
        verify_fields(self.doc, Document(self.doc.path, self.audit.sources.inventories, text))
        return text


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--receipts", required=True, type=Path)
    parser.add_argument("--revision")
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--consumer", action="append", default=[],
                        help="restrict edited consumers to an apparent repo, e.g. @clickhouse_grpc")
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    if args.revision and args.write:
        parser.error("--revision is incompatible with --write")
    sources = Sources(args.receipts, args.root)
    sources.require_complete()
    paths = [args.root / "BUILD.bazel",
             *sorted((args.root / "bazel/dependencies").glob("*.BUILD.bazel"))]
    docs = [Document(path, sources.inventories, build_text(args.root, path, args.revision)) for path in paths]
    graph = DependencyGraph(docs, sources)
    audit = LocalAudit(graph, sources)
    total, by_repo, extensions = Counter(), defaultdict(Counter), defaultdict(Counter)
    records = []
    globs = Counter()
    glob_plans = []
    rewrites = Counter()
    updated_files = {}
    for doc in docs[1:]:
        if doc.repo in SPECIAL_REPOS:
            continue
        before = len(records)
        for _, call in doc.source.rules():
            attrs = {kw.arg: kw.value for kw in call.keywords}
            if call.func.id not in CC_COMPILERS or "additional_compiler_inputs" not in attrs:
                continue
            target = label(":" + ast.literal_eval(attrs["name"]), doc.repo)
            by_cpu = {}
            for cpu in ARCHES:
                decisions, evidence = audit.target(target, cpu)
                by_cpu[cpu] = decisions
                if not decisions:
                    continue
                reasons = Counter(decisions.values())
                total.update(reasons)
                by_repo[doc.repo].update(reasons)
                for ref, reason in decisions.items():
                    extensions[reason][Path(ref).suffix or "(none)"] += 1
                records.append({
                    "target": target, "cpu": cpu, "reasons": dict(reasons),
                    "decisions": decisions, "evidence": evidence,
                })
            # Count literal glob calls by their expanded attr (including a
            # shared BUILD constant), without treating glob pattern strings
            # as candidate source filenames.
            todo = [attrs["additional_compiler_inputs"]]
            seen = set()
            while todo:
                node = todo.pop()
                if isinstance(node, ast.Name) and node.id in doc.constants:
                    if node.id not in seen:
                        seen.add(node.id)
                        todo.append(doc.constants[node.id])
                elif isinstance(node, ast.Call) and getattr(node.func, "id", "") == "glob":
                    globs[doc.repo] += 1
                    selected = Counter()
                    refs = set()
                    for cpu in ARCHES:
                        for ref in doc.labels(node, cpu):
                            if ref in by_cpu[cpu]:
                                refs.add(ref)
                                selected[by_cpu[cpu][ref]] += 1
                    glob_plans.append({
                        "target": target,
                        "expression": doc.source.text(node),
                        "line": node.lineno,
                        "distinct_files": len(refs),
                        "reasons": dict(selected),
                        "delete_all_selected_inputs": bool(selected) and not (set(selected) - REMOVABLE),
                    })
                else:
                    todo.extend(ast.iter_child_nodes(node))
        if len(records) != before:
            print(doc.repo, dict(by_repo[doc.repo]), flush=True)
        if args.consumer and doc.repo not in args.consumer:
            continue
        rewrite = LocalRewrite(doc, audit)
        text = rewrite.text()
        if text != doc.source.bytes.decode():
            rewrites.update(rewrite.stats)
            rewrites["files"] += 1
            updated_files[doc.path] = doc, text
            print("Preview:", doc.repo, dict(rewrite.stats), flush=True)
    report = {
        "revision": args.revision, "records": records,
        "decisions_per_target_cpu_file": dict(total),
        "by_repo": {repo: dict(counts) for repo, counts in sorted(by_repo.items())},
        "extensions": {reason: dict(counts) for reason, counts in sorted(extensions.items())},
        "glob_calls_expanded_per_target": dict(globs),
        "glob_plans": glob_plans,
        "rewrite_counts": dict(rewrites),
        "changed_files": [path.relative_to(args.root).as_posix() for path in updated_files],
        "computed_directives": [dict(source=key[0], line=key[1], directive=key[2],
                                      operand=key[3], targets_cpus=count)
                                for key, count in audit.computed.most_common()],
        "forced_includes": [dict(target=key[0], cpu=key[1], option=key[2],
                                 operand=key[3], count=count)
                            for key, count in audit.forced.most_common()],
        "unknown_generated": [dict(target=key[0], cpu=key[1], filename=key[2])
                              for key in audit.generated],
    }
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    if args.write:
        # Complete every proof before the first mutation. Do not overwrite
        # another agent's BUILD changes while computing source reachability.
        for path, (doc, _) in updated_files.items():
            if path.read_bytes() != doc.source.bytes:
                raise ValueError("BUILD file changed during audit: " + str(path))
        for path, (_, text) in updated_files.items():
            path.write_text(text)
    print("Scanned:", len(sources.complete), "repos; external repos with local extras:", len(by_repo))
    print("Decisions per target/CPU/file:", dict(total))
    print("Expanded native target globs:", sum(globs.values()))
    print("Rewrite:", dict(rewrites), "written" if args.write else "no BUILD files written")
    print("Report:", args.report)


if __name__ == "__main__":
    main()
