"""Audit generated proto C++ in the configured production graph."""

load("@depend_on_what_you_use//dwyu/cc:defs.bzl", "dwyu_configured_header_aliases", "dwyu_process_compiler_command")
load("@protobuf//bazel/common:proto_common.bzl", "proto_common")
load("@protobuf//bazel/common:proto_info.bzl", "ProtoInfo")
load("@rules_cc//cc:find_cc_toolchain.bzl", "find_cc_toolchain", "use_cc_toolchain")
load("@rules_cc//cc/common:cc_helper.bzl", "cc_helper")
load("@rules_cc//cc/common:cc_info.bzl", "CcInfo")

_ProtoDwyuInfo = provider(fields = {
    "identity": "Report File for the actual configured owner.",
    "reports": "Reports and original compiler-preprocessing result Files.",
})
_CC_PROTO_TOOLCHAIN = Label("@protobuf//bazel/private:cc_toolchain_type")

def _paths(files):
    return [file.path for file in files]

def _owner(dep):
    cc = dep[CcInfo].compilation_context if CcInfo in dep else None
    outputs = dep[DefaultInfo].files.to_list()
    identity = dep[_ProtoDwyuInfo].identity.path if _ProtoDwyuInfo in dep else (outputs[0].path if outputs else None)
    return {
        "label": str(dep.label),
        "configured_owner": identity,
        "outputs": _paths(outputs),
        "direct_headers": _paths(list(cc.direct_public_headers) + list(cc.direct_textual_headers)) if cc else [],
        "headers": _paths(cc.headers.to_list()) if cc else [],
        "proto_sources": _paths(dep[ProtoInfo].direct_sources) if ProtoInfo in dep else [],
        "check_deps_sources": _paths(dep[ProtoInfo].check_deps_sources.to_list()) if ProtoInfo in dep else [],
        "import_paths": [proto_common.get_import_path(f) for f in dep[ProtoInfo].check_deps_sources.to_list()] if ProtoInfo in dep else [],
    }

def _action(action):
    return {
        "mnemonic": action.mnemonic,
        "argv": action.argv,
        "environment": action.env,
        "inputs": _paths(action.inputs.to_list()),
        "outputs": _paths(action.outputs.to_list()),
    }

def _problem(problems, reason, **evidence):
    problems.append(dict(reason = reason, **evidence))

def _routing_record(record, routing_file):
    return {
        "schema": 1,
        "record_kind": "PROTO_DWYU_ROUTING",
        "routing_file": routing_file,
        "label": record["label"],
        "configured_owner": record["configured_owner"],
        "kind": record["kind"],
        "status": record["status"],
        "problems": [problem["reason"] for problem in record["problems"]],
        "generated_sources": record["generated_sources"],
        "generated_headers": record["generated_headers"],
        "compile_actions": [
            {"src": action["src"], "object": action["object"]}
            for action in record["compile_actions"]
        ],
        "direct_proto_deps": [
            {"label": dep["label"], "configured_owner": dep["configured_owner"]}
            for dep in record["direct_proto_deps"]
        ],
    }

def _compile_unit(ctx, action, sources, problems, cc):
    # CppCompileAction.argv is expanded with a null paramFilePath in Bazel 9.3.
    # Never interpret an unexpanded @file or a new driver output mode as success.
    if not ctx.target_platform_has_constraint(ctx.attr._linux[platform_common.ConstraintValueInfo]):
        _problem(problems, "unsupported_compile_platform", action = _action(action))
        return None
    argv = action.argv
    outputs = {file.path: file for file in action.outputs.to_list()}
    inputs = {file.path: file for file in action.inputs.to_list()}
    if (not argv or action.env == None or len([arg for arg in argv if arg == "-c"]) != 1 or len([arg for arg in argv if arg == "-o"]) != 1 or
        any([arg.startswith("@") for arg in argv])):
        _problem(problems, "unsupported_compile_command", action = _action(action))
        return None
    c = argv.index("-c")
    o = argv.index("-o")
    if c + 1 >= len(argv) or o + 1 >= len(argv):
        _problem(problems, "missing_compile_operand", action = _action(action))
        return None
    src = inputs.get(argv[c + 1])
    obj = outputs.get(argv[o + 1])
    if (src == None or obj == None or src not in sources or src.is_directory or
        obj.is_directory or not obj.basename.endswith((".o", ".obj"))):
        _problem(problems, "compile_inputs_not_generated", action = _action(action))
        return None
    skip = {c: True, c + 1: True, o: True, o + 1: True}
    for i in range(1, len(argv)):
        if i in skip:
            continue
        arg = argv[i]
        if arg in ["-MD", "-MMD", "-MP"]:
            skip[i] = True
        elif arg in ["-MF", "-MT", "-MQ"]:
            if (i + 1 >= len(argv) or i + 1 in skip or
                (arg == "-MF" and argv[i + 1] not in outputs) or
                (arg != "-MF" and argv[i + 1] != obj.path)):
                _problem(problems, "unsupported_dependency_output", option = arg, action = _action(action))
                return None
            skip[i] = True
            skip[i + 1] = True
        elif arg.startswith("-M") or arg in ["-E", "-S", "-emit-llvm"] or arg.startswith(("-o", "-c=")):
            _problem(problems, "unsupported_compiler_output_option", option = arg, action = _action(action))
            return None
    return struct(
        src = src,
        object = obj,
        original = argv,
        skipped_outputs = skip,
        # CppCompileAction.getSchedulingDependencies() supplies declared headers
        # to aquery; Action.inputs alone contains mandatory/discovered inputs.
        inputs = depset(transitive = [action.inputs, cc.headers]),
        # CppCompileAction.getEffectiveEnvironment() sets PWD on Linux.
        environment = dict(action.env, PWD = "/proc/self/cwd"),
        action = action,
    )

def _impl(target, ctx):
    dependencies = getattr(ctx.rule.attr, "deps", [])
    transitive = [dep[_ProtoDwyuInfo].reports for dep in dependencies if _ProtoDwyuInfo in dep]
    report = ctx.actions.declare_file(target.label.name + ".proto_dwyu_report.json")
    routing = ctx.actions.declare_file(target.label.name + ".proto_dwyu_routing.json")
    files = [report, routing]
    problems = []
    units = []
    generation = []
    generated_sources = []
    generated_headers = []
    pp_files = []
    proto_sources = []
    descriptor = None
    proto_source_root = None
    runtime = None
    manifest = None
    provided_sources = []
    status = "unsupported"
    cc = target[CcInfo].compilation_context if CcInfo in target else None

    if ctx.configuration.is_tool_configuration():
        _problem(problems, "exec_configuration")
    elif ctx.rule.kind == "cc_proto_library":
        status = "forwarder"
        if len(dependencies) != 1 or _ProtoDwyuInfo not in dependencies[0] or cc == None:
            _problem(problems, "invalid_cc_proto_forwarder")
    elif ProtoInfo in target:
        proto_sources = target[ProtoInfo].direct_sources
        descriptor = target[ProtoInfo].direct_descriptor_set
        proto_source_root = target[ProtoInfo].proto_source_root
        files.append(descriptor)
        selected = ctx.toolchains[_CC_PROTO_TOOLCHAIN]
        if selected == None or not hasattr(selected, "proto"):
            _problem(problems, "missing_cc_proto_toolchain")
        elif cc == None:
            _problem(problems, "protobuf_aspect_not_visible")
        elif not selected.proto.runtime or CcInfo not in selected.proto.runtime:
            _problem(problems, "missing_target_runtime")
        else:
            provider = selected.proto
            runtime = _owner(provider.runtime)
            provided_sources = _paths(provider.provided_proto_sources)
            runtime_headers = provider.runtime[CcInfo].compilation_context.headers.to_list()
            actual_headers = {header: True for header in cc.headers.to_list()}
            absent = [header.path for header in runtime_headers if header not in actual_headers]
            if runtime["configured_owner"] == None or not runtime_headers:
                _problem(problems, "runtime_artifacts_absent", runtime = runtime)
            if absent:
                _problem(problems, "runtime_context_absent", headers = absent)

            # Never substitute the exec protoc or an import glob for a runtime.
            generation = [action for action in target.actions if action.mnemonic == provider.mnemonic]
            gen_outputs = [file for action in generation for file in action.outputs.to_list()]
            extensions = cc_helper.extensions
            generated_sources = [file for file in gen_outputs if "." + file.extension in extensions.CC_SOURCE]
            generated_headers = [file for file in gen_outputs if "." + file.extension in extensions.CC_HEADER]
            unclassified = [file.path for file in gen_outputs if file not in generated_sources and file not in generated_headers]
            if unclassified:
                _problem(problems, "unclassified_generated_files", files = unclassified)
            sources = {file: True for file in generated_sources}
            compiles = [action for action in target.actions if action.mnemonic == "CppCompile"]
            for action in compiles:
                unit = _compile_unit(ctx, action, sources, problems, cc)
                if unit != None:
                    units.append(unit)
            should_generate = proto_common.experimental_should_generate_code(target[ProtoInfo], provider, ctx.rule.kind, target.label)
            if not proto_sources:
                status = "forwarder"
                if generation or compiles:
                    _problem(problems, "sourceless_proto_has_actions")
            elif not should_generate:
                status = "denylisted"
                if generation or compiles:
                    _problem(problems, "runtime_proto_has_actions")
            else:
                status = "compiled"
                seen = {unit.src: True for unit in units}
                if (not units or not generated_sources or not generated_headers or
                    len(sources) != len(generated_sources) or len(seen) != len(sources) or len(units) != len(compiles)):
                    _problem(problems, "incomplete_generated_compile_actions")
                if not problems:
                    # rules_cc records each virtual header adjacent to its original.
                    aliases = dwyu_configured_header_aliases(
                        target.label,
                        "proto-generated public hdrs",
                        generated_headers,
                        cc.direct_public_headers,
                    )
                    manifest = ctx.actions.declare_file(target.label.name + ".proto_dwyu_owned_headers.json")
                    ctx.actions.write(manifest, json.encode({
                        "public": aliases,
                        "private": {file.path: file.path for file in generated_sources},
                        "required": _paths(generated_sources + generated_headers),
                        "unchecked_link_inputs": [],
                        "raw_assembly": [],
                        "raw_link_candidates": [],
                    }))
                    files.append(manifest)
                    toolchain = find_cc_toolchain(ctx)
                    for index, unit in enumerate(units):
                        # PIC and non-PIC commands for the same source are separate evidence.
                        name = "{}.proto_dwyu_{}".format(target.label.name, index)
                        compiler_output = ctx.actions.declare_file(name + ".i")
                        args = ctx.actions.args()
                        args.add_all([
                            unit.original[i]
                            for i in range(1, len(unit.original))
                            if i not in unit.skipped_outputs
                        ])
                        args.add_all(["-o", compiler_output.path, "-E", unit.src.path, "-dI"])
                        public, private = dwyu_process_compiler_command(
                            ctx,
                            unit.src,
                            compiler_output,
                            name,
                            "proto",
                            unit.original[0],
                            unit.original[1:],
                            depset([manifest], transitive = [unit.inputs]),
                            toolchain.all_files,
                            manifest,
                            args,
                            environment = unit.environment,
                        )
                        pp_files.append({"public": public.path, "private": private.path})
                        files.extend([public, private])
    else:
        _problem(problems, "unsupported_rule_kind", kind = ctx.rule.kind)

    records = []
    for index, unit in enumerate(units):
        record = _action(unit.action)
        record.update({
            "src": unit.src.path,
            "object": unit.object.path,
            "environment": unit.environment,
            "inputs": _paths(unit.inputs.to_list()),
            "pp_report": pp_files[index] if index < len(pp_files) else None,
        })
        records.append(record)
    record = {
        "schema": 1,
        "label": str(target.label),
        "configured_owner": report.path,
        "kind": ctx.rule.kind,
        # compiled describes discovered actions, not successful execution.
        "status": status if not problems else "unsupported",
        "problems": problems,
        "proto_sources": _paths(proto_sources),
        "import_paths": [proto_common.get_import_path(f) for f in proto_sources],
        "proto_source_root": proto_source_root,
        "direct_descriptor_set": descriptor.path if descriptor else None,
        "exports": [_owner(dep) for dep in getattr(ctx.rule.attr, "exports", [])],
        "option_deps": [_owner(dep) for dep in getattr(ctx.rule.attr, "option_deps", [])],
        "provided_proto_sources": provided_sources,
        "generated_sources": _paths(generated_sources),
        "generated_headers": _paths(generated_headers),
        "generation_actions": [_action(action) for action in generation],
        "compile_actions": records,
        "direct_proto_deps": [_owner(dep) for dep in dependencies],
        "direct_headers": _paths(list(cc.direct_public_headers) + list(cc.direct_textual_headers)) if cc else [],
        "runtime": runtime,
        "owned_headers": manifest.path if manifest else None,
    }
    ctx.actions.write(report, json.encode(record))
    ctx.actions.write(routing, json.encode(_routing_record(record, routing.path)))
    reports = depset(direct = files, transitive = transitive)
    return [_ProtoDwyuInfo(identity = report, reports = reports), OutputGroupInfo(proto_dwyu = reports)]

# Do not require cc_proto_aspect here: the production cc_proto_library already
# attaches it. "requires" would create generated code outside that graph.
proto_dwyu = aspect(
    implementation = _impl,
    attr_aspects = ["deps"],
    required_aspect_providers = [[CcInfo]],
    provides = [_ProtoDwyuInfo],
    attrs = {
        "_linux": attr.label(default = Label("@platforms//os:linux")),
        "_tool_preprocessing": attr.label(
            default = Label("//bazel/rules:dwyu_preprocessing"),
            executable = True,
            cfg = "exec",
        ),
        "dwyu_verbose": attr.bool(default = False),
    },
    toolchains = use_cc_toolchain() + [config_common.toolchain_type(_CC_PROTO_TOOLCHAIN, mandatory = False)],
)
