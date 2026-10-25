"""Declare the generated LLVM headers for a TableGen input file."""

_VALUE_TYPES = "llvm/include/llvm/CodeGen/ValueTypes.td"
_INCLUDE_ROOT = "$$(dirname $(execpath :" + _VALUE_TYPES + "))"
_BACKENDS = {"aarch64": "AArch64", "x86_64": "X86"}

def _command(tool, flags, td, out, backend = None):
    includes = "-I " + _INCLUDE_ROOT + "/../.."
    if backend:
        includes += " -I " + _INCLUDE_ROOT + "/../../../lib/Target/" + backend
    return "$(execpath %s) %s %s $(execpath :%s) -o $(location :%s)" % (tool, flags, includes, td, out)

def llvm_tablegen(*, td, outputs, out_dir = None, tool = "llvm-tblgen", target_compatible_with = []):
    """Run TableGen once for each output filename and its TableGen flags."""
    out_dir = out_dir or td.rsplit("/", 1)[0]
    srcs = [":llvm_tablegen_inputs", td] + ([] if td == _VALUE_TYPES else [_VALUE_TYPES])
    tool_label = "@llvm-project//llvm:" + tool
    for filename, flags in outputs.items():
        path = out_dir + "/" + filename
        out = "gen/" + path
        outs = [out]
        if "-gen-register-info" in flags.split(" "):
            # RegisterInfoEmitter writes four headers beside its main output.
            outs += [out[:-4] + suffix + ".inc" for suffix in ["Enums", "MCDesc", "Header", "TargetDesc"]]
        if tool == "llvm-min-tblgen":
            cmd = _command(tool_label, flags, td, out)
        elif td.startswith("llvm/lib/Target/"):
            cmd = _command(tool_label, flags, td, out, td.split("/")[3])
        else:
            cmd = select({
                "@platforms//cpu:" + cpu: _command(tool_label, flags, td, out, backend)
                for cpu, backend in _BACKENDS.items()
            })
        native.genrule(
            name = "tablegen_" + path.replace("/", "_").replace(".", "_").replace("-", "_"),
            srcs = srcs,
            outs = outs,
            cmd = cmd,
            tools = [tool_label],
            target_compatible_with = target_compatible_with,
        )
