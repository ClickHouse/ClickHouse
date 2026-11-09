"""Arm GNU 8.3-2019.03 glibc and the four distinct GCC files used by ClickHouse."""

_GLIBC = "linux-aarch64/aarch64-linux-gnu/libc"
_GCC = "linux-aarch64/lib/gcc/aarch64-linux-gnu/8.3.0"
_SONAMES = {
    "libc": "6",
    "libdl": "2",
    "libm": "6",
    "libpthread": "0",
    "libresolv": "2",
    "librt": "1",
}
_SHARED = {
    _GLIBC + "/lib64/" + name + ".so." + major: "lib64/" + name + "-2.28.so"
    for name, major in _SONAMES.items()
} | {_GLIBC + "/lib/ld-linux-aarch64.so.1": "lib64/ld-2.28.so"}

def _arm_gnu_aarch64_impl(ctx):
    usr_lib64 = _GLIBC + "/usr/lib64/"
    members = _SHARED | {
        usr_lib64 + name: "usr/lib64/" + name
        for name in ["Scrt1.o", "crt1.o", "crti.o", "crtn.o", "libc.so", "libc_nonshared.a"]
    }
    ctx.download_and_extract(
        url = "https://developer.arm.com/-/media/Files/downloads/gnu-a/8.3-2019.03/binrel/sysroot/sysroot-glibc-8.3-2019.03-x86_64-aarch64-linux-gnu.tar.xz",
        sha256 = "6377b7ce103c193edb92e52698f33fe4e1116281c02c00875d0452ca54ac6177",
        strip_prefix = "selected",
        rename_files = {"./" + src: "selected/inputs/" + dst for dst, src in members.items()},
    )
    script = "inputs/" + usr_lib64 + "libc.so"
    ctx.template(script, script, {
        "/lib64/libc.so.6": "../../lib64/libc.so.6",
        "/usr/lib64/libc_nonshared.a": "./libc_nonshared.a",
        "/lib/ld-linux-aarch64.so.1": "../../lib/ld-linux-aarch64.so.1",
    }, executable = False)
    ctx.file("inputs/" + _GLIBC + "/usr/lib/.gitkeep", "", executable = False)

    # Arm's split glibc and GCC runtime packages omit the startup objects and libgcc.a.
    gcc_url = "https://raw.githubusercontent.com/ClickHouse/sysroot/6dd29c222896e54de24e7af1b191c92e4cf2e229/" + _GCC + "/"
    for filename, sha256 in {
        "crtbegin.o": "d665ad330e009f80df20d33c29956dc0e1e15f1c345184d708b614ef8f6b0293",
        "crtbeginS.o": "1b9df48e75b9969c9749a8ed7fb87253115c403cd342f227a3b0d00909d62b0f",
        "crtend.o": "39bc4465775504941fef2f76060abcb6c1a3353ac155459c9e8fc7ba9f65e9c6",
        "libgcc.a": "abb719b1aad77c6f6591c74c32e443fa9b78142fea57a19091d0a98b686b2c8c",
    }.items():
        ctx.download(gcc_url + filename, "inputs/" + _GCC + "/" + filename, sha256 = sha256)
    ctx.file("BUILD.bazel", ctx.read(Label("//bazel/link_runtime:arm.BUILD.bazel")), executable = False)
    return ctx.repo_metadata(reproducible = True)

arm_gnu_aarch64 = repository_rule(implementation = _arm_gnu_aarch64_impl)

def _arm_link_runtime_impl(ctx):
    # Clang and Cargo need a single directory containing the stripped libraries,
    # their relative links, the linker script, and the startup files.
    files = {}
    copies = []
    prefix = ctx.label.workspace_root + "/inputs/"
    for src in ctx.files.srcs:
        path = src.path.removeprefix(prefix)
        out = ctx.actions.declare_file(path)
        files[path] = out
        if path in _SHARED:
            ctx.actions.run(
                executable = ctx.attr._strip[DefaultInfo].files_to_run,
                arguments = ["--strip-debug", "-o", out.path, src.path],
                inputs = [src],
                outputs = [out],
                mnemonic = "StripArmGlibc",
            )
        else:
            copies.append((src, out))
        if path == _GCC + "/crtend.o":
            # Arm's GCC crtend.o and crtendS.o are identical.
            end_s = ctx.actions.declare_file(_GCC + "/crtendS.o")
            files[_GCC + "/crtendS.o"] = end_s
            copies.append((src, end_s))
    args = ctx.actions.args()
    for src, out in copies:
        args.add(src)
        args.add(out)
    ctx.actions.run_shell(
        arguments = [args],
        inputs = depset([src for src, _ in copies]),
        outputs = [out for _, out in copies],
        command = 'set -eu; while [ "$#" -gt 0 ]; do cp "$1" "$2"; shift 2; done',
        mnemonic = "CopyArmLinkFiles",
    )
    for name, major in _SONAMES.items():
        if name != "libc":
            path = _GLIBC + "/usr/lib64/" + name + ".so"
            link = ctx.actions.declare_symlink(path)
            ctx.actions.symlink(output = link, target_path = "../../lib64/" + name + ".so." + major)
            files[path] = link
    return [
        DefaultInfo(files = depset(files.values())),
        OutputGroupInfo(
            gcc_pie_startup_objects = depset([files[_GCC + "/" + name] for name in ["crtbeginS.o", "crtendS.o"]]),
            linux_link_files = depset([f for f in files.values() if f.basename == "libc_nonshared.a" or
                                                                    (".so" in f.basename and not f.basename.startswith("libresolv."))]),
        ),
    ]

arm_link_runtime = rule(
    implementation = _arm_link_runtime_impl,
    attrs = {
        "srcs": attr.label_list(allow_files = True, mandatory = True),
        "_strip": attr.label(default = "@llvm//tools:llvm-strip", allow_files = True, executable = True, cfg = "exec"),
    },
)
