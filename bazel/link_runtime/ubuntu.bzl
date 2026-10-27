"""The Ubuntu packages used for ClickHouse's x86-64 glibc and GCC link inputs."""

_TRIPLE = "x86_64-linux-gnu"
_GLIBC = "linux-x86_64/" + _TRIPLE + "/libc"
_GCC = "lib/gcc/" + _TRIPLE + "/9"
_SONAMES = {
    "libc": "6",
    "libdl": "2",
    "libm": "6",
    "libpthread": "0",
    "libresolv": "2",
    "librt": "1",
}

def _extract(ctx, package, sha256, members):
    ctx.download_and_extract(
        url = "https://launchpadlibrarian.net/" + package,
        sha256 = sha256,
        type = "deb",
        output = "package",
    )

    # Bazel renames tar members before applying strip_prefix. This extracts only
    # the named files; none of the Ubuntu package's other files or links survive.
    ctx.extract(
        "package/data.tar.xz",
        strip_prefix = "selected",
        rename_files = {"./" + src: "selected/" + dst for src, dst in members.items()},
    )
    ctx.delete("package")

def _ubuntu_focal_amd64_impl(ctx):
    libc = "lib/" + _TRIPLE + "/"
    dev = "usr/lib/" + _TRIPLE + "/"
    lib64 = _GLIBC + "/lib64/"
    usr_lib64 = _GLIBC + "/usr/lib64/"
    _extract(
        ctx,
        "511639304/libc6_2.31-0ubuntu9.2_amd64.deb",
        "be7386ac249db6f5500f483456c135553a7b5b333388afa150a1f2ff3a294bb1",
        {libc + name + "-2.31.so": lib64 + name + ".so." + major for name, major in _SONAMES.items()} |
        {libc + "ld-2.31.so": _GLIBC + "/lib/ld-linux-x86-64.so.2"},
    )
    _extract(
        ctx,
        "511639298/libc6-dev_2.31-0ubuntu9.2_amd64.deb",
        "6741a2ca31e5d324895d85461050512694f3bab45254911c5634669fe0f0d70b",
        {dev + name: usr_lib64 + name for name in ["crt1.o", "crti.o", "crtn.o", "libc.so", "libc_nonshared.a"]} |
        {dev + "Scrt1.o": lib64 + "Scrt1.o"},
    )
    _extract(
        ctx,
        "492520941/libgcc-9-dev_9.3.0-17ubuntu1~20.04_amd64.deb",
        "b0b84d9a7be62a637c151380079799764e27c05815fed64510edbf6792e38d2e",
        {
            "usr/" + _GCC + "/" + name: "linux-x86_64/" + _GCC + "/" + name
            for name in ["crtbegin.o", "crtbeginS.o", "crtbeginT.o", "crtend.o", "crtendS.o", "libgcc.a"]
        },
    )

    # Preserve the executable bits and relocatable linker script in contrib/sysroot.
    for name in ["libdl.so.2", "libm.so.6", "libresolv.so.2", "librt.so.1", "Scrt1.o"]:
        ctx.template(lib64 + name, lib64 + name, executable = True)
    script = usr_lib64 + "libc.so"
    ctx.template(script, script, {
        "/" + libc + "libc.so.6": "../../lib64/libc.so.6",
        "/" + dev + "libc_nonshared.a": "./libc_nonshared.a",
        "/" + libc + "ld-linux-x86-64.so.2": "../../lib/ld-linux-x86-64.so.2",
    }, executable = False)
    for name, major in _SONAMES.items():
        if name != "libc":
            # ctx.symlink makes an absolute link; keep the original relative name.
            result = ctx.execute(["ln", "-s", "../../lib64/" + name + ".so." + major, ctx.path(usr_lib64 + name + ".so")])
            if result.return_code:
                fail(result.stderr)

    ctx.file(_GLIBC + "/usr/lib/.gitkeep", "", executable = False)
    ctx.file("BUILD.bazel", ctx.read(Label("//bazel/link_runtime:ubuntu.BUILD.bazel")), executable = False)
    return ctx.repo_metadata(reproducible = True)

ubuntu_focal_amd64 = repository_rule(implementation = _ubuntu_focal_amd64_impl)
