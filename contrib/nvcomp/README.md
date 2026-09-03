# nvcomp 5.3.0.16 (CUDA 13) — prebuilt binary

Not a submodule, and not built from source: NVIDIA publishes nvcomp only as a
compiled artifact. `github.com/NVIDIA/nvcomp` is archived and holds nothing but
docs and examples. cuDF requires it unconditionally, so the archive is unpacked
here instead.

Version 5.3.0.16 is the pin from `contrib/cudf/cpp/cmake/thirdparty/get_nvcomp.cmake`.
It must be re-pinned together with cuDF. Refreshing it:

    V=5.3.0.16
    curl -sSfLO "https://developer.download.nvidia.com/compute/nvcomp/redist/nvcomp/linux-x86_64/nvcomp-linux-x86_64-${V}_cuda13-archive.tar.xz"
    tar -xf "nvcomp-linux-x86_64-${V}_cuda13-archive.tar.xz"
    # keep include/, lib/*_static.a, LICENSE, NOTICE - the shared objects are unused

Only the static libraries are kept; ClickHouse links statically. Redistribution
is governed by LICENSE (NVIDIA SDK agreement) - worth a legal read before this
reaches a public branch.
