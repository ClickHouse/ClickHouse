# docker build -t clickhouse/wasm-builder .
# Toolchain for the standalone WebAssembly build of the SQL parser - see utils/wasm-parser.
# Used by the `Build (wasm_parser)` job, which builds the module inside this image, drives it with
# the Node.js that the base image already provides, and publishes the result as an artifact.

FROM node:22-bookworm-slim

# `python3` is here because praktika runs the job script (ci/jobs/build_wasm_parser.py) inside this
# image; `python3-minimal` would not do, since the praktika modules it imports reach across the
# standard library that the minimal package leaves out. `git` is here because that same script
# populates the parser's contrib submodule working trees before configuring - the CI checkout
# arrives without them.
RUN apt-get update \
    && env DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends \
        ca-certificates \
        cmake \
        curl \
        git \
        gzip \
        ninja-build \
        python3 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

# The releases are named `x86_64` and `arm64`, which is neither what `uname -m` nor what
# `dpkg --print-architecture` calls them on both platforms.
ARG WASI_SDK_VERSION=33
RUN arch="$(dpkg --print-architecture)" \
    && case "$arch" in \
        amd64) wasi_arch=x86_64 ;; \
        arm64) wasi_arch=arm64 ;; \
        *) echo "no wasi-sdk release for $arch" >&2; exit 1 ;; \
    esac \
    && mkdir -p /opt/wasi-sdk \
    && curl -fsSL \
        "https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-${WASI_SDK_VERSION}/wasi-sdk-${WASI_SDK_VERSION}.0-${wasi_arch}-linux.tar.gz" \
        | tar xz -C /opt/wasi-sdk --strip-components=1

# Two thirds of the SDK is sysroots and libraries for targets this build never names, plus tooling
# (clang-tidy, lldb, libclang) that a compile and a link do not go through. Dropping them takes the
# SDK from 653 MB to 233 MB, which is what the job has to pull before it can start.
RUN cd /opt/wasi-sdk/share/wasi-sysroot \
    && find include lib -mindepth 1 -maxdepth 1 \
        ! -name wasm32-wasip1 ! -name 'c++' -exec rm -rf {} + \
    && cd /opt/wasi-sdk \
    && rm -rf share/man share/misc include lib/libclang.so* lib/liblldb.so* \
    && rm -f bin/clang-tidy bin/clang-format bin/clang-scan-deps bin/clang-apply-replacements \
             bin/lldb bin/wasm-component-ld bin/llvm-objdump

ENV WASI_SDK=/opt/wasi-sdk

# Fail the image build, rather than the test that uses it, if the pruning above went too far.
# The parser is C++ linked against a C translation unit, so probe both: the C++ sysroot and the
# C++ headers survive the pruning only by name, and nothing else here would notice if they did not.
RUN printf 'int main(void) { return 0; }\n' > /tmp/probe.c \
    && "$WASI_SDK/bin/clang" --target=wasm32-wasip1 --sysroot="$WASI_SDK/share/wasi-sysroot" \
        -flto /tmp/probe.c -lsetjmp -o /tmp/probe.wasm \
    && printf '#include <string>\nint main() { return static_cast<int>(std::string("x").size()) - 1; }\n' > /tmp/probe.cpp \
    && "$WASI_SDK/bin/clang++" --target=wasm32-wasip1 --sysroot="$WASI_SDK/share/wasi-sysroot" \
        -fno-exceptions -flto /tmp/probe.cpp -o /tmp/probe_cpp.wasm \
    && rm -f /tmp/probe.c /tmp/probe.cpp /tmp/probe.wasm /tmp/probe_cpp.wasm
