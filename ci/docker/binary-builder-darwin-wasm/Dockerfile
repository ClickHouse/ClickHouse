# docker build -t clickhouse/binary-builder-darwin-wasm .
# `clickhouse/binary-builder` plus the SDKs that only the Darwin builds and `Build (wasm64)` use.
# They are kept out of `clickhouse/binary-builder`, so that the other build jobs do not pull them.
ARG FROM_TAG
FROM clickhouse/binary-builder:$FROM_TAG
ARG TARGETARCH

# toolchain and SDK for Darwin
RUN curl -sL -O https://github.com/phracker/MacOSX-SDKs/releases/download/11.3/MacOSX11.0.sdk.tar.xz \
  && mkdir -p /build/cmake/toolchain/darwin-x86_64 \
  && tar xJf /MacOSX11.0.sdk.tar.xz -C /build/cmake/toolchain/darwin-x86_64 --strip-components=1 \
  && ln -sf darwin-x86_64 /build/cmake/toolchain/darwin-aarch64 \
  && rm /MacOSX11.0.sdk.tar.xz

# Node.js for the wasm64 smoke test (see build_clickhouse.py). Emscripten 6 emits
# Memory64 code, and its generated startup script refuses engines older than Node.js 24.
ARG NODE_VERSION=24.19.0
RUN case "${TARGETARCH}" in \
      amd64) NODE_ARCH=x64 ;; \
      arm64) NODE_ARCH=arm64 ;; \
      *) echo "Unsupported TARGETARCH: ${TARGETARCH}" && exit 1 ;; \
    esac \
  && curl -fsSL "https://nodejs.org/dist/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-${NODE_ARCH}.tar.xz" | tar -xJf - -C /opt \
  && ln -sf "/opt/node-v${NODE_VERSION}-linux-${NODE_ARCH}/bin/node" /usr/local/bin/node

# Emscripten SDK for the experimental WebAssembly build (see the `OS_WASM` arms in cmake/).
# The version has to match the one the wasm64 target is verified with.
ARG EMSDK_VERSION=6.0.5
ENV EMSDK=/opt/emsdk
ENV PATH="$PATH:/opt/emsdk/upstream/emscripten"

# Install the SDK and pre-generate the wasm64 variant of the Emscripten system libraries.
# `-sMEMORY64`, `-fwasm-exceptions` and `-pthread` are ABI flags applied to every translation unit,
# and the link settings select startup objects (`-sPROXY_TO_PTHREAD` links
# `crt1_proxy_main.o` instead of `crt1.o`). The optimization level at the link selects
# library variants too: an unoptimized link picks the `-debug` builds of libc, libc++
# and the stubs (`libstubs-debug.a`), while the `-O3 ... -g0` CI link needs the release
# ones (`libstubs.a`, `libc-mt.a`, ...). `emcc` compiles whatever the link needs
# lazily at first use into its cache, and building libc, libc++ and friends would
# cost the CI link several minutes - so the link flags here mirror the `clickhouse`
# link. Keep them in sync with the `add_link_options` in the `OS_WASM` branch of
# cmake/target.cmake plus the RelWithDebInfo optimization flags.
#
# Pre-warming cannot cover everything: the link also writes `symbol_lists/<hash>.json`
# cache entries whose hash covers the resolved settings, and some settings are derived
# from the program being linked (e.g. `MAIN_READS_PARAMS`, from the signature of `main`),
# so the real `clickhouse` link misses the entry the trivial program above cached and
# generates its own. The build in CI runs as an unprivileged user, hence the cache has
# to be writable for it. The `chmod` runs in the same `RUN` that writes the cache: in a
# later one it would store a second copy of every cache file in a new layer.
RUN curl -fsSL "https://github.com/emscripten-core/emsdk/archive/refs/tags/${EMSDK_VERSION}.tar.gz" | tar -xzf - -C /opt \
  && mv "/opt/emsdk-${EMSDK_VERSION}" /opt/emsdk \
  && /opt/emsdk/emsdk install "${EMSDK_VERSION}" \
  && /opt/emsdk/emsdk activate "${EMSDK_VERSION}" \
  && rm -rf /opt/emsdk/downloads \
  && echo 'int main() { return 0; }' > /tmp/em_prewarm.cpp \
  && em++ -O3 -sMEMORY64=1 -fwasm-exceptions -pthread \
      -sSTACK_SIZE=8388608 -sINITIAL_MEMORY=2147483648 -sALLOW_MEMORY_GROWTH=1 -sMAXIMUM_MEMORY=17179869184 \
      -g0 -sPROXY_TO_PTHREAD=1 -sEXIT_RUNTIME=1 \
      /tmp/em_prewarm.cpp -o /tmp/em_prewarm.js \
  && rm -f /tmp/em_prewarm.* \
  && chmod -R a+rwX /opt/emsdk/upstream/emscripten/cache
