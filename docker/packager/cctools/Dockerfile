# This is a hack to significantly reduce the build time of the clickhouse/binary-builder
# It's based on the assumption that we don't care of the cctools version so much
# It event does not depend on the clickhouse/fasttest in the `docker/images.json`
ARG FROM_TAG=latest
FROM clickhouse/fasttest:$FROM_TAG as builder

ENV CC=clang-${LLVM_VERSION}
ENV CXX=clang++-${LLVM_VERSION}

RUN git clone https://github.com/tpoechtrager/apple-libtapi.git \
    && cd apple-libtapi \
    && git checkout 15dfc2a8c9a2a89d06ff227560a69f5265b692f9 \
    && INSTALLPREFIX=/cctools ./build.sh \
    && ./install.sh \
    && cd .. \
    && rm -rf apple-libtapi

# Build and install tools for cross-linking to Darwin (x86-64)
# Build and install tools for cross-linking to Darwin (aarch64)
RUN git clone https://github.com/tpoechtrager/cctools-port.git \
    && cd cctools-port/cctools \
    && git checkout 2a3e1c2a6ff54a30f898b70cfb9ba1692a55fad7 \
    && ./configure --prefix=/cctools --with-libtapi=/cctools \
        --target=x86_64-apple-darwin \
    && make install -j$(nproc) \
    && make clean \
    && ./configure --prefix=/cctools --with-libtapi=/cctools \
        --target=aarch64-apple-darwin \
    && make install -j$(nproc) \
    && cd ../.. \
    && rm -rf cctools-port

FROM scratch
COPY --from=builder /cctools /cctools
