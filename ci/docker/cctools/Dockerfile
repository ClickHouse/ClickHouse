# docker build -t clickhouse/cctools .

# This is a hack to significantly reduce the build time of the clickhouse/binary-builder
# It's based on the assumption that we don't care of the cctools version so much
# It even does not depend on the clickhouse/fasttest in the `docker/images.json`
ARG FROM_TAG=latest
FROM clickhouse/fasttest:$FROM_TAG AS builder

ENV CC=clang-${LLVM_VERSION}
ENV CXX=clang++-${LLVM_VERSION}

RUN git clone https://github.com/tpoechtrager/apple-libtapi.git \
    && cd apple-libtapi \
    && git checkout 15dfc2a8c9a2a89d06ff227560a69f5265b692f9 \
    && INSTALLPREFIX=/cctools NINJA=1 ./build.sh \
    && ./install.sh \
    && cd .. \
    && rm -rf apple-libtapi

# Build and install tools for cross-linking to Darwin (x86-64)
# Build and install tools for cross-linking to Darwin (aarch64)
RUN git clone https://github.com/tpoechtrager/cctools-port.git --branch 986-ld64-711 \
    && apt-get update \
    && apt-get install --yes \
        make \
    && cd cctools-port/cctools \
    && ./configure --prefix=/cctools --with-libtapi=/cctools \
        --target=x86_64-apple-darwin \
    && make install -j$(nproc) \
    && make clean \
    && ./configure --prefix=/cctools --with-libtapi=/cctools \
        --target=aarch64-apple-darwin \
    && make install -j$(nproc) \
    && cd ../.. \
    && rm -rf cctools-port

#
# GDB
#
# ld from binutils is 2.38, which has the following error:
#
#     DWARF error: invalid or unhandled FORM value: 0x23
#
ENV LD=ld.lld-${LLVM_VERSION}
ARG GDB_VERSION=16.2
RUN apt-get update \
    && apt-get install --yes \
        libgmp-dev \
        libmpfr-dev \
    && wget https://sourceware.org/pub/gdb/releases/gdb-$GDB_VERSION.tar.gz \
    && tar -xvf gdb-$GDB_VERSION.tar.gz \
    && cd gdb-$GDB_VERSION \
    && ./configure --prefix=/opt/gdb --with-separate-debug-dir=/usr/lib/debug \
    && make -j $(nproc) \
    && make install \
    && rm -fr gdb-$GDB_VERSION gdb-$GDB_VERSION.tar.gz

# just in case
RUN /opt/gdb/bin/gdb --version

#
# OpenSSL FIPS
#
# Three things are required to set OpenSSL in FIPS-permissive mode:
# - /usr/lib/x86_64-linux-gnu/ossl-modules/fips.so
# - /etc/ssl/fipsmodule.cnf
# - Modifications to /etc/ssl/openssl.conf
# FIPS provider status may be verified with the output of `openssl list -providers`:
#
#  Providers:
#    fips
#      name: OpenSSL Default Provider
#      version: 3.1.2
#      status: active
#
# Version 3.1.2 is special, it passed FIPS 140-3 certification
# and can produce a fips.so provider module.
# Provider can be loaded by any OpenSSL version >= 3.1.
# https://openssl-library.org/post/2025-03-11-fips-140-3/
ENV OPENSSL_VERSION=3.1.2

RUN apt-get update && apt-get install -y \
    build-essential \
    wget \
    perl \
    python3 \
    make \
    git \
    ca-certificates

WORKDIR /tmp
RUN wget https://www.openssl.org/source/openssl-${OPENSSL_VERSION}.tar.gz && \
    tar xzf openssl-${OPENSSL_VERSION}.tar.gz && \
    cd openssl-${OPENSSL_VERSION} && \
    ./Configure enable-fips && \
    make -j$(nproc) && \
    mkdir -p /opt/openssl-fips && \
    mv "apps/openssl.cnf" "/opt/openssl-fips/openssl.cnf" && \
    mv "providers/fips.so" "/opt/openssl-fips/fips.so" && \
    mv "providers/fipsmodule.cnf" "/opt/openssl-fips/fipsmodule.cnf"

# Patching fipsmodule.cnf with a custom module location.
RUN cat >> /opt/openssl-fips/fipsmodule.cnf <<EOF
install-version = 1
module = /etc/ssl/fips.so
EOF

# Patching openssl.cnf to enable FIPS provider.
# Although documentation mentions setting "default_properties = fips=yes",
# we don't want to set it, as it will force FIPS mode
# for any OpenSSL usage in system (things will break).
#
# We will put fipsmodule.cnf to /etc/ssl in target containers
RUN cat >> /opt/openssl-fips/openssl.cnf <<EOF

.include /etc/ssl/fipsmodule.cnf

[openssl_init]
providers = provider_sect
ssl_conf = ssl_sect

[ssl_sect]
system_default = system_default_sect

[system_default_sect]
CipherString = DEFAULT:@SECLEVEL=2

[provider_sect]
fips = fips_sect
default = default_sect

[default_sect]
activate = 1

EOF

FROM scratch
COPY --from=builder /cctools /cctools
COPY --from=builder /opt/gdb /opt/gdb
COPY --from=builder /opt/openssl-fips /opt/openssl-fips

# NOTE: this file should be kept in sync with other docker images, since they use COPY from this image
