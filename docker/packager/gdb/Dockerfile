# docker build -t clickhouse/gdb .

ARG FROM_TAG=latest
FROM clickhouse/fasttest:$FROM_TAG

ENV CC=clang-${LLVM_VERSION}
ENV CXX=clang++-${LLVM_VERSION}
# ld from binutils is 2.38, which has the following error:
#
#     DWARF error: invalid or unhandled FORM value: 0x23
#
ENV LD=ld.lld-${LLVM_VERSION}

ARG GDB_VERSION=15.1

# gdb dependencies
RUN apt-get update \
    && apt-get install --yes \
        libgmp-dev \
        libmpfr-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

RUN wget https://sourceware.org/pub/gdb/releases/gdb-$GDB_VERSION.tar.gz \
    && tar -xvf gdb-$GDB_VERSION.tar.gz \
    && cd gdb-$GDB_VERSION \
    && ./configure --prefix=/usr \
    && make -j $(nproc) \
    && make install \
    && rm -fr gdb-$GDB_VERSION gdb-$GDB_VERSION.tar.gz
