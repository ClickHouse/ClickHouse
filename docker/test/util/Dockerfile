# docker build -t clickhouse/test-util .
FROM ubuntu:22.04

# ARG for quick switch to a given ubuntu mirror
ARG apt_archive="http://archive.ubuntu.com"
RUN sed -i "s|http://archive.ubuntu.com|$apt_archive|g" /etc/apt/sources.list

ENV DEBIAN_FRONTEND=noninteractive LLVM_VERSION=18

RUN apt-get update \
    && apt-get install \
        sudo \
        apt-transport-https \
        apt-utils \
        ca-certificates \
        curl \
        dnsutils \
        gnupg \
        iputils-ping \
        lsb-release \
        wget \
        --yes --no-install-recommends --verbose-versions \
    && export LLVM_PUBKEY_HASH="bda960a8da687a275a2078d43c111d66b1c6a893a3275271beedf266c1ff4a0cdecb429c7a5cccf9f486ea7aa43fd27f" \
    && wget -nv -O /tmp/llvm-snapshot.gpg.key https://apt.llvm.org/llvm-snapshot.gpg.key \
    && echo "${LLVM_PUBKEY_HASH} /tmp/llvm-snapshot.gpg.key" | sha384sum -c \
    && apt-key add /tmp/llvm-snapshot.gpg.key \
    && export CODENAME="$(lsb_release --codename --short | tr 'A-Z' 'a-z')" \
    && echo "deb https://apt.llvm.org/${CODENAME}/ llvm-toolchain-${CODENAME}-${LLVM_VERSION} main" >> \
        /etc/apt/sources.list \
    && apt-get update \
    && apt-get install --yes --no-install-recommends --verbose-versions llvm-${LLVM_VERSION} \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

# Install cmake 3.20+ for Rust support
# Used https://askubuntu.com/a/1157132 as reference
RUN curl -s https://apt.kitware.com/keys/kitware-archive-latest.asc | \
        gpg --dearmor - > /etc/apt/trusted.gpg.d/kitware.gpg && \
    echo "deb https://apt.kitware.com/ubuntu/ $(lsb_release -cs) main" >> /etc/apt/sources.list

# initial packages
RUN apt-get update \
    && apt-get install \
        bash \
        bsdmainutils \
        build-essential \
        git \
        gperf \
        moreutils \
        nasm \
        pigz \
        rename \
        software-properties-common \
        tzdata \
        --yes --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

COPY --from=clickhouse/cctools:0d6b90a7a490 /opt/gdb /opt/gdb
ENV PATH="/opt/gdb/bin:${PATH}"
