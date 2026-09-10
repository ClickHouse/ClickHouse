# docker build -t clickhouse/utils .

ARG FROM_TAG=latest
FROM clickhouse/fasttest:$FROM_TAG

RUN apt-get update && env DEBIAN_FRONTEND=noninteractive apt-get install --yes \
        libjson-c5 \
        p11-kit \
        p11-kit-modules \
        zip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

ARG RC_GIT_REV=607b3c1d952b06c0affa12106f9d6bad2ffbc44a
RUN ln -s "$(command -v clang-$LLVM_VERSION)" /usr/local/bin/cc \
    && cargo install --git https://github.com/indygreg/apple-platform-rs \
        --rev $RC_GIT_REV apple-codesign --features pkcs11 --locked \
    && rm -rf /rust/cargo/registry /rust/cargo/git \
    && rcodesign --version

ARG KMS_PKCS11_VERSION=v0.0.17
ARG KMS_PKCS11_SHA256=621db84872f1eacd84436b84a14f57efd155a4d389f7b457e0c012caa4e0b584
RUN mkdir -p /usr/lib/aws-kms-pkcs11 \
    && curl -fsSL -o /usr/lib/aws-kms-pkcs11/aws_kms_pkcs11.so \
        "https://github.com/JackOfMostTrades/aws-kms-pkcs11/releases/download/$KMS_PKCS11_VERSION/aws_kms_pkcs11-ubuntu24.04-x86_64.so" \
    && echo "$KMS_PKCS11_SHA256  /usr/lib/aws-kms-pkcs11/aws_kms_pkcs11.so" | sha256sum --check --strict

RUN mkdir -p /usr/share/p11-kit/modules \
    && echo "module: /usr/lib/aws-kms-pkcs11/aws_kms_pkcs11.so" > /usr/share/p11-kit/modules/awskms.module

RUN test -e /usr/lib/x86_64-linux-gnu/p11-kit-proxy.so
ENV PKCS11_PROXY_LIBRARY=/usr/lib/x86_64-linux-gnu/p11-kit-proxy.so
