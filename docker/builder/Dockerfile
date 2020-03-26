FROM ubuntu:19.10

RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
            bash \
            ccache \
            cmake \
            curl \
            expect \
            g++ \
            gcc \
            ninja-build \
            perl \
            pkg-config \
            python \
            python-lxml \
            python-requests \
            python-termcolor \
            sudo \
            tzdata \
            clang \
            clang-tidy \
            lld \
            lldb

COPY build.sh /

CMD ["/bin/bash", "/build.sh"]
