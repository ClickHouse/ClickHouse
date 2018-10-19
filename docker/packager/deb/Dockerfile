FROM ubuntu:18.04

RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
            bash \
            fakeroot \
            ccache \
            distcc \
            distcc-pump \
            cmake \
            curl \
            gcc-7 \
            g++-7 \
            gcc-8 \
            g++-8 \
            clang-6.0 \
            clang++-6.0 \
            lld-6.0 \
            libclang-6.0-dev \
            libicu-dev \
            liblld-6.0-dev \
            libreadline-dev \
            ninja-build \
            perl \
            pkg-config \
            devscripts \
            debhelper \
            git

CMD /bin/bash build/release --no-pbuilder && mv /*.deb /output
