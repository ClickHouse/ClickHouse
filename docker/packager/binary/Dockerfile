FROM ubuntu:18.10

RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
            bash \
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
            git

CMD mkdir -p build/build_result && cd build/build_result && cmake .. -DCMAKE_BUILD_TYPE=$BUILD_TYPE -DSANITIZE=$SANITIZER && ninja && mv ./dbms/programs/clickhouse* /output
