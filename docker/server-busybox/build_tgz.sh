# clone sources and run a build
git clone --depth=1 --recursive -b v20.11.1.4939-testing https://github.com/ClickHouse/ClickHouse.git

git clone \
    -b v20.11.1.4939-testing \
    --depth=1 --shallow-submodules --recurse-submodules \
    https://github.com/ClickHouse/ClickHouse.git

cd ./ClickHouse
docker pull yandex/clickhouse-deb-builder:latest

docker run --network=host --rm \
--volume="$(realpath ./PACKAGES)":/output \
--volume="$(realpath .)":/build \
--volume="$(realpath ~/.ccache)":/ccache \
-e DEB_CC=clang-11 \
-e DEB_CXX=clang++-11 \
-e CCACHE_DIR=/ccache \
-e CCACHE_BASEDIR=/build \
-e CCACHE_NOHASHDIR=true \
-e CCACHE_COMPILERCHECK=content \
-e CCACHE_MAXSIZE=15G \
-e ALIEN_PKGS='--rpm --tgz' \
-e VERSION_STRING='20.11.1.4939' \
-e AUTHOR="$(whoami)" \
-e CMAKE_FLAGS="$CMAKE_FLAGS -DADD_GDB_INDEX_FOR_GOLD=1 -DLINKER_NAME=lld" \
yandex/clickhouse-deb-builder:latest

# you can add into CMAKE_FLAGS some flags to exclude some libraries
# -DENABLE_LIBRARIES=0 -DUSE_UNWIND=1 -DENABLE_JEMALLOC=1 -DENABLE_REPLXX=1 -DENABLE_TESTS=0 -DENABLE_UTILS=0 -DENABLE_EMBEDDED_COMPILER=0 -DENABLE_THINLTO=0 -DCMAKE_BUILD_TYPE=Release

# clickhouse-deb-builder will write some files as root
sudo chown -R $(id -u):$(id -g) ClickHouse