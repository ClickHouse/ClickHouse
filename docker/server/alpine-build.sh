#!/bin/bash
set -x

REPO_CHANNEL="${REPO_CHANNEL:-stable}" # lts / testing / prestable / etc
REPO_URL="${REPO_URL:-"https://repo.yandex.ru/clickhouse/tgz/${REPO_CHANNEL}"}"
VERSION="${VERSION:-20.9.3.45}"

# where original files live
DOCKER_BUILD_FOLDER="${BASH_SOURCE%/*}"

# we will create root for our image here
CONTAINER_ROOT_FOLDER="${DOCKER_BUILD_FOLDER}/alpine-root"

# where to put downloaded tgz
TGZ_PACKAGES_FOLDER="${CONTAINER_ROOT_FOLDER}/tgz-packages"

# clean up the root from old runs
rm -rf "$CONTAINER_ROOT_FOLDER"

mkdir -p "$TGZ_PACKAGES_FOLDER"

PACKAGES=( "clickhouse-client" "clickhouse-server" "clickhouse-common-static" )

# download tars from the repo
for package in "${PACKAGES[@]}"
do
    wget -q --show-progress "${REPO_URL}/${package}-${VERSION}.tgz" -O "${TGZ_PACKAGES_FOLDER}/${package}-${VERSION}.tgz"
done

# unpack tars
for package in "${PACKAGES[@]}"
do
    tar xvzf "${TGZ_PACKAGES_FOLDER}/${package}-${VERSION}.tgz" --strip-components=2 -C "$CONTAINER_ROOT_FOLDER"
done

# prepare few more folders
mkdir -p "${CONTAINER_ROOT_FOLDER}/etc/clickhouse-server/users.d" \
         "${CONTAINER_ROOT_FOLDER}/etc/clickhouse-server/config.d" \
         "${CONTAINER_ROOT_FOLDER}/var/log/clickhouse-server" \
         "${CONTAINER_ROOT_FOLDER}/var/lib/clickhouse" \
         "${CONTAINER_ROOT_FOLDER}/docker-entrypoint-initdb.d" \
         "${CONTAINER_ROOT_FOLDER}/lib64"

cp "${DOCKER_BUILD_FOLDER}/docker_related_config.xml" "${CONTAINER_ROOT_FOLDER}/etc/clickhouse-server/config.d/"
cp "${DOCKER_BUILD_FOLDER}/entrypoint.alpine.sh"      "${CONTAINER_ROOT_FOLDER}/entrypoint.sh"

## get glibc components from ubuntu 20.04 and put them to expected place
docker pull ubuntu:20.04
ubuntu20image=$(docker create --rm ubuntu:20.04)
docker cp -L ${ubuntu20image}:/lib/x86_64-linux-gnu/libc.so.6       "${CONTAINER_ROOT_FOLDER}/lib"
docker cp -L ${ubuntu20image}:/lib/x86_64-linux-gnu/libdl.so.2      "${CONTAINER_ROOT_FOLDER}/lib"
docker cp -L ${ubuntu20image}:/lib/x86_64-linux-gnu/libm.so.6       "${CONTAINER_ROOT_FOLDER}/lib"
docker cp -L ${ubuntu20image}:/lib/x86_64-linux-gnu/libpthread.so.0 "${CONTAINER_ROOT_FOLDER}/lib"
docker cp -L ${ubuntu20image}:/lib/x86_64-linux-gnu/librt.so.1      "${CONTAINER_ROOT_FOLDER}/lib"
docker cp -L ${ubuntu20image}:/lib/x86_64-linux-gnu/libnss_dns.so.2 "${CONTAINER_ROOT_FOLDER}/lib"
docker cp -L ${ubuntu20image}:/lib/x86_64-linux-gnu/libresolv.so.2  "${CONTAINER_ROOT_FOLDER}/lib"
docker cp -L ${ubuntu20image}:/lib64/ld-linux-x86-64.so.2           "${CONTAINER_ROOT_FOLDER}/lib64"

docker build "$DOCKER_BUILD_FOLDER" -f Dockerfile.alpine -t "yandex/clickhouse-server:${VERSION}-alpine" --pull