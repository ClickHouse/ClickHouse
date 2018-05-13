#!/usr/bin/env bash
set -e -x

source default-config

./check-docker.sh

# http://fl47l1n3.net/2015/12/24/binfmt/
sudo apt-get -y install qemu-user-static

pushd docker-multiarch

sudo ./update.sh \
 -a "$DOCKER_UBUNTU_ARCH" \
 -v "$DOCKER_UBUNTU_VERSION" \
 -q "$DOCKER_UBUNTU_QUEMU_ARCH" \
 -u "$DOCKER_UBUNTU_QEMU_VER" \
 -d "$DOCKER_UBUNTU_REPO" \
 -t "$DOCKER_UBUNTU_TAG_ARCH"

docker run --rm --privileged multiarch/qemu-user-static:register

popd
