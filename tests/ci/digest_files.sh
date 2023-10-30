#!/bin/bash

DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )/../../

# Source files hash:
SOURCE_DIGEST=$(find $DIR/src $DIR/contrib/*-cmake $DIR/cmake $DIR/base $DIR/programs  -type f  | grep -vE '*.md$' | xargs md5sum | awk '{ print $1 }' | sort | md5sum | awk '{ print $1 }')
echo "SOURCE_DIGEST=${SOURCE_DIGEST}"

# Modules hash:
#   git submodule status cmd works regardeles wether modules are cloned or not, drop possible +/- sign as it shows the status (cloned/ not cloned/ changed files) and may vary
MODULES_DIGEST=$(cd $DIR; git submodule status | awk '{ print $1 }' | sed 's/^[ +-]//' | md5sum | awk '{ print $1 }')
echo "MODULES_DIGEST=${MODULES_DIGEST}"

# Docker builder image hash
DOCKER_BUILDER_DIGEST=$(find $DIR/docker/packager/ $DIR/docker/test/util $DIR/docker/test/base -type f | grep -vE '*.md$' | xargs md5sum | awk '{ print $1 }' | sort | md5sum | awk '{ print $1 }')
echo "DOCKER_BUILDER_DIGEST=${DOCKER_BUILDER_DIGEST}"

BUILD_DIGEST=$(echo $SOURCE_DIGEST-$MODULES_DIGEST-$DOCKER_BUILDER_DIGEST | md5sum | awk '{ print $1 }')
echo "BUILD_DIGEST=${BUILD_DIGEST}"

export BUILD_DIGEST=$BUILD_DIGEST
export DOCKER_BUILDER_DIGEST=${DOCKER_BUILDER_DIGEST}
