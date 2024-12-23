#!/bin/sh

set -e

git config submodule."contrib/llvm-project".update '!../sparse-checkout/update-llvm-project.sh'
git config submodule."contrib/croaring".update '!../sparse-checkout/update-croaring.sh'
git config submodule."contrib/aws".update '!../sparse-checkout/update-aws.sh'
git config submodule."contrib/openssl".update '!../sparse-checkout/update-openssl.sh'
git config submodule."contrib/arrow".update '!../sparse-checkout/update-arrow.sh'
git config submodule."contrib/grpc".update '!../sparse-checkout/update-grpc.sh'
git config submodule."contrib/orc".update '!../sparse-checkout/update-orc.sh'
git config submodule."contrib/h3".update '!../sparse-checkout/update-h3.sh'
git config submodule."contrib/icu".update '!../sparse-checkout/update-icu.sh'
git config submodule."contrib/boost".update '!../sparse-checkout/update-boost.sh'
git config submodule."contrib/aws-s2n-tls".update '!../sparse-checkout/update-aws-s2n-tls.sh'
git config submodule."contrib/protobuf".update '!../sparse-checkout/update-protobuf.sh'
git config submodule."contrib/postgres".update '!../sparse-checkout/update-postgres.sh'
git config submodule."contrib/libxml2".update '!../sparse-checkout/update-libxml2.sh'
git config submodule."contrib/brotli".update '!../sparse-checkout/update-brotli.sh'
