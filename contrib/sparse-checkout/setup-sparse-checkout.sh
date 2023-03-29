#!/bin/sh

git config submodule."contrib/llvm-project".update '!../sparse-checkout/update-llvm-project.sh'
git config submodule."contrib/croaring".update '!../sparse-checkout/update-croaring.sh'
git config submodule."contrib/aws".update '!../sparse-checkout/update-aws.sh'
git config submodule."contrib/openssl".update '!../sparse-checkout/update-openssl.sh'
git config submodule."contrib/boringssl".update '!../sparse-checkout/update-boringssl.sh'
git config submodule."contrib/arrow".update '!../sparse-checkout/update-arrow.sh'
git config submodule."contrib/grpc".update '!../sparse-checkout/update-grpc.sh'
git config submodule."contrib/orc".update '!../sparse-checkout/update-orc.sh'
git config submodule."contrib/h3".update '!../sparse-checkout/update-h3.sh'
