#!/bin/sh

echo "Using sparse checkout for aws"

FILES_TO_CHECKOUT=$(git rev-parse --git-dir)/info/sparse-checkout
echo '/*' > $FILES_TO_CHECKOUT
echo '!/*/*' >> $FILES_TO_CHECKOUT
echo '/aws-cpp-sdk-core/*' >> $FILES_TO_CHECKOUT
echo '/aws-cpp-sdk-s3/*' >> $FILES_TO_CHECKOUT

git config core.sparsecheckout true
git checkout $1
git read-tree -mu HEAD
