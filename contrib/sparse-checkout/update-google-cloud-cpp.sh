#!/bin/sh

echo "Using sparse checkout for google-cloud-cpp"

FILES_TO_CHECKOUT=$(git rev-parse --git-dir)/info/sparse-checkout
echo '!/*' > $FILES_TO_CHECKOUT
echo '/google/*' >> $FILES_TO_CHECKOUT
echo '/cmake/*' >> $FILES_TO_CHECKOUT
echo '/protos/*' >> $FILES_TO_CHECKOUT
echo '/external/googleapis' >> $FILES_TO_CHECKOUT

git config core.sparsecheckout true
git checkout $1
git read-tree -mu HEAD
