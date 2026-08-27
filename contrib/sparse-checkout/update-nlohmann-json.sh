#!/bin/sh

echo "Using sparse checkout for nlohmann's json"

FILES_TO_CHECKOUT=$(git rev-parse --git-dir)/info/sparse-checkout
echo '!/*' > $FILES_TO_CHECKOUT
echo '/single_include/**/*' >> $FILES_TO_CHECKOUT
echo '/LICENSES/*' >> $FILES_TO_CHECKOUT

git config core.sparsecheckout true
git checkout $1
git read-tree -mu HEAD
