#!/bin/sh

echo "Using sparse checkout for crc32c"

FILES_TO_CHECKOUT=$(git rev-parse --git-dir)/info/sparse-checkout
echo '/*' > $FILES_TO_CHECKOUT
echo '!/third_party' >> $FILES_TO_CHECKOUT

git config core.sparsecheckout true
git checkout $1
git read-tree -mu HEAD
