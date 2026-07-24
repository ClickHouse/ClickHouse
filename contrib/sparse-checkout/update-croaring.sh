#!/bin/sh

echo "Using sparse checkout for croaring"

FILES_TO_CHECKOUT=$(git rev-parse --git-dir)/info/sparse-checkout
echo '/*' > $FILES_TO_CHECKOUT
echo '!/benchmarks/*' >> $FILES_TO_CHECKOUT
echo '!/tests/*' >> $FILES_TO_CHECKOUT

git config core.sparsecheckout true
git checkout $1
git read-tree -mu HEAD
