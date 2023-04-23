#!/bin/sh

echo "Using sparse checkout for h3"

FILES_TO_CHECKOUT=$(git rev-parse --git-dir)/info/sparse-checkout
echo '/*' > $FILES_TO_CHECKOUT
echo '!/tests/*' >> $FILES_TO_CHECKOUT
echo '!/website/*' >> $FILES_TO_CHECKOUT

git config core.sparsecheckout true
git checkout $1
git read-tree -mu HEAD
