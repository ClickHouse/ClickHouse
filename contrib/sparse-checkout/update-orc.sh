#!/bin/sh

echo "Using sparse checkout for orc"

FILES_TO_CHECKOUT=$(git rev-parse --git-dir)/info/sparse-checkout
echo '/*' > $FILES_TO_CHECKOUT
echo '!/*/*' >> $FILES_TO_CHECKOUT
echo '/c++/*' >> $FILES_TO_CHECKOUT
echo '/proto/*' >> $FILES_TO_CHECKOUT

git config core.sparsecheckout true
git checkout $1
git read-tree -mu HEAD
