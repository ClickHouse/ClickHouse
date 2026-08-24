#!/bin/sh

echo "Using sparse checkout for libxml2"

FILES_TO_CHECKOUT=$(git rev-parse --git-dir)/info/sparse-checkout
echo '/*' > $FILES_TO_CHECKOUT
echo '!/result/*' >> $FILES_TO_CHECKOUT
echo '!/test/*' >> $FILES_TO_CHECKOUT
echo '!/doc/*' >> $FILES_TO_CHECKOUT
echo '!/os400/*' >> $FILES_TO_CHECKOUT
echo '!/fuzz/*' >> $FILES_TO_CHECKOUT
echo '!/python/*' >> $FILES_TO_CHECKOUT

git config core.sparsecheckout true
git checkout $1
git read-tree -mu HEAD
