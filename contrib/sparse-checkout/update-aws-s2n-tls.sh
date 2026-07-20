#!/bin/sh

echo "Using sparse checkout for aws-s2n-tls"

FILES_TO_CHECKOUT=$(git rev-parse --git-dir)/info/sparse-checkout
echo '/*' > $FILES_TO_CHECKOUT
echo '!/test/*' >> $FILES_TO_CHECKOUT
echo '!/docs/*' >> $FILES_TO_CHECKOUT
echo '!/compliance/*' >> $FILES_TO_CHECKOUT

git config core.sparsecheckout true
git checkout $1
git read-tree -mu HEAD
