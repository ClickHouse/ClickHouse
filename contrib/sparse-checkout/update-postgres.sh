#!/bin/sh

echo "Using sparse checkout for postgres"

FILES_TO_CHECKOUT=$(git rev-parse --git-dir)/info/sparse-checkout
echo '!/*' > $FILES_TO_CHECKOUT
echo '/src/interfaces/libpq/*' >> $FILES_TO_CHECKOUT
echo '!/src/interfaces/libpq/*/*' >> $FILES_TO_CHECKOUT
echo '/src/common/*' >> $FILES_TO_CHECKOUT
echo '!/src/port/*/*' >> $FILES_TO_CHECKOUT
echo '/src/port/*' >> $FILES_TO_CHECKOUT
echo '/src/include/*' >> $FILES_TO_CHECKOUT

git config core.sparsecheckout true
git checkout $1
git read-tree -mu HEAD
