#!/bin/sh

echo "Using sparse checkout for boringsll"

FILES_TO_CHECKOUT=$(git rev-parse --git-dir)/info/sparse-checkout
echo '/*' > $FILES_TO_CHECKOUT
echo '!/fuzz/*' >> $FILES_TO_CHECKOUT
echo '!/crypto/cipher_extra/test/*' >> $FILES_TO_CHECKOUT
echo '!/third_party/wycheproof_testvectors/*' >> $FILES_TO_CHECKOUT
echo '!/third_party/googletest/*' >> $FILES_TO_CHECKOUT

git config core.sparsecheckout true
git checkout $1
git read-tree -mu HEAD
