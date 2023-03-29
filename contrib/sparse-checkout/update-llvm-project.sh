#!/bin/sh

echo "Using sparse checkout for llvm-project"

FILES_TO_CHECKOUT=$(git rev-parse --git-dir)/info/sparse-checkout
echo '/*' > $FILES_TO_CHECKOUT
echo '!/llvm/test/*' >> $FILES_TO_CHECKOUT
echo '!/llvm/docs/*' >> $FILES_TO_CHECKOUT
echo '!/llvm/unittests/*' >> $FILES_TO_CHECKOUT
echo '!/llvm/tools/*' >> $FILES_TO_CHECKOUT
echo '!/clang/*' >> $FILES_TO_CHECKOUT
echo '!/clang-tools-extra/*' >> $FILES_TO_CHECKOUT
echo '!/lldb/*' >> $FILES_TO_CHECKOUT
echo '!/mlir/*' >> $FILES_TO_CHECKOUT
echo '!/polly/*' >> $FILES_TO_CHECKOUT
echo '!/lld/*' >> $FILES_TO_CHECKOUT
echo '!/flang/*' >> $FILES_TO_CHECKOUT
echo '!/libcxx/test/*' >> $FILES_TO_CHECKOUT
echo '!/compiler-rt/test/*' >> $FILES_TO_CHECKOUT

git config core.sparsecheckout true
git checkout $1
git read-tree -mu HEAD
