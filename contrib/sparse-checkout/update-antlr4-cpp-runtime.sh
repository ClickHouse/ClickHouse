#!/bin/sh

echo "Using sparse checkout for antlr4-cpp-runtime"

FILES_TO_CHECKOUT=$(git rev-parse --git-dir)/info/sparse-checkout
echo '!/*' > $FILES_TO_CHECKOUT
echo '/runtime/Cpp/runtime/src/*' >> $FILES_TO_CHECKOUT
echo '/runtime/Cpp/README.md' >> $FILES_TO_CHECKOUT
echo '/runtime/Cpp/VERSION' >> $FILES_TO_CHECKOUT
echo '/LICENSE.txt' >> $FILES_TO_CHECKOUT

git config core.sparsecheckout true
git checkout $1
git read-tree -mu HEAD
