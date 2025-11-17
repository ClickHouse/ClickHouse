#!/bin/sh

echo "Using sparse checkout for llvm-project"

FILES_TO_CHECKOUT=$(git rev-parse --git-dir)/info/sparse-checkout
echo '/*' > $FILES_TO_CHECKOUT
echo '!/*/*' >> $FILES_TO_CHECKOUT
echo '/llvm/*' >> $FILES_TO_CHECKOUT
echo '!/llvm/*/*' >> $FILES_TO_CHECKOUT
echo '/llvm/cmake/*' >> $FILES_TO_CHECKOUT
echo '/llvm/projects/*' >> $FILES_TO_CHECKOUT
echo '/llvm/include/*' >> $FILES_TO_CHECKOUT
echo '/llvm/lib/*' >> $FILES_TO_CHECKOUT
echo '/llvm/utils/TableGen/*' >> $FILES_TO_CHECKOUT
echo '/libcxxabi/*' >> $FILES_TO_CHECKOUT
echo '!/libcxxabi/test/*' >> $FILES_TO_CHECKOUT
echo '/libcxx/*' >> $FILES_TO_CHECKOUT
echo '!/libcxx/test/*' >> $FILES_TO_CHECKOUT
echo '/libunwind/*' >> $FILES_TO_CHECKOUT
echo '!/libunwind/test/*' >> $FILES_TO_CHECKOUT
echo '/compiler-rt/*' >> $FILES_TO_CHECKOUT
echo '/libc/*' >> $FILES_TO_CHECKOUT
echo '!/libc/test/*' >> $FILES_TO_CHECKOUT
echo '!/compiler-rt/test/*' >> $FILES_TO_CHECKOUT
echo '/cmake/*' >> $FILES_TO_CHECKOUT

git config core.sparsecheckout true
git checkout $1
git read-tree -mu HEAD
