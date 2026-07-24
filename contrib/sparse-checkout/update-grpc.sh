#!/bin/sh

echo "Using sparse checkout for grpc"

FILES_TO_CHECKOUT=$(git rev-parse --git-dir)/info/sparse-checkout
echo '/*' > $FILES_TO_CHECKOUT
echo '!/test/*' >> $FILES_TO_CHECKOUT
echo '/test/build/*' >> $FILES_TO_CHECKOUT
echo '/test/core/tsi/alts/fake_handshaker/*' >> $FILES_TO_CHECKOUT
echo '/test/core/event_engine/fuzzing_event_engine/*' >> $FILES_TO_CHECKOUT
echo '!/tools/*' >> $FILES_TO_CHECKOUT
echo '/tools/codegen/*' >> $FILES_TO_CHECKOUT
echo '!/examples/*' >> $FILES_TO_CHECKOUT
echo '!/doc/*' >> $FILES_TO_CHECKOUT
echo '!/src/csharp/*' >> $FILES_TO_CHECKOUT
echo '!/src/python/*' >> $FILES_TO_CHECKOUT
echo '!/src/objective-c/*' >> $FILES_TO_CHECKOUT
echo '!/src/php/*' >> $FILES_TO_CHECKOUT
echo '!/src/ruby/*' >> $FILES_TO_CHECKOUT

git config core.sparsecheckout true
git checkout $1
git read-tree -mu HEAD
