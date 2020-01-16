#!/bin/bash
set -ex

cd /workspace

# We will compare to the most recent testing tag in master branch, let's find it.
rm -rf ch ||:
git clone --branch master --single-branch --depth 50 --bare https://github.com/ClickHouse/ClickHouse ch
ref_tag=$(cd ch && git describe --match='v*-testing' --abbrev=0 --first-parent master)
echo Reference tag is $ref_tag
# We use annotated tags which have their own shas, so we have to further
# dereference the tag to get the commit it points to, hence the '~0' thing.
ref_sha=$(cd ch && git rev-parse $ref_tag~0)
echo Reference SHA is $ref_sha

# Set python output encoding so that we can print queries with Russian letters.
export PYTHONIOENCODING=utf-8

# Even if we have some errors, try our best to save the logs.
set +e
# compare.sh kills its process group, so put it into a separate one.
# It's probably at fault for using `kill 0` as an error handling mechanism,
# but I can't be bothered to change this now.
set -m
../compare.sh 0 $ref_sha $PR_TO_TEST $SHA_TO_TEST 2>&1 | tee compare.log
set +m

7z a /output/output.7z *.log *.tsv
cp compare.log /output
