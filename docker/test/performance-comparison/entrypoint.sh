#!/bin/bash
set -ex

chown nobody workspace output
chgrp nogroup workspace output
chmod 777 workspace output

cd workspace

# We will compare to the most recent testing tag in master branch, let's find it.
rm -rf ch ||:
git clone --branch master --single-branch --depth 50 --bare https://github.com/ClickHouse/ClickHouse ch
(cd ch && git fetch origin "$SHA_TO_TEST:to-test")
start_ref=to-test
while :
do
    ref_tag=$(cd ch && git describe --match='v*-testing' --abbrev=0 --first-parent $start_ref)
    echo Reference tag is "$ref_tag"
    # We use annotated tags which have their own shas, so we have to further
    # dereference the tag to get the commit it points to, hence the '~0' thing.
    ref_sha=$(cd ch && git rev-parse "$ref_tag~0")

    # FIXME sometimes we have testing tags on commits without published builds --
    # normally these are documentation commits. Loop to skip them.
    if curl --fail --head "https://clickhouse-builds.s3.yandex.net/0/$ref_sha/performance/performance.tgz"
    then
        break
    fi

    start_ref="$ref_sha~"
done

# Show what we're testing
(
    echo Reference SHA is "$ref_sha"
    (cd ch && git log -1 --decorate "$ref_sha") ||:
    echo
) | tee left-commit.txt
(
    echo SHA to test is "$SHA_TO_TEST"
    (cd ch && git log -1 --decorate "$SHA_TO_TEST") ||:
    echo
) | tee right-commit.txt

(cd ch && git diff --name-only "$SHA_TO_TEST" "$(git merge-base "$SHA_TO_TEST" master)" -- dbms/tests/performance) | tee changed-tests.txt

# Set python output encoding so that we can print queries with Russian letters.
export PYTHONIOENCODING=utf-8

# Use a default number of runs if not told otherwise
export CHPC_RUNS=${CHPC_RUNS:-7}

# Even if we have some errors, try our best to save the logs.
set +e
# compare.sh kills its process group, so put it into a separate one.
# It's probably at fault for using `kill 0` as an error handling mechanism,
# but I can't be bothered to change this now.
set -m
time ../compare.sh 0 "$ref_sha" "$PR_TO_TEST" "$SHA_TO_TEST" 2>&1 | ts "$(printf '%%Y-%%m-%%d %%H:%%M:%%S\t')" | tee compare.log
set +m

# Stop the servers to free memory. Normally they are restarted before getting
# the profile info, so they shouldn't use much, but if the comparison script
# fails in the middle, this might not be the case.
for _ in {1..30}
do
    killall clickhouse || break
    sleep 1
done

dmesg -T > dmesg.log

7z a /output/output.7z ./*.{log,tsv,html,txt,rep,svg} {right,left}/{performance,db/preprocessed_configs}
cp compare.log /output
