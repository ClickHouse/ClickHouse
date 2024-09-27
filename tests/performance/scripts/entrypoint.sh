#!/bin/bash
set -ex

CHPC_CHECK_START_TIMESTAMP="$(date +%s)"
export CHPC_CHECK_START_TIMESTAMP

S3_URL=${S3_URL:="https://clickhouse-builds.s3.amazonaws.com"}
BUILD_NAME=${BUILD_NAME:-package_release}
export S3_URL BUILD_NAME
SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"

# Sometimes AWS responds with DNS error and it's impossible to retry it with
# current curl version options.
function curl_with_retry
{
    for _ in 1 2 3 4 5 6 7 8 9 10; do
        if curl --fail --head "$1"
        then
            return 0
        else
            sleep 1
        fi
    done
    return 1
}

# Use the packaged repository to find the revision we will compare to.
function find_reference_sha
{
    git -C right/ch log -1 origin/master
    git -C right/ch log -1 pr
    # Go back from the revision to be tested, trying to find the closest published
    # testing release. The PR branch may be either pull/*/head which is the
    # author's branch, or pull/*/merge, which is head merged with some master
    # automatically by Github. We will use a merge base with master as a reference
    # for tesing (or some older commit). A caveat is that if we're testing the
    # master, the merge base is the tested commit itself, so we have to step back
    # once.
    start_ref=$(git -C right/ch merge-base origin/master pr)
    if [ "$PR_TO_TEST" == "0" ]
    then
        start_ref=$start_ref~
    fi

    # Loop back to find a commit that actually has a published perf test package.
    while :
    do
        # FIXME the original idea was to compare to a closest testing tag, which
        # is a version that is verified to work correctly. However, we're having
        # some test stability issues now, and the testing release can't roll out
        # for more that a weak already because of that. Temporarily switch to
        # using just closest master, so that we can go on.
        #ref_tag=$(git -C ch describe --match='v*-testing' --abbrev=0 --first-parent "$start_ref")
        ref_tag="$start_ref"

        echo Reference tag is "$ref_tag"
        # We use annotated tags which have their own shas, so we have to further
        # dereference the tag to get the commit it points to, hence the '~0' thing.
        REF_SHA=$(git -C right/ch rev-parse "$ref_tag~0")

        # FIXME sometimes we have testing tags on commits without published builds.
        # Normally these are documentation commits. Loop to skip them.
        # Historically there were various path for the performance test package,
        # test all of them.
        unset found
        declare -a urls_to_try=(
            "$S3_URL/PRs/0/$REF_SHA/$BUILD_NAME/performance.tar.zst"
            "$S3_URL/0/$REF_SHA/$BUILD_NAME/performance.tar.zst"
            "$S3_URL/0/$REF_SHA/$BUILD_NAME/performance.tgz"
        )
        for path in "${urls_to_try[@]}"
        do
            if curl_with_retry "$path"
            then
                found="$path"
                break
            fi
        done
        if [ -n "$found" ] ; then break; fi

        start_ref="$REF_SHA~"
    done

    REF_PR=0
}

chown nobody workspace output
chgrp nogroup workspace output
chmod 777 workspace output

cd workspace

[ ! -e "/artifacts/performance.tar.zst" ] && echo "ERROR: performance.tar.zst not found" && exit 1
mkdir -p right
tar -xf "/artifacts/performance.tar.zst" -C right --no-same-owner --strip-components=1 --zstd --extract --verbose

# Find reference revision if not specified explicitly
if [ "$REF_SHA" == "" ]; then find_reference_sha; fi
if [ "$REF_SHA" == "" ]; then echo Reference SHA is not specified ; exit 1 ; fi
if [ "$REF_PR" == "" ]; then echo Reference PR is not specified ; exit 1 ; fi

# Show what we're testing
(
    git -C right/ch log -1 --decorate "$REF_SHA" ||:
) | tee left-commit.txt

(
    git -C right/ch log -1 --decorate "$SHA_TO_TEST" ||:
    echo
    echo Real tested commit is:
    git -C right/ch log -1 --decorate "pr"
) | tee right-commit.txt

if [ "$PR_TO_TEST" != "0" ]
then
    # If the PR only changes the tests and nothing else, prepare a list of these
    # tests for use by compare.sh. Compare to merge base, because master might be
    # far in the future and have unrelated test changes.
    base=$(git -C right/ch merge-base pr origin/master)
    git -C right/ch diff --name-only "$base" pr -- . | tee all-changed-files.txt
    git -C right/ch diff --name-only --diff-filter=d "$base" pr -- tests/performance/*.xml | tee changed-test-definitions.txt
    git -C right/ch diff --name-only "$base" pr -- :!tests/performance/*.xml :!docker/test/performance-comparison | tee other-changed-files.txt
fi

# Set python output encoding so that we can print queries with non-ASCII letters.
export PYTHONIOENCODING=utf-8

# By default, use the main comparison script from the tested package, so that we
# can change it in PRs.
script_path="right/scripts"
if [ -v CHPC_LOCAL_SCRIPT ]
then
    script_path=".."
fi

# Even if we have some errors, try our best to save the logs.
set +e

# Use clickhouse-client and clickhouse-local from the right server.
PATH="$(readlink -f right/)":"$PATH"
export PATH

export REF_PR
export REF_SHA

# Try to collect some core dumps.
# At least we remove the ulimit and then try to pack some common file names into output.
ulimit -c unlimited
cat /proc/sys/kernel/core_pattern

# Start the main comparison script.
{
    time $SCRIPT_DIR/download.sh "$REF_PR" "$REF_SHA" "$PR_TO_TEST" "$SHA_TO_TEST" && \
    time stage=configure "$script_path"/compare.sh ; \
} 2>&1 | ts "$(printf '%%Y-%%m-%%d %%H:%%M:%%S\t')" | tee -a compare.log

# Stop the servers to free memory. Normally they are restarted before getting
# the profile info, so they shouldn't use much, but if the comparison script
# fails in the middle, this might not be the case.
for _ in {1..30}
do
    killall clickhouse || break
    sleep 1
done

dmesg -T > dmesg.log

ls -lath

7z a '-x!*/tmp' /output/output.7z ./*.{log,tsv,html,txt,rep,svg,columns} \
    {right,left}/{performance,scripts} {{right,left}/db,db0}/preprocessed_configs \
    report analyze benchmark metrics \
    ./*.core.dmp ./*.core

# If the files aren't same, copy it
cmp --silent compare.log /output/compare.log || \
  cp compare.log /output
