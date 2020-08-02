#!/bin/bash
set -ex

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
    if [ "PR_TO_TEST" == "0" ]
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
        for path in "https://clickhouse-builds.s3.yandex.net/0/$REF_SHA/"{,clickhouse_build_check/}"performance/performance.tgz"
        do
            if curl --fail --head "$path"
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

# Download the package for the version we are going to test
for path in "https://clickhouse-builds.s3.yandex.net/$PR_TO_TEST/$SHA_TO_TEST/"{,clickhouse_build_check/}"performance/performance.tgz"
do
    if curl --fail --head "$path"
    then
        right_path="$path"
    fi
done

mkdir right
wget -nv -nd -c "$right_path" -O- | tar -C right --strip-components=1 -zxv

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
    git -C right/ch diff --name-only "$base" pr | tee changed-tests.txt
    if grep -vq '^tests/performance' changed-tests.txt
    then
        # Have some other changes besides the tests, so truncate the test list,
        # meaning, run all tests.
        : > changed-tests.txt
    fi
fi

# Set python output encoding so that we can print queries with Russian letters.
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

# Start the main comparison script.
{ \
    time ../download.sh "$REF_PR" "$REF_SHA" "$PR_TO_TEST" "$SHA_TO_TEST" && \
    time stage=configure "$script_path"/compare.sh ; \
} 2>&1 | ts "$(printf '%%Y-%%m-%%d %%H:%%M:%%S\t')" | tee compare.log

# Stop the servers to free memory. Normally they are restarted before getting
# the profile info, so they shouldn't use much, but if the comparison script
# fails in the middle, this might not be the case.
for _ in {1..30}
do
    killall clickhouse || break
    sleep 1
done

dmesg -T > dmesg.log

7z a '-x!*/tmp' /output/output.7z ./*.{log,tsv,html,txt,rep,svg,columns} \
    {right,left}/{performance,scripts} {{right,left}/db,db0}/preprocessed_configs \
    report analyze benchmark metrics

cp compare.log /output
